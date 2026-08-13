import { describe, expect, it } from "vitest";

import {
  parseReplayId,
  resolveReplayContext,
} from "../../src/adapter/index.js";
import { fakeClient, REPLAY_ID, replay, TASK_ID } from "./helpers.js";

describe("adapter replay preparation", () => {
  it("keeps caller input and legacy override outside replay", async () => {
    const client = fakeClient();
    const context = await resolveReplayContext({
      callerInput: { source: "caller" },
      client,
      environment: {
        KITARU_OVERRIDE: JSON.stringify({
          model: "replacement",
          system_prompt: "instructions",
        }),
      },
      requestedModelId: "requested",
    });

    expect(context).toMatchObject({
      effectiveInput: {
        prompt: { source: "caller" },
        system_prompt: "instructions",
      },
      effectiveRuntimeInput: { source: "caller" },
      override: {
        model: "replacement",
        system_prompt: "instructions",
      },
      replacementModelId: "replacement",
    });
    expect(client.created).toHaveLength(0);
  });

  it("uses worker input and replay override precedence", async () => {
    const spec = replay();
    spec.override = {
      model: { requested: "replay-model" },
      prompt: "replay prompt",
      system_prompt: "replay instructions",
    };
    const client = fakeClient({ replay: spec });
    const context = await resolveReplayContext({
      callerInput: "caller",
      client,
      environment: {
        KITARU_OVERRIDE: JSON.stringify({ model: "ignored" }),
        KITARU_REPLAY_ID: REPLAY_ID,
        KITARU_TASK_INPUTS: JSON.stringify("worker prompt"),
      },
      requestedModelId: "requested",
    });

    expect(context).toMatchObject({
      effectiveInput: {
        prompt: "replay prompt",
        system_prompt: "replay instructions",
      },
      effectiveRuntimeInput: "replay prompt",
      replayId: REPLAY_ID,
      replacementModelId: "replay-model",
      spec,
    });
  });

  it("loads task inputs omitted from the worker environment", async () => {
    const context = await resolveReplayContext({
      callerInput: "caller prompt",
      client: fakeClient({ taskInput: "stored task prompt" }),
      environment: { KITARU_TASK_ID: TASK_ID },
      requestedModelId: "requested",
    });

    expect(context.effectiveRuntimeInput).toBe("stored task prompt");
    expect(context.effectiveInput).toBe("stored task prompt");
  });

  it("unwraps the server's effective input when only instructions change", async () => {
    const spec = replay();
    spec.override = { system_prompt: "replacement instructions" };
    const context = await resolveReplayContext({
      callerInput: "caller prompt",
      client: fakeClient({ replay: spec }),
      environment: {
        KITARU_REPLAY_ID: REPLAY_ID,
        KITARU_TASK_INPUTS: JSON.stringify({
          prompt: "worker prompt",
          system_prompt: "replacement instructions",
        }),
      },
      requestedModelId: "requested",
    });

    expect(context.effectiveRuntimeInput).toBe("worker prompt");
    expect(context.effectiveInput).toEqual({
      prompt: "worker prompt",
      system_prompt: "replacement instructions",
    });
  });

  it("applies a legacy prompt override outside replay", async () => {
    const context = await resolveReplayContext({
      callerInput: "caller prompt",
      client: fakeClient(),
      environment: {
        KITARU_OVERRIDE: JSON.stringify({ prompt: "replacement prompt" }),
      },
      requestedModelId: "requested",
    });

    expect(context.effectiveInput).toBe("replacement prompt");
    expect(context.effectiveRuntimeInput).toBe("replacement prompt");
  });

  it("rejects invalid replay state before session creation", async () => {
    const client = fakeClient();

    expect(() => parseReplayId("not-a-uuid")).toThrow(
      "KITARU_REPLAY_ID must be a UUID",
    );
    await expect(
      resolveReplayContext({
        callerInput: "caller",
        client,
        environment: { KITARU_TASK_INPUTS: "not-json" },
        requestedModelId: "requested",
      }),
    ).rejects.toThrow("KITARU_TASK_INPUTS must contain valid JSON");
    expect(client.created).toHaveLength(0);
  });

  it("rejects malformed override fields", async () => {
    const client = fakeClient();
    await expect(
      resolveReplayContext({
        callerInput: "caller",
        client,
        environment: {
          KITARU_OVERRIDE: JSON.stringify({ model: { requested: 42 } }),
        },
        requestedModelId: "requested",
      }),
    ).rejects.toThrow("KITARU_OVERRIDE.model values must be strings");
  });

  it("rejects a non-string prompt override", async () => {
    const client = fakeClient();
    await expect(
      resolveReplayContext({
        callerInput: "caller",
        client,
        environment: {
          KITARU_OVERRIDE: JSON.stringify({ prompt: { text: "invalid" } }),
        },
        requestedModelId: "requested",
      }),
    ).rejects.toThrow("KITARU_OVERRIDE.prompt must be a string");
  });
});
