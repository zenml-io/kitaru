import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

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
      allowedReplayModels: ["replacement"],
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
        source: "caller",
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
      allowedReplayModels: ["replay-model"],
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

  it("keeps sibling keys when only the system prompt is replaced", async () => {
    const spec = replay();
    spec.override = { system_prompt: "replacement instructions" };
    const context = await resolveReplayContext({
      callerInput: "caller prompt",
      client: fakeClient({ replay: spec }),
      environment: {
        KITARU_REPLAY_ID: REPLAY_ID,
        KITARU_TASK_INPUTS: JSON.stringify({
          locale: "nl",
          prompt: "worker prompt",
          system_prompt: "replacement instructions",
        }),
      },
      requestedModelId: "requested",
    });

    expect(context.effectiveRuntimeInput).toBe("worker prompt");
    expect(context.effectiveInput).toEqual({
      locale: "nl",
      prompt: "worker prompt",
      system_prompt: "replacement instructions",
    });
  });

  it("unwraps a recorded prompt wrapper without a fresh prompt override", async () => {
    const spec = replay();
    spec.override = { model: "replacement" };
    const context = await resolveReplayContext({
      allowedReplayModels: ["replacement"],
      callerInput: "caller prompt",
      client: fakeClient({ replay: spec }),
      environment: {
        KITARU_REPLAY_ID: REPLAY_ID,
        KITARU_TASK_INPUTS: JSON.stringify({
          prompt: "worker prompt",
          system_prompt: "earlier instructions",
        }),
      },
      requestedModelId: "requested",
    });

    expect(context.effectiveRuntimeInput).toBe("worker prompt");
    expect(context.effectiveInput).toEqual({
      prompt: "worker prompt",
      system_prompt: "earlier instructions",
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

interface EffectiveInputsCase {
  expected: unknown;
  inputs: unknown;
  name: string;
  override: Record<string, unknown> | null;
}

const effectiveInputsFixture = JSON.parse(
  await readFile(
    join(
      dirname(fileURLToPath(import.meta.url)),
      "../fixtures/effective-inputs.json",
    ),
    "utf8",
  ),
) as { cases: EffectiveInputsCase[]; generated_by: string };

async function contextFor(testCase: EffectiveInputsCase) {
  return resolveReplayContext({
    allowedReplayModels: ["replacement"],
    callerInput: "caller prompt",
    client: fakeClient(),
    environment: {
      KITARU_TASK_INPUTS: JSON.stringify(testCase.inputs),
      ...(testCase.override === null
        ? {}
        : { KITARU_OVERRIDE: JSON.stringify(testCase.override) }),
    },
    requestedModelId: "requested",
  });
}

// The recorded inputs have to match what the server would have sent, so the
// expectations come from the server's own effective_inputs rather than from a
// second reading of the same rule.
describe("recorded inputs against the server's rule", () => {
  it.each(
    effectiveInputsFixture.cases.map((c) => [c.name, c] as const),
  )("matches the Python effective inputs for %s", async (_name, testCase) => {
    const context = await contextFor(testCase);

    expect(context.effectiveInput).toEqual(testCase.expected);
  });
});

describe("runtime inputs handed to the agent", () => {
  it.each([
    ["no override", "baseline prompt"],
    ["system prompt override on a string input", "baseline prompt"],
    ["both prompts on a string input", "new prompt"],
    ["prompt override on a dict input", { locale: "nl", prompt: "new prompt" }],
    ["system prompt override on a dict input", "baseline prompt"],
    ["no override on a recorded wrapper", "baseline prompt"],
    ["system prompt override on a wrapper without a prompt", { locale: "nl" }],
    [
      "no override on a wrapper without a prompt",
      { locale: "nl", system_prompt: "old instructions" },
    ],
    [
      "system prompt override on a dict input without a prompt",
      { locale: "nl" },
    ],
    [
      "system prompt override on a message-array input",
      [{ content: "hi", role: "user" }],
    ],
  ])("strips the replaced system prompt for %s", async (name, expected) => {
    const testCase = effectiveInputsFixture.cases.find(
      (candidate) => candidate.name === name,
    );
    if (!testCase) {
      throw new Error(`Unknown fixture case '${name}'`);
    }

    const context = await contextFor(testCase);

    expect(context.effectiveRuntimeInput).toEqual(expected);
  });
});
