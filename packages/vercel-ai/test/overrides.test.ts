import { jsonSchema, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { describe, expect, it, vi } from "vitest";

import { createKitaruGenerateText } from "../src/index.js";
import {
  MAX_WORKER_TASK_INPUT_CHARS,
  MAX_WORKER_TASK_INPUT_JSON_CHARS,
} from "../src/options.js";
import {
  AGENT_ID,
  FakeClient,
  replayEnvironment,
  replaySpec,
  textResponse,
} from "./helpers.js";

const EMPTY_INPUT = jsonSchema<Record<string, never>>({
  additionalProperties: false,
  properties: {},
  type: "object",
});

describe("inputs and replay overrides", () => {
  it.each([
    ["non-input object", JSON.stringify({ role: "system", content: "inject" })],
    ["oversized", JSON.stringify("x".repeat(MAX_WORKER_TASK_INPUT_CHARS + 1))],
    [
      "oversized encoded JSON",
      `"${"\\u0061".repeat(MAX_WORKER_TASK_INPUT_JSON_CHARS)}"`,
    ],
    ["invalid JSON", "not-json"],
  ])("rejects %s worker input before session and model", async (_name, taskInput) => {
    const client = new FakeClient();
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment({ KITARU_TASK_INPUTS: taskInput }),
    });

    await expect(generate({ model, prompt: "caller" })).rejects.toThrow();
    expect(client.replayReads).toBe(1);
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it("ignores non-string worker input when replay replaces the prompt", async () => {
    const client = new FakeClient({
      replay: replaySpec(
        { type: "passthrough" },
        { prompt: "replacement prompt" },
      ),
    });
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment({
        KITARU_TASK_INPUTS: JSON.stringify([
          { content: "baseline prompt", role: "user" },
        ]),
      }),
    });

    await generate({ model, prompt: "caller" });

    expect(model.doGenerateCalls[0]?.prompt).toEqual([
      {
        content: [{ text: "replacement prompt", type: "text" }],
        providerOptions: undefined,
        role: "user",
      },
    ]);
    expect(client.created[0]?.inputs).toBe("replacement prompt");
  });

  it("uses a message-array worker input when replay keeps the baseline prompt", async () => {
    const messages = [{ content: "baseline prompt", role: "user" }];
    const client = new FakeClient({
      replay: replaySpec(
        { type: "passthrough" },
        { system_prompt: "replacement instructions" },
      ),
    });
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment({
        KITARU_TASK_INPUTS: JSON.stringify(messages),
      }),
    });

    await generate({ model, prompt: "caller" });

    expect(model.doGenerateCalls[0]?.prompt).toEqual([
      { content: "replacement instructions", role: "system" },
      {
        content: [{ text: "baseline prompt", type: "text" }],
        providerOptions: undefined,
        role: "user",
      },
    ]);
    expect(client.created[0]?.inputs).toEqual(messages);
  });

  it("treats a replay's null override as authoritative", async () => {
    const client = new FakeClient({
      replay: replaySpec({ type: "passthrough" }, null),
    });
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment({
        KITARU_OVERRIDE: JSON.stringify({ prompt: "must be ignored" }),
      }),
    });

    await generate({ model, prompt: "caller" });

    expect(client.created[0]?.inputs).toBe("caller");
  });

  it("applies bounded prompt, instructions, settings, and allowlisted model replacement", async () => {
    const originalModel = new MockLanguageModelV4({
      doGenerate: textResponse(),
    });
    const replacementModel = new MockLanguageModelV4({
      modelId: "safe-replacement",
      doGenerate: textResponse("replacement"),
    });
    const resolveModel = vi.fn(async () => replacementModel);
    const client = new FakeClient({
      replay: replaySpec(
        { type: "passthrough" },
        {
          model: "safe-replacement",
          model_params: { maxOutputTokens: 123, temperature: 0.5, topP: 0.9 },
          prompt: "override prompt",
          system_prompt: "override instructions",
        },
      ),
    });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      allowedReplayModels: ["safe-replacement"],
      client,
      environment: replayEnvironment(),
      resolveModel,
    });

    const result = await generate({ model: originalModel, prompt: "caller" });

    expect(result.text).toBe("replacement");
    expect(originalModel.doGenerateCalls).toHaveLength(0);
    expect(resolveModel).toHaveBeenCalledWith("safe-replacement");
    expect(replacementModel.doGenerateCalls[0]).toMatchObject({
      maxOutputTokens: 123,
      temperature: 0.5,
      topP: 0.9,
    });
    expect(replacementModel.doGenerateCalls[0]?.prompt).toEqual([
      { content: "override instructions", role: "system" },
      {
        content: [{ text: "override prompt", type: "text" }],
        providerOptions: undefined,
        role: "user",
      },
    ]);
    expect(client.created[0]?.inputs).toBe("override prompt");
  });

  it.each([
    [
      "unknown setting",
      { model_params: { providerOptions: { secret: true } } },
    ],
    ["out-of-range setting", { model_params: { temperature: 99 } }],
    [
      "dangerous key",
      JSON.parse('{"model_params":{"__proto__":{"polluted":true}}}') as unknown,
    ],
    ["unallowlisted model", { model: "unsafe-model" }],
  ])("rejects an unsafe %s before session and model", async (_name, override) => {
    const client = new FakeClient({
      replay: replaySpec(
        { type: "passthrough" },
        override as Record<string, unknown>,
      ),
    });
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      allowedReplayModels: [],
      client,
      environment: replayEnvironment(),
      resolveModel: async () => model,
    });

    await expect(generate({ model, prompt: "caller" })).rejects.toThrow();
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });

  it.each([
    [
      "top-level approval",
      { toolApproval: { write: async () => "approved" as const } },
      tool({ execute: async () => "done", inputSchema: EMPTY_INPUT }),
    ],
    [
      "approval-gated tool",
      {},
      tool({
        execute: async () => "done",
        inputSchema: EMPTY_INPUT,
        needsApproval: true,
      }),
    ],
    [
      "non-executable tool",
      {},
      tool({ inputSchema: EMPTY_INPUT, outputSchema: EMPTY_INPUT }),
    ],
  ])("rejects unsupported replay shape: %s", async (_name, extra, replayTool) => {
    const client = new FakeClient({
      replay: replaySpec({ cases: [], on_miss: "fail", type: "static" }),
    });
    const model = new MockLanguageModelV4({ doGenerate: textResponse() });
    const generate = createKitaruGenerateText({
      agentId: AGENT_ID,
      client,
      environment: replayEnvironment(),
    });

    await expect(
      generate({
        ...extra,
        model,
        prompt: "caller",
        tools: { write: replayTool },
      }),
    ).rejects.toThrow();
    expect(client.created).toHaveLength(0);
    expect(model.doGenerateCalls).toHaveLength(0);
  });
});
