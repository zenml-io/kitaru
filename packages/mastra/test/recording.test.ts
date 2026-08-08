import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import type { RecordedStep } from "../src/step-recorder.js";
import { AGENT_ID, FakeAgent, installTestApi, textStep } from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("step recording", () => {
  it("batches an LLM parent and ordered tool children with raw model arguments", async () => {
    const api = installTestApi();
    const rawFirst = { count: "2" };
    const rawSecond = { city: "Berlin" };
    const step = {
      ...textStep("tools"),
      content: [],
      finishReason: "tool-calls",
      toolCalls: [
        {
          payload: {
            args: rawFirst,
            toolCallId: "call-1",
            toolName: "normalize",
          },
        },
        {
          payload: {
            args: rawSecond,
            toolCallId: "call-2",
            toolName: "weather",
          },
        },
      ],
      toolResults: [
        {
          payload: {
            args: rawFirst,
            result: { count: 2, label: "default" },
            toolCallId: "call-1",
            toolName: "normalize",
          },
        },
        {
          payload: {
            args: rawSecond,
            result: { temperature: 21 },
            toolCallId: "call-2",
            toolName: "weather",
          },
        },
      ],
    } as unknown as RecordedStep;
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.(step);
      await options.onStepFinish?.(textStep("final"));
      return { text: "done" };
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await recorded.generate("run", {
      modelSettings: { temperature: 0.3 },
    });

    const stepBatches = api
      .nodeBatches()
      .filter((batch) => batch[0]?.node_type === "llm_call");
    expect(stepBatches).toHaveLength(2);
    const [llm, firstTool, secondTool] = stepBatches[0] ?? [];
    expect([llm?.index, firstTool?.index, secondTool?.index]).toEqual([
      1, 2, 3,
    ]);
    expect(firstTool).toMatchObject({
      external_id: "call-1",
      inputs: rawFirst,
      parent_index: llm?.index,
      status: "completed",
    });
    expect(secondTool).toMatchObject({
      external_id: "call-2",
      inputs: rawSecond,
      parent_index: llm?.index,
    });
    expect(llm).toMatchObject({
      attributes: { provider_metadata: { test: { suffix: "tools" } } },
      external_id: "response-tools",
      inputs: { request: "tools" },
      model: "effective-tools",
      model_params: { temperature: 0.3 },
      model_provider: "test-provider",
      requested_model: "requested-model",
      tokens: { input_tokens: 3, output_tokens: 2 },
    });
    expect(llm?.started_at).toBeUndefined();
    expect(stepBatches[1]?.[0]?.index).toBe(4);
  });

  it("records public tool failure text when Mastra omits toolResults", async () => {
    const api = installTestApi();
    const failedStep = {
      ...textStep("failed-tool"),
      content: [
        {
          input: { value: "raw" },
          toolCallId: "call-failed",
          toolName: "fail",
          type: "tool-call",
        },
        {
          input: { value: "raw" },
          output: { type: "text", value: "tool failed" },
          toolCallId: "call-failed",
          toolName: "fail",
          type: "tool-result",
        },
      ],
      finishReason: "tool-calls",
      toolCalls: [
        {
          payload: {
            args: { value: "raw" },
            toolCallId: "call-failed",
            toolName: "fail",
          },
        },
      ],
      toolResults: [],
    } as unknown as RecordedStep;
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.(failedStep);
      return { text: "recovered" };
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await recorded.generate("run");

    const tool = api
      .nodeBatches()
      .flat()
      .find((node) => node.external_id === "call-failed");
    expect(tool).toMatchObject({
      error: "tool failed",
      inputs: { value: "raw" },
      status: "failed",
    });
  });
});
