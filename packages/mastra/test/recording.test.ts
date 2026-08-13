import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import type { RecordedStep } from "../src/step-recorder.js";
import type { KitaruCostCalculator } from "../src/types.js";
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
      inputs: null,
      model: "effective-tools",
      model_params: { temperature: 0.3 },
      model_provider: "test-provider",
      requested_model: "requested-model",
      tokens: { input_tokens: 3, output_tokens: 2 },
    });
    expect(typeof llm?.started_at).toBe("string");
    expect(stepBatches[1]?.[0]?.index).toBe(4);
  });

  it("bounds and redacts recorded step payloads", async () => {
    const api = installTestApi();
    const longText = "x".repeat(5_000);
    const step = {
      ...textStep("bounded"),
      request: { body: { prompt: longText, system: "secret instructions" } },
      text: longText,
      toolCalls: [
        {
          payload: {
            args: { authorization: "Bearer live-token", query: longText },
            toolCallId: "call-1",
            toolName: "search",
          },
        },
      ],
      toolResults: [
        {
          payload: {
            args: { authorization: "Bearer live-token", query: longText },
            result: { api_key: "sk-live", hits: 2 },
            toolCallId: "call-1",
            toolName: "search",
          },
        },
      ],
    } as unknown as RecordedStep;
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.(step);
      return { text: "done" };
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await recorded.generate("run");

    const nodes = api.nodeBatches().flat();
    const llm = nodes.find((node) => node.node_type === "llm_call");
    const tool = nodes.find((node) => node.external_id === "call-1");
    expect(llm?.inputs).toBeNull();
    expect(JSON.stringify(llm?.outputs)).not.toContain("secret instructions");
    expect((llm?.outputs as { text: string }).text).toBe(
      `${"x".repeat(4_096)}[truncated]`,
    );
    expect(tool?.inputs).toEqual({
      authorization: "[redacted]",
      query: `${"x".repeat(4_096)}[truncated]`,
    });
    expect(tool?.outputs).toEqual({ api_key: "[redacted]", hits: 2 });
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

  it("records the bare provider family and keeps the qualified provider id", async () => {
    const api = installTestApi();
    const step = textStep("qualified");
    (step as { model?: { provider?: string } }).model = {
      provider: "openai.responses",
    };
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.(step);
      return { text: "done" };
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "openai/gpt-5-nano",
    });

    await recorded.generate("run");

    const llm = api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "llm_call");
    expect(llm).toMatchObject({
      attributes: { provider_id: "openai.responses" },
      model_provider: "openai",
      requested_model: "openai/gpt-5-nano",
    });
  });
});

describe("recorded cost", () => {
  async function generateWith(
    costCalculator?: KitaruCostCalculator,
  ): Promise<Record<string, unknown> | undefined> {
    const api = installTestApi();
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      costCalculator,
      requestedModelId: "openai/gpt-5-nano",
    });

    await recorded.generate("run");

    return api
      .nodeBatches()
      .flat()
      .find((node) => node.node_type === "llm_call");
  }

  it("records a calculated cost and marks it estimated", async () => {
    const costCalculator = vi.fn(() => 0.000125);

    const llm = await generateWith(costCalculator);

    expect(costCalculator).toHaveBeenCalledWith({
      model: "effective-one",
      provider: "test-provider",
      requestedModelId: "openai/gpt-5-nano",
      tokens: { input_tokens: 3, output_tokens: 2 },
    });
    expect(llm?.cost).toBe(0.000125);
    expect(llm?.attributes).toMatchObject({
      cost: { source: "user", status: "estimated" },
    });
  });

  it("marks cost disabled when no calculator is configured", async () => {
    const llm = await generateWith();

    expect(llm?.cost).toBeNull();
    expect(llm?.attributes).toMatchObject({
      cost: { source: "none", status: "disabled" },
    });
  });

  it("keeps the run alive when the cost calculator throws", async () => {
    const llm = await generateWith(() => {
      throw new RangeError("no price for this model");
    });

    expect(llm?.cost).toBeNull();
    expect(llm?.attributes).toMatchObject({
      cost: { error_type: "RangeError", source: "user", status: "unavailable" },
    });
  });

  it("marks cost unavailable when the calculator returns nothing", async () => {
    const llm = await generateWith(() => undefined);

    expect(llm?.cost).toBeNull();
    expect(llm?.attributes).toMatchObject({
      cost: { source: "user", status: "unavailable" },
    });
  });
});
