import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, describe, expect, it, vi } from "vitest";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  FakeAgent,
  installTestApi,
  invokeTool,
  textStep,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("KitaruAgent", () => {
  it("wraps a normally constructed Mastra Agent", async () => {
    const api = installTestApi();
    const model = new MastraLanguageModelV2Mock({
      doGenerate: async () => ({
        content: [{ text: "real Mastra result", type: "text" }],
        finishReason: "stop",
        request: { body: { prompt: "real" } },
        response: { id: "real-response", modelId: "effective-model" },
        usage: { inputTokens: 1, outputTokens: 2, totalTokens: 3 },
        warnings: [],
      }),
      modelId: "existing-model",
      provider: "test-provider",
    });
    const existingAgent = new Agent({
      id: "existing-agent",
      instructions: "Respond.",
      model,
      name: "Existing agent",
    });
    const recorded = new KitaruAgent(existingAgent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "existing-model",
    });

    const result = await recorded.generate("hello");

    expect(result.text).toBe("real Mastra result");
    expect(api.nodeBatches().flat()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ node_type: "llm_call" }),
        expect.objectContaining({ name: "run", status: "completed" }),
      ]),
    );
  });

  it("returns the existing agent result unchanged and records the lifecycle", async () => {
    const api = installTestApi();
    const expected = { nested: { result: "done" } };
    const agent = new FakeAgent(async (_messages, options) => {
      await options.onStepFinish?.({
        ...textStep("one"),
        request: { body: { prompt: "hello" } },
        response: { id: "response-1", modelId: "effective-model" },
      });
      return expected;
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    const result = await recorded.generate("hello");

    expect(result).toBe(expected);
    expect(agent.calls[0]?.messages).toBe("hello");
    expect(api.calls.map((call) => `${call.method} ${call.path}`)).toEqual([
      "POST /api/v1/sessions",
      expect.stringMatching(/^POST \/api\/v1\/sessions\/.+\/nodes$/),
      expect.stringMatching(/^POST \/api\/v1\/sessions\/.+\/nodes$/),
      expect.stringMatching(/^POST \/api\/v1\/sessions\/.+\/nodes$/),
      expect.stringMatching(/^PATCH \/api\/v1\/sessions\/.+$/),
    ]);
    const rootBatches = api.nodeBatches();
    expect(rootBatches[0]?.[0]).toMatchObject({
      name: "run",
      index: 0,
      outputs: null,
      status: "in_progress",
    });
    expect(rootBatches.at(-1)?.[0]).toMatchObject({
      index: rootBatches[0]?.[0]?.index,
      outputs: { finish_reason: null, step_count: 0, text: null },
      status: "completed",
    });
  });

  it("records configured structured output in the run summary", async () => {
    const api = installTestApi();
    const expected = {
      finishReason: "stop",
      object: { answer: "yes" },
      steps: [],
      text: "",
    };
    const agent = new FakeAgent(async () => expected);
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    const result = await recorded.generate("hello", {
      structuredOutput: { schema: {} },
    } as never);

    expect(result).toBe(expected);
    expect(api.nodeBatches().at(-1)?.[0]).toMatchObject({
      outputs: { object: { answer: "yes" } },
      status: "completed",
    });
  });

  it("rejects an explicit structured-output model before recording", async () => {
    const api = installTestApi();
    const agent = new FakeAgent();
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await expect(
      recorded.generate("hello", {
        structuredOutput: { model: "internal-model", schema: {} },
      } as never),
    ).rejects.toThrow(
      "Kitaru cannot record Mastra structuredOutput.model because Mastra does not expose the internal model call to adapter instrumentation",
    );

    expect(agent.calls).toHaveLength(0);
    expect(api.calls).toHaveLength(0);
  });

  it("composes caller tool hooks outside replay", async () => {
    installTestApi();
    const callerBefore = vi.fn();
    const agent = new FakeAgent(async (_messages, options) => {
      await invokeTool(options.hooks ?? {}, {
        args: {},
        callId: "call-1",
        output: "done",
        toolName: "lookup",
      });
      return { text: "done" };
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "requested-model",
    });

    await recorded.generate("hello", {
      hooks: { beforeToolCall: callerBefore },
    });

    expect(callerBefore).toHaveBeenCalledOnce();
  });
});
