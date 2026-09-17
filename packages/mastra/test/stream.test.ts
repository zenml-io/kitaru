import { Agent } from "@mastra/core/agent";
import type { ProcessInputArgs } from "@mastra/core/processors";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

import { KitaruAgent } from "../src/index.js";
import { isSupportedMastraStreamVersion } from "../src/stream-recording.js";
import type { RuntimeStreamOptions } from "../src/types.js";
import { AGENT_ID, FakeAgent, installTestApi, REPLAY_ID } from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function streamChunks(parts: string[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: "stream-start", warnings: [] });
      controller.enqueue({
        id: "response-stream",
        modelId: "effective-stream",
        type: "response-metadata",
      });
      controller.enqueue({ id: "text-1", type: "text-start" });
      for (const delta of parts) {
        controller.enqueue({ delta, id: "text-1", type: "text-delta" });
      }
      controller.enqueue({ id: "text-1", type: "text-end" });
      controller.enqueue({
        finishReason: "stop",
        type: "finish",
        usage: { inputTokens: 3, outputTokens: 2, totalTokens: 5 },
      });
      controller.close();
    },
  });
}

function rawChunks(values: unknown[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      for (const value of values) controller.enqueue(value);
      controller.close();
    },
  });
}

function realAgent(parts = ["hello ", "world"]): Agent {
  const model = new MastraLanguageModelV2Mock({
    doStream: async () => ({ stream: streamChunks(parts) as never }),
    modelId: "stream-model",
    provider: "test-provider",
  });
  return new Agent({
    id: "stream-agent",
    instructions: "Respond.",
    model,
    name: "Stream agent",
  });
}

async function collect(stream: {
  getReader(): {
    read(): Promise<{ done: boolean; value?: string }>;
  };
}): Promise<string> {
  let value = "";
  const reader = stream.getReader();
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    value += next.value ?? "";
  }
  return value;
}

class FakeStreamAgent extends FakeAgent {
  readonly streamCalls: Array<{
    messages: unknown;
    options: RuntimeStreamOptions;
  }> = [];
  defaults: RuntimeStreamOptions = {};
  executableTools: Record<string, unknown> = {};

  async stream(
    messages: unknown,
    options: RuntimeStreamOptions = {},
  ): Promise<Record<string, unknown>> {
    this.streamCalls.push({ messages, options });
    return { textStream: new ReadableStream() };
  }

  override async getDefaultOptions(): Promise<RuntimeStreamOptions> {
    return this.defaults;
  }

  override async getToolsForExecution(
    options: RuntimeStreamOptions = {},
  ): Promise<Record<string, unknown>> {
    expect(options.methodType).toBe("stream");
    return this.executableTools;
  }
}

describe("KitaruAgent.stream", () => {
  it("accepts only stable Mastra 1.67 patch releases", () => {
    expect(isSupportedMastraStreamVersion("1.67.0")).toBe(true);
    expect(isSupportedMastraStreamVersion("1.67.12")).toBe(true);
    expect(isSupportedMastraStreamVersion("1.66.9")).toBe(false);
    expect(isSupportedMastraStreamVersion("1.68.0")).toBe(false);
    expect(isSupportedMastraStreamVersion("1.67.0-beta.1")).toBe(false);
    expect(isSupportedMastraStreamVersion("not-a-version")).toBe(false);
  });

  it("returns the exact native result and records only after consumption", async () => {
    const api = installTestApi();
    const agent = realAgent();
    const nativeStream = agent.stream.bind(agent);
    let nativeResult: unknown;
    agent.stream = (async (...args: Parameters<typeof agent.stream>) => {
      nativeResult = await nativeStream(...args);
      return nativeResult as Awaited<ReturnType<typeof agent.stream>>;
    }) as typeof agent.stream;
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });

    const output = await recorded.stream("hello");

    expect(output).toBe(nativeResult);
    expect(api.calls.some((call) => call.method === "PATCH")).toBe(false);
    await expect(collect(output.textStream)).resolves.toBe("hello world");
    expect(api.calls.at(-1)?.body).toMatchObject({
      outputs: { finish_reason: "stop", step_count: 1, text: "hello world" },
      status: "completed",
    });
    expect(
      api
        .nodeBatches()
        .flat()
        .filter((node) => node.node_type === "llm_call"),
    ).toHaveLength(1);
  });

  it("delivers the first native chunk while generation and terminal recording are blocked", async () => {
    const api = installTestApi();
    let releaseFinish: (() => void) | undefined;
    const finishGate = new Promise<void>((resolve) => {
      releaseFinish = resolve;
    });
    const values = [
      { type: "stream-start", warnings: [] },
      { id: "gated", type: "text-start" },
      { delta: "first", id: "gated", type: "text-delta" },
      { id: "gated", type: "text-end" },
      {
        finishReason: "stop",
        type: "finish",
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      },
    ];
    let index = 0;
    const model = new MastraLanguageModelV2Mock({
      doStream: async () => ({
        stream: new ReadableStream({
          async pull(controller) {
            if (index === 3) await finishGate;
            const value = values[index++];
            if (value) controller.enqueue(value);
            else controller.close();
          },
        }) as never,
      }),
      modelId: "gated-model",
      provider: "test-provider",
    });
    const recorded = new KitaruAgent(
      new Agent({
        id: "gated-stream",
        instructions: "Respond.",
        model,
        name: "Gated stream",
      }),
      {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "gated-model",
      },
    );
    const output = await recorded.stream("hello");
    const reader = output.textStream.getReader();

    await expect(reader.read()).resolves.toMatchObject({ value: "first" });
    expect(api.calls.some((call) => call.method === "PATCH")).toBe(false);
    releaseFinish?.();
    await expect(reader.read()).resolves.toMatchObject({ done: true });
    expect(api.calls.at(-1)?.body).toMatchObject({ status: "completed" });
  });

  it("records a two-step tool stream, configured cost, and composed hooks", async () => {
    const api = installTestApi();
    let modelCall = 0;
    const model = new MastraLanguageModelV2Mock({
      doStream: async () => {
        modelCall += 1;
        return {
          stream: rawChunks(
            modelCall === 1
              ? [
                  { type: "stream-start", warnings: [] },
                  {
                    id: "response-tool",
                    modelId: "served-tool",
                    type: "response-metadata",
                  },
                  {
                    input: JSON.stringify({
                      city: "Amsterdam",
                      token: "tool-secret",
                    }),
                    toolCallId: "call-weather",
                    toolName: "weather",
                    type: "tool-call",
                  },
                  {
                    finishReason: "tool-calls",
                    type: "finish",
                    usage: {
                      inputTokens: 4,
                      outputTokens: 2,
                      totalTokens: 6,
                    },
                  },
                ]
              : [
                  { type: "stream-start", warnings: [] },
                  {
                    id: "response-answer",
                    modelId: "served-answer",
                    type: "response-metadata",
                  },
                  { id: "answer", type: "text-start" },
                  {
                    delta: "sunny",
                    id: "answer",
                    type: "text-delta",
                  },
                  { id: "answer", type: "text-end" },
                  {
                    finishReason: "stop",
                    type: "finish",
                    usage: {
                      inputTokens: 3,
                      outputTokens: 1,
                      totalTokens: 4,
                    },
                  },
                ],
          ) as never,
        };
      },
      modelId: "tool-model",
      provider: "test-provider",
    });
    const execute = vi.fn(
      async ({ city, token }: { city: string; token: string }) => ({
        forecast: `sunny in ${city}`,
        token,
      }),
    );
    const tool = createTool({
      description: "Get weather",
      execute,
      id: "weather",
      inputSchema: z.object({ city: z.string(), token: z.string() }),
      outputSchema: z.object({ forecast: z.string(), token: z.string() }),
    });
    const agent = new Agent({
      id: "tool-stream",
      instructions: "Use weather.",
      model,
      name: "Tool stream",
      tools: { weather: tool },
    });
    const events: string[] = [];
    const costCalculator = vi.fn(() => 0.25);
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      configuredAfterToolCall: () => {
        events.push("configured-tool");
      },
      configuredOnStepFinish: () => {
        events.push("configured-step");
      },
      costCalculator,
      requestedModelId: "tool-model",
    });
    const output = await recorded.stream("weather", {
      hooks: {
        afterToolCall: () => {
          events.push("caller-tool");
        },
      },
      onStepFinish: () => {
        events.push("caller-step");
      },
    });

    await expect(collect(output.textStream)).resolves.toBe("sunny");
    expect(execute).toHaveBeenCalledWith(
      { city: "Amsterdam", token: "tool-secret" },
      expect.anything(),
    );
    expect(events).toEqual([
      "configured-tool",
      "caller-tool",
      "configured-step",
      "caller-step",
      "configured-step",
      "caller-step",
    ]);
    expect(costCalculator).toHaveBeenCalledTimes(2);
    const nodes = api.nodeBatches().flat();
    expect(nodes.filter((node) => node.node_type === "llm_call")).toHaveLength(
      2,
    );
    expect(nodes.find((node) => node.node_type === "tool_call")).toMatchObject({
      inputs: { city: "Amsterdam", token: "[redacted]" },
      outputs: { forecast: "sunny in Amsterdam", token: "[redacted]" },
      status: "completed",
    });
  });

  it("supports schema-only structured output and records the resolved object", async () => {
    const api = installTestApi();
    const answer = JSON.stringify({ answer: "雪", api_key: "secret-value" });
    const recorded = new KitaruAgent(realAgent([answer]), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });

    const output = await recorded.stream("hello", {
      structuredOutput: {
        schema: z.object({ answer: z.string(), api_key: z.string() }),
      },
    });
    await expect(output.object).resolves.toEqual({
      answer: "雪",
      api_key: "secret-value",
    });
    await expect(output.text).resolves.toBe(answer);
    await output.getFullOutput();
    expect(api.calls.at(-1)?.body).toMatchObject({
      outputs: {
        object: { answer: "雪", api_key: "[redacted]" },
        text: JSON.stringify({ answer: "雪", api_key: "[redacted]" }),
      },
      status: "completed",
    });
  });

  it("uses deterministic defaults through the public merge and documents the second native resolution", async () => {
    const api = installTestApi();
    let resolutions = 0;
    const agent = realAgent();
    const defaults = vi.spyOn(agent, "getDefaultOptions");
    const originalDefaults = agent.getDefaultOptions.bind(agent);
    agent.getDefaultOptions = async (options) => {
      resolutions += 1;
      return {
        ...(await originalDefaults(options)),
        modelSettings: { temperature: 0.1, topP: 0.8 },
      };
    };
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });
    const output = await recorded.stream("hello", {
      modelSettings: { temperature: 0.2 },
    });

    await collect(output.textStream);
    // The adapter resolves defaults for preflight, public tool resolution
    // resolves them again, and Mastra resolves them for native execution.
    expect(resolutions).toBe(3);
    expect(defaults).toHaveBeenCalledTimes(3);
    expect(
      api
        .nodeBatches()
        .flat()
        .find((node) => node.node_type === "llm_call")?.model_params,
    ).toMatchObject({ temperature: 0.2, topP: 0.8 });
  });

  it("does not consume an abandoned native stream", async () => {
    const api = installTestApi();
    const recorded = new KitaruAgent(realAgent(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });

    await recorded.stream("hello");
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(api.calls.some((call) => call.method === "PATCH")).toBe(false);
  });

  it("keeps interleaved calls and their final outputs session-local", async () => {
    const api = installTestApi();
    let call = 0;
    const model = new MastraLanguageModelV2Mock({
      doStream: async () => {
        call += 1;
        return { stream: streamChunks([`answer-${call}`]) as never };
      },
      modelId: "concurrent-model",
      provider: "test-provider",
    });
    const agent = new Agent({
      id: "concurrent-stream",
      instructions: "Respond.",
      model,
      name: "Concurrent stream",
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "concurrent-model",
    });

    const first = await recorded.stream("first");
    const second = await recorded.stream("second");
    const [secondText, firstText] = await Promise.all([
      collect(second.textStream),
      collect(first.textStream),
    ]);

    expect([firstText, secondText]).toEqual(["answer-1", "answer-2"]);
    expect(api.sessionIds).toHaveLength(2);
    const outputs = api.sessionIds.map(
      (sessionId) =>
        api.calls.find(
          (entry) =>
            entry.method === "PATCH" &&
            entry.path.endsWith(sessionId) &&
            entry.body?.status === "completed",
        )?.body?.outputs,
    );
    expect(outputs).toEqual([
      expect.objectContaining({ text: "answer-1" }),
      expect.objectContaining({ text: "answer-2" }),
    ]);
  });

  it.each([
    ["requireToolApproval", true],
    ["autoResumeSuspendedTools", true],
    ["untilIdle", true],
    ["backgroundTaskPolicy", { onFinish: "continue" }],
    ["resumeContext", { resumeData: {} }],
    ["_skipBgTaskWait", true],
    ["structuredOutput", { model: "secondary", schema: {} }],
  ])(
    "rejects unsupported %s before recording or execution",
    async (key, value) => {
      const api = installTestApi();
      const agent = new FakeStreamAgent();
      agent.defaults = { [key]: value };
      const recorded = new KitaruAgent(agent, {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "stream-model",
      });

      await expect(recorded.stream("hello")).rejects.toThrow(
        /does not support|schema-only/,
      );

      expect(agent.streamCalls).toHaveLength(0);
      expect(api.sessionIds).toHaveLength(0);
    },
  );

  it.each(["per-run", "default"] as const)(
    "rejects %s prepareStep before it can replace the model",
    async (source) => {
      const api = installTestApi();
      const doStream = vi.fn(async () => ({
        stream: streamChunks(["unexpected"]) as never,
      }));
      const replacementDoStream = vi.fn(async () => ({
        stream: streamChunks(["replacement"]) as never,
      }));
      const execute = vi.fn(async () => ({ changed: true }));
      const replacementModel = new MastraLanguageModelV2Mock({
        doStream: replacementDoStream,
        modelId: "replacement-model",
        provider: "test-provider",
      });
      const prepareStep = vi.fn(() => ({ model: replacementModel }));
      const agent = new Agent({
        defaultOptions: source === "default" ? { prepareStep } : undefined,
        id: `prepare-step-${source}`,
        instructions: "Use the side-effecting tool.",
        model: new MastraLanguageModelV2Mock({
          doStream,
          modelId: "prepare-step-model",
          provider: "test-provider",
        }),
        name: `Prepare step ${source}`,
        tools: {
          sideEffect: createTool({
            description: "Make an observable change",
            execute,
            id: "sideEffect",
            inputSchema: z.object({ value: z.string() }),
          }),
        },
      });
      const recorded = new KitaruAgent(agent, {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "prepare-step-model",
      });

      await expect(
        recorded.stream("hello", source === "per-run" ? { prepareStep } : {}),
      ).rejects.toThrow("does not support prepareStep");
      expect(prepareStep).not.toHaveBeenCalled();
      expect(doStream).not.toHaveBeenCalled();
      expect(replacementDoStream).not.toHaveBeenCalled();
      expect(execute).not.toHaveBeenCalled();
      expect(api.calls).toHaveLength(0);
    },
  );

  it.each(["per-run", "default", "configured"] as const)(
    "rejects %s input processors before recording or execution",
    async (source) => {
      const api = installTestApi();
      const doStream = vi.fn(async () => ({
        stream: streamChunks(["unexpected"]) as never,
      }));
      const execute = vi.fn(async () => ({ changed: true }));
      const processInput = vi.fn((args: ProcessInputArgs) => args.messageList);
      const processor = { id: "replace-input", processInput };
      const agent = new Agent({
        defaultOptions:
          source === "default" ? { inputProcessors: [processor] } : undefined,
        id: `input-processor-${source}`,
        inputProcessors: source === "configured" ? [processor] : undefined,
        instructions: "Use the side-effecting tool.",
        model: new MastraLanguageModelV2Mock({
          doStream,
          modelId: "input-processor-model",
          provider: "test-provider",
        }),
        name: `Input processor ${source}`,
        tools: {
          sideEffect: createTool({
            description: "Make an observable change",
            execute,
            id: "sideEffect",
            inputSchema: z.object({ value: z.string() }),
          }),
        },
      });
      const configuredProcessors = vi.spyOn(
        agent,
        "listConfiguredInputProcessors",
      );
      const recorded = new KitaruAgent(agent, {
        agentId: AGENT_ID,
        apiUrl: "https://api.example",
        requestedModelId: "input-processor-model",
      });

      await expect(
        recorded.stream(
          "hello",
          source === "per-run" ? { inputProcessors: [processor] } : {},
        ),
      ).rejects.toThrow("does not support user inputProcessors");
      expect(processInput).not.toHaveBeenCalled();
      expect(doStream).not.toHaveBeenCalled();
      expect(execute).not.toHaveBeenCalled();
      expect(configuredProcessors).toHaveBeenCalledTimes(
        source === "configured" ? 1 : 0,
      );
      expect(api.calls).toHaveLength(0);
    },
  );

  it("rejects tool approval using stream-specific public tool resolution", async () => {
    const api = installTestApi();
    const agent = new FakeStreamAgent();
    agent.executableTools = { dangerous: { requireApproval: true } };
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });

    await expect(recorded.stream("hello")).rejects.toThrow(
      "approval for tool 'dangerous'",
    );
    expect(agent.streamCalls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("rejects suspend-capable tools before recording or execution", async () => {
    const api = installTestApi();
    const doStream = vi.fn(async () => ({
      stream: streamChunks(["unexpected"]) as never,
    }));
    const execute = vi.fn(async () => ({ status: "unexpected" }));
    const agent = new Agent({
      id: "suspend-tool-stream",
      instructions: "Use the suspendable tool.",
      model: new MastraLanguageModelV2Mock({
        doStream,
        modelId: "suspend-tool-model",
        provider: "test-provider",
      }),
      name: "Suspend tool stream",
      tools: {
        waitForInput: createTool({
          description: "Wait for more input",
          execute,
          id: "waitForInput",
          inputSchema: z.object({ prompt: z.string() }),
          suspendSchema: z.object({ reason: z.string() }),
        }),
      },
    });
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "suspend-tool-model",
    });

    await expect(recorded.stream("hello")).rejects.toThrow(
      "suspension for tool 'waitForInput'",
    );
    expect(api.sessionIds).toHaveLength(0);
    expect(doStream).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });

  it("rejects replay before defaults, tools, recording, or execution", async () => {
    vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
    const api = installTestApi();
    const agent = new FakeStreamAgent();
    const defaults = vi.spyOn(agent, "getDefaultOptions");
    const tools = vi.spyOn(agent, "getToolsForExecution");
    const recorded = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "stream-model",
    });

    await expect(recorded.stream("hello")).rejects.toThrow(
      "does not support replay",
    );
    expect(defaults).not.toHaveBeenCalled();
    expect(tools).not.toHaveBeenCalled();
    expect(agent.streamCalls).toHaveLength(0);
    expect(api.sessionIds).toHaveLength(0);
  });

  it("keeps generate-only wrappers constructible", () => {
    const wrapped = new KitaruAgent(new FakeAgent(), {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "generate-model",
    });

    expect(wrapped.generate).toBeTypeOf("function");
  });

  it("rejects a missing native stream before defaults, tools, or recording", async () => {
    const api = installTestApi();
    const agent = new FakeAgent();
    const defaults = vi.spyOn(agent, "getDefaultOptions");
    const tools = vi.spyOn(agent, "getToolsForExecution");
    const wrapped = new KitaruAgent(agent, {
      agentId: AGENT_ID,
      apiUrl: "https://api.example",
      requestedModelId: "generate-model",
    });

    await expect(
      (wrapped as { stream: () => Promise<unknown> }).stream(),
    ).rejects.toThrow("does not provide a stream() method");
    expect(defaults).not.toHaveBeenCalled();
    expect(tools).not.toHaveBeenCalled();
    expect(api.sessionIds).toHaveLength(0);
  });
});
