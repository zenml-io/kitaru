import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod";

import { KitaruAgent } from "../src/index.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function chunks(values: unknown[]): ReadableStream<unknown> {
  return new ReadableStream({
    start(controller) {
      for (const value of values) controller.enqueue(value);
      controller.close();
    },
  });
}

async function text(stream: {
  getReader(): { read(): Promise<{ done: boolean; value?: string }> };
}): Promise<string> {
  let result = "";
  const reader = stream.getReader();
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    result += next.value ?? "";
  }
  return result;
}

it("replays a native Mastra stream with a history-mocked tool", async () => {
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify("recorded prompt"));
  const api = installTestApi({
    replaySpec: {
      baseline_session_id: ORIGINAL_SESSION_ID,
      id: REPLAY_ID,
      override: null,
      status: "pending",
      tool_policy: {
        default: { on_miss: "fail", scope: "baseline", type: "history" },
        tools: {},
      },
    },
    lookup: () => ({
      match: { result: { forecast: "sunny" }, status: "completed" },
    }),
  });
  let calls = 0;
  const model = new MastraLanguageModelV2Mock({
    doStream: async () => ({
      stream: chunks(
        ++calls === 1
          ? [
              { type: "stream-start", warnings: [] },
              {
                input: '{"city":"Amsterdam"}',
                toolCallId: "call-weather",
                toolName: "weather",
                type: "tool-call",
              },
              {
                finishReason: "tool-calls",
                type: "finish",
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]
          : [
              { type: "stream-start", warnings: [] },
              { id: "answer", type: "text-start" },
              { delta: "sunny", id: "answer", type: "text-delta" },
              { id: "answer", type: "text-end" },
              {
                finishReason: "stop",
                type: "finish",
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ],
      ) as never,
    }),
    modelId: "stream-model",
    provider: "test-provider",
  });
  const execute = vi.fn(async () => ({ forecast: "live" }));
  const agent = new Agent({
    id: "stream-replay",
    instructions: "Use weather.",
    model,
    name: "Stream replay",
    tools: {
      weather: createTool({
        description: "Get weather",
        execute,
        id: "weather",
        inputSchema: z.object({ city: z.string() }),
      }),
    },
  });
  const nativeStream = agent.stream.bind(agent);
  let nativeResult: unknown;
  agent.stream = (async (...args: Parameters<typeof agent.stream>) => {
    nativeResult = await nativeStream(...args);
    return nativeResult as Awaited<ReturnType<typeof agent.stream>>;
  }) as typeof agent.stream;
  const wrapped = new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "stream-model",
  });

  const output = await wrapped.stream("ignored caller input");
  expect(output).toBe(nativeResult);
  expect(api.calls.some((call) => call.method === "PATCH")).toBe(false);
  await expect(text(output.textStream)).resolves.toBe("sunny");
  expect(execute).not.toHaveBeenCalled();
  expect(calls).toBe(2);
  expect(
    api.calls.find((call) => call.path === "/api/v1/sessions")?.body,
  ).toMatchObject({
    inputs: "recorded prompt",
    origin: "replay",
  });
  expect(api.calls.at(-1)?.body).toMatchObject({ status: "completed" });
});

it("stops later tools and model steps after a replay policy miss", async () => {
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  const api = installTestApi({
    replaySpec: {
      baseline_session_id: ORIGINAL_SESSION_ID,
      id: REPLAY_ID,
      override: null,
      status: "pending",
      tool_policy: {
        default: { type: "passthrough" },
        tools: {
          normalize: { cases: [], on_miss: "fail", type: "static" },
        },
      },
    },
  });
  let modelCalls = 0;
  const model = new MastraLanguageModelV2Mock({
    doStream: async () => {
      modelCalls += 1;
      return {
        stream: chunks([
          { type: "stream-start", warnings: [] },
          {
            input: "{}",
            toolCallId: "call-1",
            toolName: "normalize",
            type: "tool-call",
          },
          {
            input: "{}",
            toolCallId: "call-2",
            toolName: "sendEmail",
            type: "tool-call",
          },
          {
            finishReason: "tool-calls",
            type: "finish",
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          },
        ]) as never,
      };
    },
    modelId: "stream-model",
    provider: "test-provider",
  });
  const normalize = vi.fn(async () => ({ normalized: true }));
  const sendEmail = vi.fn(async () => ({ sent: true }));
  const agent = new Agent({
    id: "stream-replay-fail",
    instructions: "Call both tools.",
    model,
    name: "Stream replay fail",
    tools: {
      normalize: createTool({
        description: "Normalize",
        execute: normalize,
        id: "normalize",
        inputSchema: z.object({}),
      }),
      sendEmail: createTool({
        description: "Send email",
        execute: sendEmail,
        id: "sendEmail",
        inputSchema: z.object({}),
      }),
    },
  });
  const wrapped = new KitaruAgent(agent, {
    agentId: AGENT_ID,
    apiUrl: "https://api.example",
    requestedModelId: "stream-model",
  });

  const onStepFinish = vi.fn();
  const onFinish = vi.fn();
  const output = await wrapped.stream("run", {
    maxSteps: 3,
    onFinish,
    onStepFinish,
  });
  await expect(output.text).resolves.toBe("");
  expect(normalize).not.toHaveBeenCalled();
  expect(sendEmail).not.toHaveBeenCalled();
  expect(modelCalls).toBe(1);
  expect(onStepFinish).not.toHaveBeenCalled();
  expect(onFinish).not.toHaveBeenCalled();
  expect(
    api.calls
      .filter((call) => call.method === "PATCH")
      .map((call) => call.body?.status),
  ).toEqual(["failed"]);
  expect(
    api
      .nodeBatches()
      .flat()
      .some(
        (node) => node.node_type === "tool_call" && node.status === "failed",
      ),
  ).toBe(true);
});
