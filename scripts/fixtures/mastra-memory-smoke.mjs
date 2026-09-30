import assert from "node:assert/strict";
import { InMemoryStore } from "@mastra/core/storage";
import { Memory } from "@mastra/memory";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "@zenml-io/kitaru-mastra/memory";
import { z } from "zod";

const calls = [];
const requests = [];
const replayId = "018f0000-0000-7000-8000-000000000102";
const sessionId = "018f0000-0000-7000-8000-000000000101";
const originalFetch = globalThis.fetch;
// Only the Kitaru transport is replaced. Memory, tools and streaming are native.
globalThis.fetch = async (input, init = {}) => {
  const url = new URL(String(input));
  const method = init.method ?? "GET";
  const body = init.body ? JSON.parse(String(init.body)) : undefined;
  calls.push({ body, method, path: url.pathname });
  if (method === "POST" && url.pathname === "/api/v1/sessions")
    return Response.json(
      { id: sessionId, origin: "recorded", status: "in_progress" },
      { status: 201 },
    );
  if (method === "POST" && url.pathname.endsWith("/nodes"))
    return Response.json([]);
  if (method === "PATCH" && url.pathname.endsWith(sessionId))
    return Response.json({ id: sessionId, origin: "recorded", status: body.status });
  if (method === "GET" && url.pathname.endsWith(replayId))
    return Response.json({
      id: replayId,
      baseline_session_id: sessionId,
      job_id: "018f0000-0000-7000-8000-000000000104",
      override: { system_prompt: "Replay instructions" },
      status: "pending",
      tool_policy: {
        default: { type: "history", scope: "baseline", on_miss: "fail" },
        tools: {},
      },
    });
  throw new Error(`Unexpected memory smoke request: ${method} ${url.pathname}`);
};

let replaying = false;
let modelCalls = 0;
const model = {
  modelId: "memory-smoke-model",
  provider: "package-smoke",
  specificationVersion: "v2",
  supportedUrls: {},
  doGenerate: async () => {
    throw new Error("Expected native streaming");
  },
  doStream: async (request) => {
    requests.push(request);
    const toolStep = ++modelCalls % 2 === 1;
    const parts = toolStep
      ? [
          {
            type: "tool-call",
            toolCallId: `memory-${modelCalls}`,
            toolName: "updateWorkingMemory",
            input: JSON.stringify({
              memory: {
                preference: replaying ? "replay-green" : "baseline-red",
              },
            }),
          },
        ]
      : [
          { type: "text-start", id: "text" },
          { type: "text-delta", id: "text", delta: "done" },
          { type: "text-end", id: "text" },
        ];
    return {
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: "stream-start", warnings: [] });
          for (const part of parts) controller.enqueue(part);
          controller.enqueue({
            type: "finish",
            finishReason: toolStep ? "tool-calls" : "stop",
            usage: { inputTokens: 5, outputTokens: 2, totalTokens: 7 },
          });
          controller.close();
        },
      }),
    };
  },
};
const store = new InMemoryStore();
const sourceMemory = new Memory({
  storage: store,
  options: {
    semanticRecall: false,
    workingMemory: {
      enabled: true,
      scope: "thread",
      schema: z.object({ preference: z.string() }),
    },
  },
});
const selector = {
  threadId: "memory-smoke-thread",
  resourceId: "memory-smoke-resource",
};
const exclusiveAccess = createProcessLocalMemoryAccess();
try {
  await sourceMemory.createThread({ ...selector, title: "Smoke" });
  await sourceMemory.updateWorkingMemory({
    ...selector,
    workingMemory: '{"preference":"historical-blue"}',
  });
  const agent = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "memory-smoke",
      name: "Memory smoke",
      memory,
      model,
      instructions: "Baseline instructions",
      defaultOptions: { maxSteps: 3 },
    }),
    {
      agentId: sessionId,
      apiUrl: "https://api.example",
      requestedModelId: "package-smoke/memory-smoke-model",
      sourceMemory() {
        assert.equal(replaying, false, "Replay consulted production memory");
        return {
          domain: store.stores.memory,
          configuration: sourceMemory.getMergedThreadConfig(),
          settled: () => sourceMemory.settled(),
          exclusiveAccess,
        };
      },
      resolveModel: () => model,
    },
  );
  await (
    await agent.stream("Change the preference", {
      memory: { thread: selector.threadId, resource: selector.resourceId },
    })
  ).consumeStream();
  const input = calls.find(
    (call) => call.path === "/api/v1/sessions" && call.method === "POST",
  ).body.inputs;
  assert.equal(input.mastra_memory_replay.complete, true);
  await sourceMemory.updateWorkingMemory({
    ...selector,
    workingMemory: '{"preference":"production-today"}',
  });
  replaying = true;
  process.env.KITARU_REPLAY_ID = replayId;
  process.env.KITARU_TASK_INPUTS = JSON.stringify(input);
  await (await agent.stream("Ignored caller input")).consumeStream();
  assert.equal(modelCalls, 4);
  assert.match(JSON.stringify(requests[2].prompt), /historical-blue/);
  assert.doesNotMatch(JSON.stringify(requests[2].prompt), /production-today/);
  assert.match(JSON.stringify(requests[2].prompt), /Replay instructions/);
  assert.match(
    await sourceMemory.getWorkingMemory(selector),
    /production-today/,
  );
  assert.equal(
    calls.filter((call) => call.method === "PATCH").at(-1).body.status,
    "completed",
  );
  const nodes = calls
    .filter((call) => call.path.endsWith("/nodes"))
    .flatMap((call) => call.body.nodes);
  assert.ok(
    nodes.some(
      (node) =>
        node.name === "memory_mutation" &&
        JSON.stringify(node.inputs).includes("replay-green"),
    ),
  );
  assert.equal(nodes.filter((node) => node.node_type === "llm_call").length, 4);
  console.log("Packed Mastra memory recording and isolated replay passed");
} finally {
  delete process.env.KITARU_REPLAY_ID;
  delete process.env.KITARU_TASK_INPUTS;
  globalThis.fetch = originalFetch;
  await store.close();
}
