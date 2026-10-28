import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { afterEach, expect, it, vi } from "vitest";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import { getMastraVersion } from "../src/stream-recording.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  settleBuffering,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  installTestApi,
  ORIGINAL_SESSION_ID,
  REPLAY_ID,
} from "./helpers.js";

afterEach(async () => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  await settleBuffering();
});

it("reports OM divergence when an override adds a new memory call", async () => {
  const runtime = createMemoryRuntime({ messageTokens: 600 });
  await seedMemory(runtime);
  const api = installTestApi({
    replaySpec: {
      id: REPLAY_ID,
      baseline_session_id: ORIGINAL_SESSION_ID,
      status: "pending",
      override: { model: "fixture/replacement" },
      tool_policy: { default: { type: "passthrough" }, tools: {} },
    },
  });
  const requests: unknown[] = [];
  let calls = 0;
  const replacement = new MastraLanguageModelV2Mock({
    modelId: "replacement",
    provider: "fixture",
    doStream: async (args) => {
      requests.push(args);
      calls++;
      return calls === 1
        ? streamParts(
            [
              {
                type: "tool-call",
                toolCallId: "change-memory",
                toolName: "updateWorkingMemory",
                input: JSON.stringify({
                  memory: { preference: "replay-green" },
                }),
              },
            ],
            "tool-calls",
          )
        : calls === 2
          ? streamParts(
              [
                {
                  type: "tool-call",
                  toolCallId: "evidence",
                  toolName: "readEvidence",
                  input: "{}",
                },
              ],
              "tool-calls",
            )
          : textStream("evolved");
    },
  });
  const original = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => textStream("original"),
  });
  const { createTool } = await import("@mastra/core/tools");
  const { z } = await import("zod/v4");
  const adapter = createMemoryReplayAgent(
    ({ memory }) => ({
      id: "evolving",
      name: "Evolving",
      instructions: "Use evidence",
      model: original,
      memory,
      defaultOptions: { maxSteps: 5 },
      tools: {
        readEvidence: createTool({
          id: "readEvidence",
          description: "Read conversation evidence",
          inputSchema: z.object({}),
          execute: async () => {
            await memory.settled();
            return {
              evidence: "The replay user now prefers green. ".repeat(400),
            };
          },
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      allowedReplayModels: ["fixture/replacement"],
      sourceMemory: () => ({
        settled: () => runtime.memory.settled(),
        domain: runtime.domain,
        configuration: runtime.memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: async (id) =>
        id.includes("observer")
          ? runtime.observer.model
          : id.includes("reflector")
            ? runtime.reflector.model
            : id === "fixture/replacement"
              ? replacement
              : original,
    },
  );
  const baseline = await adapter.stream("Baseline turn", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await baseline.consumeStream();
  await vi.waitFor(() =>
    expect(
      api.calls.some(
        (call) =>
          call.method === "PATCH" &&
          (call.body?.metadata as Record<string, unknown> | undefined)
            ?.mastra_replay_state === "eligible",
      ),
    ).toBe(true),
  );
  const input = api.calls.find(
    (call) =>
      call.method === "PATCH" &&
      (call.body?.metadata as Record<string, unknown> | undefined)
        ?.mastra_replay_state === "eligible",
  )?.body?.inputs;
  const observerCalls = runtime.observer.calls.length;
  const reflectorCalls = runtime.reflector.calls.length;
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(input));
  const nativeStream = Agent.prototype.stream;
  let nativeResult: unknown;
  vi.spyOn(Agent.prototype, "stream").mockImplementation(async function (
    this: Agent,
    ...args
  ) {
    nativeResult = await Reflect.apply(nativeStream, this, args);
    return nativeResult as Awaited<ReturnType<Agent["stream"]>>;
  });
  const output = await adapter.stream("ignored");
  expect(output).toBe(nativeResult);
  await output.consumeStream();
  // From Mastra 1.72 the added memory call is a blocking reflection, which
  // fails the replayed turn closed before the actor answers.
  const blockingReflection = Number(getMastraVersion().split(".")[1]) >= 72;
  expect(await output.text).toBe(blockingReflection ? "" : "evolved");
  expect(runtime.observer.calls).toHaveLength(observerCalls);
  expect(runtime.reflector.calls).toHaveLength(reflectorCalls);
  await vi.waitFor(() =>
    expect(
      api.calls.filter((call) => call.method === "PATCH").at(-1)?.body,
    ).toMatchObject({
      status: "failed",
      error: "KITARU_REPLAY_DIVERGED:mastra_om_call_order",
      metadata: {
        mastra_replay_state: "diverged",
        mastra_replay_reason: "mastra_om_call_order",
      },
    }),
  );
  expect(
    await runtime.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("historical-blue");
  await runtime.store.close();
});
