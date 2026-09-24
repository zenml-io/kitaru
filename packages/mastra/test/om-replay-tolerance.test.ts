import { InMemoryStore } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { Memory } from "@mastra/memory";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod/v4";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "../src/memory.js";
import {
  type MemoryRuntime,
  memoryModel,
  RESOURCE,
  seedMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";
import {
  AGENT_ID,
  type ApiCall,
  installTestApi,
  REPLAY_ID,
} from "./helpers.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

type Observation = { messageTokens: number; bufferTokens: number | false };

function setup(options: {
  observation: Observation;
  observe: (call: number) => Promise<void> | void;
  toolSteps: () => number;
  evidenceRepeats?: number;
}) {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing memory store");
  let observed = 0;
  const observer = memoryModel("observer", async () => {
    await options.observe(++observed);
  });
  const reflector = memoryModel("reflector");
  const memory = new Memory({
    storage: store,
    options: {
      lastMessages: 20,
      semanticRecall: false,
      workingMemory: {
        enabled: true,
        scope: "thread",
        schema: z.object({ preference: z.string() }),
      },
      observationalMemory: {
        scope: "thread",
        observation: {
          model: "fixture/observer",
          ...options.observation,
          ...(options.observation.bufferTokens === false
            ? {}
            : { bufferActivation: 1, blockAfter: 1.1 }),
        },
        reflection: { model: "fixture/reflector", observationTokens: 100_000 },
      },
    },
  });
  let step = 0;
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async () => {
      step += 1;
      if (step > options.toolSteps()) {
        step = 0;
        return textStream("done");
      }
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: `evidence-${step}`,
            toolName: "evidence",
            input: "{}",
          },
        ],
        "tool-calls",
      );
    },
  });
  const api = installTestApi();
  const adapter = createMemoryReplayAgent(
    ({ memory: owned }) => ({
      id: "om-replay",
      name: "OM replay",
      instructions: "Answer",
      model: actor,
      memory: owned,
      defaultOptions: { maxSteps: 8 },
      tools: {
        evidence: createTool({
          id: "evidence",
          description: "Read evidence",
          inputSchema: z.object({}),
          execute: async () =>
            "The user now prefers replay-green. ".repeat(
              options.evidenceRepeats ?? 30,
            ),
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      sourceMemory: () => ({
        settled: () => memory.settled(),
        domain,
        configuration: memory.getMergedThreadConfig(),
        exclusiveAccess: createProcessLocalMemoryAccess(),
      }),
      resolveModel: (id) =>
        id === "fixture/observer"
          ? observer.model
          : id === "fixture/reflector"
            ? reflector.model
            : actor,
    },
  );
  const runtime = { store, domain, memory, observer, reflector };
  return { api, adapter, runtime: runtime as unknown as MemoryRuntime };
}

function patches(calls: ApiCall[]) {
  return calls.filter((call) => call.method === "PATCH" && call.body?.status);
}

function nodeNames(calls: ApiCall[]): string[] {
  return calls
    .filter((call) => call.method === "POST" && call.path.endsWith("/nodes"))
    .flatMap((call) =>
      ((call.body?.nodes ?? []) as Array<{ name: string }>).map(
        (node) => node.name,
      ),
    );
}

/**
 * Clear Mastra's process-wide buffering state for the recorded thread.
 *
 * A replay runs in its own worker process. Mastra keys buffer boundaries by
 * thread in static state, so a replay in the baseline's process would start
 * from the baseline's boundaries instead.
 */
async function clearProcessBufferingState(): Promise<void> {
  const scratch = new Memory({
    storage: new InMemoryStore(),
    options: {
      observationalMemory: { model: "fixture/observer", scope: "thread" },
    },
  });
  await (await scratch.omEngine)?.clear(THREAD, RESOURCE);
}

async function recordBaseline(fixture: ReturnType<typeof setup>) {
  await clearProcessBufferingState();
  await seedMemory(fixture.runtime);
  const output = await fixture.adapter.stream("Remember this.", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  await vi.waitFor(() => expect(patches(fixture.api.calls)).toHaveLength(1), {
    timeout: 10_000,
  });
  return patches(fixture.api.calls)[0]?.body;
}

async function replay(
  fixture: ReturnType<typeof setup>,
  inputs: unknown,
): Promise<ApiCall[]> {
  const start = fixture.api.calls.length;
  vi.stubEnv("KITARU_REPLAY_ID", REPLAY_ID);
  vi.stubEnv("KITARU_TASK_INPUTS", JSON.stringify(inputs));
  const output = await fixture.adapter.stream("ignored");
  await output.consumeStream();
  await vi.waitFor(
    () => expect(patches(fixture.api.calls.slice(start))).toHaveLength(1),
    { timeout: 10_000 },
  );
  return fixture.api.calls.slice(start);
}

it("replays a baseline whose slow observer merged buffer rounds without live OM calls", async () => {
  let delayMs = 300;
  const fixture = setup({
    observation: { messageTokens: 1200, bufferTokens: 0.2 },
    observe: () => new Promise((resolve) => setTimeout(resolve, delayMs)),
    toolSteps: () => 5,
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  const recorded = fixture.runtime.observer.calls.length;
  expect(recorded).toBeGreaterThan(0);
  delayMs = 0;
  await clearProcessBufferingState();
  const calls = await replay(fixture, baseline?.inputs);
  const [closed] = patches(calls);
  expect(closed?.body).toMatchObject({
    status: "completed",
    metadata: { mastra_om_divergence: { unused_results: 0 } },
  });
  const metadata = closed?.body?.metadata as
    | { mastra_om_divergence?: { surplus_calls: number } }
    | undefined;
  // The instant replay starts buffer rounds the slow baseline merged.
  expect(metadata?.mastra_om_divergence?.surplus_calls).toBeGreaterThan(0);
  expect(nodeNames(calls)).toContain("om_call_divergence");
  expect(fixture.runtime.observer.calls).toHaveLength(recorded);
  expect(fixture.runtime.reflector.calls).toHaveLength(0);
  await fixture.runtime.store.close();
});

it("keeps a baseline eligible when Mastra retries a failed observer call", async () => {
  const fixture = setup({
    observation: { messageTokens: 1200, bufferTokens: 0.2 },
    observe: (call) => {
      if (call === 1)
        throw Object.assign(new Error("Rate limit exceeded"), {
          statusCode: 429,
          isRetryable: true,
        });
    },
    toolSteps: () => 2,
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  const inputs = baseline?.inputs as
    | { mastra_memory_replay?: { omTape: unknown[] } }
    | undefined;
  const tape = inputs?.mastra_memory_replay?.omTape;
  expect(tape).toMatchObject([
    { phase: "observer", failed: true, output: null },
    { phase: "observer", output: expect.any(Array) },
  ]);
  const [failed, retry] = (tape ?? []) as Array<{ inputFingerprint: string }>;
  expect(retry?.inputFingerprint).toBe(failed?.inputFingerprint);
  const recorded = fixture.runtime.observer.calls.length;
  await clearProcessBufferingState();
  const calls = await replay(fixture, baseline?.inputs);
  const [closed] = patches(calls);
  expect(closed?.body?.status).toBe("completed");
  // The recorded retry is served for the replay's first call. Its input
  // matches exactly even though every message timestamp differs.
  const metadata = closed?.body?.metadata as
    | { mastra_om_divergence?: { input_mismatches: number } }
    | undefined;
  expect(metadata?.mastra_om_divergence?.input_mismatches ?? 0).toBe(0);
  expect(nodeNames(calls)).not.toContain("om_input_mismatch");
  expect(fixture.runtime.observer.calls).toHaveLength(recorded);
  await fixture.runtime.store.close();
}, 20_000);

it("closes a replay as diverged when a blocking OM call has no recorded result", async () => {
  let toolSteps = 0;
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {},
    toolSteps: () => toolSteps,
    evidenceRepeats: 400,
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  expect(fixture.runtime.observer.calls).toHaveLength(0);
  // The replayed actor reads enough evidence to need a blocking observation
  // that the baseline never made.
  toolSteps = 2;
  const calls = await replay(fixture, baseline?.inputs);
  expect(patches(calls)[0]?.body).toMatchObject({
    status: "failed",
    error: "KITARU_REPLAY_DIVERGED:mastra_om_call_order",
    metadata: {
      mastra_replay_state: "diverged",
      mastra_replay_reason: "mastra_om_call_order",
    },
  });
  expect(fixture.runtime.observer.calls).toHaveLength(0);
  await fixture.runtime.store.close();
});

it("closes a baseline session when a failed blocking observation trips Mastra", async () => {
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {
      throw new Error("observer outage");
    },
    toolSteps: () => 1,
    evidenceRepeats: 400,
  });
  await clearProcessBufferingState();
  await seedMemory(fixture.runtime);
  const output = await fixture.adapter.stream("Remember this.", {
    memory: { thread: THREAD, resource: RESOURCE },
  });
  await output.consumeStream();
  expect(await output.finishReason).toBe("other");
  await vi.waitFor(() => expect(patches(fixture.api.calls)).toHaveLength(1), {
    timeout: 10_000,
  });
  expect(patches(fixture.api.calls)[0]?.body).toMatchObject({
    status: "failed",
    metadata: {
      mastra_replay_state: "ineligible",
      mastra_native_state: "failed",
    },
  });
  await fixture.runtime.store.close();
});
