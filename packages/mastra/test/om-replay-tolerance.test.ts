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
  type ModelCall,
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
  evidenceRepeats?: number | (() => number);
  missingObservationalMemoryResults?: "fail" | "live";
}) {
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) throw new Error("Missing memory store");
  let observed = 0;
  const observerCalls: ModelCall[] = [];
  // The observation names the evidence markers the observer was shown, so a
  // test can tell which tool results an observation covers.
  const observer = {
    calls: observerCalls,
    model: new MastraLanguageModelV2Mock({
      modelId: "observer",
      provider: "fixture",
      doStream: async (call) => {
        observerCalls.push(call);
        await options.observe(++observed);
        const seen = markers(JSON.stringify(call.prompt), "EVIDENCE");
        // Providers stamp their response metadata with a Date.
        return streamParts([
          {
            type: "response-metadata",
            id: `observation-${observed}`,
            timestamp: new Date(0),
            modelId: "observer",
          },
          { type: "text-start", id: "text" },
          {
            type: "text-delta",
            id: "text",
            delta: `<observations>\nOBSERVED: ${seen.map((n) => `OBSERVED_${n}_MARK`).join(" ")} ${"The user changed the preference to replay-green. ".repeat(15)}\n</observations>\n<current-task>Continue.</current-task>`,
          },
          { type: "text-end", id: "text" },
        ]);
      },
    }),
  };
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
  let evidence = 0;
  const actorPrompts: string[] = [];
  const actor = new MastraLanguageModelV2Mock({
    modelId: "actor",
    provider: "fixture",
    doStream: async ({ prompt }) => {
      actorPrompts.push(JSON.stringify(prompt));
      step += 1;
      if (step > options.toolSteps()) {
        step = 0;
        evidence = 0;
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
            `EVIDENCE_${++evidence}_MARK ${"The user now prefers replay-green. ".repeat(
              typeof options.evidenceRepeats === "function"
                ? options.evidenceRepeats()
                : (options.evidenceRepeats ?? 30),
            )}`,
        }),
      },
    }),
    {
      agentId: AGENT_ID,
      apiUrl: "https://kitaru.invalid",
      requestedModelId: "fixture/actor",
      missingObservationalMemoryResults:
        options.missingObservationalMemoryResults,
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
  return {
    actorPrompts,
    api,
    adapter,
    runtime: runtime as unknown as MemoryRuntime,
  };
}

/** The numbers of the `<kind>_<n>_MARK` markers in `text`. */
function markers(text: string, kind: "EVIDENCE" | "OBSERVED"): string[] {
  return [
    ...new Set(
      [...text.matchAll(new RegExp(`${kind}_(\\d+)_MARK`, "g"))].map((match) =>
        String(match[1]),
      ),
    ),
  ];
}

/** Which tool results each actor prompt holds raw and which it holds observed. */
function contextOf(prompts: readonly string[]): string[] {
  return prompts.map(
    (prompt) =>
      `raw=${markers(prompt, "EVIDENCE")} observed=${markers(prompt, "OBSERVED")}`,
  );
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
  const baselineContext = contextOf(fixture.actorPrompts.splice(0));
  // The slow observer was still running over the first evidence while the
  // actor read more, and its result covers the later evidence too.
  expect(baselineContext).toContain("raw=2,3,4 observed=1");
  delayMs = 0;
  await clearProcessBufferingState();
  const calls = await replay(fixture, baseline?.inputs);
  const [closed] = patches(calls);
  expect(closed?.body).toMatchObject({ status: "completed" });
  // The instant replay starts buffer rounds earlier than the slow baseline
  // did. None of them may show the actor evidence it has not read yet.
  expect(contextOf(fixture.actorPrompts)).toEqual(baselineContext);
  const divergence = (
    closed?.body?.metadata as
      | { mastra_om_divergence?: { unused_results: number } }
      | undefined
  )?.mastra_om_divergence;
  expect(divergence?.unused_results ?? 0).toBe(0);
  expect(fixture.runtime.observer.calls).toHaveLength(recorded);
  expect(fixture.runtime.reflector.calls).toHaveLength(0);
  await fixture.runtime.store.close();
});

it("fails a replay closed when a blocking observation has no recorded result left", async () => {
  let toolSteps = 1;
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {},
    toolSteps: () => toolSteps,
    evidenceRepeats: 400,
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  const recorded = fixture.runtime.observer.calls.length;
  expect(recorded).toBe(1);
  // The replayed actor reads more evidence than the baseline did, so it needs
  // blocking observations production never made. An empty one would drop
  // that evidence from the actor's context.
  toolSteps = 4;
  const calls = await replay(fixture, baseline?.inputs);
  expect(patches(calls)[0]?.body).toMatchObject({
    status: "failed",
    error: "KITARU_REPLAY_DIVERGED:mastra_om_call_order",
    metadata: {
      mastra_replay_state: "diverged",
      mastra_replay_reason: "mastra_om_call_order",
    },
  });
  expect(fixture.runtime.observer.calls).toHaveLength(recorded);
  await fixture.runtime.store.close();
});

it("names the OM calls whose replay input matched no recorded call", async () => {
  let repeats = 400;
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {},
    toolSteps: () => 1,
    evidenceRepeats: () => repeats,
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  expect(fixture.runtime.observer.calls).toHaveLength(1);
  // The replayed tool returns different evidence, so the blocking observer
  // sees a different input and takes the recorded result anyway.
  repeats = 401;
  const calls = await replay(fixture, baseline?.inputs);
  expect(patches(calls)[0]?.body).toMatchObject({
    status: "completed",
    metadata: { mastra_om_divergence: { input_mismatches: 1 } },
  });
  const mismatch = calls
    .filter((call) => call.method === "POST" && call.path.endsWith("/nodes"))
    .flatMap(
      (call) =>
        (call.body?.nodes ?? []) as Array<{
          name: string;
          attributes: Record<string, unknown>;
        }>,
    )
    .find((node) => node.name === "om_input_mismatch");
  expect(mismatch?.attributes).toEqual({
    count: 1,
    calls: [
      {
        phase: "observer",
        method: "doStream",
        recorded_ordinal: 0,
        replay_call: 0,
      },
    ],
  });
  await fixture.runtime.store.close();
});

it("answers a missing blocking observation live when the replay opts in", async () => {
  let toolSteps = 1;
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {},
    toolSteps: () => toolSteps,
    evidenceRepeats: 400,
    missingObservationalMemoryResults: "live",
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  const recorded = fixture.runtime.observer.calls.length;
  expect(recorded).toBe(1);
  // The replayed actor reads evidence the baseline never read, so OM needs a
  // blocking observation production never made.
  toolSteps = 2;
  const calls = await replay(fixture, baseline?.inputs);
  const [closed] = patches(calls);
  expect(closed?.body).toMatchObject({
    status: "completed",
    metadata: {
      mastra_om_live_calls: 1,
      mastra_om_divergence: { live_calls: 1 },
    },
  });
  // The recorded observation still answers the call it was recorded for.
  expect(fixture.runtime.observer.calls).toHaveLength(recorded + 1);
  const liveNodes = calls
    .filter((call) => call.method === "POST" && call.path.endsWith("/nodes"))
    .flatMap(
      (call) =>
        (call.body?.nodes ?? []) as Array<{
          name: string;
          node_type: string;
          model: string | null;
          outputs: unknown;
          attributes: Record<string, unknown>;
        }>,
    )
    .filter((node) => node.attributes?.om_live === true);
  expect(liveNodes).toMatchObject([
    {
      name: "om_observer_live_call",
      node_type: "llm_call",
      model: "fixture/observer",
      attributes: {
        om_phase: "observer",
        evidence_complete: true,
        evidence_loss_reasons: [],
      },
    },
  ]);
  // The live result is kept, Date included, as the reviewer saw it.
  expect(JSON.stringify(liveNodes[0]?.outputs)).toContain("OBSERVED:");
  expect(JSON.stringify(liveNodes[0]?.outputs)).toContain(
    '{"$mastra":"date","value":"1970-01-01T00:00:00.000Z"}',
  );
  expect(nodeNames(calls)).toContain("om_call_divergence");
  await fixture.runtime.store.close();
});

it("makes no live OM call when an opted-in replay has every result", async () => {
  const fixture = setup({
    observation: { messageTokens: 600, bufferTokens: false },
    observe: () => {},
    toolSteps: () => 1,
    evidenceRepeats: 400,
    missingObservationalMemoryResults: "live",
  });
  const baseline = await recordBaseline(fixture);
  expect(baseline?.metadata).toMatchObject({ mastra_replay_state: "eligible" });
  const recorded = fixture.runtime.observer.calls.length;
  expect(recorded).toBe(1);
  const calls = await replay(fixture, baseline?.inputs);
  const [closed] = patches(calls);
  expect(closed?.body?.status).toBe("completed");
  expect(closed?.body?.metadata ?? {}).not.toHaveProperty(
    "mastra_om_live_calls",
  );
  expect(fixture.runtime.observer.calls).toHaveLength(recorded);
  expect(nodeNames(calls)).not.toContain("om_observer_live_call");
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
