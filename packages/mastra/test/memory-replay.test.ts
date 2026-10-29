import { Agent } from "@mastra/core/agent";
import { InMemoryMemory } from "@mastra/core/storage";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { expect, it } from "vitest";
import { z } from "zod/v4";
import type { MastraMemoryMutation } from "../src/memory-binding.js";
import {
  createIsolatedMemoryReplay,
  isAsyncBufferingRunning,
  onBufferingWait,
  serializeMemoryConfiguration,
  trackBufferingWaits,
} from "../src/memory-replay.js";
import {
  createMemoryRuntime,
  holdBuffering,
  type ModelCall,
  RESOURCE,
  seedMemory,
  snapshotMemory,
  streamParts,
  THREAD,
  textStream,
} from "./helpers/memory-agent.js";

async function baseline() {
  const source = createMemoryRuntime();
  await seedMemory(source);
  const initialSnapshot = {
    ...(await snapshotMemory(source, true)),
    threadId: THREAD,
    resourceId: RESOURCE,
  };
  const configuration = serializeMemoryConfiguration(
    source.memory.getMergedThreadConfig(),
  );
  return { source, initialSnapshot, configuration };
}

it("restores historical memory with original timestamps into independent native stores", async () => {
  const { source, initialSnapshot, configuration } = await baseline();
  await source.domain.updateThread({
    id: THREAD,
    metadata: { workingMemory: "production advanced" },
  });
  const options = {
    initialSnapshot,
    configuration,
    resolveModel: async (id: string) =>
      id.endsWith("observer") ? source.observer.model : source.reflector.model,
    recordMutation: async () => {},
    finalizationWaitMs: 60_000,
  };
  const one = await createIsolatedMemoryReplay({
    ...options,
    invocationId: "one",
  });
  const two = await createIsolatedMemoryReplay({
    ...options,
    invocationId: "two",
  });
  expect(one.initialSnapshot).toEqual(initialSnapshot);
  await one.memory.updateWorkingMemory({
    threadId: THREAD,
    resourceId: RESOURCE,
    workingMemory: "replay changed",
  });
  expect(
    await two.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("historical-blue");
  expect(
    (await source.domain.getThreadById({ threadId: THREAD }))?.metadata
      ?.workingMemory,
  ).toBe("production advanced");
  await one.finish();
  await two.finish();
});

it("serializes native schema and model identities without serializing live model objects", async () => {
  const { configuration } = await baseline();
  expect(JSON.stringify(configuration)).toContain('"type":"object"');
  expect(JSON.stringify(configuration)).toContain("fixture/observer");
  expect(JSON.stringify(configuration)).not.toContain("doStream");
});

it("rejects unsupported memory dependencies and unjoined work before model resolution", async () => {
  expect(() => serializeMemoryConfiguration({ semanticRecall: true })).toThrow(
    /semantic/i,
  );
  expect(() =>
    serializeMemoryConfiguration({
      workingMemory: { enabled: true, scope: "resource" },
    }),
  ).toThrow(/thread/i);
  const { initialSnapshot, configuration } = await baseline();
  const firstRecord = initialSnapshot.records[0];
  if (!firstRecord) throw new Error("Missing native memory fixture");
  firstRecord.isObserving = true;
  let resolved = false;
  await expect(
    createIsolatedMemoryReplay({
      invocationId: "bad",
      initialSnapshot,
      configuration,
      resolveModel: async () => {
        resolved = true;
        throw new Error("must not resolve");
      },
      recordMutation: async () => {},
      finalizationWaitMs: 60_000,
    }),
  ).rejects.toThrow(/Unjoined/);
  expect(resolved).toBe(false);
});

it("runs native observation, reflection and working-memory changes with ordered evidence", async () => {
  const source = createMemoryRuntime({ messageTokens: 600 });
  await seedMemory(source);
  const changes: MastraMemoryMutation[] = [];
  const runtime = await createIsolatedMemoryReplay({
    invocationId: "evolving",
    initialSnapshot: {
      ...(await snapshotMemory(source, true)),
      threadId: THREAD,
      resourceId: RESOURCE,
    },
    configuration: serializeMemoryConfiguration(
      source.memory.getMergedThreadConfig(),
    ),
    resolveModel: (id) =>
      id.endsWith("observer") ? source.observer.model : source.reflector.model,
    recordMutation: async (event) => {
      changes.push(event);
    },
    finalizationWaitMs: 60_000,
  });
  const calls: ModelCall[] = [];
  const model = new MastraLanguageModelV2Mock({
    provider: "fixture",
    modelId: "actor",
    doStream: async (options) => {
      calls.push(options);
      if (calls.length > 2) return textStream("done");
      return streamParts(
        [
          {
            type: "tool-call",
            toolCallId: `call-${calls.length}`,
            toolName: calls.length === 1 ? "updateWorkingMemory" : "evidence",
            input:
              calls.length === 1
                ? JSON.stringify({ memory: { preference: "replay-green" } })
                : "{}",
          },
        ],
        "tool-calls",
      );
    },
  });
  const agent = new Agent({
    id: "isolated",
    name: "Isolated",
    instructions: "Answer from memory",
    model,
    memory: runtime.memory,
    tools: {
      evidence: createTool({
        id: "evidence",
        description: "Read evidence",
        inputSchema: z.object({}),
        execute: async () => {
          await runtime.memory.settled();
          return "The user now prefers replay-green. ".repeat(400);
        },
      }),
    },
  });
  const result = await agent.stream("Remember my new preference.", {
    maxSteps: 5,
    memory: { thread: THREAD, resource: RESOURCE },
  });
  for await (const _ of result.textStream) {
    /* Consume the native stream. */
  }
  await runtime.finish();
  expect(source.observer.calls.length).toBeGreaterThan(0);
  expect(source.reflector.calls.length).toBeGreaterThan(0);
  expect(JSON.stringify(calls.at(-1)?.prompt)).toContain("REFLECTED_REPLAY");
  expect(
    changes.some((event) => event.method === "createReflectionGeneration"),
  ).toBe(true);
  expect(changes.map((event) => event.revision)).toEqual(
    changes.map((_, index) => index + 1),
  );
  expect(runtime.binding.incompleteReasons).toEqual([]);
  expect(
    await source.memory.getWorkingMemory({
      threadId: THREAD,
      resourceId: RESOURCE,
    }),
  ).toContain("historical-blue");
}, 20000);

it("waits for native background observation and evidence before releasing replay state", async () => {
  let release!: () => void;
  let signalStarted!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    signalStarted = resolve;
  });
  const source = createMemoryRuntime({
    messageTokens: 10000,
    observerWait: async () => {
      signalStarted();
      await blocked;
    },
  });
  await seedMemory(source);
  const runtime = await createIsolatedMemoryReplay({
    invocationId: "background",
    initialSnapshot: {
      ...(await snapshotMemory(source, true)),
      threadId: THREAD,
      resourceId: RESOURCE,
    },
    configuration: serializeMemoryConfiguration(
      source.memory.getMergedThreadConfig(),
    ),
    resolveModel: (id) =>
      id.endsWith("observer") ? source.observer.model : source.reflector.model,
    recordMutation: async () => {},
    finalizationWaitMs: 60_000,
  });
  try {
    const agent = new Agent({
      id: "background",
      name: "Background",
      instructions: "Answer",
      model: new MastraLanguageModelV2Mock({
        doStream: async () => textStream("done"),
      }),
      memory: runtime.memory,
    });
    const result = await agent.stream("Remember this preference.", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    for await (const _ of result.textStream) {
      /* Consume native stream. */
    }
    await started;
    let finished = false;
    const completion = runtime.finish().then(() => {
      finished = true;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(finished).toBe(false);
    release();
    await completion;
    expect(runtime.binding.incompleteReasons).toEqual([]);
  } finally {
    release();
    await runtime.finish();
  }
}, 20000);

it("fails a replay whose background observation misses the finalization deadline", async () => {
  let release!: () => void;
  let signalStarted!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    signalStarted = resolve;
  });
  const source = createMemoryRuntime({
    messageTokens: 10000,
    observerWait: async () => {
      signalStarted();
      await blocked;
    },
  });
  await seedMemory(source);
  const runtime = await createIsolatedMemoryReplay({
    invocationId: "hung-background",
    initialSnapshot: {
      ...(await snapshotMemory(source, true)),
      threadId: THREAD,
      resourceId: RESOURCE,
    },
    configuration: serializeMemoryConfiguration(
      source.memory.getMergedThreadConfig(),
    ),
    resolveModel: (id) =>
      id.endsWith("observer") ? source.observer.model : source.reflector.model,
    recordMutation: async () => {},
    finalizationWaitMs: 200,
  });
  try {
    const agent = new Agent({
      id: "hung-background",
      name: "Hung background",
      instructions: "Answer",
      model: new MastraLanguageModelV2Mock({
        doStream: async () => textStream("done"),
      }),
      memory: runtime.memory,
    });
    const result = await agent.stream("Remember this preference.", {
      memory: { thread: THREAD, resource: RESOURCE },
    });
    for await (const _ of result.textStream) {
      /* Consume native stream. */
    }
    await started;
    expect(await runtime.finish()).toBe(false);
    expect(runtime.binding.incompleteReasons).toContain(
      "Observational-memory work did not settle before the finalization deadline.",
    );
    expect(runtime.binding.incompleteReason).toBe("om_settle_timeout");
  } finally {
    release();
  }
}, 20000);

it("replays on the record semantics of a database store unless the source was in memory", async () => {
  const { source, initialSnapshot, configuration } = await baseline();
  const historical = initialSnapshot.records[0];
  if (!historical) throw new Error("Missing native memory fixture");
  // A database copies an unobserved generation's cursor into its reflection.
  historical.lastObservedAt = undefined;
  historical.metadata = { origin: "database" };
  const options = {
    initialSnapshot,
    configuration,
    resolveModel: async (id: string) =>
      id.endsWith("observer") ? source.observer.model : source.reflector.model,
    recordMutation: async () => {},
    finalizationWaitMs: 60_000,
  };
  const persistent = await createIsolatedMemoryReplay({
    ...options,
    invocationId: "persistent",
  });
  const domain = persistent.binding.domain;
  const current = await domain.getObservationalMemory(THREAD, RESOURCE);
  if (!current) throw new Error("Missing restored observational memory");
  current.activeObservations = "CHANGED BY THE CALLER";
  expect(
    (await domain.getObservationalMemory(THREAD, RESOURCE))?.activeObservations,
  ).toBe(historical.activeObservations);
  expect(
    await domain.swapBufferedToActive({
      id: current.id,
      activationRatio: 1,
      messageTokensThreshold: 100,
      currentPendingTokens: 50,
      bufferedChunks: [
        {
          id: "caller-chunk",
          cycleId: "caller-cycle",
          observations: "NOT PERSISTED",
          tokenCount: 3,
          messageIds: ["historical-message"],
          messageTokens: 50,
          lastObservedAt: new Date(),
          createdAt: new Date(),
        },
      ],
    }),
  ).toMatchObject({ chunksActivated: 0 });
  current.activeObservations = historical.activeObservations;
  await domain.createReflectionGeneration({
    currentRecord: current,
    reflection: "REFLECTED",
    tokenCount: 3,
  });
  const reflected = await domain.getObservationalMemory(THREAD, RESOURCE);
  if (!reflected) throw new Error("Missing reflection generation");
  expect(reflected).toMatchObject({
    originType: "reflection",
    metadata: { origin: "database" },
  });
  expect(reflected.lastObservedAt).toBeUndefined();
  const observation = {
    id: reflected.id,
    observations: "OBSERVED",
    tokenCount: 3,
    lastObservedAt: new Date("2026-01-02T00:00:00Z"),
  };
  await domain.updateActiveObservations({
    ...observation,
    observedMessageIds: ["historical-message"],
  });
  await domain.updateActiveObservations(observation);
  expect(
    (await domain.getObservationalMemory(THREAD, RESOURCE))?.observedMessageIds,
  ).toBeUndefined();
  await persistent.finish();

  const inMemory = await createIsolatedMemoryReplay({
    ...options,
    invocationId: "in-memory",
    storeSemantics: "in-memory",
  });
  const nativeDomain = inMemory.binding.domain;
  const nativeCurrent = await nativeDomain.getObservationalMemory(
    THREAD,
    RESOURCE,
  );
  if (!nativeCurrent) throw new Error("Missing restored observational memory");
  await nativeDomain.createReflectionGeneration({
    currentRecord: nativeCurrent,
    reflection: "REFLECTED",
    tokenCount: 3,
  });
  const nativeReflected = await nativeDomain.getObservationalMemory(
    THREAD,
    RESOURCE,
  );
  if ("commitActiveObservations" in nativeDomain)
    expect(nativeReflected?.lastObservedAt).toBeUndefined();
  else expect(nativeReflected?.lastObservedAt).toBeInstanceOf(Date);
  await inMemory.finish();
});

it("reports when Mastra waits on a thread's running buffer round", async () => {
  const { memory } = createMemoryRuntime();
  const engine = await memory.omEngine;
  if (!engine) throw new Error("Missing OM engine");
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  trackBufferingWaits(engine);
  const endRound = await holdBuffering(memory);
  try {
    let waits = 0;
    const stop = onBufferingWait(engine, selector, "observation", () => {
      waits++;
    });
    // Subscribing read the round from Mastra's map. Mastra also reads it
    // without waiting for some reflection activations, and a read alone
    // must not release a held result.
    expect(waits).toBe(0);
    const waited = engine.waitForBuffering(THREAD, RESOURCE);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(waits).toBe(1);
    stop();
    // A hold that starts while Mastra already waits ends at once.
    let late = 0;
    onBufferingWait(engine, selector, "observation", () => {
      late++;
    });
    expect(late).toBe(1);
    endRound();
    await waited;
  } finally {
    endRound();
  }
});

it("forgets a finished buffer round that Mastra removes only while its map holds it", async () => {
  const { memory } = createMemoryRuntime();
  const engine = await memory.omEngine;
  if (!engine) throw new Error("Missing OM engine");
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  trackBufferingWaits(engine);
  const rounds = (
    engine.buffering.constructor as unknown as {
      asyncBufferingOps: Map<string, Promise<void>>;
    }
  ).asyncBufferingOps;
  const key = engine.buffering.getObservationBufferKey(
    engine.buffering.getLockKey(THREAD, RESOURCE),
  );
  let endRound!: () => void;
  // Mastra 1.77 and later clear a finished round this way.
  const round: Promise<void> = new Promise<void>((resolve) => {
    endRound = resolve;
  }).finally(() => {
    if (rounds.get(key) === round) rounds.delete(key);
  });
  rounds.set(key, round);
  expect(isAsyncBufferingRunning(engine, selector, "observation")).toBe(true);
  endRound();
  await round;
  expect(isAsyncBufferingRunning(engine, selector, "observation")).toBe(false);
});

it.skipIf(!("commitActiveObservations" in InMemoryMemory.prototype))(
  "preserves conflict and retirement no-ops under persistent replay semantics",
  async () => {
    const { source, initialSnapshot, configuration } = await baseline();
    const runtime = await createIsolatedMemoryReplay({
      invocationId: "conflict-no-ops",
      initialSnapshot,
      configuration,
      resolveModel: (id) =>
        id.endsWith("observer")
          ? source.observer.model
          : source.reflector.model,
      recordMutation: async () => {},
      finalizationWaitMs: 60_000,
    });
    const domain = runtime.binding.domain;
    if (
      !("commitActiveObservations" in domain) ||
      typeof domain.commitActiveObservations !== "function"
    )
      throw new Error("Missing conflict-aware storage capability");
    const current = await domain.getObservationalMemory(THREAD, RESOURCE);
    if (!current) throw new Error("Missing restored record");
    const observation = {
      id: current.id,
      observations: "OBSERVED",
      tokenCount: 3,
      lastObservedAt: new Date("2026-01-02T00:00:00Z"),
    };
    expect(
      await domain.commitActiveObservations({
        ...observation,
        observedMessageIds: ["historical-message"],
      }),
    ).toEqual({ applied: true });
    const beforeConflict = await domain.getObservationalMemory(
      THREAD,
      RESOURCE,
    );
    expect(
      await domain.commitActiveObservations({
        ...observation,
        observations: "REJECTED",
        expectedActiveObservations: "STALE",
      }),
    ).toEqual({ applied: false, reason: "conflict" });
    expect(await domain.getObservationalMemory(THREAD, RESOURCE)).toEqual(
      beforeConflict,
    );
    expect(await domain.commitActiveObservations(observation)).toEqual({
      applied: true,
    });
    expect(
      (await domain.getObservationalMemory(THREAD, RESOURCE))
        ?.observedMessageIds,
    ).toBeUndefined();

    const stored = await domain.getObservationalMemory(THREAD, RESOURCE);
    if (!stored) throw new Error("Missing stored record");
    const rejected = await domain.createReflectionGeneration({
      currentRecord: {
        ...stored,
        activeObservations: "UNRELATED SNAPSHOT",
        lastObservedAt: new Date(0),
        metadata: { poisoned: true },
      },
      reflection: "REJECTED REFLECTION",
      tokenCount: 1,
    });
    expect(rejected.id).toBe(stored.id);
    expect(await domain.getObservationalMemory(THREAD, RESOURCE)).toEqual(
      stored,
    );
    const successor = await domain.createReflectionGeneration({
      currentRecord: stored,
      reflection: "REFLECTED",
      tokenCount: 1,
    });
    const beforeRetirement = await domain.getObservationalMemoryHistory(
      THREAD,
      RESOURCE,
    );
    expect(
      await domain.commitActiveObservations({
        ...observation,
        observations: "RETIRED",
      }),
    ).toEqual({ applied: false, reason: "retired" });
    const head = await domain.createReflectionGeneration({
      currentRecord: {
        ...stored,
        lastObservedAt: new Date(0),
        metadata: { poisoned: true },
      },
      reflection: "RETIRED REFLECTION",
      tokenCount: 1,
    });
    expect(head.id).toBe(successor.id);
    expect(
      await domain.getObservationalMemoryHistory(THREAD, RESOURCE),
    ).toEqual(beforeRetirement);
    expect(await snapshotMemory(source, true)).toEqual({
      thread: initialSnapshot.thread,
      resource: initialSnapshot.resource,
      messages: initialSnapshot.messages,
      records: initialSnapshot.records,
    });
    expect(await runtime.finish()).toBe(true);
  },
);

it.skipIf(!("commitActiveObservations" in InMemoryMemory.prototype))(
  "preserves metadata when persistent initialization returns an existing head",
  async () => {
    const { source, initialSnapshot, configuration } = await baseline();
    const historical = initialSnapshot.records[0];
    if (!historical) throw new Error("Missing fixture record");
    historical.metadata = { origin: "preserved" };
    const runtime = await createIsolatedMemoryReplay({
      invocationId: "idempotent-initialization",
      initialSnapshot,
      configuration,
      resolveModel: (id) =>
        id.endsWith("observer")
          ? source.observer.model
          : source.reflector.model,
      recordMutation: async () => {},
      finalizationWaitMs: 60_000,
    });
    const domain = runtime.binding.domain;
    const before = await domain.getObservationalMemory(THREAD, RESOURCE);
    expect(
      await domain.initializeObservationalMemory({
        threadId: THREAD,
        resourceId: RESOURCE,
        scope: "thread",
        config: {},
      }),
    ).toEqual(before);
    expect(await domain.getObservationalMemory(THREAD, RESOURCE)).toEqual(
      before,
    );
    expect(await runtime.finish()).toBe(true);
  },
);

it.skipIf(!("commitActiveObservations" in InMemoryMemory.prototype))(
  "carries persisted cursors and appended text through a stale reflection snapshot",
  async () => {
    const { source, initialSnapshot, configuration } = await baseline();
    const runtime = await createIsolatedMemoryReplay({
      invocationId: "append-during-reflection",
      initialSnapshot,
      configuration,
      resolveModel: (id) =>
        id.endsWith("observer")
          ? source.observer.model
          : source.reflector.model,
      recordMutation: async () => {},
      finalizationWaitMs: 60_000,
    });
    const domain = runtime.binding.domain;
    const current = await domain.getObservationalMemory(THREAD, RESOURCE);
    if (!current) throw new Error("Missing restored record");
    const cursor = new Date("2026-01-02T00:00:00Z");
    await domain.updateActiveObservations({
      id: current.id,
      observations: `${current.activeObservations}\nAPPENDED WHILE REFLECTING`,
      tokenCount: current.observationTokenCount + 2,
      lastObservedAt: cursor,
    });
    const successor = await domain.createReflectionGeneration({
      currentRecord: current,
      reflection: "REFLECTED",
      tokenCount: 1,
    });
    expect(successor.activeObservations).toBe(
      "REFLECTED\n\nAPPENDED WHILE REFLECTING",
    );
    expect(successor.lastObservedAt).toEqual(cursor);
    expect(
      (await domain.getObservationalMemory(THREAD, RESOURCE))?.lastObservedAt,
    ).toEqual(cursor);
    expect(await runtime.finish()).toBe(true);
  },
);
