import { Agent } from "@mastra/core/agent";
import { MastraLanguageModelV2Mock } from "@mastra/core/test-utils/llm-mock";
import { createTool } from "@mastra/core/tools";
import { expect, it } from "vitest";
import { z } from "zod/v4";
import type { MastraMemoryMutation } from "../src/memory-binding.js";
import {
  createIsolatedMemoryReplay,
  serializeMemoryConfiguration,
} from "../src/memory-replay.js";
import {
  createMemoryRuntime,
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
  expect(
    (await nativeDomain.getObservationalMemory(THREAD, RESOURCE))
      ?.lastObservedAt,
  ).toBeInstanceOf(Date);
  await inMemory.finish();
});
