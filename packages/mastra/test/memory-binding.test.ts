import { Extractor } from "@mastra/memory";
import { expect, it, vi } from "vitest";
import {
  createMemoryCaptureBinding,
  createProcessLocalMemoryAccess,
  type MastraMemoryLease,
} from "../src/memory-binding.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  THREAD,
} from "./helpers/memory-agent.js";

function required<T>(value: T | undefined | null): T {
  if (value === undefined || value === null)
    throw new Error("Missing fixture value");
  return value;
}

async function fixture(
  invocationId = "invocation-1",
  access = createProcessLocalMemoryAccess(),
) {
  const runtime = createMemoryRuntime();
  await seedMemory(runtime);
  const recordMutation = vi.fn(async (_event: unknown) => {});
  const binding = createMemoryCaptureBinding({
    invocationId,
    domain: runtime.domain,
    threadId: THREAD,
    resourceId: RESOURCE,
    exclusiveAccess: access,
    recordMutation,
    getRequestId: () => "request-1",
  });
  return { runtime, binding, recordMutation };
}

it("captures initial memory under a real lease, records ordered mutations, and releases", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  const initial = await binding.captureInitial(runtime.memory);
  expect(initial?.thread?.id).toBe(THREAD);
  await binding.domain.updateThread({
    id: THREAD,
    metadata: { workingMemory: "new" },
  });
  const record = await binding.domain.getObservationalMemory(THREAD, RESOURCE);
  await binding.domain.setPendingMessageTokens(required(record).id, 12);
  await binding.drain();
  expect(binding.revision).toBe(2);
  expect(recordMutation.mock.calls.map(([event]) => event)).toMatchObject([
    { id: "invocation-1:memory:1", revision: 1, method: "updateThread" },
    {
      id: "invocation-1:memory:2",
      revision: 2,
      method: "setPendingMessageTokens",
    },
  ]);
  expect(binding.incompleteReasons).toEqual([]);
  expect(initial?.thread?.metadata?.workingMemory).not.toBe("new");
  await binding.release();
});

it("rejects shared-thread overlap while allowing independent source threads", async () => {
  const access = createProcessLocalMemoryAccess();
  const one = await fixture("one", access);
  const two = await fixture("two", access);
  await one.binding.captureInitial(one.runtime.memory);
  expect(await two.binding.captureInitial(two.runtime.memory)).toBeUndefined();
  expect(two.binding.incompleteReasons.join()).toMatch(/exclusive/i);
  await one.binding.release();
  await two.binding.release();
});

it("invalidates recordings on different threads sharing a resource", async () => {
  const access = createProcessLocalMemoryAccess();
  const first = await access.acquire({
    threadId: "first-thread",
    resourceId: "shared-resource",
  });
  expect(await first.verifyEligibility()).toBe(true);
  const second = await access.acquire({
    threadId: "second-thread",
    resourceId: "shared-resource",
  });
  expect(await first.verifyEligibility()).toBe(false);
  expect(await second.verifyEligibility()).toBe(false);
  const independent = await access.acquire({
    threadId: "third-thread",
    resourceId: "other-resource",
  });
  expect(await independent.verifyEligibility()).toBe(true);
  await first();
  await second();
  await independent();
  const recovered = await access.acquire({
    threadId: "third-thread",
    resourceId: "shared-resource",
  });
  expect(await recovered.verifyEligibility()).toBe(true);
  await recovered();
});

it("keeps a shared resource unsafe after an unowned native write", async () => {
  const access = createProcessLocalMemoryAccess();
  const first = await access.acquire({
    threadId: "first-thread",
    resourceId: "shared-resource",
  });
  await access.markUnsafeWrite({
    threadId: "second-thread",
    resourceId: "shared-resource",
  });
  expect(await first.verifyEligibility()).toBe(false);
  await first();
  const later = await access.acquire({
    threadId: "third-thread",
    resourceId: "shared-resource",
  });
  expect(await later.verifyEligibility()).toBe(false);
  await later();
  await access.resetAfterQuiescence({
    threadId: "second-thread",
    resourceId: "shared-resource",
  });
  const recovered = await access.acquire({
    threadId: "third-thread",
    resourceId: "shared-resource",
  });
  expect(await recovered.verifyEligibility()).toBe(true);
  await recovered();
});

it("shows why separate process-local helpers cannot qualify for multi-server replay", async () => {
  const first = createProcessLocalMemoryAccess();
  const second = createProcessLocalMemoryAccess();
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  const firstLease = await first.acquire(selector);
  const secondLease = await second.acquire(selector);
  expect(await firstLease.verifyEligibility()).toBe(true);
  expect(await secondLease.verifyEligibility()).toBe(true);
  await firstLease();
  await secondLease();
});

it("poisons all threads when a native fallback cannot identify its selector", async () => {
  const access = createProcessLocalMemoryAccess();
  const first = await access.acquire({
    threadId: "first",
    resourceId: RESOURCE,
  });
  const second = await access.acquire({
    threadId: "second",
    resourceId: RESOURCE,
  });
  await access.markUnsafeWrite();
  expect(await first.verifyEligibility()).toBe(false);
  expect(await second.verifyEligibility()).toBe(false);
  await expect(access.resetAfterQuiescence()).rejects.toThrow(/active/);
  await first();
  await second();
  const later = await access.acquire({
    threadId: "third",
    resourceId: RESOURCE,
  });
  expect(await later.verifyEligibility()).toBe(false);
  await later();
  await access.resetAfterQuiescence();
  const recovered = await access.acquire({
    threadId: "third",
    resourceId: RESOURCE,
  });
  expect(await recovered.verifyEligibility()).toBe(true);
  await recovered();
});

it("preserves native mutation results despite evidence persistence failure", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  await binding.captureInitial(runtime.memory);
  recordMutation.mockRejectedValue(new Error("sink failed"));
  const result = await binding.domain.updateThread({
    id: THREAD,
    title: "still succeeds",
  });
  expect(result.title).toBe("still succeeds");
  await binding.drain();
  expect(binding.incompleteReasons.join()).toMatch(/persistence/);
  await binding.release();
});

it("marks failed capture or unjoined work incomplete without throwing", async () => {
  const { runtime, binding } = await fixture();
  const record = await runtime.domain.getObservationalMemory(THREAD, RESOURCE);
  await runtime.domain.setObservingFlag(required(record).id, true);
  expect(await binding.captureInitial(runtime.memory)).toBeUndefined();
  expect(binding.incompleteReasons.join()).toMatch(/Unjoined/);
  await binding.release();
});

it("invalidates the first recording when a conflicting invocation cannot get its lease", async () => {
  const access = createProcessLocalMemoryAccess();
  const one = await fixture("one", access);
  const two = await fixture("two", access);
  await one.binding.captureInitial(one.runtime.memory);
  await two.binding.captureInitial(two.runtime.memory);
  expect(one.binding.incompleteReasons.join()).toMatch(/overlapping/);
  await one.binding.release();
  await two.binding.release();
  const next = await fixture("next", access);
  expect(await next.binding.captureInitial(next.runtime.memory)).toBeDefined();
  await next.binding.release();
});

it("keeps a native write after lost ownership and recovers once overlapping turns release", async () => {
  const access = createProcessLocalMemoryAccess();
  const first = await fixture("first", access);
  const second = await fixture("second", access);
  await first.binding.captureInitial(first.runtime.memory);
  expect(
    await second.binding.captureInitial(second.runtime.memory),
  ).toBeUndefined();
  const native = await first.binding.domain.updateThread({
    id: THREAD,
    title: "native write still succeeds",
  });
  expect(native.title).toBe("native write still succeeds");
  await first.binding.verifyEligibility();
  expect(first.binding.incompleteReasons.join()).toMatch(/ownership/);
  await first.binding.release();
  await second.binding.release();
  const next = await fixture("after", access);
  expect(await next.binding.captureInitial(next.runtime.memory)).toBeDefined();
  await next.binding.release();
});

it("registers a late write for its duration instead of poisoning later turns", async () => {
  const access = createProcessLocalMemoryAccess();
  const unsafeWrite = vi.spyOn(access, "markUnsafeWrite");
  const first = await fixture("first", access);
  await first.binding.captureInitial(first.runtime.memory);
  await first.binding.release();
  const holder = await fixture("holder", access);
  expect(
    await holder.binding.captureInitial(holder.runtime.memory),
  ).toBeDefined();
  const native = await first.binding.domain.updateThread({
    id: THREAD,
    title: "late native write",
  });
  expect(native.title).toBe("late native write");
  // The late write overlapped the holder, so only the holder's turn is lost.
  await holder.binding.verifyEligibility();
  expect(holder.binding.incompleteReasons.join()).toMatch(/overlapping/);
  await holder.binding.release();
  expect(unsafeWrite).not.toHaveBeenCalled();
  const next = await fixture("next", access);
  expect(await next.binding.captureInitial(next.runtime.memory)).toBeDefined();
  await next.binding.release();
});

it("keeps a thread unsafe when a late write cannot register", async () => {
  const access = createProcessLocalMemoryAccess();
  const first = await fixture("first", access);
  await first.binding.captureInitial(first.runtime.memory);
  await first.binding.release();
  const acquire = vi
    .spyOn(access, "acquire")
    .mockRejectedValueOnce(new Error("coordination unavailable"));
  await first.binding.domain.updateThread({ id: THREAD, title: "late" });
  acquire.mockRestore();
  const next = await fixture("next", access);
  expect(
    await next.binding.captureInitial(next.runtime.memory),
  ).toBeUndefined();
  await next.binding.release();
  await access.resetAfterQuiescence({ threadId: THREAD, resourceId: RESOURCE });
  const recovered = await fixture("recovered", access);
  expect(
    await recovered.binding.captureInitial(recovered.runtime.memory),
  ).toBeDefined();
  await recovered.binding.release();
});

it("waits within waitMs for a releasing holder instead of invalidating it", async () => {
  const access = createProcessLocalMemoryAccess();
  const selector = { threadId: THREAD, resourceId: RESOURCE };
  const first = await access.acquire(selector);
  setTimeout(() => void first(), 20);
  const second = await access.acquire(selector, { waitMs: 200 });
  expect(await second.verifyEligibility()).toBe(true);
  await second();
});

it("bounds a hung acquisition and releases a lease returned after cancellation", async () => {
  const release = Object.assign(
    vi.fn(async () => {}),
    {
      verifyEligibility: vi.fn(async () => true),
    },
  ) satisfies MastraMemoryLease;
  let resolveAcquire: ((lease: MastraMemoryLease) => void) | undefined;
  const acquire = vi.fn(
    () =>
      new Promise<MastraMemoryLease>((resolve) => {
        resolveAcquire = resolve;
      }),
  );
  const runtime = createMemoryRuntime();
  await seedMemory(runtime);
  const binding = createMemoryCaptureBinding({
    invocationId: "timeout",
    domain: runtime.domain,
    threadId: THREAD,
    resourceId: RESOURCE,
    exclusiveAccess: {
      acquire,
      markUnsafeWrite: async () => {},
      resetAfterQuiescence: async () => {},
    },
    recordMutation: async () => {},
    leaseWaitMs: 20,
  });
  const start = Date.now();
  expect(await binding.captureInitial(runtime.memory)).toBeUndefined();
  expect(Date.now() - start).toBeLessThan(200);
  required(resolveAcquire)(release);
  await vi.waitFor(() => expect(release).toHaveBeenCalledOnce());
  await binding.release();
});

it("bounds a stalled pre-turn source read without changing native storage", async () => {
  const runtime = createMemoryRuntime();
  await seedMemory(runtime);
  const native = runtime.domain.updateThread.bind(runtime.domain);
  const binding = createMemoryCaptureBinding({
    invocationId: "stalled-read",
    domain: runtime.domain,
    threadId: THREAD,
    resourceId: RESOURCE,
    exclusiveAccess: createProcessLocalMemoryAccess(),
    recordMutation: async () => {},
    captureWaitMs: 20,
  });
  const read = vi.spyOn(runtime.domain, "listMessages").mockImplementation(
    async () =>
      new Promise<never>(() => {
        /* A storage read that never settles. */
      }),
  );
  const start = Date.now();
  expect(await binding.captureInitial(runtime.memory)).toBeUndefined();
  expect(Date.now() - start).toBeLessThan(200);
  expect(binding.incompleteReasons.join()).toMatch(/capture timed out/i);
  await binding.release();
  read.mockRestore();
  const updated = await native({ id: THREAD, title: "native still works" });
  expect(updated.title).toBe("native still works");
});

it("does not return a coherent initial snapshot after overlap during capture", async () => {
  const access = createProcessLocalMemoryAccess();
  const one = await fixture("one", access);
  const two = await fixture("two", access);
  const settled = async () => {
    await two.binding.captureInitial(two.runtime.memory);
  };
  expect(await one.binding.captureInitial({ settled })).toBeUndefined();
  await one.binding.release();
  await two.binding.release();
});

it("serializes overlapping native mutations and preserves original storage errors", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  await binding.captureInitial(runtime.memory);
  let unblock: (() => void) | undefined;
  const wait = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  const native = runtime.domain.updateThread.bind(runtime.domain);
  const calls: string[] = [];
  const spy = vi
    .spyOn(runtime.domain, "updateThread")
    .mockImplementation(async (args) => {
      calls.push(required(args.title));
      if (args.title === "first") await wait;
      return native(args);
    });
  const first = binding.domain.updateThread({ id: THREAD, title: "first" });
  const second = binding.domain.updateThread({ id: THREAD, title: "second" });
  await vi.waitFor(() => expect(calls).toEqual(["first"]));
  expect(calls).toEqual(["first"]);
  required(unblock)();
  await Promise.all([first, second]);
  await binding.drain();
  expect(calls).toEqual(["first", "second"]);
  expect(recordMutation.mock.calls).toHaveLength(2);
  spy.mockRestore();
  const fault = new Error("native write failed");
  vi.spyOn(runtime.domain, "saveResource").mockRejectedValueOnce(fault);
  await expect(
    binding.domain.saveResource({
      resource: { id: RESOURCE, createdAt: new Date(), updatedAt: new Date() },
    }),
  ).rejects.toBe(fault);
  expect(binding.revision).toBe(2);
  expect(binding.incompleteReasons.join()).toMatch(/Native memory storage/);
  await binding.release();
});

async function readChunksLikeDatabase(
  runtime: Awaited<ReturnType<typeof fixture>>["runtime"],
  createdAt: string,
) {
  const record = required(
    await runtime.domain.getObservationalMemory(THREAD, RESOURCE),
  );
  await runtime.domain.updateBufferedObservations({
    id: record.id,
    chunk: {
      cycleId: "cycle",
      observations: "buffer",
      tokenCount: 3,
      messageIds: ["historical-message"],
      messageTokens: 20,
      lastObservedAt: new Date("2026-02-01T10:00:00.000Z"),
    },
  });
  const history = runtime.domain.getObservationalMemoryHistory.bind(
    runtime.domain,
  );
  // @mastra/pg and LibSQL JSON.parse this column on every read.
  vi.spyOn(runtime.domain, "getObservationalMemoryHistory").mockImplementation(
    async (...args) =>
      (await history(...args)).map((value) => ({
        ...value,
        bufferedObservationChunks: value.bufferedObservationChunks?.map(
          (chunk) => ({
            ...JSON.parse(JSON.stringify(chunk)),
            createdAt,
          }),
        ),
      })),
  );
}

it("captures buffered chunk dates that a database returns as ISO strings", async () => {
  const { runtime, binding } = await fixture();
  await readChunksLikeDatabase(runtime, "2026-02-01T10:00:01.000Z");
  const initial = await binding.captureInitial(runtime.memory);
  expect(binding.incompleteReasons).toEqual([]);
  const chunk = required(initial?.records[0]?.bufferedObservationChunks?.[0]);
  expect(chunk.createdAt).toEqual(new Date("2026-02-01T10:00:01.000Z"));
  expect(chunk.lastObservedAt).toEqual(new Date("2026-02-01T10:00:00.000Z"));
  await binding.release();
});

it("still refuses buffered chunk dates that are not ISO timestamps", async () => {
  const { runtime, binding } = await fixture();
  await readChunksLikeDatabase(runtime, "Sun Feb 01 2026");
  expect(await binding.captureInitial(runtime.memory)).toBeUndefined();
  expect(binding.incompleteReasons.join()).toMatch(/Initial memory capture/);
  await binding.release();
});

it("records OM flags, buffers, config, activation and working memory with stable request identity", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  await binding.captureInitial(runtime.memory);
  const record = await binding.domain.getObservationalMemory(THREAD, RESOURCE);
  await binding.domain.setBufferingObservationFlag(
    required(record).id,
    true,
    10,
  );
  await binding.domain.updateBufferedObservations({
    id: required(record).id,
    chunk: {
      cycleId: "cycle",
      observations: "buffer",
      tokenCount: 3,
      messageIds: ["historical-message"],
      messageTokens: 20,
      lastObservedAt: new Date(20),
    },
    lastBufferedAtTime: new Date(21),
  });
  await binding.domain.setBufferingObservationFlag(required(record).id, false);
  await binding.domain.updateObservationalMemoryConfig({
    id: required(record).id,
    config: { observation: { messageTokens: 20 } },
  });
  await binding.domain.updateActiveObservations({
    id: required(record).id,
    observations: "changed",
    tokenCount: 3,
    lastObservedAt: new Date(20),
  });
  await binding.domain.updateThread({
    id: THREAD,
    metadata: { workingMemory: "changed" },
  });
  await binding.drain();
  expect(
    recordMutation.mock.calls.map(
      ([event]) => (event as { revision: number }).revision,
    ),
  ).toEqual([1, 2, 3, 4, 5, 6]);
  await binding.release();
});

it("records first-turn OM initialization by model and built-in extractor identity", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  await binding.captureInitial(runtime.memory);
  const observer = runtime.observer.model;
  const reflector = runtime.reflector.model;
  const original = {
    threadId: "first-turn-thread",
    resourceId: RESOURCE,
    scope: "thread" as const,
    config: {
      scope: "thread",
      observation: {
        model: observer,
        extractors: [
          new Extractor(
            { name: "current-task", instructions: "Continue." },
            true,
          ),
        ],
      },
      reflection: { model: reflector },
    },
  };
  const native = await binding.domain.initializeObservationalMemory(original);
  expect((native.config.observation as Record<string, unknown>).model).toBe(
    observer,
  );
  await binding.drain();
  expect(binding.incompleteReasons).toEqual([]);
  const event = recordMutation.mock.calls[0]?.[0] as {
    arguments: unknown[];
    result: Record<string, unknown>;
  };
  expect(event.arguments).toMatchObject([
    {
      config: {
        observation: {
          model: "fixture/observer",
          extractors: [{ mastraBuiltinExtractor: "current-task" }],
        },
        reflection: { model: "fixture/reflector" },
      },
    },
  ]);
  expect(event.result).toMatchObject({
    config: { observation: { model: "fixture/observer" } },
  });
  await binding.release();
});

it("keeps custom OM extractor execution native while refusing incomplete replay evidence", async () => {
  const { runtime, binding } = await fixture();
  await binding.captureInitial(runtime.memory);
  const native = await binding.domain.initializeObservationalMemory({
    threadId: "custom-extractor-thread",
    resourceId: RESOURCE,
    scope: "thread",
    config: {
      observation: {
        model: runtime.observer.model,
        extractors: [
          new Extractor({ name: "custom", instructions: "Extract a value." }),
        ],
      },
      reflection: { model: runtime.reflector.model },
    },
  });
  expect(native.threadId).toBe("custom-extractor-thread");
  expect(binding.incompleteReasons.join()).toMatch(/arguments|result/);
  await binding.release();
});

it("marks credential-altered mutation evidence incomplete but keeps native arguments and results", async () => {
  const { runtime, binding, recordMutation } = await fixture();
  await binding.captureInitial(runtime.memory);
  const result = await binding.domain.updateThread({
    id: THREAD,
    metadata: { apiKey: "private-value" },
  });
  expect(result.metadata?.apiKey).toBe("private-value");
  await binding.drain();
  expect(binding.incompleteReasons.length).toBeGreaterThan(0);
  expect(JSON.stringify(recordMutation.mock.calls)).not.toContain(
    "private-value",
  );
  expect(recordMutation.mock.calls[0]?.[0]).toMatchObject({ complete: false });
  await binding.release();
});

it("rejects same-invocation writes that interleave with initial snapshot reads", async () => {
  const { runtime, binding } = await fixture();
  const native = runtime.domain.getThreadById.bind(runtime.domain);
  vi.spyOn(runtime.domain, "getThreadById").mockImplementationOnce(
    async (args) => {
      const thread = await native(args);
      await binding.domain.updateThread({ id: THREAD, title: "interleaved" });
      return thread;
    },
  );
  expect(await binding.captureInitial(runtime.memory)).toBeUndefined();
  expect(binding.incompleteReasons.join()).toMatch(/overlapped initial/);
  await binding.release();
});
