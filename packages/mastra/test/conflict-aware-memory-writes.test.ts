import { InMemoryMemory, type MemoryStorage } from "@mastra/core/storage";
import { expect, it, vi } from "vitest";
import {
  createMemoryCaptureBinding,
  createProcessLocalMemoryAccess,
  createRegisteredWriteDomain,
  type MastraMemoryMutation,
} from "../src/memory-binding.js";
import {
  createMemoryRuntime,
  RESOURCE,
  seedMemory,
  THREAD,
} from "./helpers/memory-agent.js";

it.each(["commitActiveObservations", "appendBufferedObservations"] as const)(
  "registers %s writes made outside a capture lease",
  async (method) => {
    const runtime = createMemoryRuntime();
    const access = createProcessLocalMemoryAccess();
    const lease = await access.acquire({
      threadId: THREAD,
      resourceId: RESOURCE,
    });
    const result =
      method === "commitActiveObservations"
        ? { applied: false, reason: "conflict" }
        : { persisted: false, recordId: "existing-head" };
    const native = vi.fn(async () => result);
    const domain = createRegisteredWriteDomain(
      Object.assign(runtime.domain, { [method]: native }),
      access,
      () => ({ threadId: THREAD, resourceId: RESOURCE }),
      () => {},
    );
    expect(await Reflect.apply(Reflect.get(domain, method), domain, [{}])).toBe(
      result,
    );
    expect(await lease.verifyEligibility()).toBe(false);
    expect(native).toHaveBeenCalledTimes(1);
    await lease();
  },
);

it.skipIf(!("commitActiveObservations" in InMemoryMemory.prototype))(
  "records native conflict, duplicate and redirected writes once with their receipts",
  async () => {
    const runtime = createMemoryRuntime();
    await seedMemory(runtime);
    const events: MastraMemoryMutation[] = [];
    const binding = createMemoryCaptureBinding({
      domain: runtime.domain,
      exclusiveAccess: createProcessLocalMemoryAccess(),
      invocationId: "native-conflict-writes",
      threadId: THREAD,
      resourceId: RESOURCE,
      recordMutation: async (event) => {
        events.push(event);
      },
    });
    await binding.captureInitial(runtime.memory);
    const domain = binding.domain;
    if (
      !("commitActiveObservations" in domain) ||
      typeof domain.commitActiveObservations !== "function" ||
      !("appendBufferedObservations" in domain) ||
      typeof domain.appendBufferedObservations !== "function"
    )
      throw new Error("Missing conflict-aware storage capability");
    const row = await domain.getObservationalMemory(THREAD, RESOURCE);
    if (!row) throw new Error("Missing fixture record");
    const chunk = {
      cycleId: "native-cycle",
      observations: "BUFFERED",
      tokenCount: 1,
      messageIds: ["historical-message"],
      messageTokens: 3,
      lastObservedAt: new Date("2026-01-02T00:00:00Z"),
    };
    expect(
      await domain.appendBufferedObservations({ id: row.id, chunk }),
    ).toEqual({ persisted: true, recordId: row.id });
    expect(
      await domain.appendBufferedObservations({ id: row.id, chunk }),
    ).toEqual({ persisted: false, recordId: row.id });
    const observation = {
      id: row.id,
      observations: "OBSERVED",
      tokenCount: 1,
      lastObservedAt: chunk.lastObservedAt,
    };
    expect(
      await domain.commitActiveObservations({
        ...observation,
        expectedActiveObservations: "STALE",
      }),
    ).toEqual({ applied: false, reason: "conflict" });
    await domain.updateActiveObservations(observation);
    const successor = await domain.createReflectionGeneration({
      currentRecord: row,
      reflection: "REFLECTED",
      tokenCount: 1,
    });
    expect(await domain.commitActiveObservations(observation)).toEqual({
      applied: false,
      reason: "retired",
    });
    expect(
      await domain.appendBufferedObservations({
        id: row.id,
        chunk: {
          ...chunk,
          cycleId: "redirected-cycle",
          lastObservedAt: new Date("2026-01-03T00:00:00Z"),
        },
      }),
    ).toEqual({ persisted: true, recordId: successor.id });
    await binding.drain();
    expect(events.map(({ method }) => method)).toEqual([
      "appendBufferedObservations",
      "appendBufferedObservations",
      "commitActiveObservations",
      "updateActiveObservations",
      "createReflectionGeneration",
      "commitActiveObservations",
      "appendBufferedObservations",
    ]);
    expect(
      events
        .filter(({ method }) => method === "commitActiveObservations")
        .map(({ result }) => result),
    ).toEqual([
      { applied: false, reason: "conflict" },
      { applied: false, reason: "retired" },
    ]);
    expect(binding.incompleteReasons).toEqual([]);
    await binding.release();
  },
);

it.each([
  ["commitActiveObservations", { applied: true }],
  ["commitActiveObservations", { applied: false, reason: "conflict" }],
  ["commitActiveObservations", { applied: false, reason: "retired" }],
  [
    "appendBufferedObservations",
    { persisted: true, recordId: "existing-head" },
  ],
  [
    "appendBufferedObservations",
    { persisted: false, recordId: "existing-head" },
  ],
] as const)(
  "captures %s receipt %j without changing its native result",
  async (method, result) => {
    const runtime = createMemoryRuntime();
    await seedMemory(runtime);
    const row = await runtime.domain.getObservationalMemory(THREAD, RESOURCE);
    if (!row) throw new Error("Missing fixture record");
    const native = vi.fn(async () => result);
    const domain: MemoryStorage = Object.assign(runtime.domain, {
      [method]: native,
    });
    const events: MastraMemoryMutation[] = [];
    const binding = createMemoryCaptureBinding({
      domain,
      exclusiveAccess: createProcessLocalMemoryAccess(),
      invocationId: "conflict-aware",
      threadId: THREAD,
      resourceId: RESOURCE,
      recordMutation: async (event) => {
        events.push(event);
      },
    });
    await binding.captureInitial(runtime.memory);
    expect(
      await Reflect.apply(Reflect.get(binding.domain, method), binding.domain, [
        { id: row.id },
      ]),
    ).toBe(result);
    await binding.drain();
    expect(events).toMatchObject([
      { method, result, complete: true, revision: 1 },
    ]);
    expect(binding.incompleteReasons).toEqual([]);
    await binding.release();
  },
);

it.each(["commitActiveObservations", "appendBufferedObservations"] as const)(
  "marks %s writes to unknown records incomplete while preserving the native result",
  async (method) => {
    const runtime = createMemoryRuntime();
    await seedMemory(runtime);
    const result =
      method === "commitActiveObservations"
        ? { applied: false, reason: "retired" }
        : { persisted: false, recordId: "foreign-record" };
    Object.assign(runtime.domain, { [method]: vi.fn(async () => result) });
    const binding = createMemoryCaptureBinding({
      domain: runtime.domain,
      exclusiveAccess: createProcessLocalMemoryAccess(),
      invocationId: "foreign-record",
      threadId: THREAD,
      resourceId: RESOURCE,
      recordMutation: async () => {},
    });
    await binding.captureInitial(runtime.memory);
    expect(
      await Reflect.apply(Reflect.get(binding.domain, method), binding.domain, [
        { id: "foreign-record" },
      ]),
    ).toBe(result);
    await binding.drain();
    expect(binding.incompleteReasons.join()).toMatch(
      /outside the captured scope/,
    );
    await binding.release();
  },
);
