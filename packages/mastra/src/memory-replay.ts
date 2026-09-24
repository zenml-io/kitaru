import { createRequire } from "node:module";
import type { MastraModelConfig } from "@mastra/core/llm";
import type { MemoryConfigInternal } from "@mastra/core/memory";
import type { MemoryStorage } from "@mastra/core/storage";
import type { Memory } from "@mastra/memory";
import {
  createMemoryCaptureBinding,
  createProcessLocalMemoryAccess,
  type MastraMemoryCaptureOptions,
} from "./memory-binding.js";
import {
  decodeMemoryValue,
  encodeMemoryValue,
  type MastraMemorySnapshot,
  validateMemorySnapshot,
} from "./memory-snapshot.js";
import type { createOMResultTape } from "./om-result-tape.js";
import type { RecordedClock } from "./replay-clock.js";

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unsupported(message: string): never {
  throw new Error(`Unsupported Mastra memory replay: ${message}`);
}

/** Require the dependency pair exercised by the native memory proof. */
export function assertMemoryReplayVersions(): void {
  const require = createRequire(import.meta.url);
  for (const [name, version] of [
    ["@mastra/core", "1.67.0"],
    ["@mastra/memory", "1.30.0"],
  ]) {
    const metadata: unknown = require(`${name}/package.json`);
    if (!record(metadata) || metadata.version !== version)
      unsupported(`requires ${name}@${version}.`);
  }
}

/** Save a model identity, never the provider client or its credentials. */
export function getMemoryModelId(model: unknown): string {
  if (typeof model === "string" && model.length > 0) return model;
  if (
    record(model) &&
    typeof model.modelId === "string" &&
    typeof model.provider === "string"
  )
    return `${model.provider}/${model.modelId}`;
  if (record(model) && typeof model.id === "string") return model.id;
  return unsupported("Memory models require a static model identity.");
}

function checkConfiguration(config: Record<string, unknown>): void {
  const allowed = new Set([
    "readOnly",
    "lastMessages",
    "semanticRecall",
    "workingMemory",
    "observationalMemory",
    "generateTitle",
    "filterIncompleteToolCalls",
  ]);
  for (const key of Object.keys(config))
    if (!allowed.has(key))
      unsupported(`Memory option '${key}' is not supported.`);
  if (config.semanticRecall !== undefined && config.semanticRecall !== false)
    unsupported("Semantic recall requires external state.");
  if (config.generateTitle !== undefined && config.generateTitle !== false)
    unsupported("Automatic title generation is not supported.");
  for (const key of ["workingMemory", "observationalMemory"]) {
    const feature = config[key];
    if (feature === undefined || feature === false) continue;
    if (!record(feature))
      unsupported(`${key} requires thread-scoped configuration.`);
    if (
      feature.enabled !== false &&
      feature.scope !== undefined &&
      feature.scope !== "thread"
    )
      unsupported(`${key} must use thread scope.`);
  }
}

/** Convert the supported native schema and OM models to self-contained configuration. */
export function serializeMemoryConfiguration(
  config: MemoryConfigInternal,
): Record<string, unknown> {
  checkConfiguration(config);
  const copy: Record<string, unknown> = { ...config };
  if (config.workingMemory?.schema) {
    const { standardSchemaToJSONSchema, toStandardSchema } = createRequire(
      import.meta.url,
    )("@mastra/core/schema") as typeof import("@mastra/core/schema");
    copy.workingMemory = {
      ...config.workingMemory,
      schema: standardSchemaToJSONSchema(
        toStandardSchema(config.workingMemory.schema),
      ),
    };
  }
  if (record(config.observationalMemory)) {
    const om = { ...config.observationalMemory };
    if (om.model !== undefined) om.model = getMemoryModelId(om.model);
    for (const name of ["observation", "reflection"]) {
      if (record(om[name])) {
        const phase = { ...om[name] };
        if (phase.model !== undefined)
          phase.model = getMemoryModelId(phase.model);
        om[name] = phase;
      }
    }
    copy.observationalMemory = om;
  }
  return decodeMemoryValue(encodeMemoryValue(copy)) as Record<string, unknown>;
}

export async function restoreMemoryConfiguration(
  configuration: Record<string, unknown>,
  resolveModel: (id: string) => Promise<MastraModelConfig> | MastraModelConfig,
): Promise<MemoryConfigInternal> {
  const copy = decodeMemoryValue(encodeMemoryValue(configuration));
  if (!record(copy)) return unsupported("Missing native memory configuration.");
  checkConfiguration(copy);
  if (record(copy.observationalMemory)) {
    const om = copy.observationalMemory;
    if (om.model !== undefined) {
      if (typeof om.model !== "string")
        unsupported("Invalid observer model identity.");
      om.model = await resolveModel(om.model);
    }
    for (const name of ["observation", "reflection"]) {
      const phase = om[name];
      if (record(phase) && phase.model !== undefined) {
        if (typeof phase.model !== "string")
          unsupported("Invalid memory model identity.");
        phase.model = await resolveModel(phase.model);
      }
    }
  }
  return copy as MemoryConfigInternal;
}

/** Bind each native OM phase to its own result tape without changing actor calls. */
export async function bindOMResultModels(
  config: MemoryConfigInternal,
  resolveModel: (id: string) => Promise<MastraModelConfig> | MastraModelConfig,
  tape: ReturnType<typeof createOMResultTape>,
): Promise<MemoryConfigInternal> {
  const om = config.observationalMemory;
  if (!om || (record(om) && om.enabled === false)) return config;
  const source = om === true ? {} : (om as Record<string, unknown>);
  const top = source.model;
  const bound: Record<string, unknown> = { ...source, scope: "thread" };
  delete bound.model;
  for (const [name, phase] of [
    ["observation", "observer"],
    ["reflection", "reflector"],
  ] as const) {
    const settings = record(source[name]) ? { ...source[name] } : {};
    const other = source[name === "observation" ? "reflection" : "observation"];
    const modelIdentity =
      settings.model ?? top ?? (record(other) ? other.model : undefined);
    if (modelIdentity === undefined)
      unsupported(
        "OM requires an explicit observer and reflector model identity.",
      );
    const model =
      typeof modelIdentity === "string"
        ? await resolveModel(modelIdentity)
        : modelIdentity;
    if (!record(model) || typeof model.doStream !== "function")
      unsupported("OM model resolution did not return a stream-capable model.");
    settings.model = tape.instrument(model, phase, modelIdentity);
    bound[name] = settings;
  }
  return { ...config, observationalMemory: bound } as MemoryConfigInternal;
}

/**
 * Evaluate a Memory's observational-memory time checks on a recorded clock.
 *
 * Mastra labels observation dates relative to the current date ("today",
 * "2 weeks ago") and activates buffered observations once the last assistant
 * message is older than `activateAfterIdle`. Both read the wall clock, so a
 * replay run days or minutes after its baseline would otherwise send the
 * actor different memory than production did.
 */
export async function pinMemoryClock(
  memory: Pick<Memory, "omEngine">,
  clock: RecordedClock,
): Promise<void> {
  const engine = await memory.omEngine;
  if (!engine) return;
  const buildContext = engine.buildContextSystemMessages.bind(engine);
  // Instance properties shadow the prototype, so Mastra's own `this.` calls
  // and its processors use these versions too.
  engine.buildContextSystemMessages = (opts) =>
    buildContext({ ...opts, currentDate: opts.currentDate ?? clock.now() });
  const activate = engine.activate.bind(engine);
  engine.activate = (opts) => clock.run(() => activate(opts));
}

/**
 * How a source memory store hands out observational-memory records.
 *
 * `in-memory` is Mastra's InMemoryStore, which returns its stored objects.
 * `persistent` covers database stores such as PostgreSQL and LibSQL, which
 * return a fresh copy on every read.
 */
export type MastraMemoryStoreSemantics = "in-memory" | "persistent";

/** Classify a source memory domain by the record semantics it implements. */
export async function getMemoryStoreSemantics(
  domain: MemoryStorage,
): Promise<MastraMemoryStoreSemantics> {
  const { InMemoryMemory } = await import("@mastra/core/storage");
  return domain instanceof InMemoryMemory ? "in-memory" : "persistent";
}

/** Copy plain objects and arrays; Dates and class instances stay shared. */
function copyStoredValue<T>(value: T): T {
  if (Array.isArray(value)) return value.map(copyStoredValue) as T;
  if (
    !record(value) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value))
  )
    return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, copyStoredValue(item)]),
  ) as T;
}

/**
 * Give an InMemoryStore memory domain the record semantics of a database store.
 *
 * Observational memory decides what to observe from the record it read at the
 * start of a step. InMemoryStore returns live objects, so later writes change
 * that record under Mastra; a database returns a copy that stays stale. Its
 * `createReflectionGeneration` also substitutes the current time for a missing
 * `lastObservedAt`, which makes Mastra treat unobserved messages as observed.
 */
function applyPersistentStoreSemantics(
  domain: MemoryStorage,
  selector: Pick<MastraMemorySnapshot, "threadId" | "resourceId">,
): void {
  const read = domain.getObservationalMemory.bind(domain);
  const history = domain.getObservationalMemoryHistory.bind(domain);
  const initialize = domain.initializeObservationalMemory.bind(domain);
  const insert = domain.insertObservationalMemoryRecord.bind(domain);
  const observe = domain.updateActiveObservations.bind(domain);
  const activate = domain.swapBufferedToActive.bind(domain);
  const reflect = domain.createReflectionGeneration.bind(domain);
  // The isolated store holds one thread, and InMemoryStore's history returns
  // the stored objects themselves, so assignments below change the store.
  const getStoredRecord = async (id: string) =>
    (await history(selector.threadId, selector.resourceId)).find(
      (value) => value.id === id,
    );
  // Instance properties shadow the prototype, so InMemoryStore's own internal
  // calls, such as swapBufferedReflectionToActive, also use these versions.
  domain.getObservationalMemory = async (...args) =>
    copyStoredValue(await read(...args));
  domain.getObservationalMemoryHistory = async (...args) =>
    copyStoredValue(await history(...args));
  domain.initializeObservationalMemory = async (input) => {
    const created = await initialize(input);
    const stored = await getStoredRecord(created.id);
    if (stored) stored.metadata = undefined;
    return copyStoredValue({ ...created, metadata: undefined });
  };
  domain.insertObservationalMemoryRecord = (value) =>
    insert(copyStoredValue(value));
  domain.updateActiveObservations = async (input) => {
    await observe(input);
    // A database overwrites the column, so omitted identities become unset.
    if (input.observedMessageIds) return;
    const stored = await getStoredRecord(input.id);
    if (stored) stored.observedMessageIds = undefined;
  };
  domain.swapBufferedToActive = (input) =>
    // A database activates its persisted chunks, never the caller's copy.
    activate({ ...input, bufferedChunks: undefined });
  domain.createReflectionGeneration = async (input) => {
    const created = await reflect(input);
    const stored = await getStoredRecord(created.id);
    const copied = {
      lastObservedAt: input.currentRecord.lastObservedAt,
      metadata: input.currentRecord.metadata,
    };
    if (stored) Object.assign(stored, copied);
    return copyStoredValue({ ...created, ...copied });
  };
}

export interface IsolatedMemoryReplayOptions {
  invocationId: string;
  initialSnapshot: MastraMemorySnapshot;
  /** Defaults to `persistent`, the semantics of database-backed stores. */
  storeSemantics?: MastraMemoryStoreSemantics;
  configuration: Record<string, unknown>;
  resolveModel: (id: string) => Promise<MastraModelConfig> | MastraModelConfig;
  recordMutation: MastraMemoryCaptureOptions["recordMutation"];
  getRequestId?: MastraMemoryCaptureOptions["getRequestId"];
  onIncomplete?: MastraMemoryCaptureOptions["onIncomplete"];
  omTape?: ReturnType<typeof createOMResultTape>;
  /** Fail the replay when its memory work has not settled after this long. */
  finalizationWaitMs: number;
}

/** Restore historical state into a fresh store; no production store is accepted. */
export async function createIsolatedMemoryReplay(
  options: IsolatedMemoryReplayOptions,
) {
  assertMemoryReplayVersions();
  validateMemorySnapshot(options.initialSnapshot);
  const snapshot = decodeMemoryValue(
    encodeMemoryValue(options.initialSnapshot),
  ) as MastraMemorySnapshot;
  let configuration = await restoreMemoryConfiguration(
    options.configuration,
    options.resolveModel,
  );
  if (options.omTape)
    configuration = await bindOMResultModels(
      configuration,
      options.resolveModel,
      options.omTape,
    );
  const { Memory } = await import("@mastra/memory");
  const { InMemoryStore, MastraCompositeStore } = await import(
    "@mastra/core/storage"
  );
  const store = new InMemoryStore();
  const domain = store.stores.memory;
  if (!domain) return unsupported("Native in-memory storage is unavailable.");
  if (options.storeSemantics !== "in-memory")
    applyPersistentStoreSemantics(domain, snapshot);
  try {
    if (snapshot.thread)
      await domain.saveThread({ thread: structuredClone(snapshot.thread) });
    if (snapshot.resource)
      await domain.saveResource({ resource: snapshot.resource });
    if (snapshot.messages.length)
      await domain.saveMessages({ messages: snapshot.messages });
    // Saving messages updates thread metadata, including updatedAt.
    if (snapshot.thread)
      await domain.saveThread({ thread: structuredClone(snapshot.thread) });
    for (const value of snapshot.records)
      await domain.insertObservationalMemoryRecord(value);
    const binding = createMemoryCaptureBinding({
      invocationId: options.invocationId,
      threadId: snapshot.threadId,
      resourceId: snapshot.resourceId,
      domain,
      exclusiveAccess: createProcessLocalMemoryAccess(),
      recordMutation: options.recordMutation,
      getRequestId: options.getRequestId,
      onIncomplete: options.onIncomplete,
    });
    const storage = new MastraCompositeStore({
      id: `kitaru-replay-${options.invocationId}`,
      domains: { memory: binding.domain },
    });
    const memory = new Memory({ storage, options: configuration });
    const initialSnapshot = await binding.captureInitial(memory);
    if (!initialSnapshot) {
      await binding.release();
      return unsupported(
        "Restored memory did not produce a coherent initial snapshot.",
      );
    }
    let finished: Promise<boolean> | undefined;
    return {
      memory,
      binding,
      initialSnapshot,
      finish(): Promise<boolean> {
        finished ??= (async () => {
          try {
            // A buffering operation that another turn left hung on the same
            // thread would otherwise stall this replay indefinitely.
            const settled = await binding.settle(
              memory,
              options.finalizationWaitMs,
            );
            if (!settled)
              binding.markIncomplete(
                "Observational-memory work did not settle before the finalization deadline.",
              );
            return settled;
          } finally {
            await binding.release();
            await store.close();
          }
        })();
        return finished;
      },
      async release(): Promise<void> {
        await finished;
      },
    };
  } catch (error) {
    await store.close();
    throw error;
  }
}
