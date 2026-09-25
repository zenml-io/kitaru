import { createHash } from "node:crypto";
import type { MemoryStorage } from "@mastra/core/storage";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  boundMastraReplayEvidence,
  type MastraReplayEvidence,
} from "@zenml-io/kitaru/adapter";
import {
  decodeMemoryValue,
  encodeMemoryEvidence,
  encodeMemoryValue,
  type MastraMemorySnapshot,
  normalizeStoredMemoryDates,
  validateMemorySnapshot,
} from "./memory-snapshot.js";
import { getNativeOMModel } from "./om-result-tape.js";
import {
  describeReplayFailure,
  getReplayReason,
  type MastraReplayReason,
  MastraReplayReasonError,
} from "./replay-reasons.js";
import { collectFileNetworkUrls } from "./stateful-files.js";

export interface MastraMemorySelector {
  threadId: string;
  resourceId: string;
}

export interface MastraMemoryLeaseOptions {
  /**
   * Bound acquisition so a live answer does not wait for a delayed reflection.
   * Zero means do not wait: invalidate any current holder at once.
   */
  waitMs?: number;
  /** Cancel a waiting acquisition without releasing another writer's lease. */
  signal?: AbortSignal;
  /** An advisory notification; verifyEligibility is the authoritative check. */
  onConflict?: () => void;
  /**
   * The caller is a Kitaru turn, or a write it registers, that gives up its
   * own replay eligibility while it overlaps a finalizing holder. Such an
   * overlap leaves that holder eligible.
   */
  cooperative?: boolean;
}

/** A callable release keeps existing direct lease users source compatible. */
export interface MastraMemoryLease {
  (): Promise<void>;
  /** Read the shared coordination state, including conflict and lease loss. */
  verifyEligibility(): Promise<boolean>;
  /**
   * Record that the holder's native answer has finished and only its
   * background memory work remains. Optional: without it, every overlap
   * invalidates the holder.
   */
  markFinalizing?(): Promise<void>;
  /**
   * True when a cooperative acquisition overlapped a finalizing holder. The
   * lease is then not eligible, and it did not invalidate that holder.
   */
  readonly overlapsFinalizingTurn?: boolean;
}

/**
 * Coordinate every writer of a source thread or resource across all processes.
 *
 * `acquire` atomically reserves both selectors. When either selector is still
 * held after `waitMs`, the implementation invalidates every current holder and
 * returns a lease that is not eligible but still occupies both selectors until
 * it is released. That invalidation ends once every overlapping lease has been
 * released. Kitaru holds a turn's lease until the turn's memory writes,
 * including delayed observational-memory work, have settled or reached their
 * finalization deadline, and until the final eligibility check has passed.
 * A write Kitaru makes outside that lease registers through
 * `acquire(selector, { waitMs: 0 })` and releases immediately afterwards.
 *
 * One overlap does not invalidate the holder. Once the holder's native answer
 * has finished, Kitaru calls `markFinalizing()`: from then on only the
 * holder's own background memory work (buffered observation or reflection)
 * writes under that lease, and its recorded evidence does not depend on later
 * turns. A Kitaru turn that starts in that window, such as a quick reply,
 * acquires with `cooperative: true`. When every holder still in the way is
 * either finalizing or already ineligible, and at least one eligible holder
 * is finalizing, the implementation must not invalidate them. It returns a
 * lease that is not eligible, has `overlapsFinalizingTurn` set, and occupies
 * both selectors until it is released. Writes that turn registers use
 * `cooperative: true` as well. Any other overlap, including a non-cooperative
 * acquisition while a holder is finalizing, invalidates every holder as above.
 * An implementation without `markFinalizing` keeps the stricter behavior.
 *
 * Kitaru never renews a lease. A shared implementation must still end a lease
 * whose holder process died without releasing it, through a time-to-live
 * longer than the longest turn plus its finalization wait, or a liveness check
 * of the holder. `verifyEligibility` must return false once the lease has
 * expired, so a turn that outlives its lease is ineligible rather than unsafe.
 *
 * `markUnsafeWrite` is only for a write that could not register: its selector
 * is unknown or coordination failed. That marker must survive process loss and
 * keep both selectors ineligible until `resetAfterQuiescence`. Kitaru never
 * calls `resetAfterQuiescence`; the application calls it after every process
 * that could have written without registering has stopped or restarted. A
 * timeout or failed coordination call must fail closed for replay eligibility,
 * while native Mastra storage writes still run.
 *
 * The process-local helper below is valid only when every writer shares one
 * instance in one process. A production multi-server application must provide
 * its own implementation backed by shared atomic storage.
 */
export interface MastraExclusiveMemoryAccess {
  acquire(
    selector: MastraMemorySelector,
    options?: MastraMemoryLeaseOptions,
  ): Promise<MastraMemoryLease>;
  /** Persist an unsafe-write marker before an unregistered native write proceeds.
   * An unknown selector poisons every thread until global quiescence is proven.
   */
  markUnsafeWrite(selector?: MastraMemorySelector): Promise<void>;
  /** Clear a persistent loss marker only after all writers are proven stopped. */
  resetAfterQuiescence(selector?: MastraMemorySelector): Promise<void>;
}

/**
 * A real process-local lease. Use only when every writer runs in this process
 * and shares this instance; distributed writers require a distributed lease.
 */
export function createProcessLocalMemoryAccess(): MastraExclusiveMemoryAccess {
  type Turn = {
    onConflict?: () => void;
    invalidated: boolean;
    finalizing: boolean;
  };
  type ScopeState = {
    turns: Set<Turn>;
    poisoned: boolean;
    persistentLoss: boolean;
  };
  const scopes = new Map<string, ScopeState>();
  const releaseWaiters = new Set<() => void>();
  let unknownWriterPoisoned = false;

  function keys({ threadId, resourceId }: MastraMemorySelector): string[] {
    return [`thread:${threadId}`, `resource:${resourceId}`];
  }

  function getState(key: string): ScopeState {
    let state = scopes.get(key);
    if (!state) {
      state = {
        turns: new Set<Turn>(),
        poisoned: false,
        persistentLoss: false,
      };
      scopes.set(key, state);
    }
    return state;
  }

  function poison(states: readonly ScopeState[], persistent: boolean): void {
    const affected = new Set<Turn>();
    for (const state of states) {
      state.poisoned = true;
      if (persistent) state.persistentLoss = true;
      for (const turn of state.turns) affected.add(turn);
    }
    for (const turn of affected) {
      turn.invalidated = true;
      turn.onConflict?.();
    }
  }

  function isOccupied(scopeKeys: readonly string[]): boolean {
    return scopeKeys.some((key) => (scopes.get(key)?.turns.size ?? 0) > 0);
  }

  async function waitForRelease(
    scopeKeys: readonly string[],
    waitMs: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const deadline = Date.now() + waitMs;
    while (isOccupied(scopeKeys) && !signal?.aborted) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) return;
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          releaseWaiters.delete(done);
          signal?.removeEventListener("abort", done);
          resolve();
        };
        const timer = setTimeout(done, remaining);
        releaseWaiters.add(done);
        signal?.addEventListener("abort", done, { once: true });
      });
    }
  }

  return {
    async acquire(selector, options = {}) {
      const scopeKeys = keys(selector);
      await waitForRelease(scopeKeys, options.waitMs ?? 0, options.signal);
      if (options.signal?.aborted)
        throw new Error("Exclusive source-thread ownership was cancelled.");
      const states = scopeKeys.map(getState);
      const occupied = states.filter((state) => state.turns.size > 0);
      const holders = new Set(occupied.flatMap((state) => [...state.turns]));
      const followsFinalizing =
        options.cooperative === true &&
        [...holders].every((turn) => turn.finalizing || turn.invalidated) &&
        [...holders].some((turn) => turn.finalizing && !turn.invalidated);
      if (occupied.length > 0 && !followsFinalizing) poison(occupied, false);
      const owner: Turn = {
        onConflict: options.onConflict,
        invalidated:
          followsFinalizing ||
          states.some((state) => state.poisoned || state.persistentLoss) ||
          unknownWriterPoisoned,
        finalizing: false,
      };
      for (const state of states) state.turns.add(owner);
      let released = false;
      const release = async () => {
        if (!released) for (const state of states) state.turns.delete(owner);
        released = true;
        for (const key of scopeKeys) {
          const state = scopes.get(key);
          if (state && state.turns.size === 0 && !state.persistentLoss)
            scopes.delete(key);
        }
        for (const notify of [...releaseWaiters]) notify();
      };
      return Object.assign(release, {
        async verifyEligibility() {
          return (
            !released &&
            !owner.invalidated &&
            states.every((state) => !state.poisoned && !state.persistentLoss) &&
            !unknownWriterPoisoned
          );
        },
        async markFinalizing() {
          owner.finalizing = true;
        },
        overlapsFinalizingTurn: followsFinalizing,
      });
    },
    async markUnsafeWrite(selector) {
      if (!selector) {
        unknownWriterPoisoned = true;
        poison([...scopes.values()], true);
        return;
      }
      // The unsafe writer may outlive every current lease. Do not let the
      // final release erase the conflict before that writer quiesces.
      poison(keys(selector).map(getState), true);
    },
    async resetAfterQuiescence(selector) {
      if (!selector) {
        if ([...scopes.values()].some((state) => state.turns.size > 0))
          throw new Error("Source-thread writers are still active.");
        scopes.clear();
        unknownWriterPoisoned = false;
        return;
      }
      const scopeKeys = keys(selector);
      if (scopeKeys.some((key) => (scopes.get(key)?.turns.size ?? 0) > 0))
        throw new Error("Source-thread or resource writers are still active.");
      for (const key of scopeKeys) scopes.delete(key);
    },
  };
}

export interface MastraMemoryMutation {
  id: string;
  invocationId: string;
  revision: number;
  /** False when the arguments or result could not be recorded safely. */
  complete: boolean;
  method: string;
  arguments: JsonValue;
  result: JsonValue;
  requestId?: string;
  /** Why size bounds truncated this evidence; the storage call itself succeeded. */
  truncationReasons?: string[];
}

export interface MastraMemoryCaptureOptions extends MastraMemorySelector {
  invocationId: string;
  domain: MemoryStorage;
  exclusiveAccess: MastraExclusiveMemoryAccess;
  recordMutation: (event: MastraMemoryMutation) => Promise<void>;
  getRequestId?: () => string | undefined;
  onIncomplete?: (reason: string) => void;
  /**
   * Replace captured file URLs and redact URL credentials in evidence and in
   * the initial snapshot, without changing native reads or writes.
   */
  sanitizeEvidence?: <T>(value: T) => T;
  /**
   * Accept the file URLs that `sanitizeEvidence` leaves in thread history
   * instead of making the turn ineligible. With `sanitizeEvidence`, the
   * binding then keeps an unsanitized copy of the snapshot for
   * `sanitizeInitialAgain`.
   */
  acceptHistoryFileUrls?: (urls: readonly string[]) => void;
  leaseWaitMs?: number;
  leaseSignal?: AbortSignal;
  /** Bound pre-turn storage reads so capture cannot stall a native answer. */
  captureWaitMs?: number;
}

export interface MastraMemoryCaptureBinding {
  /** Supply this public domain to the invocation's native Memory storage. */
  domain: MemoryStorage;
  readonly revision: number;
  readonly incompleteReasons: readonly string[];
  /** The reason code of the first problem that made the recording incomplete. */
  readonly incompleteReason: MastraReplayReason | undefined;
  captureInitial(memory: {
    settled(): Promise<void>;
  }): Promise<MastraMemorySnapshot | undefined>;
  /**
   * Sanitize the initial snapshot again with the files captured since, or
   * return undefined when the snapshot held no accepted history file URL.
   */
  sanitizeInitialAgain(): MastraMemorySnapshot | undefined;
  /** Record why the invocation's evidence is incomplete; the first reason code wins. */
  markIncomplete(message: string, reason?: MastraReplayReason): void;
  /** Wait for every storage write and its evidence upload to finish. */
  drain(): Promise<void>;
  /**
   * Join the invocation's memory work, including buffered observation and
   * reflection, until its storage writes settle. Without `waitMs` this waits
   * until the work settles; with it, resolve false once `waitMs` passes first
   * and stop joining further rounds. A failed join rejects either way.
   */
  settle(memory: MastraSettlingMemory, waitMs?: number): Promise<boolean>;
  /** Check shared ownership immediately before persisting eligible inputs. */
  verifyEligibility(): Promise<void>;
  /**
   * Tell the lease that the native answer has finished, so a cooperative
   * later turn can overlap the remaining memory work without invalidating
   * this invocation. Coordination failures are ignored.
   */
  beginFinalization(): Promise<void>;
  /**
   * Release ownership once storage writes have settled. Evidence uploads can
   * still be running; `drain()` before reading `incompleteReasons`.
   */
  release(): Promise<void>;
}

/** A Memory whose background observational-memory work can be joined. */
export interface MastraSettlingMemory {
  settled(): Promise<void>;
  readonly omEngine: Promise<{
    waitForBuffering(
      threadId: string,
      resourceId: string,
      timeoutMs?: number,
    ): Promise<void>;
  } | null>;
}

const BUFFERING_WAIT_MS = 30_000;
const ACQUIRE_RESPONSE_MARGIN_MS = 25;

async function boundedCoordination<T>(
  operation: Promise<T>,
  waitMs: number,
  timeoutMessage = "Source-thread coordination timed out.",
  timeoutReason: MastraReplayReason = "memory_lease_unavailable",
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () =>
            reject(new MastraReplayReasonError(timeoutMessage, timeoutReason)),
          waitMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Acquire within `boundWaitMs`, even from a backend that ignores cancellation.
 *
 * A lease that arrives after the caller has given up is released at once so it
 * cannot keep the selectors occupied.
 */
async function acquireWithin(
  access: MastraExclusiveMemoryAccess,
  selector: MastraMemorySelector,
  options: MastraMemoryLeaseOptions,
  boundWaitMs: number,
  onLateReleaseFailure: () => void,
): Promise<MastraMemoryLease> {
  const timeout = AbortSignal.timeout(boundWaitMs);
  const signal = options.signal
    ? AbortSignal.any([timeout, options.signal])
    : timeout;
  const attempted = access.acquire(selector, { ...options, signal });
  let accepted = false;
  void attempted.then(
    async (lateLease) => {
      if (accepted || !signal.aborted) return;
      try {
        await lateLease();
      } catch {
        onLateReleaseFailure();
      }
    },
    () => undefined,
  );
  const lease = await Promise.race([
    attempted,
    new Promise<never>((_resolve, reject) => {
      if (signal.aborted) reject(signal.reason);
      else
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
    }),
  ]);
  accepted = true;
  return lease;
}

/**
 * Register storage writes that have no eligible lease behind them.
 *
 * Each registration overlaps any current holder, so that turn becomes
 * ineligible, and it ends with its write. Only a write that cannot register
 * leaves a persistent marker; an unknown selector cannot register at all. That
 * marker already covers every later write, so later writes skip coordination
 * instead of delaying the native call again.
 */
function createWriteRegistrar(
  access: MastraExclusiveMemoryAccess,
  getSelector: () => MastraMemorySelector | undefined,
  waitMs: number,
  onFailure: (reason: string) => void,
  isCooperative: () => boolean = () => false,
): () => Promise<MastraMemoryLease | undefined> {
  let markedUnsafe = false;
  return async () => {
    if (markedUnsafe) return undefined;
    const selector = getSelector();
    if (selector) {
      try {
        return await acquireWithin(
          access,
          selector,
          { waitMs: 0, cooperative: isCooperative() },
          waitMs,
          () => onFailure("Late source-thread lease release failed."),
        );
      } catch {
        // Fall through to the persistent marker below.
      }
    }
    markedUnsafe = true;
    try {
      await boundedCoordination(access.markUnsafeWrite(selector), waitMs);
    } catch {
      onFailure("Unsafe memory write could not be fenced.");
    }
    return undefined;
  };
}

async function releaseRegistration(
  registration: MastraMemoryLease | undefined,
  waitMs: number,
  onFailure: (reason: string) => void,
): Promise<void> {
  if (!registration) return;
  try {
    await boundedCoordination(registration(), waitMs);
  } catch {
    onFailure("Source-thread write registration release failed.");
  }
}

/**
 * Register each native write to a source domain for the duration of the write.
 *
 * `getSelector` is read at write time, so a caller can resolve its selector
 * after constructing the Memory that owns this domain. Coordination failures
 * are reported and never fail the native write.
 */
export function createRegisteredWriteDomain(
  domain: MemoryStorage,
  access: MastraExclusiveMemoryAccess,
  getSelector: () => MastraMemorySelector | undefined,
  onFailure: (reason: string) => void,
  waitMs = 100,
): MemoryStorage {
  const methods = new Map<PropertyKey, unknown>();
  const registerWrite = createWriteRegistrar(
    access,
    getSelector,
    waitMs,
    onFailure,
  );
  return new Proxy(domain, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      if (methods.has(property)) return methods.get(property);
      const bound = MUTATIONS.has(property as keyof MemoryStorage)
        ? async (...args: unknown[]): Promise<unknown> => {
            const registration = await registerWrite();
            try {
              return await Reflect.apply(value, target, args);
            } finally {
              await releaseRegistration(registration, waitMs, onFailure);
            }
          }
        : value.bind(target);
      methods.set(property, bound);
      return bound;
    },
  });
}

// The pinned public MemoryStorage mutation inventory. Delegation binds `this` to
// the original domain, so a native method's own helper calls record only once.
const MUTATIONS = new Set<keyof MemoryStorage>([
  "dangerouslyClearAll",
  "prune",
  "saveThread",
  "updateThread",
  "patchThread",
  "deleteThread",
  "saveMessages",
  "updateMessages",
  "deleteMessages",
  "copyThread",
  "cloneThread",
  "updateThreadResourceId",
  "saveResource",
  "updateResource",
  "initializeObservationalMemory",
  "updateActiveObservations",
  "updateBufferedObservations",
  "swapBufferedToActive",
  "createReflectionGeneration",
  "updateBufferedReflection",
  "swapBufferedReflectionToActive",
  "setReflectingFlag",
  "setObservingFlag",
  "setBufferingObservationFlag",
  "setBufferingReflectionFlag",
  "insertObservationalMemoryRecord",
  "clearObservationalMemory",
  "setPendingMessageTokens",
  "updateObservationalMemoryConfig",
]);

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function modelIdentity(model: unknown): string {
  if (typeof model === "string" && model) return model;
  if (
    record(model) &&
    typeof model.modelId === "string" &&
    typeof model.provider === "string"
  )
    return `${model.provider}/${model.modelId}`;
  if (record(model) && typeof model.id === "string") return model.id;
  throw new MastraReplayReasonError(
    "Observational-memory model has no stable identity.",
    "om_config_unsupported",
  );
}

const BUILTIN_EXTRACTORS = new Set([
  "current-task",
  "suggested-response",
  "thread-title",
]);

function extractorIdentity(extractor: unknown): {
  mastraBuiltinExtractor: string;
} {
  if (
    record(extractor) &&
    typeof extractor.mastraBuiltinExtractor === "string" &&
    BUILTIN_EXTRACTORS.has(extractor.mastraBuiltinExtractor)
  )
    return { mastraBuiltinExtractor: extractor.mastraBuiltinExtractor };
  if (
    record(extractor) &&
    extractor.internal === true &&
    typeof extractor.slug === "string" &&
    BUILTIN_EXTRACTORS.has(extractor.slug)
  )
    return { mastraBuiltinExtractor: extractor.slug };
  throw new MastraReplayReasonError(
    "Unsupported observational-memory extractor.",
    "om_config_unsupported",
  );
}

/** Replace OM models with identities and built-in extractors with their slugs. */
function projectOMConfig(
  config: Record<string, unknown>,
): Record<string, unknown> {
  const projected = { ...config };
  if (projected.model !== undefined)
    projected.model = modelIdentity(projected.model);
  for (const name of ["observation", "reflection"]) {
    const phase = projected[name];
    if (!record(phase)) continue;
    const copy = { ...phase };
    if (copy.model !== undefined) copy.model = modelIdentity(copy.model);
    if (Array.isArray(copy.extractors))
      copy.extractors = copy.extractors.map(extractorIdentity);
    projected[name] = copy;
  }
  return projected;
}

/**
 * Project the OM configuration held by stored records.
 *
 * Mastra keeps its resolved OM configuration, including model and Extractor
 * objects, in every record. Stores that keep objects in memory return them
 * as they are, and those objects have no stable encoding.
 */
function projectOMRecords(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(projectOMRecords);
  if (!record(value)) return value;
  const projected = { ...value };
  if (record(value.config)) projected.config = projectOMConfig(value.config);
  if (record(value.currentRecord))
    projected.currentRecord = projectOMRecords(value.currentRecord);
  return projected;
}

const OM_RECORD_METHODS = new Set<PropertyKey>([
  "initializeObservationalMemory",
  "insertObservationalMemoryRecord",
  "createReflectionGeneration",
  "swapBufferedReflectionToActive",
]);

/** Keep provider clients out of evidence without changing native storage calls. */
function mutationEvidenceValue(method: PropertyKey, value: unknown): unknown {
  return OM_RECORD_METHODS.has(method) ? projectOMRecords(value) : value;
}

function canonicalJson(value: JsonValue): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key] ?? null)}`)
    .join(",")}}`;
}

/**
 * Replace saved messages that repeat an argument message with a reference.
 *
 * Storage returns the messages it just saved, so recording both sides stores
 * every new message, including large tool results, twice. A returned message
 * that differs from its argument stays in full.
 */
function referenceSavedMessages(
  encodedArguments: JsonValue,
  encodedResult: JsonValue,
): JsonValue {
  const [input] = Array.isArray(encodedArguments) ? encodedArguments : [];
  if (
    !record(input) ||
    !Array.isArray(input.messages) ||
    !record(encodedResult) ||
    !Array.isArray(encodedResult.messages)
  )
    return encodedResult;
  const saved = new Map<string, string>();
  for (const message of input.messages)
    if (record(message) && typeof message.id === "string")
      saved.set(message.id, canonicalJson(message));
  return {
    ...encodedResult,
    messages: encodedResult.messages.map((message) => {
      if (!record(message) || typeof message.id !== "string") return message;
      const canonical = canonicalJson(message);
      if (saved.get(message.id) !== canonical) return message;
      return {
        savedMessageRef: {
          id: message.id,
          sha256: createHash("sha256").update(canonical).digest("hex"),
        },
      };
    }),
  };
}

/**
 * Give the source store the OM models from the source configuration.
 *
 * Mastra persists its OM configuration into the record it initializes. Tape
 * instrumented models must not reach production rows, where they would add
 * Kitaru objects or a serialized provider client.
 */
function nativeStorageArguments(
  method: PropertyKey,
  args: unknown[],
): unknown[] {
  if (method !== "initializeObservationalMemory") return args;
  return args.map((input) => {
    if (!record(input) || !record(input.config)) return input;
    let changed = false;
    const config = { ...input.config };
    for (const name of ["observation", "reflection"]) {
      const phase = config[name];
      if (!record(phase)) continue;
      const native = getNativeOMModel(phase.model);
      if (native === phase.model) continue;
      config[name] = { ...phase, model: native };
      changed = true;
    }
    return changed ? { ...input, config } : input;
  });
}

/** Capture one native invocation without changing the shared source domain or Agent. */
export function createMemoryCaptureBinding(
  options: MastraMemoryCaptureOptions,
): MastraMemoryCaptureBinding {
  let revision = 0;
  let started = false;
  let capturing = false;
  let readingSnapshot = false;
  let released = false;
  let lease: MastraMemoryLease | undefined;
  let mutations = Promise.resolve();
  let evidence = Promise.resolve();
  const reasons: string[] = [];
  let firstReason: MastraReplayReason | undefined;
  const methods = new Map<PropertyKey, unknown>();
  const waitMs = options.leaseWaitMs ?? 100;
  const selector = {
    threadId: options.threadId,
    resourceId: options.resourceId,
  };
  let settling: Promise<void> | undefined;
  // A hung buffering operation stays in Mastra's process-wide map; stop
  // polling it once a bounded caller has given up on this invocation.
  let joinAbandoned = false;
  const markLeaseUnavailable = (message: string) =>
    markIncomplete(message, "memory_lease_unavailable");
  // Only writes made under a lease that followed a finalizing earlier turn
  // may leave that turn eligible; this invocation is ineligible itself then.
  // A write after release belongs to no lease and invalidates every holder.
  const registerWrite = createWriteRegistrar(
    options.exclusiveAccess,
    () => selector,
    waitMs,
    markLeaseUnavailable,
    () => !released && lease?.overlapsFinalizingTurn === true,
  );

  function markIncomplete(
    message: string,
    reason: MastraReplayReason = "memory_evidence_incomplete",
  ): void {
    if (reasons.includes(message)) return;
    reasons.push(message);
    firstReason ??= reason;
    try {
      options.onIncomplete?.(message);
    } catch {
      /* Diagnostics must not affect native calls. */
    }
  }

  async function holdsEligibleLease(): Promise<boolean> {
    if (released || !lease) {
      markIncomplete(
        "Memory mutation occurred without source-thread ownership.",
        "memory_lease_conflict",
      );
      return false;
    }
    try {
      if (await boundedCoordination(lease.verifyEligibility(), waitMs))
        return true;
      markIncomplete(
        "Exclusive source-thread ownership was lost.",
        "memory_lease_conflict",
      );
    } catch {
      markLeaseUnavailable(
        "Exclusive source-thread ownership could not be verified.",
      );
    }
    return false;
  }

  const domain = new Proxy(options.domain, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      if (methods.has(property)) return methods.get(property);
      if (!MUTATIONS.has(property as keyof MemoryStorage)) {
        const bound = value.bind(target);
        methods.set(property, bound);
        return bound;
      }
      const bound = (...callerArgs: unknown[]): Promise<unknown> => {
        const args = nativeStorageArguments(property, callerArgs);
        let encodedArguments: JsonValue = null;
        let complete = true;
        let requestId: string | undefined;
        const truncationReasons: string[] = [];
        const keep = (evidence: MastraReplayEvidence): JsonValue => {
          if (evidence.lossReason) truncationReasons.push(evidence.lossReason);
          return evidence.value;
        };
        try {
          const evidence = mutationEvidenceValue(property, args);
          encodedArguments = keep(
            encodeMemoryEvidence(
              options.sanitizeEvidence?.(evidence) ?? evidence,
              "Memory mutation arguments",
            ),
          );
          requestId = options.getRequestId?.();
        } catch (error) {
          complete = false;
          markIncomplete(
            describeReplayFailure(
              error,
              "Memory mutation arguments or request attribution could not be recorded safely.",
            ),
            getReplayReason(error, "recorded_evidence_unsupported"),
          );
        }
        if (!started || released)
          markIncomplete(
            "Memory mutation occurred outside the owned invocation lifecycle.",
            "memory_lease_conflict",
          );
        if (readingSnapshot)
          markIncomplete(
            "Memory mutation overlapped initial snapshot reads.",
            "memory_lease_conflict",
          );
        if (property === "dangerouslyClearAll" || property === "prune")
          markIncomplete(
            "Storage-wide mutation is outside the captured thread scope.",
          );
        const duringCapture = capturing;
        const result = mutations.then(async () => {
          let output: unknown;
          let registration: MastraMemoryLease | undefined;
          try {
            if (!(await holdsEligibleLease()))
              registration = await registerWrite();
            output = await Reflect.apply(value, target, args);
          } catch (error) {
            markIncomplete(
              "Native memory storage mutation failed.",
              "memory_mutation_failed",
            );
            throw error;
          } finally {
            await releaseRegistration(
              registration,
              waitMs,
              markLeaseUnavailable,
            );
          }
          // Joined work from a previous turn belongs to the initial snapshot.
          if (duringCapture) return output;
          revision += 1;
          let encodedResult: JsonValue = null;
          try {
            const evidence = mutationEvidenceValue(property, output);
            encodedResult = keep(
              encodeMemoryEvidence(
                options.sanitizeEvidence?.(evidence) ?? evidence,
                "Memory mutation result",
              ),
            );
            if (property === "saveMessages")
              encodedResult = referenceSavedMessages(
                encodedArguments,
                encodedResult,
              );
          } catch (error) {
            complete = false;
            markIncomplete(
              describeReplayFailure(
                error,
                "Memory mutation result could not be recorded safely.",
              ),
              getReplayReason(error, "recorded_evidence_unsupported"),
            );
          }
          // Each side fits the replay budget on its own; the node carries both.
          const combined = boundMastraReplayEvidence(
            { arguments: encodedArguments, result: encodedResult },
            "Memory mutation evidence",
          );
          if (combined.lossReason) {
            truncationReasons.push(combined.lossReason);
            const bounded =
              record(combined.value) &&
              Object.hasOwn(combined.value, "arguments")
                ? combined.value
                : undefined;
            encodedArguments = bounded
              ? (bounded.arguments ?? null)
              : combined.value;
            encodedResult = bounded ? (bounded.result ?? null) : null;
          }
          // Size truncation loses diagnostic detail only: replay rebuilds
          // memory from the initial snapshot, never from these events.
          const event: MastraMemoryMutation = {
            id: `${options.invocationId}:memory:${revision}`,
            invocationId: options.invocationId,
            revision,
            complete,
            method: String(property),
            arguments: encodedArguments,
            result: encodedResult,
            ...(requestId === undefined ? {} : { requestId }),
            ...(truncationReasons.length ? { truncationReasons } : {}),
          };
          evidence = evidence.then(async () => {
            try {
              await options.recordMutation(event);
            } catch {
              markIncomplete(
                "Memory mutation evidence persistence failed.",
                "recording_evidence_failed",
              );
            }
          });
          return output;
        });
        mutations = result.then(
          () => {},
          () => {},
        );
        return result;
      };
      methods.set(property, bound);
      return bound;
    },
  });

  async function settleMutations(): Promise<void> {
    while (true) {
      const current = mutations;
      await current;
      if (current === mutations) return;
    }
  }

  async function drain(): Promise<void> {
    // Evidence can grow while a storage operation settles; follow both tails.
    while (true) {
      const currentMutations = mutations;
      await currentMutations;
      const currentEvidence = evidence;
      await currentEvidence;
      if (currentMutations === mutations && currentEvidence === evidence)
        return;
    }
  }

  async function joinMemoryWork(memory: MastraSettlingMemory): Promise<void> {
    const engine = await memory.omEngine;
    // Mastra's settled() does not join a buffered reflection, and settled
    // work can start more buffering. Repeat until a round records no write.
    // Evidence uploads do not change storage, so they are not joined here.
    while (!joinAbandoned) {
      const before = revision;
      await memory.settled();
      const waitStarted = Date.now();
      await engine?.waitForBuffering(
        options.threadId,
        options.resourceId,
        BUFFERING_WAIT_MS,
      );
      await settleMutations();
      // waitForBuffering resolves, rather than rejects, when it times out.
      if (Date.now() - waitStarted >= BUFFERING_WAIT_MS) continue;
      if (revision === before) return;
    }
  }

  async function verifyEligibility(): Promise<void> {
    if (released || !lease) {
      markLeaseUnavailable("Exclusive source-thread ownership is unavailable.");
      return;
    }
    try {
      if (!(await boundedCoordination(lease.verifyEligibility(), waitMs)))
        markIncomplete(
          "Exclusive source-thread ownership was lost.",
          "memory_lease_conflict",
        );
    } catch {
      markLeaseUnavailable(
        "Exclusive source-thread ownership could not be verified.",
      );
    }
  }

  async function readInitialState() {
    const thread = await options.domain.getThreadById({
      threadId: options.threadId,
    });
    const resource = await options.domain.getResourceById({
      resourceId: options.resourceId,
    });
    const { messages } = await options.domain.listMessages({
      threadId: options.threadId,
      perPage: false,
    });
    const records = await options.domain.getObservationalMemoryHistory(
      options.threadId,
      options.resourceId,
    );
    return { thread, resource, messages, records };
  }

  let unsanitizedSnapshot: unknown;
  /** Copy a snapshot so no storage-owned objects or Dates escape the explicit codec. */
  function copySnapshot(snapshot: unknown): MastraMemorySnapshot {
    const copy = normalizeStoredMemoryDates(
      decodeMemoryValue(encodeMemoryValue(snapshot, "Initial memory snapshot")),
    );
    validateMemorySnapshot(copy);
    return copy;
  }

  return {
    domain,
    get revision() {
      return revision;
    },
    sanitizeInitialAgain() {
      if (!unsanitizedSnapshot) return undefined;
      return copySnapshot(
        options.sanitizeEvidence?.(unsanitizedSnapshot) ?? unsanitizedSnapshot,
      );
    },
    get incompleteReasons() {
      return [...reasons];
    },
    get incompleteReason() {
      return firstReason;
    },
    markIncomplete,
    async captureInitial(memory) {
      if (started || released) {
        markIncomplete(
          "Initial memory capture may run only once per invocation.",
        );
        return undefined;
      }
      started = true;
      capturing = true;
      try {
        try {
          lease = await acquireWithin(
            options.exclusiveAccess,
            selector,
            {
              // Leave the backend time to answer with a lease that registers
              // the overlap before this invocation stops waiting for it.
              waitMs: Math.max(0, waitMs - ACQUIRE_RESPONSE_MARGIN_MS),
              signal: options.leaseSignal,
              cooperative: true,
              onConflict: () =>
                markIncomplete(
                  "Exclusive source-thread ownership was invalidated by an overlapping invocation.",
                  "memory_lease_conflict",
                ),
            },
            waitMs,
            () =>
              markLeaseUnavailable("Late source-thread lease release failed."),
          );
          if (lease.overlapsFinalizingTurn) {
            // The overlap may be on the resource alone, or with a turn that
            // has no observational memory, so the reason names neither.
            markIncomplete(
              "An earlier turn on this thread or resource was still finishing its memory work.",
              "earlier_turn_finalizing",
            );
            return undefined;
          }
          await verifyEligibility();
          if (reasons.length) return undefined;
        } catch {
          markLeaseUnavailable(
            "Exclusive source-thread ownership is unavailable.",
          );
          return undefined;
        }
        const capture = (async () => {
          await memory.settled();
          await mutations;
          readingSnapshot = true;
          try {
            // Storage errors can quote stored data, so none of their text is kept.
            const { thread, resource, messages, records } =
              await readInitialState().catch(() => {
                throw new MastraReplayReasonError(
                  "Initial memory state could not be read from storage.",
                  "memory_read_failed",
                );
              });
            const snapshot = {
              threadId: options.threadId,
              resourceId: options.resourceId,
              thread,
              resource,
              messages,
              records: projectOMRecords(records),
            };
            // Declared file URLs in history become captured references here,
            // so a replayed processor resolves recorded bytes without a token.
            const sanitized = options.sanitizeEvidence?.(snapshot) ?? snapshot;
            if (collectFileNetworkUrls(sanitized.messages).length > 0) {
              if (!options.acceptHistoryFileUrls)
                throw new MastraReplayReasonError(
                  "Thread history holds a file URL that was not declared in files.",
                  "file_url_undeclared",
                );
              // Files the turn resolves later are captured then, so the
              // snapshot is sanitized again once the turn has finished.
              options.acceptHistoryFileUrls(
                collectFileNetworkUrls(snapshot.messages),
              );
              // The replay codec redacts URL credentials, which would stop
              // the captured URLs from matching, so this copy bypasses it.
              if (options.sanitizeEvidence)
                try {
                  unsanitizedSnapshot = structuredClone(snapshot);
                } catch {
                  throw new MastraReplayReasonError(
                    "Thread history holding file URLs could not be copied.",
                    "recorded_evidence_unsupported",
                  );
                }
            }
            return copySnapshot(sanitized);
          } finally {
            readingSnapshot = false;
          }
        })();
        const copy = await boundedCoordination(
          capture,
          options.captureWaitMs ?? 5_000,
          "Initial memory capture timed out.",
          "memory_capture_timeout",
        );
        await verifyEligibility();
        return reasons.length === 0 ? copy : undefined;
      } catch (error) {
        const reason = getReplayReason(error, "memory_store_shape_unsupported");
        markIncomplete(
          reason === "memory_capture_timeout"
            ? describeReplayFailure(error, "Initial memory capture timed out.")
            : `Initial memory capture failed: ${describeReplayFailure(
                error,
                "the stored memory state cannot be represented for replay.",
              )}`,
          reason,
        );
        return undefined;
      } finally {
        capturing = false;
      }
    },
    drain,
    async settle(memory, settleWaitMs) {
      settling ??= joinMemoryWork(memory);
      if (settleWaitMs === undefined) {
        await settling;
        return true;
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const settled = await Promise.race([
          settling.then(() => true),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(() => resolve(false), settleWaitMs);
          }),
        ]);
        if (!settled) joinAbandoned = true;
        return settled;
      } finally {
        if (timer) clearTimeout(timer);
      }
    },
    verifyEligibility,
    async beginFinalization() {
      if (released || !lease?.markFinalizing) return;
      try {
        await boundedCoordination(lease.markFinalizing(), waitMs);
      } catch {
        // Without the mark, a later turn's overlap invalidates this one,
        // which is the stricter outcome.
      }
    },
    async release() {
      if (released) return;
      await settleMutations();
      released = true;
      try {
        await lease?.();
      } catch {
        markLeaseUnavailable("Exclusive source-thread lease release failed.");
      }
    },
  };
}
