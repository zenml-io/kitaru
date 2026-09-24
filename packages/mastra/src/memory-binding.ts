import type { MemoryStorage } from "@mastra/core/storage";
import { type JsonValue, toRecorderJson } from "@zenml-io/kitaru";
import { recordedToolPayloadConversion } from "@zenml-io/kitaru/adapter";
import {
  decodeMemoryValue,
  encodeMemoryValue,
  type MastraMemorySnapshot,
  normalizeStoredMemoryDates,
  validateMemorySnapshot,
} from "./memory-snapshot.js";

export interface MastraMemorySelector {
  threadId: string;
  resourceId: string;
}

export interface MastraMemoryLeaseOptions {
  /** Bound acquisition so a live answer does not wait for a delayed reflection. */
  waitMs?: number;
  /** Cancel a waiting acquisition without releasing another writer's lease. */
  signal?: AbortSignal;
  /** An advisory notification; verifyEligibility is the authoritative check. */
  onConflict?: () => void;
}

/** A callable release keeps existing direct lease users source compatible. */
export interface MastraMemoryLease {
  (): Promise<void>;
  /** Read the shared coordination state, including conflict and lease loss. */
  verifyEligibility(): Promise<boolean>;
}

/**
 * Coordinate every writer of a source thread or resource across all processes.
 *
 * The implementation must atomically poison eligibility for both selectors when
 * a competing native turn proceeds without ownership or an owner loses its lease.
 * Poison must survive process loss and prevent a later acquisition from becoming
 * eligible until all possible stale writers have quiesced. A timeout or failed
 * coordination call must fail closed for replay eligibility, while native Mastra
 * storage writes still run. Keep ownership through the final eligible-session
 * update, then release only after no delayed source write remains possible.
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
  /** Persist an unsafe-write marker before an unowned native write proceeds.
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
  type Turn = { onConflict?: () => void; invalidated: boolean };
  type ScopeState = {
    turns: Set<Turn>;
    poisoned: boolean;
    persistentLoss: boolean;
  };
  const scopes = new Map<string, ScopeState>();
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

  return {
    async acquire(selector, options = {}) {
      if (options.signal?.aborted)
        throw new Error("Exclusive source-thread ownership was cancelled.");
      const scopeKeys = keys(selector);
      const states = scopeKeys.map(getState);
      const occupied = states.filter((state) => state.turns.size > 0);
      if (occupied.length > 0) poison(occupied, false);
      const owner = {
        onConflict: options.onConflict,
        invalidated:
          states.some((state) => state.poisoned || state.persistentLoss) ||
          unknownWriterPoisoned,
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
  complete: boolean;
  method: string;
  arguments: JsonValue;
  result: JsonValue;
  requestId?: string;
}

export interface MastraMemoryCaptureOptions extends MastraMemorySelector {
  invocationId: string;
  domain: MemoryStorage;
  exclusiveAccess: MastraExclusiveMemoryAccess;
  recordMutation: (event: MastraMemoryMutation) => Promise<void>;
  getRequestId?: () => string | undefined;
  onIncomplete?: (reason: string) => void;
  /** Replace captured file URLs in evidence without changing native writes. */
  sanitizeEvidence?: <T>(value: T) => T;
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
  captureInitial(memory: {
    settled(): Promise<void>;
  }): Promise<MastraMemorySnapshot | undefined>;
  markIncomplete(reason: string): void;
  drain(): Promise<void>;
  /** Check shared ownership immediately before persisting eligible inputs. */
  verifyEligibility(): Promise<void>;
  release(): Promise<void>;
}

async function boundedCoordination<T>(
  operation: Promise<T>,
  waitMs: number,
  timeoutMessage = "Source-thread coordination timed out.",
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(timeoutMessage)), waitMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
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
  throw new Error("Observational-memory model has no stable identity.");
}

/** Keep provider clients out of evidence without changing native storage calls. */
function mutationEvidenceValue(method: PropertyKey, value: unknown): unknown {
  if (method !== "initializeObservationalMemory") return value;
  function project(input: unknown): unknown {
    if (!record(input) || !record(input.config)) return input;
    const config = { ...input.config };
    if (config.model !== undefined) config.model = modelIdentity(config.model);
    for (const name of ["observation", "reflection"]) {
      const phase = config[name];
      if (!record(phase)) continue;
      const projected = { ...phase };
      if (projected.model !== undefined)
        projected.model = modelIdentity(projected.model);
      if (Array.isArray(projected.extractors))
        projected.extractors = projected.extractors.map((extractor) => {
          if (
            record(extractor) &&
            extractor.internal === true &&
            typeof extractor.slug === "string" &&
            ["current-task", "suggested-response", "thread-title"].includes(
              extractor.slug,
            )
          )
            return { mastraBuiltinExtractor: extractor.slug };
          throw new Error("Unsupported observational-memory extractor.");
        });
      config[name] = projected;
    }
    return { ...input, config };
  }
  return Array.isArray(value) ? value.map(project) : project(value);
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
  const methods = new Map<PropertyKey, unknown>();
  const waitMs = options.leaseWaitMs ?? 100;

  function markIncomplete(reason: string): void {
    if (reasons.includes(reason)) return;
    reasons.push(reason);
    try {
      options.onIncomplete?.(reason);
    } catch {
      /* Diagnostics must not affect native calls. */
    }
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
      const bound = (...args: unknown[]): Promise<unknown> => {
        let encodedArguments: JsonValue = null;
        let complete = true;
        let requestId: string | undefined;
        try {
          const evidence = mutationEvidenceValue(property, args);
          encodedArguments = encodeMemoryValue(
            options.sanitizeEvidence?.(evidence) ?? evidence,
          );
          requestId = options.getRequestId?.();
        } catch {
          complete = false;
          markIncomplete(
            "Memory mutation arguments or request attribution could not be recorded safely.",
          );
        }
        if (!started || released)
          markIncomplete(
            "Memory mutation occurred outside the owned invocation lifecycle.",
          );
        if (readingSnapshot)
          markIncomplete("Memory mutation overlapped initial snapshot reads.");
        if (property === "dangerouslyClearAll" || property === "prune")
          markIncomplete(
            "Storage-wide mutation is outside the captured thread scope.",
          );
        const duringCapture = capturing;
        const result = mutations.then(async () => {
          let output: unknown;
          try {
            if (released || !lease) {
              markIncomplete(
                "Memory mutation occurred without source-thread ownership.",
              );
              try {
                await boundedCoordination(
                  options.exclusiveAccess.markUnsafeWrite(options),
                  waitMs,
                );
              } catch {
                markIncomplete("Unsafe memory write could not be fenced.");
              }
            } else {
              try {
                if (
                  !(await boundedCoordination(
                    lease.verifyEligibility(),
                    waitMs,
                  ))
                ) {
                  markIncomplete("Exclusive source-thread ownership was lost.");
                  await boundedCoordination(
                    options.exclusiveAccess.markUnsafeWrite(options),
                    waitMs,
                  );
                }
              } catch {
                markIncomplete(
                  "Exclusive source-thread ownership could not be verified.",
                );
                try {
                  await boundedCoordination(
                    options.exclusiveAccess.markUnsafeWrite(options),
                    waitMs,
                  );
                } catch {
                  markIncomplete("Unsafe memory write could not be fenced.");
                }
              }
            }
            output = await Reflect.apply(value, target, args);
          } catch (error) {
            markIncomplete("Native memory storage mutation failed.");
            throw error;
          }
          // Joined work from a previous turn belongs to the initial snapshot.
          if (duringCapture) return output;
          revision += 1;
          let encodedResult: JsonValue = null;
          try {
            const evidence = mutationEvidenceValue(property, output);
            encodedResult = encodeMemoryValue(
              options.sanitizeEvidence?.(evidence) ?? evidence,
            );
          } catch {
            complete = false;
            markIncomplete(
              "Memory mutation result could not be recorded safely.",
            );
          }
          let event: MastraMemoryMutation = {
            id: `${options.invocationId}:memory:${revision}`,
            invocationId: options.invocationId,
            revision,
            complete,
            method: String(property),
            arguments: encodedArguments,
            result: encodedResult,
            ...(requestId === undefined ? {} : { requestId }),
          };
          try {
            toRecorderJson(event);
            if (
              recordedToolPayloadConversion(event, "Mastra memory mutation")
                .lossy
            )
              throw new Error("Lossy event");
          } catch {
            markIncomplete(
              "Memory mutation evidence exceeds replay payload bounds or contains credentials.",
            );
            event = {
              ...event,
              complete: false,
              arguments: null,
              result: null,
            };
          }
          evidence = evidence.then(async () => {
            try {
              await options.recordMutation(event);
            } catch {
              markIncomplete("Memory mutation evidence persistence failed.");
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

  async function verifyEligibility(): Promise<void> {
    if (released || !lease) {
      markIncomplete("Exclusive source-thread ownership is unavailable.");
      return;
    }
    try {
      if (!(await boundedCoordination(lease.verifyEligibility(), waitMs)))
        markIncomplete("Exclusive source-thread ownership was lost.");
    } catch {
      markIncomplete(
        "Exclusive source-thread ownership could not be verified.",
      );
    }
  }

  return {
    domain,
    get revision() {
      return revision;
    },
    get incompleteReasons() {
      return [...reasons];
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
          const timeout = AbortSignal.timeout(waitMs);
          const signal = options.leaseSignal
            ? AbortSignal.any([timeout, options.leaseSignal])
            : timeout;
          const attempted = options.exclusiveAccess.acquire(options, {
            waitMs,
            signal,
            onConflict: () =>
              markIncomplete(
                "Exclusive source-thread ownership was invalidated by an overlapping invocation.",
              ),
          });
          let accepted = false;
          // A backend that ignores cancellation must not retain ownership if
          // its acquire resolves after the caller has resumed natively.
          void attempted.then(
            async (lateLease) => {
              if (!accepted && signal.aborted) {
                try {
                  await lateLease();
                } catch {
                  markIncomplete("Late source-thread lease release failed.");
                }
              }
            },
            () => undefined,
          );
          lease = await Promise.race([
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
          await verifyEligibility();
          if (reasons.length) return undefined;
        } catch {
          markIncomplete("Exclusive source-thread ownership is unavailable.");
          return undefined;
        }
        const capture = (async () => {
          await memory.settled();
          await mutations;
          readingSnapshot = true;
          try {
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
            const snapshot = {
              threadId: options.threadId,
              resourceId: options.resourceId,
              thread,
              resource,
              messages,
              records,
            };
            // No storage-owned objects or Dates escape the explicit codec.
            const copy = normalizeStoredMemoryDates(
              decodeMemoryValue(encodeMemoryValue(snapshot)),
            );
            validateMemorySnapshot(copy);
            return copy;
          } finally {
            readingSnapshot = false;
          }
        })();
        const copy = await boundedCoordination(
          capture,
          options.captureWaitMs ?? 5_000,
          "Initial memory capture timed out.",
        );
        await verifyEligibility();
        return reasons.length === 0 ? copy : undefined;
      } catch (error) {
        markIncomplete(
          error instanceof Error &&
            error.message === "Initial memory capture timed out."
            ? error.message
            : "Initial memory capture failed: unsupported, altered, or Unjoined observational-memory state.",
        );
        return undefined;
      } finally {
        capturing = false;
      }
    },
    drain,
    verifyEligibility,
    async release() {
      if (released) return;
      await drain();
      released = true;
      try {
        await lease?.();
      } catch {
        markIncomplete("Exclusive source-thread lease release failed.");
      }
    },
  };
}
