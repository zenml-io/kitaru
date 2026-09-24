import { createHash } from "node:crypto";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  MAX_MASTRA_REPLAY_ITEMS,
  MAX_MASTRA_REPLAY_JSON_BYTES,
} from "@zenml-io/kitaru/adapter";
import { decodeMemoryValue, encodeMemoryValue } from "./memory-snapshot.js";

export type OMPhase = "observer" | "reflector";
type OMMethod = "doGenerate" | "doStream";

export interface OMResultEntry {
  phase: OMPhase;
  ordinal: number;
  method: OMMethod;
  inputFingerprint: string;
  output: JsonValue;
}

export class MastraOMDivergenceError extends Error {
  readonly code = "mastra_om_diverged";

  constructor(reason: string) {
    super(`Recorded Mastra observational memory diverged: ${reason}`);
  }
}

function fingerprint(input: unknown): string {
  try {
    return createHash("sha256").update(JSON.stringify(input)).digest("hex");
  } catch {
    return "unavailable";
  }
}

function countJsonItems(value: JsonValue, remaining: number): number {
  let count = 0;
  function visit(current: JsonValue): void {
    if (++count > remaining) throw new Error("OM stream item limit exceeded");
    if (Array.isArray(current)) {
      for (const item of current) visit(item);
    } else if (current !== null && typeof current === "object") {
      for (const item of Object.values(current)) visit(item);
    }
  }
  visit(value);
  return count;
}

interface ModelLike {
  doGenerate?: (input: unknown) => Promise<unknown>;
  doStream?: (input: unknown) => Promise<unknown>;
}

const nativeModels = new WeakMap<object, unknown>();

/** Return the configured model value that an instrumented OM model replaced. */
export function getNativeOMModel(model: unknown): unknown {
  return typeof model === "object" && model !== null && nativeModels.has(model)
    ? nativeModels.get(model)
    : model;
}

function serializeNativeModel(native: unknown): unknown {
  if (typeof native !== "object" || native === null) return native;
  const toJSON: unknown = Reflect.get(native, "toJSON");
  return typeof toJSON === "function" ? toJSON.call(native) : native;
}

/** Intercept only OM model calls; the actor model remains untouched. */
export function createOMResultTape(
  recorded: readonly OMResultEntry[] | undefined,
  onIncomplete: (reason: string) => void,
  onInputMismatch?: (entry: OMResultEntry) => void,
) {
  const entries: Array<OMResultEntry | undefined> = recorded
    ? [...recorded]
    : [];
  const pending = new Set<Promise<void>>();
  let next = 0;
  let incomplete = false;

  function failCapture(): void {
    incomplete = true;
    try {
      onIncomplete("Observational-memory result could not be recorded safely.");
    } catch {
      // Diagnostic sinks never alter the provider's stream.
    }
  }

  function take(
    phase: OMPhase,
    method: OMMethod,
    input: unknown,
  ): OMResultEntry {
    const entry = entries[next++];
    if (
      !entry ||
      entry.phase !== phase ||
      entry.method !== method ||
      entry.ordinal !== next - 1
    )
      throw new MastraOMDivergenceError(
        "missing, extra, or reordered model call",
      );
    if (entry.inputFingerprint !== fingerprint(input)) onInputMismatch?.(entry);
    return entry;
  }

  /**
   * Wrap an OM model so its calls go through the tape.
   *
   * `native` is the value from the source memory configuration, such as a
   * model id string. Mastra persists the OM configuration into the record, so
   * a store that serializes the wrapper writes this value instead.
   */
  function instrument<T extends ModelLike>(
    model: T,
    phase: OMPhase,
    native: unknown = model,
  ): T {
    const proxy = new Proxy(model, {
      get(target, key) {
        if (key === "toJSON") return () => serializeNativeModel(native);
        const value = Reflect.get(target, key, target);
        if (key !== "doGenerate" && key !== "doStream")
          return typeof value === "function" ? value.bind(target) : value;
        const method = key as OMMethod;
        if (recorded) {
          return async (input: unknown) => {
            const entry = take(phase, method, input);
            const output = decodeMemoryValue(entry.output);
            if (method === "doGenerate") return output;
            if (!Array.isArray(output))
              throw new MastraOMDivergenceError("invalid recorded stream");
            return {
              stream: new ReadableStream({
                start(controller) {
                  for (const chunk of output) controller.enqueue(chunk);
                  controller.close();
                },
              }),
            };
          };
        }
        return async (input: unknown) => {
          const ordinal = next++;
          const inputFingerprint = fingerprint(input);
          // Track the call from its start: finish() must wait for a call that
          // is still waiting on the provider, not report its slot as missing.
          let settleCall!: () => void;
          const call = new Promise<void>((resolve) => {
            settleCall = resolve;
          });
          pending.add(call);
          void call.finally(() => pending.delete(call));
          let result: unknown;
          try {
            result = await Reflect.apply(
              value as (input: unknown) => Promise<unknown>,
              target,
              [input],
            );
          } catch (error) {
            settleCall();
            throw error;
          }
          if (method === "doGenerate") {
            try {
              entries[ordinal] = {
                phase,
                ordinal,
                method,
                inputFingerprint,
                output: encodeMemoryValue(result),
              };
            } catch {
              failCapture();
            }
            settleCall();
            return result;
          }
          const stream = (result as { stream?: ReadableStream<unknown> })
            ?.stream;
          if (!(stream instanceof ReadableStream)) {
            failCapture();
            settleCall();
            return result;
          }
          const [native, capture] = stream.tee();
          const work = (async () => {
            const reader = capture.getReader();
            try {
              const chunks: JsonValue[] = [];
              let capturedBytes = 2; // JSON array brackets.
              let capturedItems = 1; // JSON array itself.
              while (true) {
                const item = await reader.read();
                if (item.done) break;
                const encoded = encodeMemoryValue(item.value);
                capturedItems += countJsonItems(
                  encoded,
                  MAX_MASTRA_REPLAY_ITEMS - capturedItems,
                );
                capturedBytes +=
                  Buffer.byteLength(JSON.stringify(encoded), "utf8") +
                  (chunks.length > 0 ? 1 : 0);
                if (capturedBytes > MAX_MASTRA_REPLAY_JSON_BYTES)
                  throw new Error("OM stream byte limit exceeded");
                chunks.push(encoded);
              }
              entries[ordinal] = {
                phase,
                ordinal,
                method,
                inputFingerprint,
                output: chunks,
              };
            } catch {
              failCapture();
              // Tee cancellation may wait for the native branch to finish.
              void reader.cancel().catch(() => {});
            } finally {
              reader.releaseLock();
            }
          })();
          void work.finally(settleCall);
          return { ...(result as object), stream: native };
        };
      },
    });
    nativeModels.set(proxy, native);
    return proxy;
  }

  async function finish(): Promise<OMResultEntry[]> {
    while (pending.size > 0) await Promise.all([...pending]);
    if (recorded) {
      if (next !== entries.length)
        throw new MastraOMDivergenceError("recorded model call was not made");
    } else if (
      incomplete ||
      Array.from({ length: next }, (_, ordinal) => entries[ordinal]).some(
        (entry) => !entry,
      )
    ) {
      throw new Error("Observational-memory result tape is incomplete.");
    }
    return entries as OMResultEntry[];
  }

  return { instrument, finish };
}
