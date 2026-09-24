import { createHash } from "node:crypto";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  MAX_MASTRA_REPLAY_ITEMS,
  MAX_MASTRA_REPLAY_JSON_BYTES,
  redactUrlCredentials,
} from "@zenml-io/kitaru/adapter";
import { decodeMemoryValue, encodeMemoryValue } from "./memory-snapshot.js";
import { MastraReplayReasonError } from "./replay-reasons.js";

export type OMPhase = "observer" | "reflector";
type OMMethod = "doGenerate" | "doStream";

export interface OMResultEntry {
  phase: OMPhase;
  ordinal: number;
  method: OMMethod;
  inputFingerprint: string;
  output: JsonValue;
  /** The provider call failed, so `output` is null. Mastra may have retried it. */
  failed?: true;
}

/** How a replay's OM calls departed from the recorded calls without a live call. */
export interface OMReplayDivergence {
  /** Calls whose input matched no unused recorded call and took the next one. */
  inputMismatches: number;
  /** Calls after every recorded result of their phase was used. */
  surplusCalls: number;
  /** Recorded results that the replay never requested. */
  unusedResults: number;
}

export interface OMTapeResult {
  entries: OMResultEntry[];
  divergence: OMReplayDivergence;
}

export class MastraOMDivergenceError extends Error {
  readonly code = "mastra_om_diverged";

  constructor(reason: string) {
    super(`Recorded Mastra observational memory diverged: ${reason}`);
  }
}

/** Replays a provider failure that every recorded attempt of a call ended in. */
export class MastraOMRecordedFailureError extends Error {
  readonly code = "mastra_om_recorded_failure";

  constructor() {
    super("The recorded observational-memory model call failed.");
  }
}

const VOLATILE_KEYS = new Set(["createdAt", "updatedAt", "abortSignal"]);
const VOLATILE_TEXT: ReadonlyArray<readonly [RegExp, string]> = [
  [
    /\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?/g,
    "<timestamp>",
  ],
  [
    /\b(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,? )?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d{1,2},? \d{4}\b/g,
    "<date>",
  ],
  [/\b\d{1,2}:\d{2}(?::\d{2})?\s?[AP]M\b/gi, "<time>"],
  [
    /\b\d+ (?:second|minute|hour|day|week|month|year)s? (?:ago|later)\b/g,
    "<relative-time>",
  ],
  [
    /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi,
    "<id>",
  ],
];

function normalizeText(text: string): string {
  // Replay history holds redacted URLs, so the baseline hashes them redacted too.
  return VOLATILE_TEXT.reduce(
    (current, [pattern, replacement]) => current.replace(pattern, replacement),
    redactUrlCredentials(text),
  );
}

/**
 * Hash an OM call's input without the values that change on every run.
 *
 * Mastra renders message times and ids into the observer prompt and stamps
 * each prompt part with its creation time, so an unchanged replay would never
 * match its baseline if those values were hashed.
 */
export function getOMInputFingerprint(input: unknown): string {
  try {
    const text = JSON.stringify(input, (key, value: unknown) =>
      VOLATILE_KEYS.has(key)
        ? undefined
        : typeof value === "string"
          ? normalizeText(value)
          : value,
    );
    return createHash("sha256")
      .update(text ?? "undefined")
      .digest("hex");
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

/** One logical recorded call: its failed attempts and the result they led to. */
interface RecordedCall {
  fingerprint: string;
  result?: OMResultEntry;
  used: boolean;
}

function isRecordedEntry(value: unknown): value is OMResultEntry {
  if (typeof value !== "object" || value === null) return false;
  const entry = value as Record<string, unknown>;
  return (
    (entry.phase === "observer" || entry.phase === "reflector") &&
    (entry.method === "doGenerate" || entry.method === "doStream") &&
    typeof entry.ordinal === "number" &&
    typeof entry.inputFingerprint === "string" &&
    (entry.failed === undefined || entry.failed === true)
  );
}

/**
 * Group recorded attempts into calls, keyed by phase and method.
 *
 * A retry repeats its failed attempt's input, so a failed attempt belongs to
 * the next attempt with the same phase, method and fingerprint.
 */
function groupRecordedCalls(
  entries: readonly OMResultEntry[],
): Map<string, RecordedCall[]> {
  const calls = new Map<string, RecordedCall[]>();
  const retrying = new Map<string, RecordedCall>();
  for (const entry of [...entries].sort((a, b) => a.ordinal - b.ordinal)) {
    const kind = `${entry.phase}:${entry.method}`;
    const attempt = `${kind}:${entry.inputFingerprint}`;
    let call = retrying.get(attempt);
    if (!call) {
      call = { fingerprint: entry.inputFingerprint, used: false };
      const list = calls.get(kind) ?? [];
      list.push(call);
      calls.set(kind, list);
    }
    if (entry.failed) {
      retrying.set(attempt, call);
    } else {
      call.result = entry;
      retrying.delete(attempt);
    }
  }
  return calls;
}

function isTextPart(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const type = (value as Record<string, unknown>).type;
  return (
    type === "text" ||
    type === "text-delta" ||
    type === "reasoning" ||
    type === "reasoning-delta"
  );
}

/**
 * Intercept only OM model calls; the actor model remains untouched.
 *
 * With `recorded`, every call is answered from the recorded results and never
 * reaches the provider. Mastra's number of OM calls depends on timing: a slow
 * production observer merges buffer rounds that an instant replay makes
 * separately. A call therefore takes the unused recorded call of its phase
 * with the same input fingerprint, else the next unused one. Once a phase's
 * results are all used, an observer call gets an empty observation, so no
 * observation is duplicated, and a reflector call repeats the last
 * reflection, because Mastra refuses an empty one. Only a phase with no
 * recorded result at all fails replay.
 */
export function createOMResultTape(
  recorded: readonly OMResultEntry[] | undefined,
  onIncomplete: (reason: string) => void,
) {
  const entries: Array<OMResultEntry | undefined> = [];
  const pending = new Set<Promise<void>>();
  const malformed = recorded?.some((entry) => !isRecordedEntry(entry)) ?? false;
  const recordedCalls = groupRecordedCalls(
    recorded?.filter(isRecordedEntry) ?? [],
  );
  const divergence: OMReplayDivergence = {
    inputMismatches: 0,
    surplusCalls: 0,
    unusedResults: 0,
  };
  let failedClosed: MastraOMDivergenceError | undefined;
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

  function failClosed(reason: string): never {
    failedClosed ??= new MastraOMDivergenceError(reason);
    throw failedClosed;
  }

  function play(
    entry: OMResultEntry,
    method: OMMethod,
    withoutText = false,
  ): unknown {
    const output = decodeMemoryValue(entry.output);
    if (method === "doGenerate") {
      if (
        !withoutText ||
        typeof output !== "object" ||
        output === null ||
        !Array.isArray((output as Record<string, unknown>).content)
      )
        return output;
      const content = (output as { content: unknown[] }).content;
      return {
        ...output,
        content: content.filter((part) => !isTextPart(part)),
      };
    }
    if (!Array.isArray(output)) failClosed("invalid recorded stream");
    const chunks = withoutText
      ? output.filter((chunk) => !isTextPart(chunk))
      : output;
    return {
      stream: new ReadableStream({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(chunk);
          controller.close();
        },
      }),
    };
  }

  function serve(phase: OMPhase, method: OMMethod, input: unknown): unknown {
    if (malformed) failClosed("malformed recorded tape");
    const calls = recordedCalls.get(`${phase}:${method}`) ?? [];
    if (calls.length === 0) failClosed(`no recorded ${phase} result`);
    const fingerprint = getOMInputFingerprint(input);
    let call = calls.find(
      (candidate) => !candidate.used && candidate.fingerprint === fingerprint,
    );
    if (!call) {
      call = calls.find((candidate) => !candidate.used);
      if (call) divergence.inputMismatches++;
    }
    if (call) {
      call.used = true;
      if (!call.result) throw new MastraOMRecordedFailureError();
      return play(call.result, method);
    }
    divergence.surplusCalls++;
    const succeeded = calls.filter((candidate) => candidate.result);
    const reused = (
      succeeded.findLast(
        (candidate) => candidate.fingerprint === fingerprint,
      ) ?? succeeded.at(-1)
    )?.result;
    if (!reused) throw new MastraOMRecordedFailureError();
    return play(reused, method, phase === "observer");
  }

  function record(
    ordinal: number,
    phase: OMPhase,
    method: OMMethod,
    inputFingerprint: string,
    output: JsonValue | undefined,
  ): void {
    entries[ordinal] =
      output === undefined
        ? {
            phase,
            ordinal,
            method,
            inputFingerprint,
            output: null,
            failed: true,
          }
        : { phase, ordinal, method, inputFingerprint, output };
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
        if (recorded)
          return async (input: unknown) => serve(phase, method, input);
        return async (input: unknown) => {
          const ordinal = next++;
          const inputFingerprint = getOMInputFingerprint(input);
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
            // Mastra retries transient provider errors. Keeping the failed
            // attempt lets replay pair it with the retry that succeeded.
            record(ordinal, phase, method, inputFingerprint, undefined);
            settleCall();
            throw error;
          }
          if (method === "doGenerate") {
            try {
              record(
                ordinal,
                phase,
                method,
                inputFingerprint,
                encodeMemoryValue(result),
              );
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
                let item: ReadableStreamReadResult<unknown>;
                try {
                  item = await reader.read();
                } catch {
                  // The provider stream failed and Mastra sees the same error.
                  record(ordinal, phase, method, inputFingerprint, undefined);
                  return;
                }
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
              record(ordinal, phase, method, inputFingerprint, chunks);
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

  /**
   * Wait for started calls and return the recorded entries.
   *
   * A replay fails only when a call had no recorded result to use; the other
   * departures are counted in `divergence`.
   */
  async function finish(): Promise<OMTapeResult> {
    while (pending.size > 0) await Promise.all([...pending]);
    if (recorded) {
      if (malformed) failClosed("malformed recorded tape");
      if (failedClosed) throw failedClosed;
      divergence.unusedResults = [...recordedCalls.values()]
        .flat()
        .filter((call) => !call.used).length;
      return { entries: [...recorded], divergence: { ...divergence } };
    }
    const recordedEntries = Array.from(
      { length: next },
      (_, ordinal) => entries[ordinal],
    );
    if (incomplete || recordedEntries.some((entry) => !entry))
      throw new MastraReplayReasonError(
        "Observational-memory result tape is incomplete.",
        "om_tape_incomplete",
      );
    return {
      entries: recordedEntries as OMResultEntry[],
      divergence: { ...divergence },
    };
  }

  return { instrument, finish };
}
