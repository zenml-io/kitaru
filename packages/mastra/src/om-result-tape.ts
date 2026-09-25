import { createHash } from "node:crypto";
import type { JsonValue } from "@zenml-io/kitaru";
import {
  MAX_MASTRA_REPLAY_ITEMS,
  MAX_MASTRA_REPLAY_JSON_BYTES,
  redactUrlCredentials,
} from "@zenml-io/kitaru/adapter";
import { decodeMemoryValue, encodeMemoryValue } from "./memory-snapshot.js";
import { MastraReplayReasonError } from "./replay-reasons.js";
import { fileReference } from "./stateful-files.js";

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

/** How a replay's OM calls departed from the recorded calls. */
export interface OMReplayDivergence {
  /** Blocking calls whose input matched no unused recorded call and took the next one. */
  inputMismatches: number;
  /** Buffered calls after every recorded result of their phase was used. */
  surplusCalls: number;
  /** Recorded results that the replay never requested. */
  unusedResults: number;
  /** Blocking calls with no recorded result that the live OM model answered. */
  liveCalls: number;
}

/** A replay OM call that the live model answered because nothing was recorded for it. */
export interface OMLiveCall {
  phase: OMPhase;
  method: OMMethod;
  /** The configured model value, such as a model id string. */
  model: unknown;
  /** The call's prompt, with captured files still held as references. */
  prompt: unknown;
  /** The encoded result, or null when the call failed or its capture did. */
  output: JsonValue;
  failed: boolean;
  /** Whether `output` holds the whole result the model returned. */
  captured: boolean;
  startedAt: string;
  endedAt: string;
}

export interface OMTapeResult {
  entries: OMResultEntry[];
  divergence: OMReplayDivergence;
  liveCalls: OMLiveCall[];
}

/**
 * What a replay does with a blocking OM call that has no recorded result.
 *
 * `fail` ends the replay as diverged. `live` calls the configured OM model.
 */
export type MissingOMResults = "fail" | "live";

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

/** Ends a buffered reflection that matched no recorded call without a result. */
export class MastraOMSkippedCallError extends Error {
  readonly code = "mastra_om_skipped_call";

  constructor() {
    super(
      "Replay skipped a buffered reflection that matched no recorded result.",
    );
  }
}

/** Options for a result tape. */
export interface OMResultTapeOptions {
  /**
   * Map each string in an OM call's input before it is fingerprinted, such as
   * a declared file URL to its captured reference. A replay's history holds
   * the mapped form, so the baseline must fingerprint that form too.
   */
  mapString?: (value: string) => string;
  /**
   * Whether a replay call of `phase` runs inside Mastra's async buffering
   * rather than blocking the actor.
   */
  isBuffered?: (phase: OMPhase) => boolean;
  /** Defaults to `fail`. Only a tape with recorded results reads it. */
  missingResults?: MissingOMResults;
  /**
   * Return the captured bytes for a `kitaru-file://` reference, which a live
   * OM call must send as content because no provider can fetch it.
   */
  resolveFileReference?: (
    reference: string,
  ) => Promise<{ bytes: Uint8Array; mediaType: string }>;
  /**
   * Return the `kitaru-file://` references of the files the turn captured.
   * A recording tape reads them once the turn has finished.
   */
  getCapturedFiles?: () => ReadonlySet<string>;
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

// Mastra labels an attachment by its file name or URL, and a replay's history
// holds the captured reference in place of the URL. The part's data still
// identifies the file.
const ATTACHMENT_LABEL = /\[(File|Image) #(\d+): [^\]\n]*\]/g;

function normalizeText(text: string): string {
  // Replay history holds redacted URLs, so the baseline hashes them redacted too.
  return VOLATILE_TEXT.reduce(
    (current, [pattern, replacement]) => current.replace(pattern, replacement),
    redactUrlCredentials(text),
  ).replace(ATTACHMENT_LABEL, "[$1 #$2]");
}

/**
 * Hash an OM call's input without the values that change on every run.
 *
 * Mastra renders message times and ids into the observer prompt and stamps
 * each prompt part with its creation time, so an unchanged replay would never
 * match its baseline if those values were hashed. Attachment bytes hash as
 * their content reference, the form a replay's recorded history holds.
 */
export function getOMInputFingerprint(
  input: unknown,
  mapString: (value: string) => string = (value) => value,
  onFileContent?: (reference: string) => void,
): string {
  try {
    const text = JSON.stringify(
      input,
      function (this: unknown, key: string, value: unknown) {
        if (VOLATILE_KEYS.has(key)) return undefined;
        if (typeof value === "string") return normalizeText(mapString(value));
        // The holder keeps the bytes as they were before a Buffer's toJSON.
        const holder =
          typeof this === "object" && this !== null
            ? (this as Record<string, unknown>)
            : undefined;
        const bytes = holder?.[key];
        if (
          bytes instanceof Uint8Array &&
          typeof holder?.mediaType === "string"
        ) {
          const reference = fileReference({
            bytes,
            mediaType: holder.mediaType,
          });
          onFileContent?.(reference);
          return reference;
        }
        return value;
      },
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

// A replay's file parts hold captured references that only the tape reads.
const FILE_REFERENCE_URL = /^kitaru-file:\/\//i;
// A replay's history can also hold a redacted history URL that the baseline
// never resolved. Mastra must not download it, because the tape answers
// without the file and a live call refuses to send it.
const REPLAY_FILE_URL = /^(?:https?|kitaru-file):\/\//i;

async function withReplayFileUrls(
  supported: unknown,
): Promise<Record<string, RegExp[]>> {
  const urls = await supported;
  const record =
    typeof urls === "object" && urls !== null
      ? (urls as Record<string, RegExp[]>)
      : {};
  return { ...record, "*/*": [...(record["*/*"] ?? []), REPLAY_FILE_URL] };
}

/**
 * Intercept only OM model calls; the actor model remains untouched.
 *
 * With `recorded`, every call is answered from the recorded results and never
 * reaches the provider. A call takes the unused recorded call of its phase
 * with the same input fingerprint. Otherwise what it gets depends on how
 * Mastra made it:
 *
 * - A buffered call runs beside the actor, so an instant replay makes it
 *   earlier than a slow production observer did, over fewer messages.
 *   Another recorded result could describe messages the replay has not
 *   produced yet. A buffered observation therefore observes nothing, which
 *   leaves its messages in context as production had them while its observer
 *   ran, and a buffered reflection ends without a result.
 * - A blocking call takes the next unused recorded call of its phase. With
 *   none left, replay fails, because an empty observation would drop the
 *   observed messages from the actor's context.
 *
 * A phase with no recorded result at all also fails replay. With
 * `missingResults: "live"`, a blocking call that would fail for either reason
 * calls the live OM model instead, and `finish()` returns what it answered.
 */
export function createOMResultTape(
  recorded: readonly OMResultEntry[] | undefined,
  onIncomplete: (reason: string) => void,
  options: OMResultTapeOptions = {},
) {
  const entries: Array<Omit<OMResultEntry, "inputFingerprint"> | undefined> =
    [];
  // A turn can capture a history file after an OM call has seen its URL, so
  // a recording fingerprints each input only once the turn has finished.
  const inputs: unknown[] = [];
  const pending = new Set<Promise<void>>();
  const malformed = recorded?.some((entry) => !isRecordedEntry(entry)) ?? false;
  const recordedCalls = groupRecordedCalls(
    recorded?.filter(isRecordedEntry) ?? [],
  );
  const divergence: OMReplayDivergence = {
    inputMismatches: 0,
    surplusCalls: 0,
    unusedResults: 0,
    liveCalls: 0,
  };
  const liveCalls: OMLiveCall[] = [];
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

  function use(call: RecordedCall, method: OMMethod): unknown {
    call.used = true;
    if (!call.result) throw new MastraOMRecordedFailureError();
    return play(call.result, method);
  }

  function serve(
    phase: OMPhase,
    method: OMMethod,
    input: unknown,
    callLive: () => Promise<unknown>,
  ): unknown {
    if (malformed) failClosed("malformed recorded tape");
    const calls = recordedCalls.get(`${phase}:${method}`) ?? [];
    const buffered = options.isBuffered?.(phase) ?? false;
    const live = options.missingResults === "live" && !buffered;
    if (calls.length === 0) {
      if (live) return callLive();
      failClosed(`no recorded ${phase} result`);
    }
    const fingerprint = getOMInputFingerprint(input, options.mapString);
    const matching = calls.find(
      (candidate) => !candidate.used && candidate.fingerprint === fingerprint,
    );
    if (matching) return use(matching, method);
    const unused = calls.find((candidate) => !candidate.used);
    if (buffered) {
      // A later buffered call usually covers the window production recorded,
      // so only a call with nothing left to match is a departure.
      if (!unused) divergence.surplusCalls++;
      if (phase === "reflector") throw new MastraOMSkippedCallError();
      // Mastra stores no buffered chunk for an empty observation.
      const template = calls.find((candidate) => candidate.result)?.result;
      if (!template) throw new MastraOMRecordedFailureError();
      return play(template, method, true);
    }
    if (!unused) {
      if (live) return callLive();
      failClosed(`no recorded ${phase} result left for a blocking call`);
    }
    divergence.inputMismatches++;
    return use(unused, method);
  }

  function record(
    ordinal: number,
    phase: OMPhase,
    method: OMMethod,
    output: JsonValue | undefined,
  ): void {
    entries[ordinal] =
      output === undefined
        ? { phase, ordinal, method, output: null, failed: true }
        : { phase, ordinal, method, output };
  }

  /**
   * Run a provider call and hand its encoded result to `save`.
   *
   * `save` receives undefined when the provider call or its stream failed.
   * When the result cannot be captured, `onCaptureFailed` runs instead and
   * the caller still gets the native result unchanged.
   */
  async function callAndCapture(
    invoke: () => Promise<unknown>,
    method: OMMethod,
    save: (output: JsonValue | undefined) => void,
    onCaptureFailed: () => void,
  ): Promise<unknown> {
    // Track the call from its start: finish() must wait for a call that is
    // still waiting on the provider, not report its result as missing.
    let settleCall!: () => void;
    const call = new Promise<void>((resolve) => {
      settleCall = resolve;
    });
    pending.add(call);
    void call.finally(() => pending.delete(call));
    let result: unknown;
    try {
      result = await invoke();
    } catch (error) {
      // Mastra retries transient provider errors. Keeping the failed
      // attempt lets replay pair it with the retry that succeeded.
      save(undefined);
      settleCall();
      throw error;
    }
    if (method === "doGenerate") {
      let encoded: JsonValue | undefined;
      try {
        encoded = encodeMemoryValue(result);
      } catch {
        onCaptureFailed();
      }
      if (encoded !== undefined) save(encoded);
      settleCall();
      return result;
    }
    const stream = (result as { stream?: ReadableStream<unknown> })?.stream;
    if (!(stream instanceof ReadableStream)) {
      onCaptureFailed();
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
            save(undefined);
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
        save(chunks);
      } catch {
        onCaptureFailed();
        // Tee cancellation may wait for the native branch to finish.
        void reader.cancel().catch(() => {});
      } finally {
        reader.releaseLock();
      }
    })();
    void work.finally(settleCall);
    return { ...(result as object), stream: native };
  }

  /**
   * Replace captured file references in a live call's prompt with their bytes.
   *
   * A replay's history holds `kitaru-file://` references, which no provider
   * can fetch, and any other file URL in it is a redacted history URL that
   * the baseline never resolved.
   */
  async function withFileContent(input: unknown): Promise<unknown> {
    const call = input as { prompt?: unknown };
    if (
      typeof call !== "object" ||
      call === null ||
      !Array.isArray(call.prompt)
    )
      return input;
    const prompt = await Promise.all(
      call.prompt.map(async (message: unknown) => {
        const content = (message as { content?: unknown })?.content;
        if (!Array.isArray(content)) return message;
        return {
          ...(message as object),
          content: await Promise.all(
            content.map(async (part: unknown) => {
              const file = part as { type?: unknown; data?: unknown };
              if (file?.type !== "file") return part;
              const url =
                file.data instanceof URL
                  ? file.data.href
                  : typeof file.data === "string" &&
                      /^[a-z][a-z0-9+.-]*:\/\//i.test(file.data)
                    ? file.data
                    : undefined;
              if (url === undefined) return part;
              if (
                !FILE_REFERENCE_URL.test(url) ||
                !options.resolveFileReference
              )
                failClosed(
                  "a live observational-memory call would send a file URL",
                );
              const resolved = await options.resolveFileReference(url);
              return { ...file, data: resolved.bytes };
            }),
          ),
        };
      }),
    );
    return { ...call, prompt };
  }

  /** Answer a replay call that has no recorded result from the live model. */
  async function callLive(
    phase: OMPhase,
    method: OMMethod,
    model: unknown,
    input: unknown,
    invoke: (input: unknown) => Promise<unknown>,
  ): Promise<unknown> {
    const liveInput = await withFileContent(input);
    const startedAt = new Date().toISOString();
    divergence.liveCalls++;
    const save = (output: JsonValue | undefined, captured: boolean) =>
      liveCalls.push({
        phase,
        method,
        model,
        prompt: (input as { prompt?: unknown })?.prompt ?? null,
        output: output ?? null,
        failed: captured && output === undefined,
        captured,
        startedAt,
        endedAt: new Date().toISOString(),
      });
    return callAndCapture(
      () => invoke(liveInput),
      method,
      (output) => save(output, true),
      () => save(undefined, false),
    );
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
        // Mastra would otherwise download a captured reference before the
        // call, and the tape answers without reading the file.
        if (recorded && key === "supportedUrls")
          return withReplayFileUrls(value);
        if (key !== "doGenerate" && key !== "doStream")
          return typeof value === "function" ? value.bind(target) : value;
        const method = key as OMMethod;
        const invoke = (input: unknown) =>
          Reflect.apply(value as (input: unknown) => Promise<unknown>, target, [
            input,
          ]) as Promise<unknown>;
        if (recorded)
          return async (input: unknown) =>
            serve(phase, method, input, () =>
              callLive(phase, method, native, input, invoke),
            );
        return async (input: unknown) => {
          const ordinal = next++;
          inputs[ordinal] = input;
          return callAndCapture(
            () => invoke(input),
            method,
            (output) => record(ordinal, phase, method, output),
            failCapture,
          );
        };
      },
    });
    nativeModels.set(proxy, native);
    return proxy;
  }

  /**
   * Wait for started calls and return the recorded entries.
   *
   * A replay fails when a call had no recorded result to use; the other
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
      return {
        entries: [...recorded],
        divergence: { ...divergence },
        liveCalls: [...liveCalls],
      };
    }
    const recordedEntries = Array.from(
      { length: next },
      (_, ordinal) => entries[ordinal],
    ).filter((entry) => entry !== undefined);
    if (incomplete || recordedEntries.length < next)
      throw new MastraReplayReasonError(
        "Observational-memory result tape is incomplete.",
        "om_tape_incomplete",
      );
    const captured = options.getCapturedFiles?.();
    let uncaptured = false;
    const fingerprinted = recordedEntries.map(
      (entry): OMResultEntry => ({
        ...entry,
        inputFingerprint: getOMInputFingerprint(
          inputs[entry.ordinal],
          options.mapString,
          (reference) => {
            if (captured && !captured.has(reference)) uncaptured = true;
          },
        ),
      }),
    );
    inputs.length = 0;
    // Mastra downloads a file URL the OM model cannot read before the call.
    // Content no resolveFile call captured came from such a download, which
    // a replay cannot repeat without fetching the URL.
    if (uncaptured)
      throw new MastraReplayReasonError(
        "Observational memory read a thread history file that no resolveFile call captured.",
        "file_url_undeclared",
      );
    return {
      entries: fingerprinted,
      divergence: { ...divergence },
      liveCalls: [],
    };
  }

  return { instrument, finish };
}
