import { isPlainObject } from "../json.js";
import type { JsonValue } from "../types.js";

export const MAX_RECORDED_STRING_CHARS = 4_096;
const MAX_RECORDED_ITEMS = 100;
const MAX_RECORDED_DEPTH = 8;
const MAX_RECORDED_JSON_CHARS = 65_536;
export const MAX_RECORDED_PAYLOAD_CHARS = 1_048_576;
const MAX_RECORDED_PAYLOAD_ITEMS = 10_000;
const MAX_RECORDED_PAYLOAD_DEPTH = 64;

const CIRCULAR_MARKER = "[circular]";
const REDACTED_MARKER = "[redacted]";
const TRUNCATED_MARKER = "[truncated]";
const UNSUPPORTED_MARKER = "[unsupported]";

const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"]);

/** Keys whose value is a credential rather than part of the payload's meaning. */
const SECRET_KEYS: ReadonlySet<string> = new Set([
  "api_key",
  "apikey",
  "authorization",
  "cookie",
  "password",
  "secret",
  "token",
]);

/**
 * Keys whose value is a blob, a transport envelope, or a framework context
 * object that no bounded recording reproduces faithfully.
 */
const UNRECORDABLE_KEYS: ReadonlySet<string> = new Set([
  "data",
  "file",
  "provideroptions",
  "request",
  "runtimecontext",
  "toolscontext",
  "url",
]);

const SENSITIVE_KEYS: ReadonlySet<string> = new Set([
  ...SECRET_KEYS,
  ...UNRECORDABLE_KEYS,
]);

type SensitiveKeyMode = "allow" | "redact" | "reject";

interface CloneBudget {
  chars: number;
}

/** A converted payload together with whether converting it lost information. */
export interface RecordedConversion {
  lossy: boolean;
  value: JsonValue;
}

interface CloneOptions {
  // Counts down the characters the recorded JSON will need, so a runaway
  // payload stops early instead of being fully cloned and fully stringified
  // before anything notices its size. The allowance is deliberately looser
  // than the size ceiling: a payload merely near the ceiling still gets
  // converted, and the final size check decides what happens to it.
  budget: CloneBudget;
  // Set by the walk when it drops or alters part of the original value. A
  // converted value that lost information no longer identifies the value it
  // came from: two different tool inputs can truncate, redact, or collapse
  // onto one recorded value, and so onto one cache key. A caller that uses the
  // recorded value as an identity reads this to find out whether it may.
  lossy: boolean;
  maxDepth: number;
  maxItems: number;
  maxStringChars: number;
  path: string;
  rejectLongStrings: boolean;
  sensitiveKeyMode: SensitiveKeyMode;
  sensitiveKeys: ReadonlySet<string>;
}

function markLossy(options: CloneOptions): void {
  options.lossy = true;
}

function spendBudget(options: CloneOptions, characters: number): void {
  options.budget.chars -= characters;
  if (options.budget.chars < 0) {
    throw new TypeError(`${options.path} exceeds its recorded JSON size`);
  }
}

function boundedString(value: string, options: CloneOptions): JsonValue {
  if (value.length <= options.maxStringChars) {
    spendBudget(options, value.length);
    return value;
  }
  if (options.rejectLongStrings) {
    throw new TypeError(
      `${options.path} exceeds maximum string length ${options.maxStringChars}`,
    );
  }
  spendBudget(options, options.maxStringChars);
  markLossy(options);
  return `${value.slice(0, options.maxStringChars)}${TRUNCATED_MARKER}`;
}

function unconvertible(options: CloneOptions, reason: string): JsonValue {
  if (options.rejectLongStrings) {
    throw new TypeError(`${options.path} ${reason}`);
  }
  markLossy(options);
  return null;
}

function cloneJson(
  value: unknown,
  options: CloneOptions,
  depth: number,
  seen: Set<object>,
): JsonValue {
  spendBudget(options, 1);
  if (value === null || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value)
      ? value
      : unconvertible(options, "contains a non-finite number");
  }
  if (typeof value === "string") {
    return boundedString(value, options);
  }
  if (typeof value === "bigint") {
    // The decimal text is indistinguishable from the same digits passed as a
    // string, so the recorded value no longer says which one arrived.
    markLossy(options);
    return boundedString(value.toString(10), options);
  }
  if (value === undefined) {
    markLossy(options);
    return null;
  }
  if (typeof value !== "object") {
    if (options.rejectLongStrings) {
      throw new TypeError(`${options.path} must be JSON-compatible`);
    }
    markLossy(options);
    return UNSUPPORTED_MARKER;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime())
      ? unconvertible(options, "contains an invalid Date")
      : value.toISOString();
  }
  if (value instanceof Error) {
    // The stack and any custom fields the error carries are dropped, so two
    // errors that differ only there record as one value.
    markLossy(options);
    return {
      message: boundedString(value.message, options),
      name: value.name,
    };
  }
  if (seen.has(value)) {
    if (options.rejectLongStrings) {
      throw new TypeError(`${options.path} contains a cycle`);
    }
    markLossy(options);
    return CIRCULAR_MARKER;
  }
  if (depth >= options.maxDepth) {
    if (options.rejectLongStrings) {
      throw new TypeError(
        `${options.path} exceeds maximum depth ${options.maxDepth}`,
      );
    }
    markLossy(options);
    return TRUNCATED_MARKER;
  }

  seen.add(value);
  try {
    if (Array.isArray(value)) {
      return cloneArray(value, options, depth, seen);
    }
    return cloneRecord(value, options, depth, seen);
  } finally {
    seen.delete(value);
  }
}

function cloneArray(
  value: readonly unknown[],
  options: CloneOptions,
  depth: number,
  seen: Set<object>,
): JsonValue {
  if (value.length > options.maxItems) {
    if (options.rejectLongStrings) {
      throw new TypeError(
        `${options.path} exceeds maximum array length ${options.maxItems}`,
      );
    }
    markLossy(options);
  }
  const limit = Math.min(value.length, options.maxItems);
  const items: JsonValue[] = [];
  for (let index = 0; index < limit; index += 1) {
    items.push(cloneJson(value[index], options, depth + 1, seen));
  }
  return items;
}

function cloneRecord(
  value: object,
  options: CloneOptions,
  depth: number,
  seen: Set<object>,
): JsonValue {
  // A null prototype keeps a literal "__proto__" key as data instead of
  // reassigning the prototype of the recorded object, so an argument that
  // happens to be named "__proto__" is recorded and cache-keyed as it stands.
  const result: Record<string, JsonValue> = Object.create(null);
  if (!isPlainObject(value)) {
    markLossy(options);
  }
  let kept = 0;
  for (const key in value) {
    if (!Object.hasOwn(value, key)) {
      continue;
    }
    if (kept >= options.maxItems) {
      if (options.rejectLongStrings) {
        throw new TypeError(
          `${options.path} exceeds maximum object size ${options.maxItems}`,
        );
      }
      markLossy(options);
      break;
    }
    kept += 1;
    spendBudget(options, key.length);
    if (
      options.sensitiveKeyMode !== "allow" &&
      options.sensitiveKeys.has(key.toLowerCase())
    ) {
      if (options.sensitiveKeyMode === "reject") {
        throw new TypeError(
          `${options.path} contains unsupported sensitive key '${key}'`,
        );
      }
      markLossy(options);
      result[key] = REDACTED_MARKER;
      continue;
    }
    const item = (value as Record<string, unknown>)[key];
    result[key] = cloneJson(item, options, depth + 1, seen);
  }
  return result;
}

function convert(value: unknown, options: CloneOptions): JsonValue {
  return cloneJson(value, options, 0, new Set());
}

function assertJsonSize(value: JsonValue, path: string, maximum: number): void {
  if (JSON.stringify(value).length > maximum) {
    throw new TypeError(`${path} exceeds maximum JSON size ${maximum}`);
  }
}

function degradedPayload(path: string, reason: string): JsonValue {
  return { kitaru_recording: "degraded", path, reason };
}

function withoutFailing(
  options: CloneOptions,
  convertValue: () => JsonValue,
): RecordedConversion {
  try {
    const value = convertValue();
    return { lossy: options.lossy, value };
  } catch (error) {
    return {
      lossy: true,
      value: degradedPayload(
        options.path,
        error instanceof Error ? error.message : String(error),
      ),
    };
  }
}

/**
 * Convert a runtime payload for recording, reporting what the bounds dropped.
 *
 * Model text, tool arguments, and tool results routinely exceed any bound an
 * adapter picks, so oversized values are truncated, unconvertible ones get an
 * inline marker, and a payload past the size ceiling collapses to a degraded
 * marker. Throwing here would abort a generation the caller's own code handled.
 */
function largePayloadConversion(
  value: unknown,
  path: string,
  sensitiveKeyMode: SensitiveKeyMode,
  sensitiveKeys: ReadonlySet<string>,
): RecordedConversion {
  const options: CloneOptions = {
    budget: { chars: MAX_RECORDED_PAYLOAD_CHARS * 2 },
    lossy: false,
    maxDepth: MAX_RECORDED_PAYLOAD_DEPTH,
    maxItems: MAX_RECORDED_PAYLOAD_ITEMS,
    maxStringChars: MAX_RECORDED_PAYLOAD_CHARS,
    path,
    rejectLongStrings: false,
    sensitiveKeyMode,
    sensitiveKeys,
  };
  return withoutFailing(options, () => {
    const converted = convert(value, options);
    assertJsonSize(converted, path, MAX_RECORDED_PAYLOAD_CHARS);
    return converted;
  });
}

export function recordedPayloadConversion(
  value: unknown,
  path: string,
): RecordedConversion {
  return largePayloadConversion(value, path, "allow", SENSITIVE_KEYS);
}

/**
 * Convert a runtime payload for recording without ever failing the run.
 */
export function recordedPayloadJson(value: unknown, path: string): JsonValue {
  return recordedPayloadConversion(value, path).value;
}

/**
 * Convert tool arguments or results with replay-sized bounds and credentials hidden.
 *
 * Redacting a credential makes an input lossy, so callers must preserve the
 * returned flag and refuse to use that value as a history cache key.
 */
export function recordedToolPayloadConversion(
  value: unknown,
  path: string,
): RecordedConversion {
  return largePayloadConversion(value, path, "redact", SECRET_KEYS);
}

/** Convert tool arguments or results for recording without exposing credentials. */
export function recordedToolPayloadJson(
  value: unknown,
  path: string,
): JsonValue {
  return recordedToolPayloadConversion(value, path).value;
}

/**
 * Convert a payload for recording with narrow bounds and credentials hidden.
 *
 * This path produces the recorded tool arguments a replay looks results up by,
 * and a redacted argument marks the conversion lossy, which disqualifies the
 * call from a history lookup altogether. Redacting the wider `SENSITIVE_KEYS`
 * here would therefore take every tool that takes a `url`, `data`, `file`,
 * `request`, or `providerOptions` argument out of recorded history in every
 * replay, so only keys whose value is always a credential are redacted.
 */
export function boundedRecorderConversion(
  value: unknown,
  path: string,
): RecordedConversion {
  const options: CloneOptions = {
    // The per-string, per-item, and depth bounds already cap this payload, and
    // a whole-payload degraded marker is lossy, so budgeting the total size
    // here would take every merely large tool input out of recorded history.
    budget: { chars: Number.POSITIVE_INFINITY },
    lossy: false,
    maxDepth: MAX_RECORDED_DEPTH,
    maxItems: MAX_RECORDED_ITEMS,
    maxStringChars: MAX_RECORDED_STRING_CHARS,
    path,
    rejectLongStrings: false,
    sensitiveKeyMode: "redact",
    sensitiveKeys: SECRET_KEYS,
  };
  return withoutFailing(options, () => convert(value, options));
}

/**
 * Convert a payload for recording with narrow bounds and credentials hidden.
 */
export function boundedRecorderJson(value: unknown, path: string): JsonValue {
  return boundedRecorderConversion(value, path).value;
}

/**
 * Convert provider metadata for recording, hiding every sensitive key.
 *
 * Nothing looks a replay result up by metadata, so keys that carry credentials
 * and keys that carry blobs or transport envelopes are all replaced.
 */
export function projectRecordedMetadata(
  value: unknown,
  path = "recorded metadata",
): JsonValue {
  const options: CloneOptions = {
    budget: { chars: MAX_RECORDED_JSON_CHARS * 2 },
    lossy: false,
    maxDepth: MAX_RECORDED_DEPTH,
    maxItems: MAX_RECORDED_ITEMS,
    maxStringChars: MAX_RECORDED_STRING_CHARS,
    path,
    rejectLongStrings: false,
    sensitiveKeyMode: "redact",
    sensitiveKeys: SENSITIVE_KEYS,
  };
  return withoutFailing(options, () => {
    const converted = convert(value, options);
    assertJsonSize(converted, path, MAX_RECORDED_JSON_CHARS);
    return converted;
  }).value;
}

/**
 * Convert session inputs, refusing anything a replay could not reproduce.
 *
 * Recorded inputs are the replay contract: a truncated or redacted input would
 * replay as a different run, so an input that cannot be recorded as it stands
 * fails the run before the model sees it.
 */
export function projectRecordedInput(
  value: unknown,
  path = "recorded input",
): JsonValue {
  const converted = convert(value, {
    budget: { chars: MAX_RECORDED_PAYLOAD_CHARS * 2 },
    lossy: false,
    maxDepth: MAX_RECORDED_PAYLOAD_DEPTH,
    maxItems: MAX_RECORDED_PAYLOAD_ITEMS,
    maxStringChars: MAX_RECORDED_PAYLOAD_CHARS,
    path,
    rejectLongStrings: true,
    sensitiveKeyMode: "reject",
    sensitiveKeys: SENSITIVE_KEYS,
  });
  assertJsonSize(converted, path, MAX_RECORDED_PAYLOAD_CHARS);
  return converted;
}

/**
 * Convert a value with narrow bounds, rejecting anything that exceeds them.
 */
export function strictRecordedJson(value: unknown, path: string): JsonValue {
  const converted = convert(value, {
    budget: { chars: MAX_RECORDED_JSON_CHARS * 2 },
    lossy: false,
    maxDepth: MAX_RECORDED_DEPTH,
    maxItems: MAX_RECORDED_ITEMS,
    maxStringChars: MAX_RECORDED_STRING_CHARS,
    path,
    rejectLongStrings: true,
    sensitiveKeyMode: "allow",
    sensitiveKeys: SENSITIVE_KEYS,
  });
  assertJsonSize(converted, path, MAX_RECORDED_JSON_CHARS);
  return converted;
}

/**
 * Truncate recorded text to the payload bound, keeping it a plain string.
 *
 * A field converted on its own would degrade on its own, hiding the fact that
 * the payload around it is what went over the ceiling, so this only shortens
 * the text and leaves the whole-payload decision to the size check.
 */
export function boundedRecordedText(value: unknown): JsonValue {
  if (typeof value !== "string") {
    return value === undefined || value === null ? null : UNSUPPORTED_MARKER;
  }
  return value.length <= MAX_RECORDED_PAYLOAD_CHARS
    ? value
    : `${value.slice(0, MAX_RECORDED_PAYLOAD_CHARS)}${TRUNCATED_MARKER}`;
}

/**
 * Cap the size of an already converted payload, degrading it when it is over.
 */
export function boundRecordedSize(
  value: JsonValue,
  path: string,
  maximum = MAX_RECORDED_PAYLOAD_CHARS,
): JsonValue {
  return JSON.stringify(value).length > maximum
    ? degradedPayload(path, `${path} exceeds maximum JSON size ${maximum}`)
    : value;
}

/**
 * Reject prototype-poisoning keys and unbounded shapes in server-supplied JSON.
 */
export function assertSafeKeys(
  value: unknown,
  path = "value",
  depth = 0,
): void {
  if (depth > MAX_RECORDED_DEPTH) {
    throw new TypeError(`${path} exceeds maximum depth ${MAX_RECORDED_DEPTH}`);
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_RECORDED_ITEMS) {
      throw new TypeError(
        `${path} exceeds maximum array length ${MAX_RECORDED_ITEMS}`,
      );
    }
    for (const [index, item] of value.entries()) {
      assertSafeKeys(item, `${path}[${index}]`, depth + 1);
    }
    return;
  }
  if (typeof value !== "object" || value === null) {
    return;
  }
  const keys = Object.keys(value);
  if (keys.length > MAX_RECORDED_ITEMS) {
    throw new TypeError(
      `${path} exceeds maximum object size ${MAX_RECORDED_ITEMS}`,
    );
  }
  for (const key of keys) {
    if (DANGEROUS_KEYS.has(key)) {
      throw new TypeError(`${path} contains dangerous key '${key}'`);
    }
    assertSafeKeys(
      (value as Record<string, unknown>)[key],
      `${path}.${key}`,
      depth + 1,
    );
  }
}

/**
 * Project the fields both adapters record as a run's output summary.
 *
 * A generation result carries the framework's own step objects, which are far
 * too large and too circular to record, so only the small fields an operator
 * reads survive. Frameworks opt in to reading a configured structured output
 * because some result objects expose it through a getter that can throw.
 * Non-object results pass through for the converter to bound.
 */
export function runResultSummary(
  result: unknown,
  options: { structuredOutputField?: "object" | "output" } = {},
): unknown {
  if (typeof result !== "object" || result === null || Array.isArray(result)) {
    return result;
  }
  const candidate = result as Record<string, unknown>;
  const structuredOutputField = options.structuredOutputField;
  let structuredOutput: unknown;
  let hasStructuredOutput = false;
  if (structuredOutputField) {
    try {
      structuredOutput = candidate[structuredOutputField];
      hasStructuredOutput = true;
    } catch {
      // Some SDK result objects expose structured output through a getter that
      // throws when generation completed without a usable structured value.
      // Recording must not turn that successful generation into a failed run.
    }
  }
  return {
    finish_reason: candidate.finishReason,
    ...(hasStructuredOutput ? { object: structuredOutput } : {}),
    step_count: Array.isArray(candidate.steps) ? candidate.steps.length : 0,
    text: typeof candidate.text === "string" ? candidate.text : undefined,
  };
}
