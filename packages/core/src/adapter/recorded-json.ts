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

interface CloneOptions {
  // Counts down the characters the recorded JSON will need, so a runaway
  // payload stops early instead of being fully cloned and fully stringified
  // before anything notices its size. The allowance is deliberately looser
  // than the size ceiling: a payload merely near the ceiling still gets
  // converted, and the final size check decides what happens to it.
  budget: CloneBudget;
  maxDepth: number;
  maxItems: number;
  maxStringChars: number;
  path: string;
  rejectLongStrings: boolean;
  sensitiveKeyMode: SensitiveKeyMode;
  sensitiveKeys: ReadonlySet<string>;
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
  return `${value.slice(0, options.maxStringChars)}${TRUNCATED_MARKER}`;
}

function unconvertible(options: CloneOptions, reason: string): JsonValue {
  if (options.rejectLongStrings) {
    throw new TypeError(`${options.path} ${reason}`);
  }
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
    return boundedString(value.toString(10), options);
  }
  if (value === undefined) {
    return null;
  }
  if (typeof value !== "object") {
    if (options.rejectLongStrings) {
      throw new TypeError(`${options.path} must be JSON-compatible`);
    }
    return UNSUPPORTED_MARKER;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime())
      ? unconvertible(options, "contains an invalid Date")
      : value.toISOString();
  }
  if (value instanceof Error) {
    return {
      message: boundedString(value.message, options),
      name: value.name,
    };
  }
  if (seen.has(value)) {
    if (options.rejectLongStrings) {
      throw new TypeError(`${options.path} contains a cycle`);
    }
    return CIRCULAR_MARKER;
  }
  if (depth >= options.maxDepth) {
    if (options.rejectLongStrings) {
      throw new TypeError(
        `${options.path} exceeds maximum depth ${options.maxDepth}`,
      );
    }
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
  if (value.length > options.maxItems && options.rejectLongStrings) {
    throw new TypeError(
      `${options.path} exceeds maximum array length ${options.maxItems}`,
    );
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
  path: string,
  convertValue: () => JsonValue,
): JsonValue {
  try {
    return convertValue();
  } catch (error) {
    return degradedPayload(
      path,
      error instanceof Error ? error.message : String(error),
    );
  }
}

/**
 * Convert a runtime payload for recording without ever failing the run.
 *
 * Model text, tool arguments, and tool results routinely exceed any bound an
 * adapter picks, so oversized values are truncated, unconvertible ones get an
 * inline marker, and a payload past the size ceiling collapses to a degraded
 * marker. Throwing here would abort a generation the caller's own code handled.
 */
export function recordedPayloadJson(value: unknown, path: string): JsonValue {
  return withoutFailing(path, () => {
    const converted = convert(value, {
      budget: { chars: MAX_RECORDED_PAYLOAD_CHARS * 2 },
      maxDepth: MAX_RECORDED_PAYLOAD_DEPTH,
      maxItems: MAX_RECORDED_PAYLOAD_ITEMS,
      maxStringChars: MAX_RECORDED_PAYLOAD_CHARS,
      path,
      rejectLongStrings: false,
      sensitiveKeyMode: "allow",
      sensitiveKeys: SENSITIVE_KEYS,
    });
    assertJsonSize(converted, path, MAX_RECORDED_PAYLOAD_CHARS);
    return converted;
  });
}

/**
 * Convert a payload for recording with narrow bounds and credentials hidden.
 *
 * The bounds also decide the recorded tool arguments a replay looks results up
 * by, so only keys whose value is always a credential are redacted: redacting
 * an ordinary argument would make two different calls share one cache key.
 */
export function boundedRecorderJson(value: unknown, path: string): JsonValue {
  return withoutFailing(path, () =>
    convert(value, {
      // The per-string, per-item, and depth bounds already cap this payload,
      // and a whole-payload marker here would give two different oversized
      // tool inputs the same recorded value, and so the same cache key.
      budget: { chars: Number.POSITIVE_INFINITY },
      maxDepth: MAX_RECORDED_DEPTH,
      maxItems: MAX_RECORDED_ITEMS,
      maxStringChars: MAX_RECORDED_STRING_CHARS,
      path,
      rejectLongStrings: false,
      sensitiveKeyMode: "redact",
      sensitiveKeys: SECRET_KEYS,
    }),
  );
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
  return withoutFailing(path, () => {
    const converted = convert(value, {
      budget: { chars: MAX_RECORDED_JSON_CHARS * 2 },
      maxDepth: MAX_RECORDED_DEPTH,
      maxItems: MAX_RECORDED_ITEMS,
      maxStringChars: MAX_RECORDED_STRING_CHARS,
      path,
      rejectLongStrings: false,
      sensitiveKeyMode: "redact",
      sensitiveKeys: SENSITIVE_KEYS,
    });
    assertJsonSize(converted, path, MAX_RECORDED_JSON_CHARS);
    return converted;
  });
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
 * too large and too circular to record, so only the three fields an operator
 * reads survive. Non-object results pass through for the converter to bound.
 */
export function runResultSummary(result: unknown): unknown {
  if (typeof result !== "object" || result === null || Array.isArray(result)) {
    return result;
  }
  const candidate = result as Record<string, unknown>;
  return {
    finish_reason: candidate.finishReason,
    step_count: Array.isArray(candidate.steps) ? candidate.steps.length : 0,
    text: typeof candidate.text === "string" ? candidate.text : undefined,
  };
}
