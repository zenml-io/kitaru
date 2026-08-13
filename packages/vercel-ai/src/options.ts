import type { JsonValue, ReplayOverride } from "@zenml-io/kitaru";

export const MAX_WORKER_TASK_INPUT_CHARS = 4_096;
export const MAX_OVERRIDE_JSON_CHARS = 32_768;
export const MAX_RECORDED_STRING_CHARS = 4_096;
export const MAX_RECORDED_JSON_CHARS = 65_536;
export const MAX_RECORDED_ARRAY_ITEMS = 100;
export const MAX_RECORDED_OBJECT_KEYS = 100;
export const MAX_RECORDED_DEPTH = 8;
export const MAX_STOP_SEQUENCES = 16;
export const MAX_STOP_SEQUENCE_CHARS = 256;
export const MAX_OUTPUT_TOKENS = 1_000_000;
export const MAX_TOP_K = 1_000_000;

const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"]);
const OVERRIDE_KEYS = new Set([
  "model",
  "model_params",
  "prompt",
  "system_prompt",
]);
const MODEL_SETTING_KEYS = new Set([
  "frequencyPenalty",
  "maxOutputTokens",
  "presencePenalty",
  "seed",
  "stopSequences",
  "temperature",
  "topK",
  "topP",
]);
const REDACTED_INPUT_KEYS = new Set([
  "authorization",
  "apikey",
  "api_key",
  "cookie",
  "data",
  "file",
  "password",
  "provideroptions",
  "request",
  "runtimecontext",
  "secret",
  "token",
  "toolscontext",
  "url",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertSafeKeys(value: unknown, path = "value", depth = 0): void {
  if (depth > MAX_RECORDED_DEPTH) {
    throw new TypeError(`${path} exceeds maximum depth ${MAX_RECORDED_DEPTH}`);
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_RECORDED_ARRAY_ITEMS) {
      throw new TypeError(
        `${path} exceeds maximum array length ${MAX_RECORDED_ARRAY_ITEMS}`,
      );
    }
    for (const [index, item] of value.entries()) {
      assertSafeKeys(item, `${path}[${index}]`, depth + 1);
    }
    return;
  }
  if (!isRecord(value)) {
    return;
  }
  const keys = Object.keys(value);
  if (keys.length > MAX_RECORDED_OBJECT_KEYS) {
    throw new TypeError(
      `${path} exceeds maximum object size ${MAX_RECORDED_OBJECT_KEYS}`,
    );
  }
  for (const key of keys) {
    if (DANGEROUS_KEYS.has(key)) {
      throw new TypeError(`${path} contains dangerous key '${key}'`);
    }
    assertSafeKeys(value[key], `${path}.${key}`, depth + 1);
  }
}

function cloneJson(
  value: unknown,
  options: {
    path: string;
    rejectLongStrings: boolean;
    sensitiveKeys: "allow" | "redact" | "reject";
  },
  depth = 0,
  seen = new WeakSet<object>(),
): JsonValue {
  if (depth > MAX_RECORDED_DEPTH) {
    throw new TypeError(
      `${options.path} exceeds maximum depth ${MAX_RECORDED_DEPTH}`,
    );
  }
  if (value === null || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${options.path} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value === "string") {
    if (value.length <= MAX_RECORDED_STRING_CHARS) {
      return value;
    }
    if (options.rejectLongStrings) {
      throw new TypeError(
        `${options.path} exceeds maximum string length ${MAX_RECORDED_STRING_CHARS}`,
      );
    }
    return `${value.slice(0, MAX_RECORDED_STRING_CHARS)}[truncated]`;
  }
  if (value === undefined) {
    return null;
  }
  if (typeof value !== "object") {
    throw new TypeError(`${options.path} must be JSON-compatible`);
  }
  if (seen.has(value)) {
    throw new TypeError(`${options.path} contains a cycle`);
  }
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      const items = value
        .slice(0, MAX_RECORDED_ARRAY_ITEMS)
        .map((item) => cloneJson(item, options, depth + 1, seen));
      if (
        value.length > MAX_RECORDED_ARRAY_ITEMS &&
        options.rejectLongStrings
      ) {
        throw new TypeError(
          `${options.path} exceeds maximum array length ${MAX_RECORDED_ARRAY_ITEMS}`,
        );
      }
      return items;
    }
    const entries = Object.entries(value);
    if (
      entries.length > MAX_RECORDED_OBJECT_KEYS &&
      options.rejectLongStrings
    ) {
      throw new TypeError(
        `${options.path} exceeds maximum object size ${MAX_RECORDED_OBJECT_KEYS}`,
      );
    }
    const result: Record<string, JsonValue> = Object.create(null);
    for (const [key, item] of entries.slice(0, MAX_RECORDED_OBJECT_KEYS)) {
      if (DANGEROUS_KEYS.has(key)) {
        throw new TypeError(`${options.path} contains dangerous key '${key}'`);
      }
      if (REDACTED_INPUT_KEYS.has(key.toLowerCase())) {
        if (options.sensitiveKeys === "reject") {
          throw new TypeError(
            `${options.path} contains unsupported sensitive key '${key}'`,
          );
        }
        if (options.sensitiveKeys === "redact") {
          result[key] = "[redacted]";
          continue;
        }
      }
      result[key] = cloneJson(item, options, depth + 1, seen);
    }
    return result;
  } finally {
    seen.delete(value);
  }
}

function assertJsonSize(value: JsonValue, path: string): void {
  if (JSON.stringify(value).length > MAX_RECORDED_JSON_CHARS) {
    throw new TypeError(
      `${path} exceeds maximum JSON size ${MAX_RECORDED_JSON_CHARS}`,
    );
  }
}

export function boundedRecorderJson(value: unknown, path: string): JsonValue {
  const result = cloneJson(value, {
    path,
    rejectLongStrings: true,
    sensitiveKeys: "allow",
  });
  assertJsonSize(result, path);
  return result;
}

export function projectRecordedInput(value: unknown): JsonValue {
  const result = cloneJson(value, {
    path: "recorded input",
    rejectLongStrings: true,
    sensitiveKeys: "reject",
  });
  assertJsonSize(result, "recorded input");
  return result;
}

export function projectRecordedMetadata(value: unknown): JsonValue {
  const result = cloneJson(value, {
    path: "recorded metadata",
    rejectLongStrings: false,
    sensitiveKeys: "redact",
  });
  assertJsonSize(result, "recorded metadata");
  return result;
}

export function parseWorkerTaskInput(
  value: unknown,
): string | JsonValue[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value === "string") {
    if (value.length > MAX_WORKER_TASK_INPUT_CHARS) {
      throw new TypeError(
        `KITARU_TASK_INPUTS exceeds maximum length ${MAX_WORKER_TASK_INPUT_CHARS}`,
      );
    }
    return value;
  }
  if (!Array.isArray(value)) {
    throw new TypeError(
      "KITARU_TASK_INPUTS must contain a JSON string or message array",
    );
  }
  const messages = boundedRecorderJson(value, "KITARU_TASK_INPUTS");
  if (!Array.isArray(messages)) {
    throw new TypeError("KITARU_TASK_INPUTS must contain a message array");
  }
  return messages;
}

function boundedNumber(
  value: unknown,
  name: string,
  minimum: number,
  maximum: number,
  integer = false,
): number {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < minimum ||
    value > maximum ||
    (integer && !Number.isInteger(value))
  ) {
    throw new TypeError(
      `${name} must be ${integer ? "an integer " : ""}between ${minimum} and ${maximum}`,
    );
  }
  return value;
}

export function parseModelSettings(
  value: unknown,
): Record<string, JsonValue> | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new TypeError("replay override model_params must be an object");
  }
  assertSafeKeys(value, "replay override model_params");
  const result: Record<string, JsonValue> = Object.create(null);
  for (const [key, item] of Object.entries(value)) {
    if (!MODEL_SETTING_KEYS.has(key)) {
      throw new TypeError(`Unsupported replay model setting '${key}'`);
    }
    switch (key) {
      case "maxOutputTokens":
        result[key] = boundedNumber(item, key, 1, MAX_OUTPUT_TOKENS, true);
        break;
      case "temperature":
        result[key] = boundedNumber(item, key, 0, 2);
        break;
      case "topP":
        result[key] = boundedNumber(item, key, 0, 1);
        break;
      case "topK":
        result[key] = boundedNumber(item, key, 1, MAX_TOP_K, true);
        break;
      case "presencePenalty":
      case "frequencyPenalty":
        result[key] = boundedNumber(item, key, -2, 2);
        break;
      case "seed":
        result[key] = boundedNumber(
          item,
          key,
          Number.MIN_SAFE_INTEGER,
          Number.MAX_SAFE_INTEGER,
          true,
        );
        break;
      case "stopSequences": {
        if (
          !Array.isArray(item) ||
          item.length > MAX_STOP_SEQUENCES ||
          item.some(
            (sequence) =>
              typeof sequence !== "string" ||
              sequence.length > MAX_STOP_SEQUENCE_CHARS,
          )
        ) {
          throw new TypeError(
            `stopSequences must contain at most ${MAX_STOP_SEQUENCES} strings of at most ${MAX_STOP_SEQUENCE_CHARS} characters`,
          );
        }
        result[key] = [...item];
        break;
      }
    }
  }
  return result;
}

export interface SafeReplayOverride {
  model?: string | Record<string, string>;
  modelSettings?: Record<string, JsonValue>;
  prompt?: string;
  systemPrompt?: string;
}

export function parseVercelReplayOverride(
  value: ReplayOverride | unknown,
  source: string,
): SafeReplayOverride | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new TypeError(`${source} must be a JSON object`);
  }
  assertSafeKeys(value, source);
  const serialized = JSON.stringify(value);
  if (serialized.length > MAX_OVERRIDE_JSON_CHARS) {
    throw new TypeError(
      `${source} exceeds maximum JSON size ${MAX_OVERRIDE_JSON_CHARS}`,
    );
  }
  for (const key of Object.keys(value)) {
    if (!OVERRIDE_KEYS.has(key)) {
      throw new TypeError(`${source} contains unsupported key '${key}'`);
    }
  }
  let model: string | Record<string, string> | undefined;
  if (typeof value.model === "string") {
    model = value.model;
  } else if (value.model !== undefined && value.model !== null) {
    if (!isRecord(value.model)) {
      throw new TypeError(`${source}.model must be a string or object`);
    }
    const replacements: Record<string, string> = Object.create(null);
    for (const [key, replacement] of Object.entries(value.model)) {
      if (typeof replacement !== "string") {
        throw new TypeError(`${source}.model values must be strings`);
      }
      replacements[key] = replacement;
    }
    model = replacements;
  }
  const prompt = value.prompt;
  if (
    prompt !== undefined &&
    prompt !== null &&
    (typeof prompt !== "string" || prompt.length > MAX_WORKER_TASK_INPUT_CHARS)
  ) {
    throw new TypeError(`${source}.prompt must be a bounded string`);
  }
  const systemPrompt = value.system_prompt;
  if (
    systemPrompt !== undefined &&
    systemPrompt !== null &&
    (typeof systemPrompt !== "string" ||
      systemPrompt.length > MAX_WORKER_TASK_INPUT_CHARS)
  ) {
    throw new TypeError(`${source}.system_prompt must be a bounded string`);
  }
  return {
    model,
    modelSettings: parseModelSettings(value.model_params),
    prompt: typeof prompt === "string" ? prompt : undefined,
    systemPrompt: typeof systemPrompt === "string" ? systemPrompt : undefined,
  };
}

export function callerModelSettings(
  options: Record<string, unknown>,
): Record<string, JsonValue> | undefined {
  const values = Object.fromEntries(
    [...MODEL_SETTING_KEYS]
      .filter((key) => options[key] !== undefined)
      .map((key) => [key, options[key]]),
  );
  return Object.keys(values).length === 0
    ? undefined
    : parseModelSettings(values);
}
