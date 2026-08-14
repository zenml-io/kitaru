import type { JsonValue, ReplayOverride } from "@zenml-io/kitaru";
import {
  assertSafeKeys,
  MODEL_SETTING_KEYS,
  parseModelSettings,
  strictRecordedJson,
} from "@zenml-io/kitaru/adapter";

export const MAX_WORKER_TASK_INPUT_CHARS = 4_096;
export const MAX_OVERRIDE_JSON_CHARS = 32_768;

const OVERRIDE_KEYS = new Set([
  "model",
  "model_params",
  "prompt",
  "system_prompt",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
  const messages = strictRecordedJson(value, "KITARU_TASK_INPUTS");
  if (!Array.isArray(messages)) {
    throw new TypeError("KITARU_TASK_INPUTS must contain a message array");
  }
  return messages;
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
