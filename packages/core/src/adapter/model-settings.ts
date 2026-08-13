import type { JsonValue } from "../types.js";
import { assertSafeKeys } from "./recorded-json.js";

const MAX_OUTPUT_TOKENS = 1_000_000;
const MAX_TOP_K = 1_000_000;
const MAX_STOP_SEQUENCES = 16;
const MAX_STOP_SEQUENCE_CHARS = 256;

export const MODEL_SETTING_KEYS: ReadonlySet<string> = new Set([
  "frequencyPenalty",
  "maxOutputTokens",
  "presencePenalty",
  "seed",
  "stopSequences",
  "temperature",
  "topK",
  "topP",
]);

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

/**
 * Validate replay model parameters against the settings adapters forward to a model.
 *
 * The value arrives from the server as part of a replay override, so the
 * prototype guard runs before any key is copied onto the result.
 */
export function parseModelSettings(
  value: unknown,
): Record<string, JsonValue> | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== "object" || Array.isArray(value)) {
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
