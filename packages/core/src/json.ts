import type { JsonValue } from "./types.js";

const MAX_RECORDER_DEPTH = 64;
const MAX_RECORDER_ITEMS = 10_000;
const MAX_RECORDER_STRING_CHARS = 1_048_576;

interface ConversionBudget {
  items: number;
}

function isPlainObject(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function convert(
  value: unknown,
  activeObjects: Set<object>,
  budget: ConversionBudget,
  path: string,
  depth: number,
): JsonValue {
  if (depth > MAX_RECORDER_DEPTH) {
    throw new TypeError(`${path} exceeds maximum depth ${MAX_RECORDER_DEPTH}`);
  }
  budget.items += 1;
  if (budget.items > MAX_RECORDER_ITEMS) {
    throw new TypeError(`$ exceeds maximum item count ${MAX_RECORDER_ITEMS}`);
  }
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    if (typeof value === "string" && value.length > MAX_RECORDER_STRING_CHARS) {
      throw new TypeError(
        `${path} exceeds maximum string length ${MAX_RECORDER_STRING_CHARS}`,
      );
    }
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${path} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return value.toString(10);
  }
  if (typeof value === "undefined") {
    return null;
  }
  if (typeof value === "function" || typeof value === "symbol") {
    throw new TypeError(`${path} contains an unsupported ${typeof value}`);
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new TypeError(`${path} contains an invalid Date`);
    }
    return value.toISOString();
  }
  if (value instanceof Error) {
    return { message: value.message, name: value.name };
  }
  if (activeObjects.has(value)) {
    throw new TypeError(`${path} contains a circular reference`);
  }

  activeObjects.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((item, index) =>
        convert(item, activeObjects, budget, `${path}[${index}]`, depth + 1),
      );
    }
    if (!isPlainObject(value)) {
      throw new TypeError(
        `${path} contains unsupported object ${value.constructor?.name ?? "Object"}`,
      );
    }

    const result: Record<string, JsonValue> = {};
    for (const [key, item] of Object.entries(value)) {
      if (item !== undefined) {
        Object.defineProperty(result, key, {
          configurable: true,
          enumerable: true,
          value: convert(
            item,
            activeObjects,
            budget,
            `${path}.${key}`,
            depth + 1,
          ),
          writable: true,
        });
      }
    }
    return result;
  } finally {
    activeObjects.delete(value);
  }
}

/**
 * Convert a value at the recorder boundary without changing the runtime value.
 */
export function toRecorderJson(value: unknown): JsonValue {
  return convert(value, new Set(), { items: 0 }, "$", 0);
}

export function recorderError(error: unknown): {
  message: string;
  name: string;
} {
  const converted = toRecorderJson(
    error instanceof Error ? error : new Error(String(error)),
  );
  return converted as { message: string; name: string };
}
