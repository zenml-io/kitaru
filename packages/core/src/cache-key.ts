import { createHash } from "node:crypto";

import type { JsonValue } from "./types.js";

function compareCodePoints(left: string, right: string): number {
  const leftPoints = Array.from(
    left,
    (character) => character.codePointAt(0) ?? 0,
  );
  const rightPoints = Array.from(
    right,
    (character) => character.codePointAt(0) ?? 0,
  );
  const length = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < length; index += 1) {
    const difference = (leftPoints[index] ?? 0) - (rightPoints[index] ?? 0);
    if (difference !== 0) {
      return difference;
    }
  }
  return leftPoints.length - rightPoints.length;
}

function pythonJsonString(value: string): string {
  // Python's json.dumps with ensure_ascii escapes everything above 0x7e, so
  // DEL has to be escaped here too even though JSON.stringify leaves it bare.
  return JSON.stringify(value).replace(/[\u007f-\uffff]/g, (character) => {
    return `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`;
  });
}

function pythonJsonNumber(value: number): string | undefined {
  if (!Number.isFinite(value)) {
    return undefined;
  }
  const wireValue = JSON.stringify(value);
  if (!wireValue.includes(".") && !/[eE]/.test(wireValue)) {
    return wireValue;
  }
  const absolute = Math.abs(value);
  if (absolute >= 0.0001 && absolute < 1e16) {
    return wireValue;
  }
  const [mantissa, rawExponent] = value.toExponential().split("e");
  if (rawExponent === undefined) {
    return wireValue;
  }
  const sign = rawExponent.startsWith("-") ? "-" : "+";
  const digits = rawExponent.replace(/^[+-]/, "").padStart(2, "0");
  return `${mantissa}e${sign}${digits}`;
}

function canonicalJson(value: JsonValue): string | undefined {
  if (value === null || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    return pythonJsonNumber(value);
  }
  if (typeof value === "string") {
    return pythonJsonString(value);
  }
  if (Array.isArray(value)) {
    const items = value.map(canonicalJson);
    return items.some((item) => item === undefined)
      ? undefined
      : `[${items.join(",")}]`;
  }
  const fields: string[] = [];
  for (const key of Object.keys(value).sort(compareCodePoints)) {
    const item = canonicalJson(value[key] as JsonValue);
    if (item === undefined) {
      return undefined;
    }
    fields.push(`${pythonJsonString(key)}:${item}`);
  }
  return `{${fields.join(",")}}`;
}

export function computeToolCacheKey(
  toolName: string,
  inputs: JsonValue,
): string | undefined {
  if (inputs === null) {
    return undefined;
  }
  const canonical = canonicalJson(inputs);
  if (canonical === undefined) {
    return undefined;
  }
  return createHash("sha256")
    .update(toolName, "utf8")
    .update("\0", "utf8")
    .update(canonical, "utf8")
    .digest("hex");
}

/**
 * Compute the key a history lookup uses, or nothing when the call has no key.
 *
 * Recorded results are keyed by the converted arguments, and a conversion that
 * truncated, redacted, or collapsed part of the arguments maps different calls
 * onto one key. Such a call has no key of its own, so anything that reasons
 * about which recorded result a call resolves to asks here rather than keying
 * arguments that no longer identify the call they came from.
 */
export function historyCacheKey(
  toolName: string,
  inputs: JsonValue,
  inputsLossy: boolean,
): string | undefined {
  return inputsLossy ? undefined : computeToolCacheKey(toolName, inputs);
}
