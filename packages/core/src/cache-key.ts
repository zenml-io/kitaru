import type { JsonValue } from "./types.js";

// Cache keys are synchronous public API, so use a runtime-neutral implementation
// instead of Node's crypto module or the asynchronous Web Crypto API.
const SHA256_CONSTANTS = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
] as const;

function rotateRight(value: number, bits: number): number {
  return (value >>> bits) | (value << (32 - bits));
}

function sha256(value: string): string {
  const bytes = new TextEncoder().encode(value);
  const paddedLength = Math.ceil((bytes.length + 9) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bytes.length / 0x20000000));
  view.setUint32(paddedLength - 4, bytes.length << 3);

  const hash = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
    0x1f83d9ab, 0x5be0cd19,
  ]);
  const words = new Uint32Array(64);

  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) {
      words[index] = view.getUint32(offset + index * 4);
    }
    for (let index = 16; index < 64; index += 1) {
      const left = words[index - 15] ?? 0;
      const right = words[index - 2] ?? 0;
      const sigma0 =
        rotateRight(left, 7) ^ rotateRight(left, 18) ^ (left >>> 3);
      const sigma1 =
        rotateRight(right, 17) ^ rotateRight(right, 19) ^ (right >>> 10);
      words[index] =
        ((words[index - 16] ?? 0) +
          sigma0 +
          (words[index - 7] ?? 0) +
          sigma1) >>>
        0;
    }

    let a = hash[0] ?? 0;
    let b = hash[1] ?? 0;
    let c = hash[2] ?? 0;
    let d = hash[3] ?? 0;
    let e = hash[4] ?? 0;
    let f = hash[5] ?? 0;
    let g = hash[6] ?? 0;
    let h = hash[7] ?? 0;
    for (let index = 0; index < 64; index += 1) {
      const sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temporary1 =
        (h +
          sum1 +
          choice +
          (SHA256_CONSTANTS[index] ?? 0) +
          (words[index] ?? 0)) >>>
        0;
      const sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temporary2 = (sum0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temporary1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temporary1 + temporary2) >>> 0;
    }

    for (const [index, value] of [a, b, c, d, e, f, g, h].entries()) {
      hash[index] = ((hash[index] ?? 0) + value) >>> 0;
    }
  }

  return Array.from(hash, (word) => word.toString(16).padStart(8, "0")).join(
    "",
  );
}

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
  return sha256(`${toolName}\0${canonical}`);
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
