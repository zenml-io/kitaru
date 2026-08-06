import { describe, expect, it } from "vitest";

import { toRecorderJson } from "../src/json.js";

describe("recorder JSON boundary", () => {
  it("converts supported non-JSON values without changing structure", () => {
    expect(
      toRecorderJson({
        bigint: 12n,
        date: new Date("2026-07-24T12:00:00Z"),
        error: new TypeError("broken"),
        omitted: undefined,
        values: [undefined, null, true, 3, "text"],
      }),
    ).toEqual({
      bigint: "12",
      date: "2026-07-24T12:00:00.000Z",
      error: { message: "broken", name: "TypeError" },
      values: [null, null, true, 3, "text"],
    });
    expect(toRecorderJson(undefined)).toBeNull();
  });

  it.each([
    [Number.NaN, "non-finite"],
    [Number.POSITIVE_INFINITY, "non-finite"],
    [() => undefined, "function"],
    [Symbol("value"), "symbol"],
  ])("rejects unsupported value %s", (value, message) => {
    expect(() => toRecorderJson(value)).toThrow(message);
  });

  it("preserves an own __proto__ property", () => {
    const value = JSON.parse('{"__proto__":{"tenant":"a"}}') as Record<
      string,
      unknown
    >;

    const converted = toRecorderJson(value);

    expect(Object.hasOwn(converted as object, "__proto__")).toBe(true);
    expect(JSON.stringify(converted)).toBe('{"__proto__":{"tenant":"a"}}');
  });

  it("rejects circular references and class instances", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    expect(() => toRecorderJson(circular)).toThrow("circular reference");
    expect(() => toRecorderJson(new URL("https://example.com"))).toThrow(
      "unsupported object URL",
    );
  });

  it("rejects excessively deep recorder values before stack exhaustion", () => {
    let value: unknown = "leaf";
    for (let index = 0; index < 100; index += 1) {
      value = [value];
    }

    expect(() => toRecorderJson(value)).toThrow("exceeds maximum depth");
  });
});
