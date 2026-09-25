import { describe, expect, it } from "vitest";
import {
  assertSafeKeys,
  boundedRecordedText,
  boundedRecorderConversion,
  boundRecordedSize,
  MAX_RECORDED_PAYLOAD_CHARS,
  normalizeRecordingLimits,
  projectRecordedInput,
  projectRecordedMetadata,
  RecordedSensitiveKeyError,
  strictMastraReplayValue,
  strictRecordedJson,
} from "../../src/adapter/index.js";

const nested = (levels: number): unknown =>
  levels === 0 ? 0 : [nested(levels - 1)];
const keys = (count: number) =>
  Object.fromEntries(Array.from({ length: count }, (_, i) => [`k${i}`, i]));
const items = (count: number) => Array.from({ length: count }, () => 0);
const cyclic = () => {
  const value: Record<string, unknown> = {};
  value.self = value;
  return value;
};
// Fifteen full-length strings plus one of `last` characters serialize to
// 61_489 + `last` JSON characters, so 4_047 lands exactly on the 65_536 bound.
const nearJsonBound = (last: number) => [
  ...Array.from({ length: 15 }, () => "x".repeat(4_096)),
  "x".repeat(last),
];

describe("strict recorded JSON", () => {
  it.each([
    ["a string at the length bound", "x".repeat(4_096)],
    ["an array at the item bound", items(100)],
    ["an object at the key bound", keys(100)],
    ["nesting at the depth bound", nested(8)],
    ["JSON at the size bound", nearJsonBound(4_047)],
  ])("accepts %s", (_name, value) => {
    expect(strictRecordedJson(value, "input")).toEqual(value);
  });

  it.each([
    [
      "an over-long string",
      "x".repeat(4_097),
      "exceeds maximum string length 4096",
    ],
    ["an over-long array", items(101), "exceeds maximum array length 100"],
    ["an over-large object", keys(101), "exceeds maximum object size 100"],
    ["over-deep nesting", nested(9), "exceeds maximum depth 8"],
    [
      "over-large JSON",
      nearJsonBound(4_048),
      "exceeds maximum JSON size 65536",
    ],
    ["a cycle", cyclic(), "contains a cycle"],
    ["a non-finite number", [Number.NaN], "contains a non-finite number"],
    ["a function", [() => 1], "must be JSON-compatible"],
  ])("rejects %s instead of recording a different value", (_n, value, why) => {
    expect(() => strictRecordedJson(value, "input")).toThrow(`input ${why}`);
  });
});

describe("recorded session inputs", () => {
  // A redacted or truncated input would replay as a different run, so these
  // fail the run before the model sees the input.
  it.each([
    [{ API_KEY: "test" }, "API_KEY"],
    [{ nested: { url: "https://example.test" } }, "url"],
  ])("rejects sensitive key in %j", (value, key) => {
    expect(() => projectRecordedInput(value)).toThrow(
      `recorded input contains unsupported sensitive key '${key}'`,
    );
  });

  it("accepts a payload at the depth, item and size bounds", () => {
    const text = "x".repeat(MAX_RECORDED_PAYLOAD_CHARS - 2);
    expect(projectRecordedInput(nested(64))).toEqual(nested(64));
    expect(projectRecordedInput(items(10_000))).toHaveLength(10_000);
    expect(projectRecordedInput(text)).toBe(text);
  });

  it.each([
    ["over-deep nesting", nested(65), "maximum depth 64"],
    ["an over-long array", items(10_001), "maximum array length 10000"],
    [
      "over-large JSON",
      "x".repeat(MAX_RECORDED_PAYLOAD_CHARS - 1),
      "maximum JSON size 1048576",
    ],
  ])("rejects %s", (_name, value, why) => {
    expect(() => projectRecordedInput(value)).toThrow(
      `recorded input exceeds ${why}`,
    );
  });
});

describe("recorded provider metadata", () => {
  it("bounds metadata instead of failing the run", () => {
    expect(
      projectRecordedMetadata({
        headers: { Token: "test-token" },
        loop: cyclic(),
        text: "x".repeat(4_097),
        url: "https://example.test/file",
      }),
    ).toEqual({
      headers: { Token: "[redacted]" },
      loop: { self: "[circular]" },
      text: `${"x".repeat(4_096)}[truncated]`,
      url: "[redacted]",
    });
  });

  it("degrades metadata over the JSON size bound", () => {
    expect(projectRecordedMetadata(nearJsonBound(4_048), "meta")).toEqual({
      kitaru_recording: "degraded",
      path: "meta",
      reason: "meta exceeds maximum JSON size 65536",
    });
  });
});

describe("recorded size and text bounds", () => {
  it("degrades a converted payload only once it is over the size bound", () => {
    expect(boundRecordedSize("x".repeat(8), "out", 10)).toBe("x".repeat(8));
    expect(boundRecordedSize("x".repeat(9), "out", 10)).toEqual({
      kitaru_recording: "degraded",
      path: "out",
      reason: "out exceeds maximum JSON size 10",
    });
  });

  it("truncates text only past the payload bound", () => {
    const text = "x".repeat(MAX_RECORDED_PAYLOAD_CHARS);
    expect(boundedRecordedText(text)).toBe(text);
    expect(boundedRecordedText(`${text}y`)).toBe(`${text}[truncated]`);
  });

  it.each([
    [undefined, null],
    [null, null],
    [{ text: "hi" }, "[unsupported]"],
  ])("records non-string text %j as %j", (value, expected) => {
    expect(boundedRecordedText(value)).toBe(expected);
  });
});

describe("server-supplied JSON key checks", () => {
  it.each([
    ['{"a": {"__proto__": {"polluted": true}}}', "value.a", "__proto__"],
    ['{"a": [{"constructor": 1}]}', "value.a[0]", "constructor"],
    ['{"prototype": 1}', "value", "prototype"],
  ])("rejects %s", (json, path, key) => {
    expect(() => assertSafeKeys(JSON.parse(json))).toThrow(
      `${path} contains dangerous key '${key}'`,
    );
  });

  it("accepts values at the depth and size bounds", () => {
    expect(() => assertSafeKeys(nested(8))).not.toThrow();
    expect(() => assertSafeKeys(items(100))).not.toThrow();
    expect(() => assertSafeKeys(keys(100))).not.toThrow();
  });

  it.each([
    ["over-deep nesting", nested(9), "maximum depth 8"],
    ["an over-long array", items(101), "maximum array length 100"],
    ["an over-large object", keys(101), "maximum object size 100"],
  ])("rejects %s", (_name, value, why) => {
    expect(() => assertSafeKeys(value)).toThrow(why);
  });
});

describe("recording limits", () => {
  it("defaults to the narrow recording bounds", () => {
    expect(normalizeRecordingLimits()).toEqual({
      maxDepth: 8,
      maxItems: 100,
      maxStringChars: 4_096,
    });
  });

  it("accepts limits at their ceilings", () => {
    const ceilings = {
      maxDepth: 64,
      maxItems: 9_000,
      maxStringChars: MAX_RECORDED_PAYLOAD_CHARS - "[truncated]".length,
    };
    expect(normalizeRecordingLimits(ceilings)).toEqual(ceilings);
  });

  it.each([
    [{ maxItems: 9_001 }, "maxItems must be an integer from 1 to 9000"],
    [{ maxStringChars: MAX_RECORDED_PAYLOAD_CHARS - 10 }, "maxStringChars"],
    [{ maxDepth: 0 }, "maxDepth must be an integer from 1 to 64"],
    [{ maxItems: 1.5 }, "maxItems must be an integer from 1 to 9000"],
  ])("rejects %j", (limits, why) => {
    expect(() => normalizeRecordingLimits(limits)).toThrow(why);
  });
});

describe("credential keys", () => {
  it.each([
    ["a snake-case token", { profile: { access_token: "test-value" } }],
    ["a camel-case token", { accessToken: "test-value" }],
    ["a client secret", { oauth: { clientSecret: "test-value" } }],
    ["a header-style API key", { "x-api-key": "test-value" }],
    ["a private key", { signing: { private_key: "test-value" } }],
  ])(
    "refuses %s in replay input and redacts it in evidence",
    (_name, value) => {
      expect(() => strictMastraReplayValue(value)).toThrow(
        RecordedSensitiveKeyError,
      );
      const converted = boundedRecorderConversion(value, "tool input");
      expect(converted.lossy).toBe(true);
      expect(JSON.stringify(converted.value)).not.toContain("test-value");
    },
  );

  it.each([
    ["token counts", { usage: { max_tokens: 10, inputTokens: 4 } }],
    ["a pagination token", { nextPageToken: "abc", page_token: "def" }],
    ["data keys", { sortKey: "name", cacheKey: "k", tokenType: "bearer" }],
  ])("keeps %s", (_name, value) => {
    expect(strictMastraReplayValue(value)).toEqual(value);
  });
});
