import { describe, expect, it } from "vitest";
import { parseModelSettings } from "../../src/adapter/index.js";

const STOP_SEQUENCE = "x".repeat(256);

describe("replay override model settings", () => {
  it.each([undefined, null])("treats %s as no override", (value) => {
    expect(parseModelSettings(value)).toBeUndefined();
  });

  it.each([
    ["maxOutputTokens", 1],
    ["maxOutputTokens", 1_000_000],
    ["temperature", 0],
    ["temperature", 2],
    ["topP", 0],
    ["topP", 1],
    ["topK", 1],
    ["topK", 1_000_000],
    ["presencePenalty", -2],
    ["frequencyPenalty", 2],
    ["seed", Number.MIN_SAFE_INTEGER],
    ["seed", Number.MAX_SAFE_INTEGER],
    ["stopSequences", Array.from({ length: 16 }, () => STOP_SEQUENCE)],
  ])("accepts %s at its bound %j", (key, value) => {
    expect(parseModelSettings({ [key]: value })).toEqual({ [key]: value });
  });

  // The adapter forwards these values straight into a model call, so an
  // out-of-range one must fail the replay before any provider request.
  it.each([
    ["maxOutputTokens", 0],
    ["maxOutputTokens", 1_000_001],
    ["maxOutputTokens", 1.5],
    ["temperature", -0.1],
    ["temperature", 2.1],
    ["temperature", Number.NaN],
    ["topP", 1.1],
    ["topK", 0],
    ["topK", 1_000_001],
    ["presencePenalty", -2.1],
    ["frequencyPenalty", 2.1],
    ["seed", 1.5],
  ])("rejects %s set to %j", (key, value) => {
    expect(() => parseModelSettings({ [key]: value })).toThrow(
      new RegExp(`^${key} must be`),
    );
  });

  it.each([
    ["too many sequences", Array.from({ length: 17 }, () => "stop")],
    ["an over-long sequence", [`${STOP_SEQUENCE}x`]],
    ["a non-string sequence", [42]],
    ["a non-array value", "stop"],
  ])("rejects stopSequences with %s", (_name, value) => {
    expect(() => parseModelSettings({ stopSequences: value })).toThrow(
      /^stopSequences must contain at most 16 strings/,
    );
  });

  it("rejects settings no adapter forwards", () => {
    expect(() => parseModelSettings({ temprature: 0.4 })).toThrow(
      "Unsupported replay model setting 'temprature'",
    );
  });

  it.each([["a string"], [[{ temperature: 1 }]]])(
    "rejects a non-object override %j",
    (value) => {
      expect(() => parseModelSettings(value)).toThrow(
        "replay override model_params must be an object",
      );
    },
  );

  it("rejects prototype-poisoning keys before copying any setting", () => {
    expect(() =>
      parseModelSettings(JSON.parse('{"__proto__": {"temperature": 1}}')),
    ).toThrow("contains dangerous key '__proto__'");
  });
});
