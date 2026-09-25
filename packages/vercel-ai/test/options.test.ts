import { describe, expect, it } from "vitest";

import {
  MAX_OVERRIDE_JSON_CHARS,
  MAX_WORKER_TASK_INPUT_CHARS,
  parseVercelReplayOverride,
  parseWorkerTaskInput,
} from "../src/options.js";

const SOURCE = "replay override";

describe("replay option parsing", () => {
  it("treats an absent worker input as no input", () => {
    expect(parseWorkerTaskInput(undefined)).toBeUndefined();
  });

  it.each([undefined, null])("treats a %s override as no override", (value) => {
    expect(parseVercelReplayOverride(value, SOURCE)).toBeUndefined();
  });

  it("copies a model replacement map", () => {
    const override = parseVercelReplayOverride(
      { model: { "gpt-a": "gpt-b" } },
      SOURCE,
    );

    expect(override?.model).toEqual({ "gpt-a": "gpt-b" });
    expect(Object.getPrototypeOf(override?.model)).toBeNull();
  });

  it.each([
    ["an array", [], `${SOURCE} must be a JSON object`],
    [
      "an oversized object",
      { prompt: "x".repeat(MAX_OVERRIDE_JSON_CHARS) },
      `${SOURCE} exceeds maximum JSON size ${MAX_OVERRIDE_JSON_CHARS}`,
    ],
    [
      "an unsupported key",
      { tools: {} },
      `${SOURCE} contains unsupported key 'tools'`,
    ],
    [
      "a non-object model",
      { model: 42 },
      `${SOURCE}.model must be a string or object`,
    ],
    [
      "a non-string model replacement",
      { model: { "gpt-a": 7 } },
      `${SOURCE}.model values must be strings`,
    ],
    [
      "a non-string prompt",
      { prompt: ["inject"] },
      `${SOURCE}.prompt must be a bounded string`,
    ],
    [
      "an oversized prompt",
      { prompt: "x".repeat(MAX_WORKER_TASK_INPUT_CHARS + 1) },
      `${SOURCE}.prompt must be a bounded string`,
    ],
    [
      "a non-string system prompt",
      { system_prompt: { role: "system" } },
      `${SOURCE}.system_prompt must be a bounded string`,
    ],
    [
      "an oversized system prompt",
      { system_prompt: "x".repeat(MAX_WORKER_TASK_INPUT_CHARS + 1) },
      `${SOURCE}.system_prompt must be a bounded string`,
    ],
  ])("rejects %s", (_name, value, message) => {
    expect(() => parseVercelReplayOverride(value, SOURCE)).toThrow(
      new TypeError(message),
    );
  });
});
