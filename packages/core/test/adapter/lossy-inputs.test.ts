import { describe, expect, it } from "vitest";
import {
  boundedRecorderConversion,
  decideToolCall,
  MAX_RECORDED_STRING_CHARS,
  recordedPayloadConversion,
  recordedToolPayloadConversion,
  type ToolCallInput,
} from "../../src/adapter/index.js";
import type { RunState } from "../../src/adapter/run-state.js";
import { computeToolCacheKey } from "../../src/cache-key.js";
import { ToolPolicyMissError } from "../../src/errors.js";
import { type FakeClient, runState } from "./helpers.js";

type Converter = typeof boundedRecorderConversion;
type OnMiss = "error_result" | "fail" | "passthrough";

const TOOL = "lookup";
const RECORDED = { value: "the recorded result for the first call" };

function toolInput(
  value: unknown,
  convert: Converter = boundedRecorderConversion,
): ToolCallInput {
  const converted = convert(value, `tool '${TOOL}' input`);
  return {
    callId: "call-1",
    inputs: converted.value,
    inputsLossy: converted.lossy,
    toolName: TOOL,
  };
}

/**
 * Build a run whose recorded history holds one result, keyed by `recordedFor`.
 *
 * Anything else the run looks up misses, so a decision that returns the
 * recorded result proves the lookup matched that one recorded call.
 */
function runWithHistory(
  recordedFor: unknown,
  onMiss: OnMiss = "fail",
  convert: Converter = boundedRecorderConversion,
): { client: FakeClient; run: RunState } {
  const recordedKey = computeToolCacheKey(
    TOOL,
    convert(recordedFor, `tool '${TOOL}' input`).value,
  );
  return runState(
    {
      default: { on_miss: onMiss, scope: "baseline", type: "history" },
      tools: {},
    },
    (request) =>
      request.cache_key === recordedKey
        ? { found: true, result: RECORDED }
        : { found: false, result: null },
  );
}

const longText = (suffix: string) =>
  `${"a".repeat(MAX_RECORDED_STRING_CHARS)}${suffix}`;

const longArray = (changedTail: number) => {
  const items = Array.from({ length: 120 }, (_, index) => index);
  items[110] = changedTail;
  return { xs: items };
};

// Each pair converts to one recorded value, so before the loss check the second
// call's cache key equaled the first's and history handed back the first call's
// result for arguments it never saw.
const COLLIDING_PAIRS: readonly [string, unknown, unknown][] = [
  [
    "an exotic object with no own enumerable properties",
    { u: new URL("https://example.com/a") },
    { u: new URL("https://example.com/b") },
  ],
  ["an array truncated at the item bound", longArray(110), longArray(999)],
  [
    "a string truncated at the character bound",
    { query: longText("first") },
    { query: longText("second") },
  ],
  [
    "a redacted credential argument",
    { api_key: "key-one", query: "weather" },
    { api_key: "key-two", query: "weather" },
  ],
];

describe("history lookup of lossily converted tool arguments", () => {
  it("redacts credentials without truncating ordinary replay-sized payloads", () => {
    const secret = recordedToolPayloadConversion(
      { api_key: "SECRET_SENTINEL", query: "weather" },
      "input",
    );
    const long = recordedToolPayloadConversion(
      { query: "a".repeat(MAX_RECORDED_STRING_CHARS + 1) },
      "input",
    );

    expect(secret).toEqual({
      lossy: true,
      value: { api_key: "[redacted]", query: "weather" },
    });
    expect(long.lossy).toBe(false);
    expect(long.value).toEqual({
      query: "a".repeat(MAX_RECORDED_STRING_CHARS + 1),
    });
  });

  it.each(
    COLLIDING_PAIRS,
  )("converts %s to one value that no longer identifies either call", (_name, recorded, other) => {
    const convertedRecorded = boundedRecorderConversion(recorded, "input");
    const convertedOther = boundedRecorderConversion(other, "input");

    expect(convertedRecorded.value).toEqual(convertedOther.value);
    expect(convertedRecorded.lossy).toBe(true);
    expect(convertedOther.lossy).toBe(true);
  });

  it.each(
    COLLIDING_PAIRS,
  )("refuses to answer a call whose arguments lost %s", async (_name, recorded, other) => {
    const { client, run } = runWithHistory(recorded);

    await expect(decideToolCall(run, toolInput(other))).rejects.toThrow(
      ToolPolicyMissError,
    );
    // The lookup never happened, so no recorded result could come back.
    expect(client.lookups).toEqual([]);
  });

  it("still answers a call whose arguments converted losslessly", async () => {
    const inputs = { limit: 3, query: "weather" };
    const { run } = runWithHistory(inputs);

    await expect(decideToolCall(run, toolInput(inputs))).resolves.toEqual({
      output: RECORDED,
      type: "mocked_result",
    });
  });

  it("says the arguments could not be recorded rather than not found", async () => {
    const { run } = runWithHistory({ query: "weather" }, "error_result");
    const call = toolInput({ u: new URL("https://example.com/a") });

    await expect(decideToolCall(run, call)).resolves.toEqual({
      output: {
        error:
          "Tool 'lookup' arguments could not be recorded losslessly, so they " +
          "could not be matched against recorded history",
      },
      type: "mocked_error",
    });
  });

  it("routes the refusal through the configured on_miss policy", async () => {
    const { run } = runWithHistory({ query: "weather" }, "passthrough");
    const call = toolInput({ u: new URL("https://example.com/a") });

    await expect(decideToolCall(run, call)).resolves.toEqual({
      type: "execute",
    });
  });

  it("marks the recorded call so the trace shows the arguments were bounded", async () => {
    const { run } = runWithHistory({ query: "weather" }, "passthrough");
    const call = toolInput({ u: new URL("https://example.com/a") });

    await decideToolCall(run, call);

    expect(run.getToolCall(call.callId)?.inputsLossy).toBe(true);
  });

  it("refuses the same call on the wider payload bounds too", async () => {
    const { client, run } = runWithHistory(
      { u: new URL("https://example.com/a") },
      "fail",
      recordedPayloadConversion,
    );
    const call = toolInput(
      { u: new URL("https://example.com/b") },
      recordedPayloadConversion,
    );

    await expect(decideToolCall(run, call)).rejects.toThrow(
      ToolPolicyMissError,
    );
    expect(client.lookups).toEqual([]);
  });
});
