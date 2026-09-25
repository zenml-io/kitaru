import { expect, it } from "vitest";
import {
  boundedRecorderConversion,
  boundMastraReplayEvidence,
  MAX_MASTRA_REPLAY_ITEMS,
  MastraReplayBudgetError,
  mastraReplayToolConversion,
  strictMastraReplayValue,
} from "../../src/adapter/index.js";

const OVER_BUDGET = Array.from(
  { length: MAX_MASTRA_REPLAY_ITEMS + 1 },
  (_, index) => index,
);

it("reports a replay input over its item budget as a budget error", () => {
  expect(() => strictMastraReplayValue(OVER_BUDGET, "input")).toThrow(
    new MastraReplayBudgetError("input exceeds maximum item count 200000"),
  );
  expect(() => strictMastraReplayValue({ token: "x" }, "input")).not.toThrow(
    MastraReplayBudgetError,
  );
});

it("keeps evidence past the generic 1 MiB and 10,000-value bounds unchanged", () => {
  const rows = Array.from({ length: 1_400 }, (_, row) =>
    Object.fromEntries(
      Array.from({ length: 10 }, (_, field) => [`f${field}`, `${row}`]),
    ),
  );
  const value = { rows, text: "x".repeat(2 * 1_048_576) };
  expect(boundMastraReplayEvidence(value, "evidence")).toEqual({ value });
});

it("degrades over-budget evidence and truncates to explicit limits with a reason", () => {
  expect(boundMastraReplayEvidence(OVER_BUDGET, "evidence")).toEqual({
    value: {
      kitaru_recording: "degraded",
      path: "evidence",
      reason: "evidence exceeds maximum item count 200000",
    },
    lossReason: "evidence exceeds maximum item count 200000",
  });
  expect(
    boundMastraReplayEvidence({ text: "abcdef" }, "evidence", {
      maxStringChars: 3,
    }),
  ).toEqual({
    value: { text: "abc[truncated]" },
    lossReason:
      "evidence exceeds the configured recordingLimits and was truncated",
  });
});

it("keeps a tool result past the tool recorder's bounds whole on the replay budget", () => {
  const rows = Array.from({ length: 1_400 }, (_, row) =>
    Object.fromEntries(
      Array.from({ length: 10 }, (_, field) => [`f${field}`, `${row}`]),
    ),
  );
  expect(boundedRecorderConversion(rows, "tool output").lossy).toBe(true);
  expect(mastraReplayToolConversion(rows, "tool output")).toEqual({
    lossy: false,
    value: rows,
  });
  // Credentials are still redacted, and limits the application set apply.
  expect(
    mastraReplayToolConversion({ rows, token: "secret" }, "tool output"),
  ).toMatchObject({ lossy: true, value: { rows, token: "[redacted]" } });
  expect(
    mastraReplayToolConversion(rows, "tool output", { maxItems: 100 }).lossy,
  ).toBe(true);
});
