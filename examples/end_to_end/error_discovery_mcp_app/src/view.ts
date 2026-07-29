import type { TraceStep, ValidationRow } from "./types.js";

export function orderedSteps(
  steps: TraceStep[],
  direction: "backward" | "forward",
): TraceStep[] {
  return direction === "backward" ? [...steps].reverse() : [...steps];
}

export function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export function validationCounts(rows: ValidationRow[]): {
  agreements: number;
  falsePasses: number;
  falseFails: number;
} {
  return rows.reduce(
    (counts, row) => {
      if (row.result === "agreement") counts.agreements += 1;
      if (row.result === "false-pass") counts.falsePasses += 1;
      if (row.result === "false-fail") counts.falseFails += 1;
      return counts;
    },
    { agreements: 0, falsePasses: 0, falseFails: 0 },
  );
}

export function shortId(traceId: string): string {
  return traceId.slice(0, 8);
}
