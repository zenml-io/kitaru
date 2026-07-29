import { describe, expect, it } from "vitest";

import type { TraceStep, ValidationRow } from "../src/types.js";
import {
  canUseFullscreen,
  orderedSteps,
  reviewPresentation,
  validationCounts,
} from "../src/view.js";

describe("review UI helpers", () => {
  it("uses fullscreen only when the host advertises it", () => {
    expect(canUseFullscreen(["inline", "fullscreen"])).toBe(true);
    expect(canUseFullscreen(["inline", "pip"])).toBe(false);
    expect(canUseFullscreen(undefined)).toBe(false);
  });

  it("keeps the full workspace out of the inline card by default", () => {
    expect(reviewPresentation("inline", false)).toBe("launcher");
    expect(reviewPresentation("fullscreen", false)).toBe("review");
    expect(reviewPresentation("inline", true)).toBe("review");
  });

  it("reads execution from outcome backward by default", () => {
    const steps = [1, 2, 3].map(
      (index): TraceStep => ({
        index,
        name: `tool-${index}`,
        kind: "read",
        arguments: {},
        result: {},
        blocked: false,
        wroteState: false,
        evidenceIds: [],
      }),
    );
    expect(orderedSteps(steps, "backward").map((step) => step.index)).toEqual([
      3, 2, 1,
    ]);
    expect(orderedSteps(steps, "forward").map((step) => step.index)).toEqual([
      1, 2, 3,
    ]);
    expect(steps.map((step) => step.index)).toEqual([1, 2, 3]);
  });

  it("separates false passes from false fails", () => {
    const rows = [
      { result: "agreement" },
      { result: "false-pass" },
      { result: "false-fail" },
      { result: "agreement" },
    ] as ValidationRow[];
    expect(validationCounts(rows)).toEqual({
      agreements: 2,
      falsePasses: 1,
      falseFails: 1,
    });
  });
});
