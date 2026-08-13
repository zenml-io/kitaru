import { describe, expect, it } from "vitest";

import { computeToolCacheKey } from "../src/cache-key.js";

describe("tool cache keys", () => {
  it.each([
    [
      "search",
      { a: 1, b: 2 },
      "7a965388b94ce5eecfc759d9d056e24fdc607a58d8523c6bd00c5edecf959657",
    ],
    [
      "normalize",
      { a: [true, null, { β: "值" }], z: "München 😀" },
      "d77c0e001e6560dfaacf386e85e01d197918a4ed5c6416ed3f0dc173287ab4db",
    ],
    [
      "queueRefundReview",
      { orderId: "ord-1001" },
      "1219eb952f60ae2d8737324ec9f1a752c1abf7a5d6dda96090914bd03152de22",
    ],
    [
      "tiny",
      { x: 1e-7 },
      "216074cc5eae8beedf68bfad835d806e57d495ebfc0980a20c00fb164915f8c2",
    ],
    [
      "small",
      { x: 0.00001 },
      "2b1224bc2bbb61700820cf5840be194e234c92c0da497ab34ea52d34c32673e6",
    ],
    [
      "large-int-wire",
      { x: 10_000_000_000_000_000 },
      "d288c9651931b6ec3b4cb1b1b9b724e6c468a393ac81f9d0a969258256c1c90e",
    ],
    [
      "large-float-wire",
      { x: 1e21 },
      "4a74a9b1501cfc1adb593065ec0f419079b87491caad01afbadce9ecf51987ca",
    ],
    [
      "search",
      { q: "weird\u007fchar" },
      "630fd6b20b7f4d1bfd02a6a29d9a41035ff7953561091fd783f9bd01a2dcb36a",
    ],
    [
      "search",
      { q: "del\u007f\u001f\u0080end" },
      "236d4613c418242045937ed89feaea74b05981c5981557b4efee958fd28058aa",
    ],
  ])("matches the Python key for %s", (toolName, inputs, expected) => {
    expect(computeToolCacheKey(toolName, inputs)).toBe(expected);
  });

  it("omits a key when inputs are absent", () => {
    expect(computeToolCacheKey("search", null)).toBeUndefined();
  });

  it("omits a key for a non-finite number", () => {
    expect(
      computeToolCacheKey("search", { value: Number.NaN }),
    ).toBeUndefined();
  });
});
