import { describe, expect, it } from "vitest";
import { type CostInput, resolveCost } from "../../src/adapter/index.js";

const INPUT: CostInput = {
  model: "gpt-4o",
  provider: "openai",
  requestedModelId: "gpt-4o",
  tokens: null,
};

const UNAVAILABLE = {
  attribute: { source: "user", status: "unavailable" },
  cost: null,
};

const estimated = (cost: number | string) => ({
  attribute: { source: "user", status: "estimated" },
  cost,
});

describe("cost calculator results the server would reject", () => {
  // A node batch carrying one of these fails the whole upsert with a 422, and
  // the recording callback turns an otherwise successful generation into a
  // failed run. A price Kitaru cannot read is a missing price, not a failure.
  it.each([
    ["a currency symbol", "$0.01"],
    ["a comma decimal separator", "1,23"],
    ["an empty string", ""],
    ["whitespace only", "   "],
    ["a unit suffix", "0.01 USD"],
    ["a negative decimal string", "-0.01"],
    ["a negative number", -0.01],
    ["a non-finite number", Number.POSITIVE_INFINITY],
    ["not a number", Number.NaN],
    ["a non-finite string", "Infinity"],
  ])("records %s as unavailable", async (_name, value) => {
    await expect(resolveCost(() => value, INPUT)).resolves.toEqual(UNAVAILABLE);
  });

  it.each([
    ["a decimal string", "0.0123", "0.0123"],
    ["a padded decimal string", " 0.0123 ", "0.0123"],
    [
      "a high-precision decimal string",
      "0.000000000000001234",
      "0.000000000000001234",
    ],
    ["an exponent string", "1.2e-5", "1.2e-5"],
    ["zero", "0", "0"],
  ])("records %s as an estimated cost", async (_name, value, expected) => {
    await expect(resolveCost(() => value, INPUT)).resolves.toEqual(
      estimated(expected),
    );
  });

  it("records a valid number as an estimated cost", async () => {
    await expect(resolveCost(() => 0.0123, INPUT)).resolves.toEqual(
      estimated(0.0123),
    );
  });

  it("keeps every digit of a decimal string a float could not hold", async () => {
    const precise = "0.12345678901234567890123";
    const resolved = await resolveCost(() => precise, INPUT);

    expect(resolved.cost).toBe(precise);
  });
});
