import type { JsonValue, SessionNodeCreateRequest } from "../types.js";

export interface CostInput {
  model: string;
  provider: string;
  requestedModelId: string;
  tokens: SessionNodeCreateRequest["tokens"];
}

export type CostCalculator = (
  input: CostInput,
) =>
  | number
  | string
  | null
  | undefined
  | Promise<number | string | null | undefined>;

export interface ResolvedCost {
  attribute: JsonValue;
  cost: number | string | null;
}

const DISABLED_COST: ResolvedCost = {
  attribute: { source: "none", status: "disabled" },
  cost: null,
};

const UNAVAILABLE_COST: ResolvedCost = {
  attribute: { source: "user", status: "unavailable" },
  cost: null,
};

// Matches the decimal literals the server's Decimal field accepts. A currency
// symbol, a comma decimal separator, a unit suffix, or an empty string are all
// plausible things for a hand-written calculator to return, and the server
// rejects the whole batch of nodes when one of them arrives.
const DECIMAL_TEXT = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;

/**
 * Convert a calculator result into a cost the server accepts, or `undefined`.
 *
 * Mirrors `normalize_cost` in the Python adapters: a value that is not a
 * finite, non-negative decimal has no price in it, so it is treated the same
 * as a calculator that returned nothing. The string form is kept as text so a
 * calculator using arbitrary precision does not lose digits to a float.
 */
function normalizeCost(value: number | string): number | string | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) && value >= 0 ? value : undefined;
  }
  const text = value.trim();
  if (!DECIMAL_TEXT.test(text)) {
    return undefined;
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) && parsed >= 0 ? text : undefined;
}

// Kitaru never prices a call itself, so a run without a calculator records
// `null` rather than a fabricated number. A calculator that fails records the
// reason and lets the run continue: a missing price is not a failed agent.
export async function resolveCost(
  calculator: CostCalculator | undefined,
  input: CostInput,
): Promise<ResolvedCost> {
  if (!calculator) {
    return DISABLED_COST;
  }
  let calculated: number | string | null | undefined;
  try {
    calculated = await calculator(input);
  } catch (error) {
    return {
      attribute: {
        error_type: error instanceof Error ? error.name : typeof error,
        source: "user",
        status: "unavailable",
      },
      cost: null,
    };
  }
  if (calculated === undefined || calculated === null) {
    return UNAVAILABLE_COST;
  }
  const normalized = normalizeCost(calculated);
  if (normalized === undefined) {
    return UNAVAILABLE_COST;
  }
  return {
    attribute: { source: "user", status: "estimated" },
    cost: normalized,
  };
}
