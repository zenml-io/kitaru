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
  if (typeof calculated === "number" && !Number.isFinite(calculated)) {
    return UNAVAILABLE_COST;
  }
  return {
    attribute: { source: "user", status: "estimated" },
    cost: calculated,
  };
}
