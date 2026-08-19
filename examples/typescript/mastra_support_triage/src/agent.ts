import { openai } from "@ai-sdk/openai";
import { Agent } from "@mastra/core/agent";
import type { KitaruCostInput } from "@zenml-io/kitaru-mastra";

import { createDeterministicModel } from "./deterministic-model.js";
import { supportTools } from "./tools.js";

export const REQUESTED_MODEL_ID = "openai/gpt-5-nano";

// Published gpt-5-nano list prices in US dollars per million tokens. Kitaru
// stores whatever cost the adapter sends and never prices a call itself, so a
// session has no cost estimate without a calculator like this one. The sibling
// vercel_ai_support_triage example carries the same two prices and the same
// estimateSupportCost helper; each example stays copy-pasteable on its own, so
// update both whenever OpenAI reprices the model.
const INPUT_USD_PER_MILLION = 0.05;
const OUTPUT_USD_PER_MILLION = 0.4;

export const SUPPORT_INSTRUCTIONS = `
You are a support-triage agent. For every case:
1. Call lookupAccount with the account ID from the request.
2. Call lookupOrder with the order ID from the request.
3. If the order is delayed and chargeCount is greater than one, call
   queueRefundReview exactly once with only the orderId.
4. Return a concise JSON decision with decision, evidence, risk, and nextAction.
Never claim that an action was queued unless the tool returned queued=true.
`.trim();

function configuredModel(): unknown {
  if (process.env.KITARU_MASTRA_TEST_MODEL === "1") {
    return createDeterministicModel(process.env.KITARU_REPLAY_ID !== undefined);
  }
  return openai("gpt-5-nano");
}

export function resolveModel(modelId: string): unknown {
  if (modelId !== REQUESTED_MODEL_ID) {
    throw new Error(`Unsupported model override: ${modelId}`);
  }
  return configuredModel();
}

export function estimateSupportCost({
  tokens,
}: KitaruCostInput): number | null {
  if (!tokens) {
    return null;
  }
  const input = tokens.input_tokens ?? 0;
  const output = tokens.output_tokens ?? 0;
  const dollars =
    (input * INPUT_USD_PER_MILLION + output * OUTPUT_USD_PER_MILLION) /
    1_000_000;
  // Binary floating point turns a price like 0.00019215 into a long tail of
  // digits that the dashboard would show verbatim.
  return Number(dollars.toFixed(10));
}

export function createSupportAgent(): Agent {
  return new Agent({
    id: "kitaru-mastra-support-triage",
    name: "Kitaru Mastra support triage",
    instructions: SUPPORT_INSTRUCTIONS,
    model: configuredModel() as never,
    tools: supportTools,
  });
}
