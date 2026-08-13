import { openai } from "@ai-sdk/openai";
import { Agent } from "@mastra/core/agent";

import { createDeterministicModel } from "./deterministic-model.js";
import { supportTools } from "./tools.js";

export const REQUESTED_MODEL_ID = "openai/gpt-5-nano";

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

export function createSupportAgent(): Agent {
  return new Agent({
    id: "kitaru-mastra-support-triage",
    name: "Kitaru Mastra support triage",
    instructions: SUPPORT_INSTRUCTIONS,
    model: configuredModel() as never,
    tools: supportTools,
  });
}
