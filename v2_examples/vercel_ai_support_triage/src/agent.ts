import { openai } from "@ai-sdk/openai";
import type { AdapterClient } from "@zenml-io/kitaru/adapter";
import { createKitaruGenerateText } from "@zenml-io/kitaru-vercel-ai";
import { type LanguageModel, stepCountIs } from "ai";

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

function configuredModel(modelId?: string): LanguageModel {
  if (process.env.KITARU_VERCEL_AI_TEST_MODEL === "1") {
    return createDeterministicModel(modelId);
  }
  return openai("gpt-5-nano");
}

export function resolveModel(modelId: string): LanguageModel {
  if (modelId !== REQUESTED_MODEL_ID) {
    throw new Error(`Unsupported model override: ${modelId}`);
  }
  return configuredModel(modelId);
}

export function createSupportGenerateText(client?: AdapterClient) {
  const generateText = createKitaruGenerateText({
    agentId: requiredEnvironment("KITARU_AGENT_ID"),
    agentVersionId: process.env.KITARU_AGENT_VERSION_ID,
    allowedReplayModels: [REQUESTED_MODEL_ID],
    client,
    requestedModelId: REQUESTED_MODEL_ID,
    resolveModel,
    sessionName: "Vercel AI SDK support triage",
  });

  return () =>
    generateText({
      instructions: SUPPORT_INSTRUCTIONS,
      maxOutputTokens: 900,
      model: configuredModel(),
      prompt: DEFAULT_PROMPT,
      stopWhen: stepCountIs(5),
      tools: supportTools,
    });
}

export const DEFAULT_PROMPT = `
Investigate account acct-1001 and delayed order ord-1001. The customer reports
that the card may have been charged twice. Use the support tools and queue a
refund review when the fixture evidence confirms the report.
`.trim();

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}
