import { KitaruAgent } from "@zenml-io/kitaru-mastra";

import {
  createSupportAgent,
  estimateSupportCost,
  REQUESTED_MODEL_ID,
  resolveModel,
} from "./agent.js";

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

async function main(): Promise<void> {
  const versionId = process.env.KITARU_AGENT_VERSION_ID;
  if (process.env.KITARU_MASTRA_TEST_MODEL !== "1") {
    requiredEnvironment("OPENAI_API_KEY");
  }
  const agent = new KitaruAgent(createSupportAgent(), {
    agentId: requiredEnvironment("KITARU_AGENT_ID"),
    agentVersionId: versionId,
    allowedReplayModels: [REQUESTED_MODEL_ID],
    costCalculator: estimateSupportCost,
    requestedModelId: REQUESTED_MODEL_ID,
    resolveModel: (modelId) => resolveModel(modelId) as never,
    sessionName: "Mastra support triage",
  });
  const result = await agent.generate(DEFAULT_PROMPT, {
    modelSettings: { maxOutputTokens: 900 },
  });
  console.log(result.text);
}

await main();
