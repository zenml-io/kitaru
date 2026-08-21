import { createSupportAgent } from "./agent.js";
import { BASELINE_PROMPT } from "./prompts.js";
import { SmokeClient } from "./smoke-client.js";

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

async function main(): Promise<void> {
  const smoke = process.env.KITARU_VERCEL_AI_SMOKE === "1";
  if (!smoke) {
    requiredEnvironment("KITARU_API_URL");
  }
  if (process.env.KITARU_VERCEL_AI_TEST_MODEL !== "1") {
    requiredEnvironment("OPENAI_API_KEY");
  }
  const result = await createSupportAgent(
    smoke ? new SmokeClient() : undefined,
  ).generate({ prompt: BASELINE_PROMPT });
  console.log(result.text);
}

await main();
