import { createSupportGenerateText } from "./agent.js";
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
  const result = await createSupportGenerateText(
    smoke ? new SmokeClient() : undefined,
  )();
  console.log(result.text);
}

await main();
