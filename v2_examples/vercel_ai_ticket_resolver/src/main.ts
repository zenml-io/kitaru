import {
  createTicketRun,
  type ModelProvider,
  type PolicyMode,
} from "./agent.js";
import { ticketCases } from "./fixtures.js";
import { renderTicketPrompt } from "./models.js";
import { SmokeClient } from "./smoke-client.js";

const defaultTicket = ticketCases.find(
  ({ ticket }) => ticket.ticket_id === "ticket-001",
)?.ticket;
if (!defaultTicket) {
  throw new Error("ticket-001 fixture is missing");
}
const DEFAULT_PROMPT = renderTicketPrompt(defaultTicket);

function policyMode(): PolicyMode {
  const value = process.env.RETURNS_POLICY_MODE ?? "baseline";
  if (value !== "baseline" && value !== "strict") {
    throw new Error("RETURNS_POLICY_MODE must be baseline or strict");
  }
  return value;
}

function modelProvider(): ModelProvider {
  const value = process.env.RETURNS_MODEL_PROVIDER ?? "deterministic";
  if (value !== "deterministic" && value !== "openai") {
    throw new Error("RETURNS_MODEL_PROVIDER must be deterministic or openai");
  }
  return value;
}

const smoke = process.env.KITARU_VERCEL_AI_SMOKE === "1";
const result = await createTicketRun({
  client: smoke ? new SmokeClient() : undefined,
  mode: policyMode(),
  prompt: DEFAULT_PROMPT,
  provider: modelProvider(),
}).generate();
console.log(result.text);
