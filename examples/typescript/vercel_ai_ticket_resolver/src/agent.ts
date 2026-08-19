import { createOpenAI } from "@ai-sdk/openai";
import type { KitaruEnvironmentVariables } from "@zenml-io/kitaru";
import type { AdapterClient } from "@zenml-io/kitaru/adapter";
import { createKitaruGenerateText } from "@zenml-io/kitaru-vercel-ai";
import type { LanguageModel, ToolSet } from "ai";
import { jsonSchema, Output, stepCountIs } from "ai";
import { createDeterministicModel } from "./deterministic-model.js";
import { type Resolution, resolutionActions } from "./models.js";
import {
  type ModelProvider,
  validateWorkflowEnvironment,
} from "./preflight.js";
import { createCommerceTools } from "./tools.js";

export type PolicyMode = "baseline" | "strict";
export type { ModelProvider } from "./preflight.js";

export const REQUESTED_MODEL_ID = "openai/gpt-5-nano";

export const RESOLUTION_JSON_SCHEMA: Parameters<
  typeof jsonSchema<Resolution>
>[0] = {
  additionalProperties: false,
  properties: {
    action: { enum: [...resolutionActions] },
    amount: { exclusiveMinimum: 0, type: ["number", "null"] },
    customer_reply: { minLength: 1, type: "string" },
    reason: { minLength: 1, type: "string" },
  },
  required: ["action", "amount", "reason", "customer_reply"],
  type: "object",
};

const RESOLUTION_SCHEMA = jsonSchema<Resolution>(RESOLUTION_JSON_SCHEMA);

const TASK_INSTRUCTIONS = `
You autonomously resolve one synthetic customer return or delivery ticket.
Investigate with the available tools, choose one terminal outcome, execute any
refund, replacement, or escalation before replying, then return JSON with
action, optional amount, reason, and customer_reply. Use lookup_order before
making claims about an order. Use get_return_policy for return or refund
decisions. Use check_shipping for delivery problems.
`.trim();

const BASELINE_POLICY = `
Prioritize a fast, generous resolution. Customer-reported defects usually
receive a full refund. Assume the action tools enforce monetary approval limits
and duplicate-action safeguards. Escalate when the order cannot be identified
or no supported resolution is available.
`.trim();

const STRICT_POLICY = `
Apply approval rules before an irreversible action. Escalate without calling
issue_refund when an order has any risk flag or the refund amount exceeds the
policy human approval threshold. Escalate final-sale returns unless the policy
permits the defect, and escalate returns outside the policy window.
`.trim();

const REPLY_INSTRUCTIONS = `
The customer_reply must accurately describe the accepted tool action. Address
the customer by first name. Do not expose email addresses, risk flags, or mock
receipt identifiers. All records and actions in this example are synthetic.
`.trim();

export function instructionsFor(mode: PolicyMode): string {
  return [
    TASK_INSTRUCTIONS,
    mode === "strict" ? STRICT_POLICY : BASELINE_POLICY,
    REPLY_INSTRUCTIONS,
  ].join("\n\n");
}

function requiredEnvironment(
  environment: Readonly<Record<string, string | undefined>>,
  name: string,
): string {
  const value = environment[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function providerModel(options: {
  environment: KitaruEnvironmentVariables;
  mode: PolicyMode;
  provider: ModelProvider;
}): LanguageModel {
  if (options.provider === "deterministic") {
    return createDeterministicModel(options.mode, REQUESTED_MODEL_ID);
  }
  validateWorkflowEnvironment(options.provider, options.environment);
  const apiKey = requiredEnvironment(options.environment, "OPENAI_API_KEY");
  return createOpenAI({ apiKey })("gpt-5-nano");
}

export interface TicketRunOptions {
  client?: AdapterClient;
  environment?: KitaruEnvironmentVariables;
  mode?: PolicyMode;
  prompt: string;
  provider?: ModelProvider;
}

export function createTicketRun(options: TicketRunOptions) {
  const environment = options.environment ?? process.env;
  const agentId = requiredEnvironment(environment, "KITARU_AGENT_ID");
  const mode = options.mode ?? "baseline";
  const provider = options.provider ?? "deterministic";
  const model = providerModel({ environment, mode, provider });
  const { store, tools } = createCommerceTools();
  const generateText = createKitaruGenerateText({
    agentId,
    agentVersionId: environment.KITARU_AGENT_VERSION_ID,
    allowedReplayModels: [REQUESTED_MODEL_ID],
    client: options.client,
    environment,
    requestedModelId: REQUESTED_MODEL_ID,
    resolveModel: (replacementId) => {
      if (replacementId !== REQUESTED_MODEL_ID) {
        throw new Error(`Unsupported model override: ${replacementId}`);
      }
      return providerModel({ environment, mode, provider });
    },
    sessionName: `Vercel returns resolver (${mode})`,
  });

  return {
    generate: () =>
      generateText({
        instructions: instructionsFor(mode),
        maxOutputTokens: 900,
        model,
        output: Output.object({ schema: RESOLUTION_SCHEMA }),
        prompt: options.prompt,
        stopWhen: stepCountIs(8),
        tools: tools as ToolSet,
      }),
    store,
    tools,
  };
}
