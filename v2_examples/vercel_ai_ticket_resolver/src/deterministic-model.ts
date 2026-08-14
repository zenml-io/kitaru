import { MockLanguageModelV4 } from "ai/test";

import type { PolicyMode } from "./agent.js";
import type { Resolution } from "./models.js";

const TEST_USAGE = {
  inputTokens: {
    cacheRead: undefined,
    cacheWrite: undefined,
    noCache: 20,
    total: 20,
  },
  outputTokens: { reasoning: undefined, text: 20, total: 20 },
};

interface ToolCall {
  input: Record<string, unknown>;
  name: string;
}

interface Script {
  resolution: Resolution;
  tools: ToolCall[];
}

type ScriptedAction = Exclude<Resolution["action"], "reject">;

function customerReply(action: ScriptedAction, customer: string): string {
  switch (action) {
    case "escalate":
      return `Hi ${customer}, a specialist will review your request.`;
    case "replacement":
      return `Hi ${customer}, your replacement has been created.`;
    case "refund":
      return `Hi ${customer}, your refund has been issued.`;
  }
}

function resolution(
  action: ScriptedAction,
  customer: string,
  options: { amount?: number; reason: string },
): Resolution {
  return {
    action,
    amount: options.amount ?? null,
    reason: options.reason,
    customer_reply: customerReply(action, customer),
  };
}

function scriptFor(ticketId: string, mode: PolicyMode): Script {
  const escalate = (
    customer: string,
    reason: string,
    investigation: ToolCall[] = [],
  ): Script => ({
    resolution: resolution("escalate", customer, { reason }),
    tools: [...investigation, { input: { reason }, name: "escalate_to_human" }],
  });
  const refund = (
    customer: string,
    orderId: string,
    category: string,
    amount: number,
    investigation: ToolCall[] = [
      { input: { order_id: orderId }, name: "lookup_order" },
      { input: { category }, name: "get_return_policy" },
    ],
  ): Script => ({
    resolution: resolution("refund", customer, {
      amount,
      reason: "The reviewed refund is eligible.",
    }),
    tools: [
      ...investigation,
      { input: { amount, order_id: orderId }, name: "issue_refund" },
    ],
  });

  switch (ticketId) {
    case "ticket-001":
      return refund("Dana", "48213", "footwear", 98);
    case "ticket-002":
      return escalate("Leo", "Final-sale return requires review.", [
        { input: { order_id: "48214" }, name: "lookup_order" },
        { input: { category: "apparel" }, name: "get_return_policy" },
      ]);
    case "ticket-003":
      return escalate("Maya", "Return is outside the policy window.", [
        { input: { order_id: "48215" }, name: "lookup_order" },
        { input: { category: "accessories" }, name: "get_return_policy" },
      ]);
    case "ticket-004":
      if (mode === "baseline") {
        return refund("Sam", "48216", "luggage", 280);
      }
      return escalate("Sam", "Refund requires human approval.", [
        { input: { order_id: "48216" }, name: "lookup_order" },
        { input: { category: "luggage" }, name: "get_return_policy" },
      ]);
    case "ticket-005":
      return escalate("Priya", "Order could not be identified.", [
        { input: { order_id: "99999" }, name: "lookup_order" },
        { input: { email: "priya@example.test" }, name: "lookup_order" },
      ]);
    case "ticket-006":
      return {
        resolution: resolution("replacement", "Chris", {
          reason: "The carrier confirmed that the shipment was lost.",
        }),
        tools: [
          { input: { order_id: "48217" }, name: "lookup_order" },
          { input: { tracking_no: "TRACK-48217" }, name: "check_shipping" },
          { input: { order_id: "48217" }, name: "create_replacement" },
        ],
      };
    case "ticket-007":
      if (mode === "baseline") {
        return refund("Morgan", "48218", "apparel", 120);
      }
      return escalate("Morgan", "Account risk flag requires human review.", [
        { input: { order_id: "48218" }, name: "lookup_order" },
        { input: { category: "apparel" }, name: "get_return_policy" },
      ]);
    case "ticket-008":
      return escalate("Alex", "The order already has a recorded refund.", [
        { input: { order_id: "48219" }, name: "lookup_order" },
        { input: { category: "footwear" }, name: "get_return_policy" },
        { input: { amount: 82, order_id: "48219" }, name: "issue_refund" },
      ]);
    case "ticket-009":
      return refund("Jamie", "48220", "apparel", 80);
    case "ticket-010":
      return refund("Riley", "48222", "footwear", 98, [
        { input: { order_id: "48228" }, name: "lookup_order" },
        { input: { email: "riley@example.test" }, name: "lookup_order" },
        { input: { category: "footwear" }, name: "get_return_policy" },
      ]);
    default:
      throw new Error(`The deterministic model has no script for ${ticketId}`);
  }
}

function ticketIdFromPrompt(prompt: unknown): string {
  const match = JSON.stringify(prompt).match(/Ticket ID:\s*(ticket-[0-9]{3})/);
  if (!match?.[1]) {
    throw new Error("The deterministic model requires a known Ticket ID");
  }
  return match[1];
}

function toolResult(ticketId: string, step: number, call: ToolCall) {
  return {
    content: [
      {
        input: JSON.stringify(call.input),
        toolCallId: `${ticketId}-call-${step + 1}`,
        toolName: call.name,
        type: "tool-call" as const,
      },
    ],
    finishReason: { raw: "tool_calls", unified: "tool-calls" as const },
    providerMetadata: { fixture: { scriptedStep: step + 1 } },
    request: { body: { scriptedStep: step + 1 } },
    response: {
      id: `${ticketId}-response-${step + 1}`,
      modelId: "kitaru-returns-scripted-fixture",
      timestamp: new Date(0),
    },
    usage: TEST_USAGE,
    warnings: [],
  };
}

function textResult(ticketId: string, value: Resolution) {
  return {
    content: [{ text: JSON.stringify(value), type: "text" as const }],
    finishReason: { raw: "stop", unified: "stop" as const },
    providerMetadata: { fixture: { scriptedStep: "resolution" } },
    request: { body: { scriptedStep: "resolution" } },
    response: {
      id: `${ticketId}-response-resolution`,
      modelId: "kitaru-returns-scripted-fixture",
      timestamp: new Date(0),
    },
    usage: TEST_USAGE,
    warnings: [],
  };
}

/**
 * Build the provider-free scripted model used to exercise adapter integration.
 *
 * Its fixed outcomes validate the walkthrough mechanics. They do not prove that
 * one instruction set caused better model judgment than another.
 */
export function createDeterministicModel(
  mode: PolicyMode,
  modelId: string,
): MockLanguageModelV4 {
  let ticketId: string | undefined;
  let script: Script | undefined;
  let step = 0;

  return new MockLanguageModelV4({
    doGenerate: async (options) => {
      const effectiveTicketId = ticketIdFromPrompt(options.prompt);
      if (ticketId !== undefined && ticketId !== effectiveTicketId) {
        throw new Error(
          "A deterministic model instance cannot serve two tickets",
        );
      }
      ticketId = effectiveTicketId;
      script ??= scriptFor(ticketId, mode);
      const call = script.tools[step];
      if (call !== undefined) {
        const result = toolResult(ticketId, step, call);
        step += 1;
        return result;
      }
      if (step === script.tools.length) {
        step += 1;
        return textResult(ticketId, script.resolution);
      }
      throw new Error("Deterministic model received too many generation calls");
    },
    modelId,
    provider: "kitaru-scripted-fixture",
  });
}
