import { jsonSchema, tool } from "ai";

import { MockCommerceStore } from "./store.js";

export function createCommerceTools() {
  const store = new MockCommerceStore();
  const tools = {
    lookup_order: tool({
      description: "Look up an order by order number or customer email.",
      inputSchema: jsonSchema<{ order_id?: string; email?: string }>({
        additionalProperties: false,
        anyOf: [{ required: ["order_id"] }, { required: ["email"] }],
        properties: {
          order_id: { type: "string" },
          email: { type: "string" },
        },
        type: "object",
      }),
      execute: async (input) => store.lookupOrder(input),
    }),
    get_return_policy: tool({
      description: "Look up the return policy for a product category.",
      inputSchema: jsonSchema<{ category: string }>({
        additionalProperties: false,
        properties: { category: { type: "string" } },
        required: ["category"],
        type: "object",
      }),
      execute: async ({ category }) => store.getReturnPolicy(category),
    }),
    check_shipping: tool({
      description: "Check a shipment by its tracking number.",
      inputSchema: jsonSchema<{ tracking_no: string }>({
        additionalProperties: false,
        properties: { tracking_no: { type: "string" } },
        required: ["tracking_no"],
        type: "object",
      }),
      execute: async ({ tracking_no }) => store.checkShipping(tracking_no),
    }),
    issue_refund: tool({
      description: "Record a local mock refund for an order.",
      inputSchema: jsonSchema<{ order_id: string; amount: number }>({
        additionalProperties: false,
        properties: {
          order_id: { type: "string" },
          amount: { exclusiveMinimum: 0, type: "number" },
        },
        required: ["order_id", "amount"],
        type: "object",
      }),
      execute: async ({ order_id, amount }) =>
        store.issueRefund(order_id, amount),
    }),
    create_replacement: tool({
      description: "Record a local mock replacement for an order.",
      inputSchema: jsonSchema<{ order_id: string }>({
        additionalProperties: false,
        properties: { order_id: { type: "string" } },
        required: ["order_id"],
        type: "object",
      }),
      execute: async ({ order_id }) => store.createReplacement(order_id),
    }),
    escalate_to_human: tool({
      description: "Record a local mock escalation with its reason.",
      inputSchema: jsonSchema<{ reason: string }>({
        additionalProperties: false,
        properties: { reason: { minLength: 1, type: "string" } },
        required: ["reason"],
        type: "object",
      }),
      execute: async ({ reason }) => store.escalateToHuman(reason),
    }),
  };
  return { store, tools };
}
