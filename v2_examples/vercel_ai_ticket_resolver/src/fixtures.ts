import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  type Order,
  type ResolutionAction,
  type ReturnPolicy,
  resolutionActions,
  type ShippingStatus,
  type TicketCase,
  type TicketInput,
} from "./models.js";

const EXAMPLE_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function record(value: unknown, context: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${context} must be an object`);
  }
  return value as Record<string, unknown>;
}

function string(value: unknown, context: string): string {
  if (typeof value !== "string") {
    throw new Error(`${context} must be a string`);
  }
  return value;
}

function number(value: unknown, context: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${context} must be a finite number`);
  }
  return value;
}

function boolean(value: unknown, context: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`${context} must be a boolean`);
  }
  return value;
}

function records(name: string, key: string): unknown[] {
  const path = resolve(EXAMPLE_DIR, "fixtures", name);
  const root = record(JSON.parse(readFileSync(path, "utf8")), name);
  const values = root[key];
  if (!Array.isArray(values)) {
    throw new Error(`${name} must contain an array named ${key}`);
  }
  return values;
}

function parseStringArray(value: unknown, context: string): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${context} must be an array`);
  }
  return value.map((item, index) => string(item, `${context}[${index}]`));
}

export function parseTicketInput(
  value: unknown,
  context = "ticket",
): TicketInput {
  const item = record(value, context);
  return {
    ticket_id: string(item.ticket_id, `${context}.ticket_id`),
    customer_name: string(item.customer_name, `${context}.customer_name`),
    email: string(item.email, `${context}.email`),
    subject: string(item.subject, `${context}.subject`),
    body: string(item.body, `${context}.body`),
  };
}

function parseAction(value: unknown, context: string): ResolutionAction {
  if (!resolutionActions.includes(value as ResolutionAction)) {
    throw new Error(`${context} must be a supported resolution action`);
  }
  return value as ResolutionAction;
}

function parseOrder(value: unknown, index: number): Order {
  const context = `orders[${index}]`;
  const item = record(value, context);
  const days = item.days_since_delivery;
  const tracking = item.tracking_no;
  return {
    order_id: string(item.order_id, `${context}.order_id`),
    email: string(item.email, `${context}.email`),
    product: string(item.product, `${context}.product`),
    category: string(item.category, `${context}.category`),
    amount_paid: number(item.amount_paid, `${context}.amount_paid`),
    status: string(item.status, `${context}.status`),
    days_since_delivery:
      days === null ? null : number(days, `${context}.days_since_delivery`),
    final_sale: boolean(item.final_sale, `${context}.final_sale`),
    tracking_no:
      tracking === null ? null : string(tracking, `${context}.tracking_no`),
    risk_flags: parseStringArray(item.risk_flags, `${context}.risk_flags`),
    already_refunded: boolean(
      item.already_refunded,
      `${context}.already_refunded`,
    ),
  };
}

function parsePolicy(value: unknown, index: number): ReturnPolicy {
  const context = `policies[${index}]`;
  const item = record(value, context);
  return {
    category: string(item.category, `${context}.category`),
    window_days: number(item.window_days, `${context}.window_days`),
    defective_full_refund: boolean(
      item.defective_full_refund,
      `${context}.defective_full_refund`,
    ),
    unused_return: boolean(item.unused_return, `${context}.unused_return`),
    final_sale_defect_exception: boolean(
      item.final_sale_defect_exception,
      `${context}.final_sale_defect_exception`,
    ),
    human_approval_threshold: number(
      item.human_approval_threshold,
      `${context}.human_approval_threshold`,
    ),
  };
}

function parseShipment(value: unknown, index: number): ShippingStatus {
  const context = `shipments[${index}]`;
  const item = record(value, context);
  return {
    tracking_no: string(item.tracking_no, `${context}.tracking_no`),
    status: string(item.status, `${context}.status`),
    detail: string(item.detail, `${context}.detail`),
  };
}

function parseTicketCase(value: unknown, index: number): TicketCase {
  const context = `cases[${index}]`;
  const item = record(value, context);
  return {
    scenario: string(item.scenario, `${context}.scenario`),
    ticket: parseTicketInput(item.ticket, `${context}.ticket`),
    expected_action: parseAction(
      item.expected_action,
      `${context}.expected_action`,
    ),
  };
}

export const orders = records("orders.v1.json", "orders").map(parseOrder);
export const policies = records("policies.v1.json", "policies").map(
  parsePolicy,
);
export const shipments = records("shipments.v1.json", "shipments").map(
  parseShipment,
);
export const ticketCases = records("tickets.v1.json", "cases").map(
  parseTicketCase,
);
