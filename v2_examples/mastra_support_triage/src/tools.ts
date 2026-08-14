import { appendFile, mkdir, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { createTool } from "@mastra/core/tools";
import { z } from "zod";

const EXAMPLE_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_STATE_DIR = resolve(EXAMPLE_DIR, ".state");

interface AccountFixture {
  accountId: string;
  email: string;
  name: string;
  standing: string;
}

interface OrderFixture {
  accountId: string;
  amountUsd: number;
  chargeCount: number;
  expectedDelivery: string;
  orderId: string;
  status: string;
}

async function fixtureRecords<T>(name: string, key: string): Promise<T[]> {
  const path = resolve(EXAMPLE_DIR, "fixtures", name);
  const raw = JSON.parse(await readFile(path, "utf8")) as Record<
    string,
    unknown
  >;
  const records = raw[key];
  if (!Array.isArray(records)) {
    throw new Error(`Fixture ${name} does not contain ${key}`);
  }
  return records as T[];
}

export function outboxPath(): string {
  const stateDir =
    process.env.KITARU_SUPPORT_TRIAGE_STATE_DIR ?? DEFAULT_STATE_DIR;
  return resolve(stateDir, "refund-review-outbox.jsonl");
}

export const lookupAccount = createTool({
  id: "lookupAccount",
  description: "Look up a support account by its stable account ID.",
  inputSchema: z.object({ accountId: z.string() }),
  execute: async ({ accountId }) => {
    const accounts = await fixtureRecords<AccountFixture>(
      "accounts.v1.json",
      "accounts",
    );
    const account = accounts.find(
      (candidate) => candidate.accountId === accountId,
    );
    if (!account) {
      throw new Error(`Unknown account: ${accountId}`);
    }
    return account;
  },
});

export const lookupOrder = createTool({
  id: "lookupOrder",
  description: "Look up an order and its recorded charge count.",
  inputSchema: z.object({ orderId: z.string() }),
  execute: async ({ orderId }) => {
    const orders = await fixtureRecords<OrderFixture>(
      "orders.v1.json",
      "orders",
    );
    const order = orders.find((candidate) => candidate.orderId === orderId);
    if (!order) {
      throw new Error(`Unknown order: ${orderId}`);
    }
    return order;
  },
});

export const queueRefundReview = createTool({
  id: "queueRefundReview",
  description:
    "Append a refund-review request for an order. Call only after account and order checks.",
  inputSchema: z.object({ orderId: z.string() }),
  execute: async ({ orderId }) => {
    const path = outboxPath();
    await mkdir(dirname(path), { recursive: true });
    const event = {
      action: "refund_review_queued",
      orderId,
      queuedAt: new Date().toISOString(),
    };
    await appendFile(path, `${JSON.stringify(event)}\n`, "utf8");
    return { queued: true, orderId };
  },
});

export const supportTools = {
  lookupAccount,
  lookupOrder,
  queueRefundReview,
};
