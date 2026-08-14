import { describe, expect, it } from "vitest";

import { parseTicketInput, ticketCases } from "../src/fixtures.js";
import { renderTicketPrompt } from "../src/models.js";
import { MockCommerceStore } from "../src/store.js";
import { createCommerceTools } from "../src/tools.js";

describe("canonical ticket fixtures", () => {
  it("cover ten distinct scenarios and all terminal action types", () => {
    expect(ticketCases).toHaveLength(10);
    expect(new Set(ticketCases.map(({ scenario }) => scenario))).toHaveLength(
      10,
    );
    expect(
      new Set(ticketCases.map(({ ticket }) => ticket.ticket_id)),
    ).toHaveLength(10);
    expect(
      new Set(ticketCases.map(({ expected_action }) => expected_action)),
    ).toEqual(new Set(["refund", "replacement", "escalate"]));
    expect(
      ticketCases.every(({ ticket }) => ticket.email.endsWith("@example.test")),
    ).toBe(true);
  });

  it("renders replay-safe prompts without fixture-only oracle fields", () => {
    for (const ticketCase of ticketCases) {
      const prompt = renderTicketPrompt(ticketCase.ticket);
      expect(prompt).toContain(ticketCase.ticket.ticket_id);
      expect(prompt).toContain(ticketCase.ticket.email);
      expect(prompt).toContain(ticketCase.ticket.subject);
      expect(prompt).toContain(ticketCase.ticket.body);
      expect(prompt).not.toContain(ticketCase.scenario);
      expect(prompt).not.toContain("expected_action");
    }
  });

  it("rejects malformed ticket fixture records at load boundaries", () => {
    expect(() =>
      parseTicketInput({
        ticket_id: "ticket-bad",
        customer_name: "No Email",
        subject: "Missing fixture field",
        body: "This record intentionally omits its email.",
      }),
    ).toThrow("ticket.email must be a string");
  });
});

describe("MockCommerceStore", () => {
  it("records a valid refund only in its own isolated store", () => {
    const store = new MockCommerceStore();
    const otherStore = new MockCommerceStore();

    const receipt = store.issueRefund("48213", 98);

    expect(receipt).toMatchObject({
      accepted: true,
      action: "refund",
      amount: 98,
      order_id: "48213",
      receipt_id: "mock-refund-48213",
    });
    expect(
      store.lookupOrder({ order_id: "48213" }).orders[0]?.already_refunded,
    ).toBe(true);
    expect(
      otherStore.lookupOrder({ order_id: "48213" }).orders[0]?.already_refunded,
    ).toBe(false);
    expect(otherStore.actions).toEqual([]);
  });

  it("rejects a second refund without changing the accepted receipt", () => {
    const store = new MockCommerceStore();

    const accepted = store.issueRefund("48213", 98);
    const duplicate = store.issueRefund("48213", 98);

    expect(accepted.accepted).toBe(true);
    expect(duplicate).toMatchObject({
      accepted: false,
      action: "refund",
      order_id: "48213",
    });
    expect(duplicate.receipt_id).toBeUndefined();
  });

  it.each([
    ["over-refund", "48213", 120],
    ["duplicate refund", "48219", 82],
    ["unknown order", "99999", 10],
  ])("rejects %s without mutating an order", (_case, orderId, amount) => {
    const store = new MockCommerceStore();
    const before = store.lookupOrder({ order_id: orderId });

    const receipt = store.issueRefund(orderId, amount);

    expect(receipt.accepted).toBe(false);
    expect(receipt.receipt_id).toBeUndefined();
    expect(store.lookupOrder({ order_id: orderId })).toEqual(before);
  });

  it("recovers from a wrong order number through email lookup", () => {
    const store = new MockCommerceStore();

    expect(store.lookupOrder({ order_id: "48228" }).found).toBe(false);
    expect(store.lookupOrder({ email: "riley@example.test" })).toMatchObject({
      found: true,
      orders: [{ order_id: "48222" }],
    });
  });

  it.each([
    ["backpack", "accessories"],
    ["carry-on", "luggage"],
    ["hoodie", "apparel"],
    ["jacket", "apparel"],
    ["loafers", "footwear"],
    ["shoes", "footwear"],
    ["tote", "accessories"],
  ])("normalizes the %s alias to %s", (alias, category) => {
    expect(
      new MockCommerceStore().getReturnPolicy(alias).policy?.category,
    ).toBe(category);
  });
});

describe("createCommerceTools", () => {
  it("creates the six canonical tools with a fresh store per invocation", () => {
    const first = createCommerceTools();
    const second = createCommerceTools();

    expect(Object.keys(first.tools)).toEqual([
      "lookup_order",
      "get_return_policy",
      "check_shipping",
      "issue_refund",
      "create_replacement",
      "escalate_to_human",
    ]);
    expect(first.store).not.toBe(second.store);
  });
});
