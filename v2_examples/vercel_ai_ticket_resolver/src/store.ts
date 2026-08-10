import { orders, policies, shipments } from "./fixtures.js";
import type {
  ActionReceipt,
  Order,
  OrderLookup,
  PolicyLookup,
  ShippingStatus,
} from "./models.js";

const POLICY_ALIASES: Readonly<Record<string, string>> = {
  backpack: "accessories",
  "carry-on": "luggage",
  hoodie: "apparel",
  jacket: "apparel",
  loafers: "footwear",
  shoes: "footwear",
  tote: "accessories",
};

function cloneOrder(order: Order): Order {
  return { ...order, risk_flags: [...order.risk_flags] };
}

export class MockCommerceStore {
  readonly actions: ActionReceipt[] = [];
  readonly #orders = new Map(
    orders.map((order) => [order.order_id, cloneOrder(order)]),
  );

  lookupOrder(input: { order_id?: string; email?: string }): OrderLookup {
    if (input.order_id !== undefined) {
      const order = this.#orders.get(input.order_id);
      if (order !== undefined) {
        return {
          found: true,
          orders: [cloneOrder(order)],
          message: "One order matched the supplied order number.",
        };
      }
    }
    if (input.email !== undefined) {
      const matches = [...this.#orders.values()]
        .filter((order) => order.email === input.email)
        .map(cloneOrder);
      if (matches.length > 0) {
        return {
          found: true,
          orders: matches,
          message: `${matches.length} order(s) matched the supplied email.`,
        };
      }
    }
    return {
      found: false,
      orders: [],
      message: "No order matched the supplied information.",
    };
  }

  getReturnPolicy(category: string): PolicyLookup {
    const requested = category.toLowerCase();
    const normalized = POLICY_ALIASES[requested] ?? requested;
    const policy = policies.find(
      (candidate) => candidate.category === normalized,
    );
    if (policy === undefined) {
      return {
        found: false,
        message: `No policy matched ${JSON.stringify(category)}. Use the category returned by lookup_order.`,
      };
    }
    return {
      found: true,
      policy: { ...policy },
      message: `Policy matched canonical category ${JSON.stringify(normalized)}.`,
    };
  }

  checkShipping(trackingNo: string): ShippingStatus {
    const shipment = shipments.find(
      (candidate) => candidate.tracking_no === trackingNo,
    );
    return shipment === undefined
      ? {
          tracking_no: trackingNo,
          status: "delivered",
          detail: "Carrier reports the package as delivered.",
        }
      : { ...shipment };
  }

  issueRefund(orderId: string, amount: number): ActionReceipt {
    const order = this.#orders.get(orderId);
    if (order === undefined) {
      return this.#record({
        accepted: false,
        action: "refund",
        order_id: orderId,
        amount,
        message: "Refund rejected because the order does not exist.",
      });
    }
    if (order.already_refunded) {
      return this.#record({
        accepted: false,
        action: "refund",
        order_id: orderId,
        amount,
        message: "Refund rejected because the order was already refunded.",
      });
    }
    if (!Number.isFinite(amount) || amount <= 0 || amount > order.amount_paid) {
      return this.#record({
        accepted: false,
        action: "refund",
        order_id: orderId,
        amount,
        message: `Refund rejected. The maximum is ${order.amount_paid}.`,
      });
    }
    order.already_refunded = true;
    return this.#record({
      accepted: true,
      action: "refund",
      order_id: orderId,
      amount,
      receipt_id: `mock-refund-${orderId}`,
      message: "Mock refund recorded.",
    });
  }

  createReplacement(orderId: string): ActionReceipt {
    if (!this.#orders.has(orderId)) {
      return this.#record({
        accepted: false,
        action: "replacement",
        order_id: orderId,
        message: "Replacement rejected because the order does not exist.",
      });
    }
    return this.#record({
      accepted: true,
      action: "replacement",
      order_id: orderId,
      receipt_id: `mock-replacement-${orderId}`,
      message: "Mock replacement recorded.",
    });
  }

  escalateToHuman(reason: string): ActionReceipt {
    return this.#record({
      accepted: true,
      action: "escalate",
      receipt_id: `mock-escalation-${this.actions.length + 1}`,
      message: `Mock escalation recorded: ${reason}`,
    });
  }

  #record(receipt: ActionReceipt): ActionReceipt {
    this.actions.push(receipt);
    return receipt;
  }
}
