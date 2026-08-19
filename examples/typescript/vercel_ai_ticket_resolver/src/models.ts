export const resolutionActions = [
  "refund",
  "replacement",
  "escalate",
  "reject",
] as const;

export type ResolutionAction = (typeof resolutionActions)[number];

export interface TicketInput {
  ticket_id: string;
  customer_name: string;
  email: string;
  subject: string;
  body: string;
}

export interface Resolution {
  action: ResolutionAction;
  amount: number | null;
  reason: string;
  customer_reply: string;
}

export interface Order {
  order_id: string;
  email: string;
  product: string;
  category: string;
  amount_paid: number;
  status: string;
  days_since_delivery: number | null;
  final_sale: boolean;
  tracking_no: string | null;
  risk_flags: string[];
  already_refunded: boolean;
}

export interface ReturnPolicy {
  category: string;
  window_days: number;
  defective_full_refund: boolean;
  unused_return: boolean;
  final_sale_defect_exception: boolean;
  human_approval_threshold: number;
}

export interface PolicyLookup {
  found: boolean;
  policy?: ReturnPolicy;
  message: string;
}

export interface ShippingStatus {
  tracking_no: string;
  status: string;
  detail: string;
}

export interface OrderLookup {
  found: boolean;
  orders: Order[];
  message: string;
}

export interface ActionReceipt {
  accepted: boolean;
  action: ResolutionAction;
  order_id?: string;
  amount?: number;
  receipt_id?: string;
  message: string;
}

export interface TicketCase {
  scenario: string;
  ticket: TicketInput;
  expected_action: ResolutionAction;
}

export function renderTicketPrompt(ticket: TicketInput): string {
  return [
    `Ticket ID: ${ticket.ticket_id}`,
    `Customer: ${ticket.customer_name} <${ticket.email}>`,
    `Subject: ${ticket.subject}`,
    "",
    ticket.body,
  ].join("\n");
}
