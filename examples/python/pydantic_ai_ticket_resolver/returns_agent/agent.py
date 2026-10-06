"""PydanticAI returns resolver run directly or by a Kitaru worker."""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from kitaru.task import get_task_inputs
from kitaru_pydantic_ai import KitaruAgent
from pydantic_ai import Agent
from pydantic_ai.models import KnownModelName, Model

from returns_agent.models import Order, Resolution, ReturnPolicy, TicketInput
from returns_agent.store import MockCommerceStore

MODEL: KnownModelName = "openai:gpt-5-nano"

_TASK_INSTRUCTIONS = (
    "You autonomously resolve one customer return or delivery ticket.\n\n"
    "Investigate the ticket with the available tools, choose one terminal "
    "outcome, execute any refund, replacement, or escalation before replying, "
    "then return the structured resolution. Use lookup_order before making "
    "claims about an order. For a return, refund, or replacement, call "
    "get_return_policy only after lookup_order and pass the exact category "
    "returned by lookup_order. Never call refund or replacement until a "
    "successful policy lookup returns found=true for that exact canonical "
    "category and establishes eligibility. If the lookup returns found=false, "
    "stop and escalate to a human; do not retry with a guessed category or "
    "take a terminal action. Use check_shipping for delivery problems.\n\n"
)

_BASELINE_POLICY = (
    "Apply the returned policy literally: verify the return window, whether "
    "the customer reported a defect, and the final-sale rule. A final-sale "
    "exception applies only when the customer reported a defect and the "
    "policy allows that exception. Preserve the policy human_approval_threshold "
    "and every risk_flags value from lookup_order before acting; escalate when "
    "the order has risk flags or the refund amount is at or above the approval "
    "threshold. Pass defect_claimed=true to refund or replacement only when "
    "the customer actually reported a defect. Escalate when the order cannot "
    "be identified, policy eligibility is not established, or no supported "
    "resolution is available.\n\n"
)

_REPLY_INSTRUCTIONS = (
    "The customer reply must accurately describe the accepted tool action. "
    "Address the customer by first name. Do not expose email addresses, "
    "internal risk flags, or mock receipt identifiers. All records and actions "
    "in this example are synthetic."
)

INSTRUCTIONS = _TASK_INSTRUCTIONS + _BASELINE_POLICY + _REPLY_INSTRUCTIONS


@dataclass
class _ReturnGuard:
    """Track the canonical evidence required before a return action."""

    order: Order | None = None
    policy: ReturnPolicy | None = None
    policy_found: bool = False
    policy_lookup_category: str | None = None

    def record_order(self, found: bool, orders: list[Order]) -> None:
        """Store the one order whose policy may authorize an action."""
        self.order = orders[0] if found and len(orders) == 1 else None
        self.policy = None
        self.policy_found = False
        self.policy_lookup_category = None

    def record_policy(
        self, requested_category: str, found: bool, policy: ReturnPolicy | None
    ) -> None:
        """Store the latest policy lookup without treating misses as eligible."""
        self.policy = policy if found else None
        self.policy_found = found and policy is not None
        self.policy_lookup_category = requested_category if found else None

    def get_block_reason(
        self,
        amount: Decimal | None,
        defect_claimed: bool,
    ) -> str | None:
        """Return why a refund or replacement must not be executed."""
        if self.order is None:
            return "canonical order lookup is missing or did not identify one order"
        if not self.policy_found or self.policy is None:
            return "successful canonical return-policy lookup is required"
        if self.policy_lookup_category != self.order.category:
            return "policy lookup did not use the lookup_order category"
        if self.policy.category != self.order.category:
            return "return policy category does not match lookup_order category"
        if self.order.risk_flags:
            return "order risk flags require human review"
        if self.order.amount_paid >= self.policy.human_approval_threshold:
            return "order amount meets or exceeds the policy human-approval threshold"
        if self.order.final_sale and not (
            defect_claimed and self.policy.final_sale_defect_exception
        ):
            return "final-sale order is eligible only for an allowed defect exception"
        if defect_claimed and not self.policy.defective_full_refund:
            return "policy does not allow a full refund for the reported defect"
        if not defect_claimed and not self.policy.unused_return:
            return "policy does not allow an unused return"
        if (
            self.order.days_since_delivery is None
            or self.order.days_since_delivery > self.policy.window_days
        ) and not (defect_claimed and self.policy.defective_full_refund):
            return "return is outside the policy window without an eligible defect"
        if amount is not None and amount >= self.policy.human_approval_threshold:
            return (
                "requested refund meets or exceeds the policy human-approval threshold"
            )
        return None


def get_instructions() -> str:
    """Build the resolver instructions."""
    return INSTRUCTIONS


def get_ticket_input(value: Any) -> TicketInput:
    """Unwrap the latest imported turn into one ticket input."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        value = turns[-1].get("inputs")
    return TicketInput.model_validate(value)


def build_prompt(ticket: TicketInput) -> str:
    """Render one incoming email without adding hidden case labels."""
    return (
        f"Ticket: {ticket.ticket_id}\n"
        f"From: {ticket.customer_name} <{ticket.email}>\n"
        f"Subject: {ticket.subject}\n\n"
        f"{ticket.body}"
    )


def build_agent(
    store: MockCommerceStore,
    model: Model | KnownModelName = MODEL,
) -> Agent[None, Resolution]:
    """Build the baseline resolver around one isolated mock store."""
    agent = Agent[None, Resolution](
        model,
        output_type=Resolution,
        instructions=get_instructions(),
        retries=2,
        model_settings={"openai_reasoning_summary": "auto"},
    )
    guard = _ReturnGuard()

    @agent.tool_plain
    def lookup_order(
        order_id: str | None = None, email: str | None = None
    ) -> dict[str, Any]:
        """Look up an order by exact order number or customer email."""
        result = store.lookup_order(order_id, email)
        guard.record_order(result.found, result.orders)
        return result.model_dump(mode="json")

    @agent.tool_plain
    def get_return_policy(category: str) -> dict[str, Any]:
        """Get the return window, defect rules, final-sale rule, and approval limit."""
        result = store.get_return_policy(category)
        guard.record_policy(category, result.found, result.policy)
        return result.model_dump(mode="json")

    @agent.tool_plain
    def check_shipping(tracking_no: str) -> dict[str, Any]:
        """Check carrier status for a shipped or missing order."""
        return store.check_shipping(tracking_no).model_dump(mode="json")

    @agent.tool_plain
    def issue_refund(
        order_id: str, amount: Decimal, defect_claimed: bool
    ) -> dict[str, Any]:
        """Record a refund only after the canonical policy guard approves it."""
        block_reason = guard.get_block_reason(amount, defect_claimed)
        if block_reason is not None or guard.order.order_id != order_id:
            reason = block_reason or "action order does not match the looked-up order"
            return {
                "accepted": False,
                "action": "refund",
                "order_id": order_id,
                "amount": str(amount),
                "message": f"Refund blocked: {reason}. Escalate to a human.",
            }
        return store.issue_refund(order_id, amount).model_dump(mode="json")

    @agent.tool_plain
    def create_replacement(order_id: str, defect_claimed: bool) -> dict[str, Any]:
        """Record a replacement only after the canonical policy guard approves it."""
        block_reason = guard.get_block_reason(None, defect_claimed)
        if block_reason is not None or guard.order.order_id != order_id:
            reason = block_reason or "action order does not match the looked-up order"
            return {
                "accepted": False,
                "action": "replacement",
                "order_id": order_id,
                "message": f"Replacement blocked: {reason}. Escalate to a human.",
            }
        return store.create_replacement(order_id).model_dump(mode="json")

    @agent.tool_plain
    def escalate_to_human(reason: str) -> dict[str, Any]:
        """Record a mock escalation with a concise internal reason."""
        return store.escalate_to_human(reason).model_dump(mode="json")

    return agent


async def main() -> None:
    """Resolve one replayed ticket and record its session in Kitaru."""
    ticket = get_ticket_input(get_task_inputs())
    pydantic_agent = build_agent(MockCommerceStore())
    agent = KitaruAgent(
        pydantic_agent,
        session_name=f"Returns ticket: {ticket.ticket_id}",
    )
    result = await agent.run(build_prompt(ticket))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
