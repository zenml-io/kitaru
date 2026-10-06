"""PydanticAI returns resolver run directly or by a Kitaru worker."""

import asyncio
from decimal import Decimal
from typing import Any

from kitaru.task import get_task_inputs
from kitaru_pydantic_ai import KitaruAgent
from pydantic_ai import Agent, ModelRetry
from pydantic_ai.models import KnownModelName, Model
from pydantic_ai.tools import RunContext

from returns_agent.models import (
    ActionReceipt,
    Order,
    OrderLookup,
    PolicyLookup,
    Resolution,
    ResolutionAction,
    ReturnPolicy,
    TicketInput,
)
from returns_agent.store import MockCommerceStore

MODEL: KnownModelName = "openai:gpt-5-nano"

_TASK_INSTRUCTIONS = (
    "You autonomously resolve one customer return or delivery ticket.\n\n"
    "Investigate the ticket with the available tools, choose one terminal "
    "outcome, execute any refund, replacement, or escalation before replying, "
    "then return the structured resolution. Use lookup_order before making "
    "claims about an order. Use get_return_policy for return or refund "
    "decisions. Use check_shipping for delivery problems. If a policy lookup "
    "returns no match, retry once with the exact category from the order; if "
    "that also fails, escalate and do not refund, replace, or claim that you did.\n\n"
)

_BASELINE_POLICY = (
    "Prioritize a fast, generous resolution. Customer-reported defects usually "
    "receive a full refund. Only treat a refund or replacement as complete when "
    "its action tool returns accepted=true. Escalate when the order cannot be "
    "identified, policy remains unavailable, or no supported resolution exists.\n\n"
)

_REPLY_INSTRUCTIONS = (
    "The customer reply must accurately describe the accepted tool action. "
    "Address the customer by first name. Do not expose email addresses, "
    "internal risk flags, or mock receipt identifiers. All records and actions "
    "in this example are synthetic."
)

INSTRUCTIONS = _TASK_INSTRUCTIONS + _BASELINE_POLICY + _REPLY_INSTRUCTIONS


class _ResolutionGuard:
    """Track the verified order and policy required for terminal actions."""

    def __init__(self) -> None:
        self.order: Order | None = None
        self.policy: ReturnPolicy | None = None
        self.last_action: ActionReceipt | None = None

    def record_order_lookup(self, result: OrderLookup) -> None:
        """Store one unambiguous order lookup and clear stale policy state."""
        self.order = (
            result.orders[0] if result.found and len(result.orders) == 1 else None
        )
        self.policy = None
        self.last_action = None

    def record_policy_lookup(self, result: PolicyLookup) -> None:
        """Store a policy only when it matches the order's canonical category."""
        if (
            self.order is not None
            and result.found
            and result.policy is not None
            and result.policy.category == self.order.category
        ):
            self.policy = result.policy
        else:
            self.policy = None

    def get_action_block(self, order_id: str, action: ResolutionAction) -> str | None:
        """Explain why a refund or replacement cannot run yet."""
        if self.order is None:
            return "lookup_order must find exactly one order before taking an action."
        if self.order.order_id != order_id:
            return f"The verified order is {self.order.order_id}; look it up before acting."
        if self.policy is None:
            return (
                "get_return_policy must return a valid policy for the order's "
                f"canonical category ({self.order.category}) before {action.value}."
            )
        return None

    def record_action(self, receipt: ActionReceipt) -> ActionReceipt:
        """Remember the latest action receipt for output validation."""
        self.last_action = receipt
        return receipt

    def validate_resolution(self, resolution: Resolution) -> Resolution:
        """Prevent the final response from claiming an unrecorded action."""
        if resolution.action in {
            ResolutionAction.REFUND,
            ResolutionAction.REPLACEMENT,
            ResolutionAction.ESCALATE,
        }:
            receipt = self.last_action
            if (
                receipt is None
                or not receipt.accepted
                or receipt.action != resolution.action
            ):
                raise ModelRetry(
                    "The claimed terminal action was not accepted by its tool. "
                    "Retry the required lookup or escalate to a human."
                )
        return resolution


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
    guard = _ResolutionGuard()
    agent = Agent[None, Resolution](
        model,
        output_type=Resolution,
        instructions=get_instructions(),
        retries=2,
        model_settings={"openai_reasoning_summary": "auto"},
    )

    @agent.output_validator
    def validate_resolution(
        ctx: RunContext[None], resolution: Resolution
    ) -> Resolution:
        """Require the final response to match an accepted terminal action."""
        del ctx
        return guard.validate_resolution(resolution)

    @agent.tool_plain
    def lookup_order(
        order_id: str | None = None, email: str | None = None
    ) -> dict[str, Any]:
        """Look up an order by exact order number or customer email."""
        result = store.lookup_order(order_id, email)
        guard.record_order_lookup(result)
        return result.model_dump(mode="json")

    @agent.tool_plain
    def get_return_policy(category: str) -> dict[str, Any]:
        """Get the policy and require its canonical category to match the order."""
        result = store.get_return_policy(category)
        if result.found and result.policy is not None and guard.order is not None:
            if result.policy.category != guard.order.category:
                result = PolicyLookup(
                    found=False,
                    message=(
                        f"Policy category {result.policy.category!r} does not match "
                        f"the order category {guard.order.category!r}. Retry with "
                        "the category returned by lookup_order."
                    ),
                )
        guard.record_policy_lookup(result)
        return result.model_dump(mode="json")

    @agent.tool_plain
    def check_shipping(tracking_no: str) -> dict[str, Any]:
        """Check carrier status for a shipped or missing order."""
        return store.check_shipping(tracking_no).model_dump(mode="json")

    @agent.tool_plain
    def issue_refund(order_id: str, amount: Decimal) -> dict[str, Any]:
        """Record a mock refund only after order and policy verification."""
        block = guard.get_action_block(order_id, ResolutionAction.REFUND)
        if block is not None:
            return guard.record_action(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REFUND,
                    order_id=order_id,
                    amount=amount,
                    message=f"Refund blocked: {block}",
                )
            ).model_dump(mode="json")
        return guard.record_action(store.issue_refund(order_id, amount)).model_dump(
            mode="json"
        )

    @agent.tool_plain
    def create_replacement(order_id: str) -> dict[str, Any]:
        """Record a mock replacement only after order and policy verification."""
        block = guard.get_action_block(order_id, ResolutionAction.REPLACEMENT)
        if block is not None:
            return guard.record_action(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REPLACEMENT,
                    order_id=order_id,
                    message=f"Replacement blocked: {block}",
                )
            ).model_dump(mode="json")
        return guard.record_action(store.create_replacement(order_id)).model_dump(
            mode="json"
        )

    @agent.tool_plain
    def escalate_to_human(reason: str) -> dict[str, Any]:
        """Record a mock escalation with a concise internal reason."""
        return guard.record_action(store.escalate_to_human(reason)).model_dump(
            mode="json"
        )

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
