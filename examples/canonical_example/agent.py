"""PydanticAI returns resolver run directly or by a Kitaru worker."""

import asyncio
import os
from decimal import Decimal
from typing import Any

from pydantic_ai import Agent

from examples.canonical_example.models import Resolution, TicketInput
from examples.canonical_example.store import MockCommerceStore
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.task import get_task_inputs

MODEL = os.environ.get("BASELINE_MODEL", "openai:gpt-5-mini")

_TASK_INSTRUCTIONS = (
    "You autonomously resolve one customer return or delivery ticket.\n\n"
    "Investigate the ticket with the available tools, choose one terminal "
    "outcome, execute any refund, replacement, or escalation before replying, "
    "then return the structured resolution. Use lookup_order before making "
    "claims about an order. Use get_return_policy for return or refund "
    "decisions. Use check_shipping for delivery problems.\n\n"
)

_BASELINE_POLICY = (
    "Prioritize a fast, generous resolution. Customer-reported defects usually "
    "receive a full refund. Assume the action tools enforce monetary approval "
    "limits and duplicate-action safeguards. Escalate when the order cannot be "
    "identified or no supported resolution is available.\n\n"
)

_STRICT_POLICY = (
    "Apply the approval rules before taking an irreversible action. Escalate "
    "without calling issue_refund when the order contains any risk flag or the "
    "refund amount exceeds the policy's human approval threshold. Escalate "
    "final-sale returns unless the policy explicitly permits the reported "
    "defect, and escalate returns outside the policy window. Do not assume that "
    "an action tool enforces these policy rules for you.\n\n"
)

_REPLY_INSTRUCTIONS = (
    "The customer reply must accurately describe the accepted tool action. "
    "Address the customer by first name. Do not expose email addresses, "
    "internal risk flags, or mock receipt identifiers. All records and actions "
    "in this example are synthetic."
)

INSTRUCTIONS = _TASK_INSTRUCTIONS + _BASELINE_POLICY + _REPLY_INSTRUCTIONS


def get_instructions(strict_policy: bool = False) -> str:
    """Build the resolver instructions for one registered agent version."""
    policy = _STRICT_POLICY if strict_policy else _BASELINE_POLICY
    return _TASK_INSTRUCTIONS + policy + _REPLY_INSTRUCTIONS


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
    store: MockCommerceStore, model: str = MODEL, *, strict_policy: bool = False
) -> Agent[None, Resolution]:
    """Build the baseline resolver around one isolated mock store."""
    agent = Agent(
        model,
        output_type=Resolution,
        instructions=get_instructions(strict_policy),
        retries=2,
    )

    @agent.tool_plain
    def lookup_order(
        order_id: str | None = None, email: str | None = None
    ) -> dict[str, Any]:
        """Look up an order by exact order number or customer email."""
        return store.lookup_order(order_id, email).model_dump(mode="json")

    @agent.tool_plain
    def get_return_policy(category: str) -> dict[str, Any]:
        """Get the return window, defect rules, final-sale rule, and approval limit."""
        return store.get_return_policy(category).model_dump(mode="json")

    @agent.tool_plain
    def check_shipping(tracking_no: str) -> dict[str, Any]:
        """Check carrier status for a shipped or missing order."""
        return store.check_shipping(tracking_no).model_dump(mode="json")

    @agent.tool_plain
    def issue_refund(order_id: str, amount: Decimal) -> dict[str, Any]:
        """Record a mock refund; no payment processor is contacted."""
        return store.issue_refund(order_id, amount).model_dump(mode="json")

    @agent.tool_plain
    def create_replacement(order_id: str) -> dict[str, Any]:
        """Record a mock replacement; no fulfillment order is created."""
        return store.create_replacement(order_id).model_dump(mode="json")

    @agent.tool_plain
    def escalate_to_human(reason: str) -> dict[str, Any]:
        """Record a mock escalation with a concise internal reason."""
        return store.escalate_to_human(reason).model_dump(mode="json")

    return agent


async def main() -> None:
    """Resolve one replayed ticket and record its session in Kitaru."""
    ticket = get_ticket_input(get_task_inputs())
    strict_policy = os.environ.get("RETURNS_POLICY_MODE") == "strict"
    pydantic_agent = build_agent(MockCommerceStore(), strict_policy=strict_policy)
    agent = KitaruAgent(
        pydantic_agent,
        session_name=f"Returns ticket: {ticket.ticket_id}",
    )
    result = await agent.run(build_prompt(ticket))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
