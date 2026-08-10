"""PydanticAI returns resolver run directly or by a Kitaru worker."""

import asyncio
import os
from decimal import Decimal
from typing import Any, cast

from pydantic_ai import Agent
from pydantic_ai.models import KnownModelName, Model

from examples.pydantic_ai_ticket_resolver.models import Resolution, TicketInput
from examples.pydantic_ai_ticket_resolver.store import MockCommerceStore
from kitaru.task import get_task_inputs
from kitaru_pydantic_ai import KitaruAgent

MODEL = cast(KnownModelName, os.environ.get("BASELINE_MODEL", "openai:gpt-5-nano"))

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
    store: MockCommerceStore,
    model: Model | KnownModelName = MODEL,
    *,
    strict_policy: bool = False,
) -> Agent[None, Resolution]:
    """Build the baseline resolver around one isolated mock store."""
    agent = Agent[None, Resolution](
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


def _build_ci_model(ticket: TicketInput, strict_policy: bool) -> Model:
    """Build a provider-free model for the end-to-end CI walkthrough."""
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        ToolCallPart,
        ToolReturnPart,
    )
    from pydantic_ai.models.function import FunctionModel

    cases = {
        "ticket-001": ("48213", "footwear", "refund", Decimal("98.00")),
        "ticket-004": ("48216", "luggage", "refund", Decimal("280.00")),
        "ticket-007": ("48218", "apparel", "refund", Decimal("120.00")),
        "ticket-009": ("48220", "apparel", "refund", Decimal("80.00")),
        "ticket-010": ("48222", "footwear", "refund", Decimal("98.00")),
    }
    try:
        order_id, category, baseline_action, amount = cases[ticket.ticket_id]
    except KeyError as exc:
        raise ValueError(
            f"The CI model has no scenario for {ticket.ticket_id}."
        ) from exc
    action = (
        "escalate"
        if strict_policy and ticket.ticket_id in {"ticket-004", "ticket-007"}
        else baseline_action
    )

    def model(messages: list[Any], info: Any) -> Any:
        latest_tool: str | None = None
        for message in reversed(messages):
            if not isinstance(message, ModelRequest):
                continue
            for part in reversed(message.parts):
                if isinstance(part, ToolReturnPart):
                    latest_tool = part.tool_name
                    break
            if latest_tool is not None:
                break

        if latest_tool is None:
            lookup = (
                {"email": ticket.email}
                if ticket.ticket_id == "ticket-010"
                else {"order_id": order_id}
            )
            return ModelResponse(parts=[ToolCallPart("lookup_order", lookup)])
        if latest_tool == "lookup_order":
            return ModelResponse(
                parts=[ToolCallPart("get_return_policy", {"category": category})]
            )
        if latest_tool == "get_return_policy":
            if action == "escalate":
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            "escalate_to_human",
                            {"reason": "Refund requires human approval."},
                        )
                    ]
                )
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        "issue_refund",
                        {"order_id": order_id, "amount": str(amount)},
                    )
                ]
            )
        if latest_tool not in {"issue_refund", "escalate_to_human"}:
            raise RuntimeError(f"Unexpected CI tool result: {latest_tool}")
        if not info.output_tools:
            raise RuntimeError("The CI model requires structured output.")
        resolution = {
            "action": action,
            "amount": str(amount) if action == "refund" else None,
            "reason": (
                "Refund requires human approval."
                if action == "escalate"
                else "The reviewed refund is eligible."
            ),
            "customer_reply": (
                f"Hi {ticket.customer_name}, a specialist will review your request."
                if action == "escalate"
                else f"Hi {ticket.customer_name}, your refund has been issued."
            ),
        }
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, resolution)]
        )

    return FunctionModel(model, model_name="canonical-example-ci")


async def main() -> None:
    """Resolve one replayed ticket and record its session in Kitaru."""
    ticket = get_ticket_input(get_task_inputs())
    strict_policy = os.environ.get("RETURNS_POLICY_MODE") == "strict"
    model = (
        _build_ci_model(ticket, strict_policy)
        if os.environ.get("KITARU_EXAMPLE_TEST_MODEL") == "1"
        else MODEL
    )
    pydantic_agent = build_agent(
        MockCommerceStore(), model=model, strict_policy=strict_policy
    )
    agent = KitaruAgent(
        pydantic_agent,
        session_name=f"Returns ticket: {ticket.ticket_id}",
    )
    result = await agent.run(build_prompt(ticket))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
