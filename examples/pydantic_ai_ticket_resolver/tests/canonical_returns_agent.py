"""Provider-free candidate agent for the canonical end-to-end contract."""

import asyncio
from decimal import Decimal
from importlib import import_module
from typing import Any

from examples.pydantic_ai_ticket_resolver.agent import (
    build_agent,
    build_prompt,
    get_ticket_input,
)
from examples.pydantic_ai_ticket_resolver.models import TicketInput
from examples.pydantic_ai_ticket_resolver.store import MockCommerceStore
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.function import FunctionModel

from kitaru.task import get_task_inputs

KitaruAgent = import_module("kitaru_pydantic_ai").KitaruAgent

CASES = {
    "ticket-001": ("48213", "footwear", "refund", Decimal("98.00")),
    "ticket-004": ("48216", "luggage", "escalate", Decimal("280.00")),
    "ticket-007": ("48218", "apparel", "escalate", Decimal("120.00")),
    "ticket-009": ("48220", "apparel", "refund", Decimal("80.00")),
    "ticket-010": ("48222", "footwear", "refund", Decimal("98.00")),
}


def _build_model(ticket: TicketInput) -> Model:
    """Build the deterministic candidate model used by the smoke test."""
    try:
        order_id, category, action, amount = CASES[ticket.ticket_id]
    except KeyError as exc:
        raise ValueError(f"The test model has no case for {ticket.ticket_id}.") from exc

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
            raise RuntimeError(f"Unexpected test tool result: {latest_tool}")
        if not info.output_tools:
            raise RuntimeError("The test model requires structured output.")
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
    """Resolve one replay input with the provider-free candidate."""
    ticket = get_ticket_input(get_task_inputs())
    pydantic_agent = build_agent(MockCommerceStore(), model=_build_model(ticket))
    agent = KitaruAgent(
        pydantic_agent,
        session_name=f"Returns ticket: {ticket.ticket_id}",
    )
    result = await agent.run(build_prompt(ticket))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
