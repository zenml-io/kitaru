"""Candidate support agent run by Kitaru workers."""

import asyncio
import json
import os
import uuid
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.task import get_task_inputs

ORDER_DATA = {
    "A-1042": {
        "action": "share_tracking",
        "eta": "2026-08-03",
        "order_id": "A-1042",
        "status": "shipped",
    },
    "A-2088": {
        "action": "confirm_refund",
        "amount": "49.00 EUR",
        "order_id": "A-2088",
        "status": "refunded",
    },
    "A-3091": {
        "action": "explain_address_lock",
        "order_id": "A-3091",
        "status": "out_for_delivery",
    },
}


def _get_prompt_inputs(messages: list[ModelMessage]) -> dict[str, Any]:
    """Read the JSON input projected into the PydanticAI prompt."""
    for message in reversed(messages):
        if not isinstance(message, ModelRequest):
            continue
        for part in message.parts:
            if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                value = json.loads(part.content)
                if isinstance(value, dict):
                    return value
    raise ValueError("The agent prompt does not contain an input object.")


def _support_model(messages: list[ModelMessage], _: AgentInfo) -> ModelResponse:
    """Call order lookup once, then return its structured result."""
    for message in reversed(messages):
        if not isinstance(message, ModelRequest):
            continue
        for part in message.parts:
            if isinstance(part, ToolReturnPart):
                return ModelResponse(
                    parts=[TextPart(json.dumps(part.content, sort_keys=True))]
                )
    inputs = _get_prompt_inputs(messages)
    return ModelResponse(
        parts=[ToolCallPart("lookup_order", {"order_id": inputs["order_id"]})]
    )


def lookup_order(order_id: str) -> dict[str, str]:
    """Look up the current support outcome for an order."""
    return ORDER_DATA[order_id]


async def main() -> None:
    """Run the candidate against the input supplied by the worker."""
    inputs = get_task_inputs()
    if not isinstance(inputs, dict):
        raise RuntimeError("This agent must run as a Kitaru task.")
    pydantic_agent = Agent(FunctionModel(_support_model, model_name="support-v2"))
    pydantic_agent.tool_plain(lookup_order)
    version_value = os.environ.get("KITARU_AGENT_VERSION_ID")
    agent = KitaruAgent(
        pydantic_agent,
        agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]),
        agent_version_id=uuid.UUID(version_value) if version_value else None,
    )
    result = await agent.run(json.dumps(inputs, sort_keys=True))
    print(result.output)


if __name__ == "__main__":
    asyncio.run(main())
