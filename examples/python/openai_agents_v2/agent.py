"""Run an OpenAI Agents SDK agent through the `kitaru-openai-agents` adapter."""

import argparse
import os
import uuid

from agents import Agent, function_tool

from kitaru_openai_agents import KitaruRunner

_DEFAULT_PROMPT = "Use the order lookup tool for ORD-1007"


@function_tool
def lookup_order(order_id: str) -> str:
    """Return deterministic shipping information for an example order."""
    if order_id == "ORD-1007":
        return "Order ORD-1007 shipped and is due tomorrow."
    return f"No order found for {order_id}."


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the Kitaru OpenAI Agents v2 example (kitaru-openai-agents)."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default=_DEFAULT_PROMPT,
        help="prompt to send to the agent",
    )
    return parser.parse_args()


def _get_uuid(name: str) -> uuid.UUID | None:
    """Read an optional UUID from the environment."""
    value = os.environ.get(name)
    return uuid.UUID(value) if value else None


def _require_environment() -> None:
    """Raise a clear error when a real run lacks required configuration."""
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("Set OPENAI_API_KEY before running the example")
    if not os.environ.get("KITARU_TASK_ID") and not (
        os.environ.get("KITARU_AGENT_ID") or os.environ.get("KITARU_AGENT_VERSION_ID")
    ):
        raise RuntimeError(
            "Set KITARU_AGENT_ID or KITARU_AGENT_VERSION_ID for a standalone run"
        )


def main() -> None:
    """Run the example once and print the native result's final output."""
    args = _parse_args()
    _require_environment()
    agent = Agent(
        name="order_support",
        instructions=(
            "Use lookup_order when the user asks about an order. "
            "Answer in one concise sentence."
        ),
        model="gpt-5-nano",
        tools=[lookup_order],
    )
    runner = KitaruRunner(
        agent_id=_get_uuid("KITARU_AGENT_ID"),
        agent_version_id=_get_uuid("KITARU_AGENT_VERSION_ID"),
        session_name="openai-agents-v2-example",
    )
    result = runner.run_sync(agent, args.prompt)
    print(result.final_output)


if __name__ == "__main__":
    main()
