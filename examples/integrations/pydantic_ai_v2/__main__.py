"""Run a PydanticAI agent through the Kitaru adapter."""

import asyncio
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic_ai import Agent

from plugins.adapters.pydantic_ai import KitaruAgent

PROMPT = """
Call both available tools exactly once:
1. Call get_current_utc_time.
2. Call multiply with left=17 and right=6.
After both tools return, report both results in one sentence. Do not calculate or
invent either result yourself.
""".strip()
_REQUIRED_ENV = ("OPENAI_API_KEY",)
_LANGFUSE_ENV = (
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
    "LANGFUSE_BASE_URL",
)
_LANGFUSE_ENABLED_ENV = "KITARU_EXAMPLE_LANGFUSE"


def get_current_utc_time() -> str:
    """Return the current UTC time from the local Python process."""
    return datetime.now(UTC).isoformat()


def multiply(left: int, right: int) -> int:
    """Multiply two integers."""
    return left * right


def _require_environment() -> None:
    """Raise a clear error when required configuration is missing."""
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if os.environ.get(_LANGFUSE_ENABLED_ENV) == "1":
        missing.extend(name for name in _LANGFUSE_ENV if not os.environ.get(name))
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"Missing required environment variables: {names}")
    if not os.environ.get("KITARU_TASK_ID") and not (
        os.environ.get("KITARU_AGENT_ID") or os.environ.get("KITARU_AGENT_VERSION_ID")
    ):
        raise RuntimeError(
            "Set KITARU_AGENT_ID or KITARU_AGENT_VERSION_ID for a local run"
        )


def _configure_langfuse() -> Any | None:
    """Enable optional PydanticAI tracing and return the Langfuse client."""
    if os.environ.get(_LANGFUSE_ENABLED_ENV) != "1":
        return None
    try:
        from langfuse import get_client
    except ImportError as exc:
        raise RuntimeError(
            "Langfuse tracing requires the 'langfuse' package. Run this example "
            "with: uv run --with langfuse python -m "
            "examples.integrations.pydantic_ai_v2"
        ) from exc

    Agent.instrument_all()
    return get_client()


async def main() -> None:
    """Run the example once and print the final answer."""
    _require_environment()
    agent_value = os.environ.get("KITARU_AGENT_ID")
    version_value = os.environ.get("KITARU_AGENT_VERSION_ID")
    langfuse = _configure_langfuse()
    pydantic_agent = Agent("openai:gpt-5-nano")
    pydantic_agent.tool_plain(get_current_utc_time)
    pydantic_agent.tool_plain(multiply)
    agent = KitaruAgent(
        pydantic_agent,
        agent_id=uuid.UUID(agent_value) if agent_value else None,
        agent_version_id=uuid.UUID(version_value) if version_value else None,
    )

    try:
        if langfuse is None:
            result = await agent.run(PROMPT)
            trace_id = None
        else:
            with langfuse.start_as_current_observation(
                as_type="span",
                name="kitaru-pydantic-ai-example",
                input=PROMPT,
            ) as observation:
                trace_id = langfuse.get_current_trace_id()
                result = await agent.run(PROMPT)
                observation.update(output=result.output)
    finally:
        if langfuse is not None:
            langfuse.flush()

    print(result.output)
    if trace_id is not None:
        print(f"langfuse_trace_id={trace_id}")


if __name__ == "__main__":
    asyncio.run(main())
