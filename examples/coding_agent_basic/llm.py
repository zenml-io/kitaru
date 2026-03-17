"""Model setup and LLM tool-calling loop."""

import json
import logging
import os
from typing import Any

from litellm import completion

from kitaru.config import resolve_model_selection
from kitaru.llm import _resolve_credential_overlay  # noqa: PLC2701

try:
    from .tools import dispatch_tool
except (ImportError, SystemError):
    from tools import dispatch_tool

logger = logging.getLogger(__name__)

from kitaru.config import register_model_alias                                                                                                                                                             
                                                                                                                                                                                                             
register_model_alias(                                                                                                                                                                                      
    "coding-agent",                                                                                                                                                                                        
    model="anthropic/claude-sonnet-4-20250514",                                                                                                                                                            
    secret="anthropic-creds",
)

# ---------------------------------------------------------------------------
# Model setup
# ---------------------------------------------------------------------------

_selection = resolve_model_selection(
    os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
)
MODEL: str = _selection.resolved_model
_ENV_OVERLAY, _ = _resolve_credential_overlay(_selection)
os.environ.update(_ENV_OVERLAY)

_MAX_TOOL_ROUNDS: int = int(os.environ.get("CODING_AGENT_MAX_TOOL_ROUNDS", "30"))

# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------


def _chat(
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]] | None = None,
) -> Any:
    """Single LiteLLM completion call."""
    kwargs: dict[str, Any] = {"model": MODEL, "messages": messages}
    if tools:
        kwargs["tools"] = tools
    return completion(**kwargs)


def _message_to_dict(message: Any) -> dict[str, Any]:
    """Convert a LiteLLM message object to a plain dict."""
    result: dict[str, Any] = {"role": message.role, "content": message.content}
    if getattr(message, "tool_calls", None):
        result["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
            }
            for tc in message.tool_calls
        ]
    return result


def tool_loop(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    cwd: str,
    max_rounds: int = _MAX_TOOL_ROUNDS,
) -> tuple[str, int, int]:
    """Run LLM with tools until it produces a final text response.

    Returns (response_text, tool_calls_made, rounds_used).
    """
    tool_calls_made = 0

    for round_num in range(max_rounds):
        response = _chat(messages, tools=tools)
        assistant_msg = response.choices[0].message

        if not getattr(assistant_msg, "tool_calls", None):
            return (assistant_msg.content or "", tool_calls_made, round_num + 1)

        messages.append(_message_to_dict(assistant_msg))

        for tool_call in assistant_msg.tool_calls:
            name = tool_call.function.name
            try:
                arguments = json.loads(tool_call.function.arguments)
            except json.JSONDecodeError:
                arguments = {}

            logger.info("Tool call: %s(%s)", name, arguments)
            tool_result = dispatch_tool(cwd, name, arguments)
            tool_calls_made += 1

            messages.append(
                {"role": "tool", "tool_call_id": tool_call.id, "content": tool_result}
            )

    # Exhausted rounds — ask for a summary
    messages.append(
        {"role": "user", "content": "Tool call limit reached. Summarize what you accomplished."}
    )
    response = _chat(messages, tools=tools)
    return (response.choices[0].message.content or "", tool_calls_made, max_rounds)
