"""Model setup and provider adapter for the coding agent.

Resolves the model alias, credentials, and dispatches tool-calling
completions through the correct provider SDK (OpenAI or Anthropic).
"""

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal

from models import LLMResponse, ToolCallFunction, ToolCallRequest

from kitaru.config import register_model_alias, resolve_model_selection
from kitaru.errors import KitaruUsageError
from kitaru.llm import (
    _ANTHROPIC_DEFAULT_MAX_TOKENS,
    _extract_usage_anthropic,
    _extract_usage_openai,
    _LLMUsage,
    _parse_provider_target,
    _resolve_credential_overlay,
    _temporary_env,
)

# ---------------------------------------------------------------------------
# Model registration (runs once at import time)
# ---------------------------------------------------------------------------

register_model_alias(
    "coding-agent",
    model="anthropic/claude-sonnet-4-20250514",
    secret="anthropic-creds",
)

MAX_TOOL_ROUNDS: int = int(os.environ.get("CODING_AGENT_MAX_TOOL_ROUNDS", "30"))


# ---------------------------------------------------------------------------
# Provider config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AgentModelConfig:
    """Resolved model config for the coding agent."""

    resolved_model: str
    provider: Literal["openai", "anthropic"]
    provider_model: str
    env_overlay: dict[str, str]


@lru_cache(maxsize=1)
def get_agent_model_config() -> AgentModelConfig:
    """Resolve the agent's model alias and credentials (cached)."""
    selection = resolve_model_selection(
        os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
    )
    env_overlay, _ = _resolve_credential_overlay(selection)
    target = _parse_provider_target(selection.resolved_model)

    return AgentModelConfig(
        resolved_model=target.resolved_model,
        provider=target.provider,
        provider_model=target.provider_model,
        env_overlay=env_overlay,
    )


MODEL: str = get_agent_model_config().resolved_model


# ---------------------------------------------------------------------------
# Anthropic tool schema + message translation
# ---------------------------------------------------------------------------


def openai_tools_to_anthropic(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert OpenAI function-calling tool schemas to Anthropic format."""
    anthropic_tools: list[dict[str, Any]] = []
    for tool in tools:
        func = tool["function"]
        anthropic_tools.append(
            {
                "name": func["name"],
                "description": func["description"],
                "input_schema": func["parameters"],
            }
        )
    return anthropic_tools


def _messages_to_anthropic(
    messages: list[dict[str, Any]],
) -> tuple[str | None, list[dict[str, Any]]]:
    """Convert canonical OpenAI-style messages to Anthropic format.

    Returns (system_prompt, anthropic_messages).
    """
    system_parts: list[str] = []
    anthropic_msgs: list[dict[str, Any]] = []

    for msg in messages:
        role = msg["role"]

        if role == "system":
            if anthropic_msgs:
                raise KitaruUsageError("System messages must appear at the beginning.")
            system_parts.append(msg["content"])
            continue

        if role == "user":
            anthropic_msgs.append(
                {"role": "user", "content": [{"type": "text", "text": msg["content"]}]}
            )
            continue

        if role == "assistant":
            content_blocks: list[dict[str, Any]] = []
            if msg.get("content"):
                content_blocks.append({"type": "text", "text": msg["content"]})
            for tc in msg.get("tool_calls", []):
                func = tc["function"]
                try:
                    input_data = json.loads(func["arguments"])
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Malformed tool call arguments for {func['name']}: {exc}"
                    ) from exc
                content_blocks.append(
                    {
                        "type": "tool_use",
                        "id": tc["id"],
                        "name": func["name"],
                        "input": input_data,
                    }
                )
            anthropic_msgs.append({"role": "assistant", "content": content_blocks})
            continue

        if role == "tool":
            # Anthropic expects tool results as user messages with tool_result blocks.
            # Coalesce consecutive tool messages into one user message.
            tool_result_block = {
                "type": "tool_result",
                "tool_use_id": msg["tool_call_id"],
                "content": msg["content"],
            }
            if (
                anthropic_msgs
                and anthropic_msgs[-1]["role"] == "user"
                and isinstance(anthropic_msgs[-1]["content"], list)
                and anthropic_msgs[-1]["content"]
                and anthropic_msgs[-1]["content"][0].get("type") == "tool_result"
            ):
                anthropic_msgs[-1]["content"].append(tool_result_block)
            else:
                anthropic_msgs.append({"role": "user", "content": [tool_result_block]})
            continue

    system = "\n\n".join(system_parts) if system_parts else None
    return system, anthropic_msgs


def _anthropic_response_to_llm_response(response: Any) -> LLMResponse:
    """Convert an Anthropic Messages response to our canonical LLMResponse."""
    text_parts: list[str] = []
    tool_calls: list[ToolCallRequest] = []

    for block in response.content:
        if getattr(block, "type", None) == "text":
            text_parts.append(block.text)
        elif getattr(block, "type", None) == "tool_use":
            tool_calls.append(
                ToolCallRequest(
                    id=block.id,
                    function=ToolCallFunction(
                        name=block.name,
                        arguments=json.dumps(block.input, sort_keys=True),
                    ),
                )
            )

    return LLMResponse(
        role="assistant",
        content="\n".join(text_parts) if text_parts else None,
        tool_calls=tool_calls if tool_calls else None,
    )


# ---------------------------------------------------------------------------
# Provider dispatch
# ---------------------------------------------------------------------------


def complete_agent_turn(
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]],
) -> tuple[LLMResponse, _LLMUsage]:
    """Execute one agent turn through the configured provider SDK."""
    config = get_agent_model_config()

    if config.provider == "openai":
        return _complete_openai(config, messages, tools)
    else:
        return _complete_anthropic(config, messages, tools)


def _complete_openai(
    config: AgentModelConfig,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
) -> tuple[LLMResponse, _LLMUsage]:
    """OpenAI Chat Completions with tool calling."""
    from openai import OpenAI

    with _temporary_env(config.env_overlay):
        client = OpenAI()
        response = client.chat.completions.create(
            model=config.provider_model,
            messages=messages,
            tools=tools,
        )

    msg = response.choices[0].message
    tool_calls = None
    if msg.tool_calls:
        tool_calls = [
            ToolCallRequest(
                id=tc.id,
                function=ToolCallFunction(
                    name=tc.function.name,
                    arguments=tc.function.arguments,
                ),
            )
            for tc in msg.tool_calls
        ]

    llm_response = LLMResponse(
        role=msg.role, content=msg.content, tool_calls=tool_calls
    )
    usage = _extract_usage_openai(response)
    return llm_response, usage


def _complete_anthropic(
    config: AgentModelConfig,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
) -> tuple[LLMResponse, _LLMUsage]:
    """Anthropic Messages API with tool calling."""
    from anthropic import Anthropic

    system, anthropic_messages = _messages_to_anthropic(messages)
    anthropic_tools = openai_tools_to_anthropic(tools)

    kwargs: dict[str, Any] = {
        "model": config.provider_model,
        "messages": anthropic_messages,
        "tools": anthropic_tools,
        "max_tokens": _ANTHROPIC_DEFAULT_MAX_TOKENS,
    }
    if system:
        kwargs["system"] = system

    with _temporary_env(config.env_overlay):
        client = Anthropic()
        response = client.messages.create(**kwargs)

    llm_response = _anthropic_response_to_llm_response(response)
    usage = _extract_usage_anthropic(response)
    return llm_response, usage
