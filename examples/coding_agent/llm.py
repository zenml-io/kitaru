"""Model setup and provider adapter for the coding agent.

Resolves the model alias, credentials, and dispatches tool-calling
completions through the correct provider SDK (OpenAI or Anthropic).

Provider routing, env overlay, and usage parsing mirror ``kitaru.llm()`` but
live here so this example does not depend on private ``kitaru.llm`` symbols
(which are not stable across Kitaru versions in remote images).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal

from models import LLMResponse, ToolCallFunction, ToolCallRequest
from zenml.client import Client

from kitaru.config import (
    ResolvedModelSelection,
    register_model_alias,
    resolve_model_selection,
)
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruUsageError

# ---------------------------------------------------------------------------
# Local LLM helpers (aligned with kitaru.llm; not imported from kitaru.llm)
# ---------------------------------------------------------------------------

ANTHROPIC_DEFAULT_MAX_TOKENS = 4096

_SUPPORTED_PROVIDERS = ("openai", "anthropic", "ollama", "openrouter")
_MODEL_PROVIDER_HINTS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "ollama": (),
    "openai": ("OPENAI_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
}


@dataclass(frozen=True)
class LLMUsage:
    """Normalized token usage for logging (matches kitaru agent metadata)."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(frozen=True)
class _ProviderTarget:
    provider: Literal["openai", "anthropic", "ollama", "openrouter"]
    provider_model: str
    resolved_model: str


def _provider_name(model: str) -> str | None:
    if "/" not in model:
        return None
    provider, _, _ = model.partition("/")
    normalized = provider.strip().lower()
    return normalized or None


def _provider_credential_keys(model: str) -> tuple[str, ...] | None:
    provider = _provider_name(model)
    if provider is None:
        return None
    return _MODEL_PROVIDER_HINTS.get(provider)


def _parse_provider_target(resolved_model: str) -> _ProviderTarget:
    if "/" not in resolved_model:
        raise KitaruUsageError(
            f"Model `{resolved_model}` does not include a provider prefix. "
            "Use a string like `openai/gpt-4o-mini` or "
            "`anthropic/claude-sonnet-4-20250514`, or register an alias that "
            "resolves to one."
        )

    provider, _, model_name = resolved_model.partition("/")
    provider = provider.strip().lower()
    model_name = model_name.strip()

    if not model_name:
        raise KitaruUsageError(
            f"Model `{resolved_model}` has an empty model name after the "
            "provider prefix."
        )

    if provider not in _SUPPORTED_PROVIDERS:
        supported = ", ".join(f"`{p}/*`" for p in _SUPPORTED_PROVIDERS)
        raise KitaruUsageError(
            f"Provider `{provider}` (from model `{resolved_model}`) is not "
            f"supported here. Built-in routing covers {supported}."
        )

    return _ProviderTarget(
        provider=provider,  # type: ignore[arg-type]
        provider_model=model_name,
        resolved_model=resolved_model,
    )


def _read_secret_values(secret_name: str) -> dict[str, str]:
    try:
        secret = Client().get_secret(
            name_id_or_prefix=secret_name,
            allow_partial_name_match=False,
            allow_partial_id_match=False,
        )
    except KeyError as exc:
        raise KitaruRuntimeError(f"Secret `{secret_name}` was not found.") from exc
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load secret `{secret_name}`: {exc}"
        ) from exc

    secret_values = getattr(secret, "secret_values", None)
    if not isinstance(secret_values, Mapping) or not secret_values:
        raise KitaruRuntimeError(
            f"Secret `{secret_name}` does not contain readable key/value pairs."
        )
    normalized_values: dict[str, str] = {}
    for key, value in secret_values.items():
        key_string = str(key).strip()
        if not key_string:
            continue
        if value is None:
            continue
        normalized_values[key_string] = str(value)

    if not normalized_values:
        raise KitaruRuntimeError(
            f"Secret `{secret_name}` does not contain non-empty values."
        )
    return normalized_values


def _resolve_credential_overlay(
    selection: ResolvedModelSelection,
) -> tuple[dict[str, str], str]:
    provider_keys = _provider_credential_keys(selection.resolved_model)

    if provider_keys:
        if any(os.environ.get(key) for key in provider_keys):
            return {}, "environment"

        if selection.secret is None:
            required_keys = ", ".join(provider_keys)
            raise KitaruRuntimeError(
                "No provider credentials found for "
                f"`{selection.resolved_model}`. Set one of [{required_keys}] in the "
                "environment or register an alias with `--secret` via "
                "`kitaru model register ...`."
            )
        secret_values = _read_secret_values(selection.secret)
        if not any(
            secret_values.get(key) or os.environ.get(key) for key in provider_keys
        ):
            required_keys = ", ".join(provider_keys)
            raise KitaruRuntimeError(
                f"Secret `{selection.secret}` does not provide required credential "
                f"keys for `{selection.resolved_model}`. Expected one of "
                f"[{required_keys}]."
            )
        return secret_values, "secret"

    if selection.secret is None:
        return {}, "environment"

    return _read_secret_values(selection.secret), "secret"


@contextmanager
def _temporary_env(additions: Mapping[str, str]) -> Any:
    previous_values: dict[str, str | None] = {}
    for key, value in additions.items():
        previous_values[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        yield
    finally:
        for key, previous in previous_values.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def _read_usage_int(usage_payload: Any, key: str) -> int | None:
    if usage_payload is None:
        return None
    if isinstance(usage_payload, Mapping):
        raw_value = usage_payload.get(key)
    else:
        raw_value = getattr(usage_payload, key, None)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _extract_usage_openai(raw_response: Any) -> LLMUsage:
    usage = getattr(raw_response, "usage", None)
    return LLMUsage(
        prompt_tokens=_read_usage_int(usage, "prompt_tokens"),
        completion_tokens=_read_usage_int(usage, "completion_tokens"),
        total_tokens=_read_usage_int(usage, "total_tokens"),
    )


def _extract_usage_anthropic(raw_response: Any) -> LLMUsage:
    usage = getattr(raw_response, "usage", None)
    input_tokens = _read_usage_int(usage, "input_tokens")
    output_tokens = _read_usage_int(usage, "output_tokens")
    total = None
    if input_tokens is not None and output_tokens is not None:
        total = input_tokens + output_tokens
    return LLMUsage(
        prompt_tokens=input_tokens,
        completion_tokens=output_tokens,
        total_tokens=total,
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

# Retries for transient API failures; recovery rounds append an error hint each time.
LLM_MAX_RETRIES: int = max(1, int(os.environ.get("CODING_AGENT_LLM_MAX_RETRIES", "3")))
LLM_RETRY_BACKOFF_CAP: int = max(
    1, int(os.environ.get("CODING_AGENT_LLM_RETRY_BACKOFF_MAX", "8"))
)
LLM_MAX_RECOVERY_ROUNDS: int = max(
    0, int(os.environ.get("CODING_AGENT_LLM_RECOVERY_ROUNDS", "2"))
)


# ---------------------------------------------------------------------------
# Provider config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AgentModelConfig:
    """Resolved model config for the coding agent."""

    resolved_model: str
    provider: Literal["openai", "anthropic", "ollama", "openrouter"]
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

        raise KitaruUsageError(
            f"Unsupported message role `{role}`. "
            "Supported roles: system, user, assistant, tool."
        )

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
) -> tuple[LLMResponse, LLMUsage]:
    """Execute one agent turn through the configured provider SDK."""
    config = get_agent_model_config()

    if config.provider == "openai":
        return _complete_openai(config, messages, tools)
    else:
        return _complete_anthropic(config, messages, tools)


def complete_agent_turn_resilient(
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]],
) -> tuple[LLMResponse, LLMUsage]:
    """Provider call with backoff retries and multiple self-healing turns.

    Each **recovery round** appends a user message describing the last failure
    so the model can adjust. Inner loop retries the same messages with
    exponential backoff before advancing to the next recovery round.
    """
    msgs: list[dict[str, Any]] = list(messages)
    last_exc: Exception | None = None

    for recovery_idx in range(LLM_MAX_RECOVERY_ROUNDS + 1):
        if recovery_idx > 0 and last_exc is not None:
            msgs.append(
                {
                    "role": "user",
                    "content": (
                        f"The model request failed ({type(last_exc).__name__}: "
                        f"{last_exc}). Continue with valid tool calls or a brief "
                        "answer; change approach if needed."
                    ),
                }
            )

        last_exc = None
        for attempt in range(LLM_MAX_RETRIES):
            try:
                return complete_agent_turn(msgs, tools=tools)
            except Exception as exc:
                last_exc = exc
                if attempt < LLM_MAX_RETRIES - 1:
                    backoff = min(2**attempt, LLM_RETRY_BACKOFF_CAP)
                    time.sleep(backoff)

        if recovery_idx == LLM_MAX_RECOVERY_ROUNDS:
            assert last_exc is not None
            raise last_exc


def _complete_openai(
    config: AgentModelConfig,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
) -> tuple[LLMResponse, LLMUsage]:
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
) -> tuple[LLMResponse, LLMUsage]:
    """Anthropic Messages API with tool calling."""
    from anthropic import Anthropic

    system, anthropic_messages = _messages_to_anthropic(messages)
    anthropic_tools = openai_tools_to_anthropic(tools)

    kwargs: dict[str, Any] = {
        "model": config.provider_model,
        "messages": anthropic_messages,
        "tools": anthropic_tools,
        "max_tokens": ANTHROPIC_DEFAULT_MAX_TOKENS,
    }
    if system:
        kwargs["system"] = system

    with _temporary_env(config.env_overlay):
        client = Anthropic()
        response = client.messages.create(**kwargs)

    llm_response = _anthropic_response_to_llm_response(response)
    usage = _extract_usage_anthropic(response)
    return llm_response, usage
