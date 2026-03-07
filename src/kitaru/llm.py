"""LLM call primitive for tracked model interactions.

`kitaru.llm()` wraps one LiteLLM completion call with Kitaru tracking.
"""

import os
import re
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from litellm import completion
from pydantic import BaseModel, ConfigDict
from zenml.client import Client

from kitaru.artifacts import save
from kitaru.checkpoint import checkpoint
from kitaru.config import ResolvedModelSelection, resolve_model_selection
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint, _is_inside_flow, _next_llm_call_name

_LLM_OUTSIDE_FLOW_ERROR = "kitaru.llm() can only be called inside a @kitaru.flow."
_MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE"
_MODEL_PROVIDER_HINTS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "azure": ("AZURE_API_KEY", "AZURE_OPENAI_API_KEY"),
    "bedrock": (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ),
    "cohere": ("COHERE_API_KEY",),
    "gemini": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
    "google": ("GOOGLE_API_KEY", "GEMINI_API_KEY"),
    "groq": ("GROQ_API_KEY",),
    "mistral": ("MISTRAL_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "xai": ("XAI_API_KEY",),
}


class _LLMUsage(BaseModel):
    """Normalized usage/cost details returned by LiteLLM."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None


class _LLMRequest(BaseModel):
    """Normalized request payload used by `kitaru.llm()` internals."""

    prompt: str | list[dict[str, Any]]
    model: str | None = None
    system: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    call_name: str

    model_config = ConfigDict(arbitrary_types_allowed=True)


def _normalize_call_name(name: str | None) -> str:
    """Normalize optional user call names into ID-safe call names."""
    if name is None:
        return _next_llm_call_name()

    normalized_name = re.sub(r"\W+", "_", name.strip())
    if not normalized_name:
        raise KitaruUsageError("LLM call name cannot be empty.")
    if normalized_name[0].isdigit():
        normalized_name = f"llm_{normalized_name}"
    return normalized_name


def _provider_name(model: str) -> str | None:
    """Extract the provider prefix from a LiteLLM model identifier."""
    if "/" not in model:
        return None

    provider, _, _ = model.partition("/")
    normalized_provider = provider.strip().lower()
    return normalized_provider or None


def _provider_credential_keys(model: str) -> tuple[str, ...] | None:
    """Return known environment-variable credential keys for a model provider."""
    provider = _provider_name(model)
    if provider is None:
        return None
    return _MODEL_PROVIDER_HINTS.get(provider)


def _read_secret_values(secret_name: str) -> dict[str, str]:
    """Read secret key/value pairs from ZenML for env injection."""
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
            f"Failed to load secret `{secret_name}` for kitaru.llm(): {exc}"
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
    """Resolve env-first credentials with optional ZenML secret fallback."""
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


def _normalize_messages(
    prompt: str | list[dict[str, Any]],
    *,
    system: str | None,
) -> list[dict[str, Any]]:
    """Normalize string/chat prompt input into LiteLLM message format."""
    messages: list[dict[str, Any]] = []

    if system is not None:
        system_prompt = system.strip()
        if not system_prompt:
            raise KitaruUsageError("System prompt cannot be empty when provided.")
        messages.append({"role": "system", "content": system_prompt})

    if isinstance(prompt, str):
        prompt_value = prompt.strip()
        if not prompt_value:
            raise KitaruUsageError("Prompt cannot be empty.")
        messages.append({"role": "user", "content": prompt_value})
        return messages

    for message in prompt:
        if not isinstance(message, Mapping):
            raise KitaruUsageError(
                "Prompt message lists must contain dict-like items with `role` and "
                "`content` keys."
            )
        if "role" not in message or "content" not in message:
            raise KitaruUsageError(
                "Each prompt message must contain `role` and `content` keys."
            )
        messages.append(dict(message))

    if not messages:
        raise KitaruUsageError("Prompt message list cannot be empty.")
    return messages


def _extract_response_text(raw_response: Any) -> str:
    """Extract the text response from a LiteLLM completion response."""
    choices = getattr(raw_response, "choices", None)
    if choices is None and isinstance(raw_response, Mapping):
        choices = raw_response.get("choices")

    if not isinstance(choices, Sequence) or not choices:
        raise KitaruRuntimeError("LiteLLM returned no response choices.")
    first_choice = choices[0]
    if isinstance(first_choice, Mapping):
        message = first_choice.get("message")
        if isinstance(message, Mapping):
            content = message.get("content")
        else:
            content = first_choice.get("text")
    else:
        message = getattr(first_choice, "message", None)
        content = getattr(message, "content", None)
        if content is None:
            content = getattr(first_choice, "text", None)

    if isinstance(content, str):
        return content

    if isinstance(content, Sequence):
        text_parts: list[str] = []
        for part in content:
            if isinstance(part, Mapping) and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
            elif isinstance(part, str):
                text_parts.append(part)
        if text_parts:
            return "\n".join(text_parts)

    raise KitaruRuntimeError("LiteLLM returned an unsupported response content format.")


def _extract_usage(raw_response: Any) -> _LLMUsage:
    """Extract usage/cost values from a LiteLLM completion response."""
    usage_payload = getattr(raw_response, "usage", None)

    def _read_int(key: str) -> int | None:
        raw_value: Any
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

    cost_usd: float | None = None
    hidden_params = getattr(raw_response, "_hidden_params", None)
    if isinstance(hidden_params, Mapping):
        raw_cost = hidden_params.get("response_cost")
        if raw_cost is not None:
            try:
                cost_usd = float(raw_cost)
            except (TypeError, ValueError):
                cost_usd = None

    return _LLMUsage(
        prompt_tokens=_read_int("prompt_tokens"),
        completion_tokens=_read_int("completion_tokens"),
        total_tokens=_read_int("total_tokens"),
        cost_usd=cost_usd,
    )


@contextmanager
def _temporary_env(additions: Mapping[str, str]) -> Any:
    """Temporarily add/override environment variables for one call."""
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


def _execute_llm_call(request: _LLMRequest) -> str:
    """Execute one normalized LLM call and persist artifacts/metadata."""
    model_selection = resolve_model_selection(request.model)
    messages = _normalize_messages(request.prompt, system=request.system)
    env_overlay, credential_source = _resolve_credential_overlay(model_selection)

    completion_kwargs: dict[str, Any] = {
        "model": model_selection.resolved_model,
        "messages": messages,
    }
    if request.temperature is not None:
        completion_kwargs["temperature"] = request.temperature
    if request.max_tokens is not None:
        completion_kwargs["max_tokens"] = request.max_tokens
    if (mock_response := os.environ.get(_MOCK_RESPONSE_ENV)) is not None:
        completion_kwargs["mock_response"] = mock_response

    started_at = time.perf_counter()
    try:
        with _temporary_env(env_overlay):
            raw_response = completion(**completion_kwargs)
    except Exception as exc:
        raise KitaruBackendError(
            "kitaru.llm() failed while calling the provider backend "
            f"for model `{model_selection.resolved_model}`: {exc}"
        ) from exc
    latency_ms = round((time.perf_counter() - started_at) * 1000, 3)

    response_text = _extract_response_text(raw_response)
    usage = _extract_usage(raw_response)

    save(f"{request.call_name}_prompt", messages, type="prompt")
    save(f"{request.call_name}_response", response_text, type="response")

    llm_metadata: dict[str, Any] = {
        "requested_model": model_selection.requested_model,
        "alias": model_selection.alias,
        "resolved_model": model_selection.resolved_model,
        "credential_source": credential_source,
        "latency_ms": latency_ms,
        "tokens_input": usage.prompt_tokens,
        "tokens_output": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "cost_usd": usage.cost_usd,
    }
    filtered_metadata = {
        key: value for key, value in llm_metadata.items() if value is not None
    }
    log(llm_calls={request.call_name: filtered_metadata})

    return response_text


@checkpoint(type="llm_call")
def _llm_checkpoint_call(request: _LLMRequest) -> str:
    """Synthetic checkpoint used for flow-body `kitaru.llm()` calls."""
    return _execute_llm_call(request)


def llm(
    prompt: str | list[dict[str, Any]],
    *,
    model: str | None = None,
    system: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    name: str | None = None,
) -> str:
    """Make a tracked LLM call.

    Args:
        prompt: User prompt text or a chat-style message list.
        model: Model alias or concrete LiteLLM model identifier.
        system: Optional system prompt.
        temperature: Optional sampling temperature.
        max_tokens: Optional maximum response tokens.
        name: Optional display name for this call.

    Returns:
        The model response text.

    Raises:
        RuntimeError: If called outside a flow.
        ValueError: If prompt or model input is invalid.
    """
    if not _is_inside_flow():
        raise KitaruContextError(_LLM_OUTSIDE_FLOW_ERROR)
    request = _LLMRequest(
        prompt=prompt,
        model=model,
        system=system,
        temperature=temperature,
        max_tokens=max_tokens,
        call_name=_normalize_call_name(name),
    )

    if _is_inside_checkpoint():
        return _execute_llm_call(request)

    return _llm_checkpoint_call(request, id=request.call_name)
