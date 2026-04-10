"""LLM call primitive for tracked model interactions.

`kitaru.llm()` wraps one provider SDK completion call with Kitaru tracking.
Built-in runtime support covers ``openai/*``, ``anthropic/*``, ``ollama/*``,
and ``openrouter/*`` models. Ollama and OpenRouter use the OpenAI-compatible
API and require the ``openai`` package (``pip install kitaru[openai]``).
"""

import os
import re
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from zenml.client import Client

from kitaru._safe_save import _safe_save
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

_LLM_OUTSIDE_FLOW_ERROR = "kitaru.llm() can only be called inside a @flow."
_MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE"
_ANTHROPIC_DEFAULT_MAX_TOKENS = 4096
_OLLAMA_HOST_ENV = "OLLAMA_HOST"
_OLLAMA_DEFAULT_HOST = "http://localhost:11434"
_OLLAMA_DUMMY_API_KEY = "ollama"  # Ollama needs no auth; prevents OpenAI SDK env lookup
_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

_SUPPORTED_PROVIDERS = ("openai", "anthropic", "ollama", "openrouter")

_MODEL_PROVIDER_HINTS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "ollama": (),
    "openai": ("OPENAI_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
}


class _LLMUsage(BaseModel):
    """Normalized usage details from a provider SDK response."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class _LLMRequest(BaseModel):
    """Normalized request payload used by `kitaru.llm()` internals."""

    prompt: str | list[dict[str, Any]]
    model: str | None = None
    system: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    call_name: str

    model_config = ConfigDict(arbitrary_types_allowed=True)


@dataclass(frozen=True)
class _ProviderTarget:
    """Parsed routing result for a resolved model string."""

    provider: Literal["openai", "anthropic", "ollama", "openrouter"]
    provider_model: str
    resolved_model: str


@dataclass(frozen=True)
class _ProviderCallResult:
    """Normalized boundary between provider SDK response and Kitaru persistence."""

    response_text: str
    usage: _LLMUsage


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Provider routing
# ---------------------------------------------------------------------------


def _provider_name(model: str) -> str | None:
    """Extract the provider prefix from a provider/model identifier."""
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


def _parse_provider_target(resolved_model: str) -> _ProviderTarget:
    """Parse a resolved model string into a provider routing target.

    Raises:
        KitaruUsageError: If the model string has no provider prefix or the
            provider is not supported by the built-in runtime.
    """
    if "/" not in resolved_model:
        raise KitaruUsageError(
            f"Model `{resolved_model}` does not include a provider prefix. "
            "The built-in kitaru.llm() runtime requires a provider-qualified "
            "model string like `openai/gpt-5-nano`, "
            "`anthropic/claude-sonnet-4-20250514`, or `ollama/qwen3.5`. "
            "If you registered an alias, make sure it resolves to a "
            "provider/model string. For other providers, call the SDK "
            "directly inside a @checkpoint."
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
            "supported by the built-in kitaru.llm() runtime. "
            f"Built-in support covers {supported}. "
            "For other providers, call the SDK directly inside a @checkpoint."
        )

    return _ProviderTarget(
        provider=provider,  # type: ignore[arg-type]
        provider_model=model_name,
        resolved_model=resolved_model,
    )


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Message normalization
# ---------------------------------------------------------------------------


def _normalize_messages(
    prompt: str | list[dict[str, Any]],
    *,
    system: str | None,
) -> list[dict[str, Any]]:
    """Normalize string/chat prompt input into a canonical message list."""
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


# ---------------------------------------------------------------------------
# Provider SDK helpers (lazy imports)
# ---------------------------------------------------------------------------


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


def _call_openai(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float | None,
    max_tokens: int | None,
    env_overlay: Mapping[str, str],
    base_url: str | None = None,
    api_key: str | None = None,
    provider_label: str = "openai",
) -> _ProviderCallResult:
    """Execute one OpenAI-compatible Chat Completions call.

    Used directly for OpenAI, and with ``base_url``/``api_key`` overrides
    for Ollama and OpenRouter.
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise KitaruUsageError(
            f"Model '{provider_label}/{model}' requires the openai package. "
            "Install with: pip install kitaru[openai]"
        ) from None

    kwargs: dict[str, Any] = {"model": model, "messages": messages}
    if temperature is not None:
        kwargs["temperature"] = temperature
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens

    client_kwargs: dict[str, Any] = {}
    if base_url is not None:
        client_kwargs["base_url"] = base_url
    if api_key is not None:
        client_kwargs["api_key"] = api_key

    with _temporary_env(env_overlay):
        client = OpenAI(**client_kwargs)
        try:
            response = client.chat.completions.create(**kwargs)
        except Exception as exc:
            raise KitaruBackendError(
                f"kitaru.llm() failed while calling {provider_label} for "
                f"model `{provider_label}/{model}`: {exc}"
            ) from exc

    return _ProviderCallResult(
        response_text=_extract_response_text_openai(response),
        usage=_extract_usage_openai(response),
    )


def _call_anthropic(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float | None,
    max_tokens: int | None,
    env_overlay: Mapping[str, str],
) -> _ProviderCallResult:
    """Execute one Anthropic Messages API call."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise KitaruUsageError(
            f"Model 'anthropic/{model}' requires the anthropic package. "
            "Install with: pip install kitaru[anthropic]"
        ) from None

    # Separate system messages from the conversation
    system_parts: list[str] = []
    anthropic_messages: list[dict[str, Any]] = []
    seen_non_system = False
    for msg in messages:
        if msg["role"] == "system":
            if seen_non_system:
                raise KitaruUsageError(
                    "System messages must appear at the beginning of the "
                    "message list. Anthropic does not support interleaved "
                    "system messages."
                )
            system_parts.append(msg["content"])
        else:
            seen_non_system = True
            anthropic_messages.append(msg)

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": anthropic_messages,
        "max_tokens": max_tokens
        if max_tokens is not None
        else _ANTHROPIC_DEFAULT_MAX_TOKENS,
    }
    if system_parts:
        kwargs["system"] = "\n\n".join(system_parts)
    if temperature is not None:
        kwargs["temperature"] = temperature

    with _temporary_env(env_overlay):
        client = Anthropic()
        try:
            response = client.messages.create(**kwargs)
        except Exception as exc:
            raise KitaruBackendError(
                f"kitaru.llm() failed while calling Anthropic for model "
                f"`anthropic/{model}`: {exc}"
            ) from exc

    return _ProviderCallResult(
        response_text=_extract_response_text_anthropic(response),
        usage=_extract_usage_anthropic(response),
    )


# ---------------------------------------------------------------------------
# Response parsing (provider-aware)
# ---------------------------------------------------------------------------


def _extract_response_text_openai(raw_response: Any) -> str:
    """Extract text from an OpenAI Chat Completions response."""
    choices = getattr(raw_response, "choices", None)
    if choices is None and isinstance(raw_response, Mapping):
        choices = raw_response.get("choices")

    if not isinstance(choices, Sequence) or not choices:
        raise KitaruRuntimeError(
            "OpenAI returned no response choices. kitaru.llm() is a "
            "text-only primitive."
        )
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

    raise KitaruRuntimeError(
        "OpenAI returned no text content. kitaru.llm() is a text-only "
        "primitive — for tool calling or structured output, call the "
        "provider SDK directly inside a @checkpoint."
    )


def _extract_response_text_anthropic(raw_response: Any) -> str:
    """Extract text from an Anthropic Messages response."""
    content = getattr(raw_response, "content", None)
    if content is None and isinstance(raw_response, Mapping):
        content = raw_response.get("content")

    if not isinstance(content, Sequence) or not content:
        raise KitaruRuntimeError(
            "Anthropic returned no response content. kitaru.llm() is a "
            "text-only primitive."
        )

    text_parts: list[str] = []
    for block in content:
        block_type: str | None = None
        block_text: str | None = None
        if isinstance(block, Mapping):
            block_type = block.get("type")
            block_text = block.get("text")
        else:
            block_type = getattr(block, "type", None)
            block_text = getattr(block, "text", None)

        if block_type == "text" and isinstance(block_text, str) and block_text:
            text_parts.append(block_text)

    if text_parts:
        return "\n".join(text_parts)

    raise KitaruRuntimeError(
        "Anthropic returned no text content. kitaru.llm() is a text-only "
        "primitive — for tool calling or structured output, call the "
        "provider SDK directly inside a @checkpoint."
    )


# ---------------------------------------------------------------------------
# Usage extraction (provider-aware)
# ---------------------------------------------------------------------------


def _read_usage_int(usage_payload: Any, key: str) -> int | None:
    """Read an integer field from a usage payload (Mapping or object)."""
    if usage_payload is None:
        return None
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


def _extract_usage_openai(raw_response: Any) -> _LLMUsage:
    """Extract usage from an OpenAI Chat Completions response."""
    usage = getattr(raw_response, "usage", None)
    return _LLMUsage(
        prompt_tokens=_read_usage_int(usage, "prompt_tokens"),
        completion_tokens=_read_usage_int(usage, "completion_tokens"),
        total_tokens=_read_usage_int(usage, "total_tokens"),
    )


def _extract_usage_anthropic(raw_response: Any) -> _LLMUsage:
    """Extract usage from an Anthropic Messages response."""
    usage = getattr(raw_response, "usage", None)
    input_tokens = _read_usage_int(usage, "input_tokens")
    output_tokens = _read_usage_int(usage, "output_tokens")
    total = None
    if input_tokens is not None and output_tokens is not None:
        total = input_tokens + output_tokens
    return _LLMUsage(
        prompt_tokens=input_tokens,
        completion_tokens=output_tokens,
        total_tokens=total,
    )


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------


def _dispatch_provider_call(
    *,
    model_selection: ResolvedModelSelection,
    messages: list[dict[str, str]],
    temperature: float | None,
    max_tokens: int | None,
    env_overlay: dict[str, str] | None = None,
) -> _ProviderCallResult:
    """Route a normalized LLM call to the correct provider SDK.

    Shared by ``_execute_llm_call`` (flow-scoped, with tracking) and
    ``_compact_impl`` (admin operation, no tracking needed).
    """
    if env_overlay is None:
        env_overlay, _ = _resolve_credential_overlay(model_selection)
    target = _parse_provider_target(model_selection.resolved_model)
    if target.provider == "openai":
        return _call_openai(
            model=target.provider_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            env_overlay=env_overlay,
        )
    if target.provider == "anthropic":
        return _call_anthropic(
            model=target.provider_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            env_overlay=env_overlay,
        )
    if target.provider in ("ollama", "openrouter"):
        if target.provider == "ollama":
            ollama_host = os.environ.get(_OLLAMA_HOST_ENV, _OLLAMA_DEFAULT_HOST)
            compat_base_url = ollama_host.rstrip("/") + "/v1"
            compat_api_key: str | None = _OLLAMA_DUMMY_API_KEY
        else:
            compat_base_url = _OPENROUTER_BASE_URL
            key_name = _MODEL_PROVIDER_HINTS["openrouter"][0]
            compat_api_key = env_overlay.get(key_name) or os.environ.get(key_name)
        return _call_openai(
            model=target.provider_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            env_overlay=env_overlay,
            base_url=compat_base_url,
            api_key=compat_api_key,
            provider_label=target.provider,
        )
    raise KitaruUsageError(f"Provider `{target.provider}` is not supported.")


def _track_llm_call_analytics(
    *,
    model_selection: ResolvedModelSelection,
    credential_source: str,
    mocked: bool,
    extra_metadata: Mapping[str, Any] | None = None,
) -> None:
    """Emit the canonical `LLM_CALLED` analytics event."""
    from kitaru.analytics import AnalyticsEvent, track

    metadata: dict[str, Any] = {
        "resolved_model": model_selection.resolved_model,
        "credential_source": credential_source,
        "mocked": mocked,
    }
    if extra_metadata is not None:
        metadata.update(
            {key: value for key, value in extra_metadata.items() if value is not None}
        )
    track(AnalyticsEvent.LLM_CALLED, metadata)


def _execute_llm_call(request: _LLMRequest) -> str:
    """Execute one normalized LLM call and persist artifacts/metadata."""
    model_selection = resolve_model_selection(request.model)
    messages = _normalize_messages(request.prompt, system=request.system)

    # Mock short-circuit: skip credential resolution and provider SDK entirely
    if (mock_response := os.environ.get(_MOCK_RESPONSE_ENV)) is not None:
        result = _ProviderCallResult(response_text=mock_response, usage=_LLMUsage())
        env_overlay: dict[str, str] = {}
        credential_source = "environment"
        latency_ms = 0.0
        is_mocked = True
    else:
        env_overlay, credential_source = _resolve_credential_overlay(model_selection)
        started_at = time.perf_counter()
        result = _dispatch_provider_call(
            model_selection=model_selection,
            messages=messages,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            env_overlay=env_overlay,
        )
        latency_ms = round((time.perf_counter() - started_at) * 1000, 3)
        is_mocked = False

    response_text = result.response_text
    usage = result.usage

    _safe_save(
        f"{request.call_name}_prompt",
        messages,
        artifact_type="prompt",
        save_func=save,
    )
    _safe_save(
        f"{request.call_name}_response",
        response_text,
        artifact_type="response",
        save_func=save,
    )

    llm_metadata: dict[str, Any] = {
        "requested_model": model_selection.requested_model,
        "alias": model_selection.alias,
        "resolved_model": model_selection.resolved_model,
        "credential_source": credential_source,
        "latency_ms": latency_ms,
        "tokens_input": usage.prompt_tokens,
        "tokens_output": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
    }
    filtered_metadata = {
        key: value for key, value in llm_metadata.items() if value is not None
    }
    log(llm_calls={request.call_name: filtered_metadata})

    _track_llm_call_analytics(
        model_selection=model_selection,
        credential_source=credential_source,
        mocked=is_mocked,
    )

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
        model: Model alias or provider/model identifier
            (e.g. ``openai/gpt-5-nano``).
        system: Optional system prompt.
        temperature: Optional sampling temperature.
        max_tokens: Optional maximum response tokens.
        name: Optional display name for this call.

    Returns:
        The model response text.

    Raises:
        KitaruContextError: If called outside a flow.
        KitaruUsageError: If prompt, model input, or provider is invalid,
            or if the required provider SDK is not installed.
        KitaruRuntimeError: If credentials or response content are invalid.
        KitaruBackendError: If secret retrieval or the provider call fails.
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
