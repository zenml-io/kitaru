"""LLM call primitive for tracked model interactions.

`kitaru.llm()` wraps one provider SDK completion call with Kitaru tracking.
Built-in runtime support covers ``openai/*``, ``anthropic/*``, ``ollama/*``,
and ``openrouter/*`` models. Ollama and OpenRouter use the OpenAI-compatible
API and require the ``openai`` package (``pip install kitaru[openai]``).
"""

import importlib.metadata
import os
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core import to_jsonable_python

from kitaru._env import _temporary_env
from kitaru._llm_usage import build_usage_record, coerce_cost_usd, usage_record_metadata
from kitaru._safe_save import _safe_save
from kitaru.artifacts import save
from kitaru.checkpoint import (
    _raise_if_checkpoint_output_handle,
    _raise_if_checkpoint_output_handle_in_value,
    checkpoint,
)
from kitaru.config import (
    ResolvedModelSelection,
    resolve_llm_estimated_cost_policy,
    resolve_model_selection,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.logging import log
from kitaru.runtime import _is_inside_checkpoint, _is_inside_flow, _next_llm_call_name
from kitaru.secrets import _read_secret_values

_LLM_OUTSIDE_FLOW_ERROR = "kitaru.llm() can only be called inside a @flow."
_MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE"
_ANTHROPIC_DEFAULT_MAX_TOKENS = 4096
_OLLAMA_HOST_ENV = "OLLAMA_HOST"
_OLLAMA_DEFAULT_HOST = "http://localhost:11434"
_OLLAMA_DUMMY_API_KEY = "ollama"  # Ollama needs no auth; prevents OpenAI SDK env lookup
_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
_MIN_REDACTABLE_SECRET_LENGTH = 8
_CREDENTIAL_KEY_PARTS = {
    "CREDENTIAL",
    "CREDENTIALS",
    "KEY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
}

_SUPPORTED_PROVIDERS = ("openai", "anthropic", "ollama", "openrouter")
_OpenAITokenLimitParam = Literal["max_tokens", "max_completion_tokens"]

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
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None
    cache_creation_input_tokens: int | None = None
    cache_read_input_tokens: int | None = None
    raw_usage: dict[str, Any] | None = None


class _LLMRequest(BaseModel):
    """Normalized request payload used by `kitaru.llm()` internals."""

    prompt: str | list[dict[str, Any]]
    model: str | None = None
    system: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    call_name: str

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("prompt", mode="before")
    @classmethod
    def _reject_prompt_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle_in_value(
            value,
            field_name="_LLMRequest.prompt",
            expected="materialized prompt content",
        )
        return value

    @field_validator("system", mode="before")
    @classmethod
    def _reject_system_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle(
            value,
            field_name="_LLMRequest.system",
            expected="a materialized system prompt string",
        )
        return value


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


@dataclass(frozen=True)
class _DirectCostEstimate:
    """Estimated cost result for one direct LLM call."""

    estimated_cost_usd: float | None = None
    cost_source_label: str | None = None
    pricing_version: str | None = None


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


def _openai_token_limit_param(model: str) -> _OpenAITokenLimitParam:
    """Return the Chat Completions output-limit field for an OpenAI model."""
    normalized_model = model.strip().lower()
    if normalized_model.startswith("gpt-5"):
        return "max_completion_tokens"
    if re.match(r"o[134](?:$|[-_.])", normalized_model):
        return "max_completion_tokens"
    return "max_tokens"


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
        provider=provider,  # ty: ignore[invalid-argument-type]
        provider_model=model_name,
        resolved_model=resolved_model,
    )


# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------


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


def _looks_like_credential_name(name: str) -> bool:
    """Return whether an environment-style name usually carries credentials."""
    normalized = name.upper()
    parts = set(re.split(r"[^A-Z0-9]+", normalized))
    return bool(parts & _CREDENTIAL_KEY_PARTS) or "APIKEY" in normalized


def _redactable_secret(value: str | None) -> str | None:
    """Return a value only when redacting it is unlikely to erase harmless text."""
    if value is None or value == _OLLAMA_DUMMY_API_KEY:
        return None
    if len(value) < _MIN_REDACTABLE_SECRET_LENGTH:
        return None
    return value


def _redact_provider_error_text(
    text: str,
    *,
    env_overlay: Mapping[str, str],
    env_names: Sequence[str] = (),
    extra_secrets: Sequence[str | None] = (),
) -> str:
    """Strip known credential values from provider SDK error text.

    Provider SDK exceptions can echo request credentials. Kitaru only redacts
    values from credential-like overlay keys, explicit provider credential names,
    or explicit SDK credential arguments so ordinary secret fields do not hide
    useful provider diagnostics.
    """
    credential_names = {
        name
        for name in set(env_overlay) | set(env_names)
        if _looks_like_credential_name(name)
    }
    secrets: list[str] = []
    for name in credential_names:
        if secret := _redactable_secret(env_overlay.get(name)):
            secrets.append(secret)
        if secret := _redactable_secret(os.environ.get(name)):
            secrets.append(secret)
    for candidate in extra_secrets:
        if secret := _redactable_secret(candidate):
            secrets.append(secret)

    unique_secrets: list[str] = sorted(
        set(secrets), key=lambda secret: len(secret), reverse=True
    )
    redacted = text
    for secret in unique_secrets:
        redacted = redacted.replace(secret, "[redacted]")
    return redacted


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
    token_limit_param: _OpenAITokenLimitParam = "max_tokens",
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
        kwargs[token_limit_param] = max_tokens

    client_kwargs: dict[str, Any] = {}
    if base_url is not None:
        client_kwargs["base_url"] = base_url
    if api_key is not None:
        client_kwargs["api_key"] = api_key

    with _temporary_env(env_overlay):
        try:
            client = OpenAI(**client_kwargs)
            response = client.chat.completions.create(**kwargs)
        except Exception as exc:
            safe_text = _redact_provider_error_text(
                str(exc),
                env_overlay=env_overlay,
                env_names=_MODEL_PROVIDER_HINTS.get(provider_label, ()),
                extra_secrets=(api_key,),
            )
            raise KitaruBackendError(
                f"kitaru.llm() failed while calling {provider_label} for "
                f"model `{provider_label}/{model}`: {safe_text}"
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
        try:
            client = Anthropic()
            response = client.messages.create(**kwargs)
        except Exception as exc:
            safe_text = _redact_provider_error_text(
                str(exc),
                env_overlay=env_overlay,
                env_names=_MODEL_PROVIDER_HINTS["anthropic"],
            )
            raise KitaruBackendError(
                f"kitaru.llm() failed while calling Anthropic for model "
                f"`anthropic/{model}`: {safe_text}"
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


def _read_usage_value(usage_payload: Any, key: str) -> Any:
    """Read one field from a usage payload (Mapping or object)."""
    if usage_payload is None:
        return None
    try:
        if isinstance(usage_payload, Mapping):
            return usage_payload.get(key)
        return getattr(usage_payload, key, None)
    except Exception:
        return None


def _read_usage_int(usage_payload: Any, key: str) -> int | None:
    """Read an integer field from a usage payload (Mapping or object)."""
    raw_value = _read_usage_value(usage_payload, key)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _jsonable_usage_payload(
    usage_payload: Any,
    *,
    keys: Sequence[str],
    detail_keys: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any] | None:
    """Convert known provider usage fields into a JSON-safe mapping."""
    if usage_payload is None:
        return None
    try:
        if isinstance(usage_payload, Mapping):
            jsonable = to_jsonable_python(dict(usage_payload), serialize_unknown=True)
            if isinstance(jsonable, Mapping):
                return {str(key): value for key, value in jsonable.items()}
            return {"value": jsonable}
        model_dump = getattr(usage_payload, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="python")
            if isinstance(dumped, Mapping):
                jsonable = to_jsonable_python(dumped, serialize_unknown=True)
                if isinstance(jsonable, Mapping):
                    return {str(key): value for key, value in jsonable.items()}
    except Exception:
        pass

    raw: dict[str, Any] = {}
    for key in keys:
        value = _read_usage_value(usage_payload, key)
        if value is not None:
            try:
                raw[key] = to_jsonable_python(value, serialize_unknown=True)
            except Exception:
                continue
    for key, nested_keys in (detail_keys or {}).items():
        nested = _read_usage_value(usage_payload, key)
        if nested_mapping := _jsonable_usage_payload(nested, keys=nested_keys):
            raw[key] = nested_mapping
    return raw or None


def _extract_usage_openai(raw_response: Any) -> _LLMUsage:
    """Extract usage from an OpenAI Chat Completions response."""
    usage = _read_usage_value(raw_response, "usage")
    prompt_details = _read_usage_value(usage, "prompt_tokens_details")
    completion_details = _read_usage_value(usage, "completion_tokens_details")
    return _LLMUsage(
        prompt_tokens=_read_usage_int(usage, "prompt_tokens"),
        completion_tokens=_read_usage_int(usage, "completion_tokens"),
        total_tokens=_read_usage_int(usage, "total_tokens"),
        cached_input_tokens=_read_usage_int(prompt_details, "cached_tokens"),
        reasoning_tokens=_read_usage_int(completion_details, "reasoning_tokens"),
        raw_usage=_jsonable_usage_payload(
            usage,
            keys=("prompt_tokens", "completion_tokens", "total_tokens"),
            detail_keys={
                "prompt_tokens_details": ("cached_tokens",),
                "completion_tokens_details": ("reasoning_tokens",),
            },
        ),
    )


def _extract_usage_anthropic(raw_response: Any) -> _LLMUsage:
    """Extract usage from an Anthropic Messages response."""
    usage = _read_usage_value(raw_response, "usage")
    input_tokens = _read_usage_int(usage, "input_tokens")
    output_tokens = _read_usage_int(usage, "output_tokens")
    total = None
    if input_tokens is not None and output_tokens is not None:
        total = input_tokens + output_tokens
    return _LLMUsage(
        prompt_tokens=input_tokens,
        completion_tokens=output_tokens,
        total_tokens=total,
        cache_creation_input_tokens=_read_usage_int(
            usage, "cache_creation_input_tokens"
        ),
        cache_read_input_tokens=_read_usage_int(usage, "cache_read_input_tokens"),
        raw_usage=_jsonable_usage_payload(
            usage,
            keys=(
                "input_tokens",
                "output_tokens",
                "cache_creation_input_tokens",
                "cache_read_input_tokens",
            ),
        ),
    )


# ---------------------------------------------------------------------------
# Direct estimated cost calculation
# ---------------------------------------------------------------------------


def _resolve_llm_estimated_cost_policy() -> str:
    """Resolve the direct LLM estimated-cost policy for the current process."""
    return resolve_llm_estimated_cost_policy()


def _usage_has_pricing_tokens(usage: _LLMUsage) -> bool:
    """Return whether there is enough usage information to try pricing."""
    return any(
        value is not None
        for value in (
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.cached_input_tokens,
            usage.reasoning_tokens,
            usage.cache_creation_input_tokens,
            usage.cache_read_input_tokens,
        )
    )


def _direct_pricing_usage_payload(provider: str, usage: _LLMUsage) -> dict[str, Any]:
    """Build the provider usage mapping expected by genai-prices."""
    if provider == "openai":
        payload: dict[str, Any] = {}
        if usage.prompt_tokens is not None:
            payload["prompt_tokens"] = usage.prompt_tokens
        if usage.completion_tokens is not None:
            payload["completion_tokens"] = usage.completion_tokens
        if usage.total_tokens is not None:
            payload["total_tokens"] = usage.total_tokens
        if usage.cached_input_tokens is not None:
            payload["prompt_tokens_details"] = {
                "cached_tokens": usage.cached_input_tokens
            }
        if usage.reasoning_tokens is not None:
            payload["completion_tokens_details"] = {
                "reasoning_tokens": usage.reasoning_tokens
            }
        return payload

    payload = {}
    if usage.prompt_tokens is not None:
        payload["input_tokens"] = usage.prompt_tokens
    if usage.completion_tokens is not None:
        payload["output_tokens"] = usage.completion_tokens
    if usage.cache_creation_input_tokens is not None:
        payload["cache_creation_input_tokens"] = usage.cache_creation_input_tokens
    if usage.cache_read_input_tokens is not None:
        payload["cache_read_input_tokens"] = usage.cache_read_input_tokens
    return payload


@lru_cache(maxsize=1)
def _genai_prices_version() -> str:
    """Return the installed genai-prices package version for provenance."""
    try:
        return importlib.metadata.version("genai-prices")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _estimate_direct_llm_cost(
    *,
    resolved_model: str,
    usage: _LLMUsage,
    warnings: list[str],
) -> _DirectCostEstimate:
    """Estimate direct OpenAI/Anthropic call cost with genai-prices."""
    try:
        policy = _resolve_llm_estimated_cost_policy()
    except Exception as exc:
        warnings.append(
            "Kitaru could not resolve the direct LLM estimated-cost policy; "
            "tokens were recorded without an estimated cost: "
            f"{type(exc).__name__}: {exc}"
        )
        return _DirectCostEstimate()
    if policy == "off":
        return _DirectCostEstimate()

    provider = _provider_name(resolved_model)
    if provider not in {"openai", "anthropic"}:
        return _DirectCostEstimate()
    _, _, provider_model = resolved_model.partition("/")
    if not provider_model or not _usage_has_pricing_tokens(usage):
        return _DirectCostEstimate()

    try:
        from genai_prices import extract_usage
    except ImportError:
        warnings.append(
            "genai-prices is not installed; direct LLM tokens were recorded "
            "without an estimated cost."
        )
        return _DirectCostEstimate()

    response_data = {
        "model": provider_model,
        "usage": _direct_pricing_usage_payload(provider, usage),
    }
    extract_kwargs: dict[str, Any] = {"provider_id": provider}
    if provider == "openai":
        extract_kwargs["api_flavor"] = "chat"

    try:
        extracted_usage = extract_usage(response_data, **extract_kwargs)
        price = extracted_usage.calc_price()
    except Exception as exc:
        warnings.append(
            "genai-prices could not estimate direct LLM cost for "
            f"{resolved_model!r}; tokens were recorded without an estimated "
            f"cost: {type(exc).__name__}: {exc}"
        )
        return _DirectCostEstimate()

    estimated_cost = coerce_cost_usd(getattr(price, "total_price", None))
    if estimated_cost is None:
        warnings.append(
            "genai-prices returned an invalid direct LLM estimated cost for "
            f"{resolved_model!r}; tokens were recorded without an estimated cost."
        )
        return _DirectCostEstimate()

    version = _genai_prices_version()
    return _DirectCostEstimate(
        estimated_cost_usd=estimated_cost,
        cost_source_label="genai-prices",
        pricing_version=f"genai-prices:{version}",
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

    Shared by ``_execute_llm_call`` (flow-scoped) and ``_compact_impl``
    (admin operation).  Both callers now track LLM usage via
    ``_track_llm_call_analytics``.
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
            token_limit_param=_openai_token_limit_param(target.provider_model),
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
            token_limit_param="max_tokens",
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
        "model": model_selection.resolved_model,  # dashboard compat alias
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
    usage_warnings: list[str] = []
    cost_estimate = _estimate_direct_llm_cost(
        resolved_model=model_selection.resolved_model,
        usage=usage,
        warnings=usage_warnings,
    )

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
        "estimated_cost_usd": cost_estimate.estimated_cost_usd,
    }
    filtered_metadata = {
        key: value for key, value in llm_metadata.items() if value is not None
    }
    usage_record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        call_name=request.call_name,
        record_id=f"{request.call_name}:{uuid4().hex}",
        requested_model=model_selection.requested_model,
        resolved_model=model_selection.resolved_model,
        model=model_selection.resolved_model,
        provider=_provider_name(model_selection.resolved_model),
        input_tokens=usage.prompt_tokens,
        output_tokens=usage.completion_tokens,
        total_tokens=usage.total_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        reasoning_tokens=usage.reasoning_tokens,
        raw_usage=usage.raw_usage,
        estimated_cost_usd=cost_estimate.estimated_cost_usd,
        cost_source_label=cost_estimate.cost_source_label,
        pricing_version=cost_estimate.pricing_version,
        latency_ms=latency_ms,
        billing_effect="unknown" if is_mocked else "incurred",
        cache_status="unknown" if is_mocked else "executed",
        warnings=usage_warnings,
    )
    log(
        llm_calls={request.call_name: filtered_metadata},
        **usage_record_metadata(usage_record),
    )

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
