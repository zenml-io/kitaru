"""LLM call primitive for tracked model interactions.

`kitaru.llm()` wraps one provider SDK completion call with Kitaru tracking and
returns a normalized :class:`LLMResponse`. Built-in runtime support covers
``openai/*``, ``anthropic/*``, ``ollama/*``, and ``openrouter/*`` models.
Ollama and OpenRouter use the OpenAI-compatible API and require the ``openai``
package (``pip install kitaru[openai]``).
"""

import json
import logging
import os
import re
import time
import uuid
import warnings
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from zenml.client import Client

from kitaru._safe_save import _safe_save
from kitaru.artifacts import save
from kitaru.checkpoint import checkpoint
from kitaru.config import ResolvedModelSelection, resolve_model_selection
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruLLMRateLimitError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.logging import log
from kitaru.runtime import (
    _is_inside_checkpoint,
    _is_inside_flow,
    _next_llm_call_name,
    _register_llm_call_name,
)

_LLM_OUTSIDE_FLOW_ERROR = "kitaru.llm() can only be called inside a @flow."
_MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE"
_STRUCTURED_MOCK_RESPONSE_ENV = "KITARU_LLM_MOCK_RESPONSE_JSON"
_ANTHROPIC_DEFAULT_MAX_TOKENS = 4096
_OLLAMA_HOST_ENV = "OLLAMA_HOST"
_OLLAMA_DEFAULT_HOST = "http://localhost:11434"
_OLLAMA_DUMMY_API_KEY = "ollama"  # Ollama needs no auth; prevents OpenAI SDK env lookup
_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
_LLM_RESPONSE_DEPRECATION_DOCS_URL = (
    "https://kitaru.ai/docs/guides/llm-calls#migrating-from-string-responses"
)

_SUPPORTED_PROVIDERS = ("openai", "anthropic", "ollama", "openrouter")

logger = logging.getLogger(__name__)
_LLM_DEPRECATION_WARNED = False
_MOCK_ENV_WARNED = False

_MODEL_PROVIDER_HINTS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "ollama": (),
    "openai": ("OPENAI_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
}


LLMFinishReason = Literal[
    "completed",
    "tool_calls",
    "max_tokens",
    "content_filter",
    "pause",
    "unknown",
]


class LLMUsage(BaseModel):
    """Normalized token usage details from a provider SDK response."""

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class LLMToolCall(BaseModel):
    """Normalized model-requested tool call intent.

    ``arguments_json`` preserves the provider's raw JSON string whenever the
    provider supplied one. ``arguments_parse_error`` records parse failures so
    callers can inspect or repair malformed arguments without losing data.
    """

    id: str | None = None
    name: Any
    name_parse_error: str | None = None
    arguments_json: str
    arguments: dict[str, Any] | None = None
    arguments_parse_error: str | None = None

    @model_validator(mode="after")
    def _enforce_argument_parse_invariant(self) -> "LLMToolCall":
        """Keep parsed arguments and parse errors mutually exclusive."""
        if self.arguments_parse_error is not None:
            if self.arguments is not None:
                raise ValueError(
                    "`arguments` must be None when `arguments_parse_error` is set."
                )
            return self

        if self.arguments is None:
            arguments_json, arguments, parse_error = _normalize_tool_arguments(
                self.arguments_json
            )
            self.arguments_json = arguments_json
            self.arguments = arguments
            self.arguments_parse_error = parse_error

        if self.arguments_parse_error is None and self.arguments is None:
            raise ValueError(
                "`arguments` must be present when `arguments_parse_error` is None."
            )
        return self


class LLMToolDefinition(BaseModel):
    """OpenAI-style function tool definition accepted by ``kitaru.llm()``.

    Raw OpenAI-style dictionaries are also accepted. This helper is lightweight
    sugar for callers who prefer a typed object while the internal provider
    request format remains the same canonical dictionary shape.
    """

    name: str
    description: str | None = None
    parameters: dict[str, Any] = Field(
        default_factory=lambda: {"type": "object", "properties": {}}
    )
    model_config = ConfigDict(frozen=True)


class LLMResponse(BaseModel):
    """Normalized assistant response returned by ``kitaru.llm()``.

    Text-only responses set ``content``. Tool-only responses set
    ``tool_calls`` and may leave ``content`` as ``None``. Mixed responses may
    include both. Kitaru does not execute tools automatically; callers inspect
    ``tool_calls`` and decide how to continue the conversation.
    """

    role: Literal["assistant"] = "assistant"
    content: str | None = None
    tool_calls: list[LLMToolCall] = Field(default_factory=list)
    finish_reason: LLMFinishReason
    provider_finish_reason: str | None = None
    usage: LLMUsage = Field(default_factory=LLMUsage)
    requested_model: str | None = None
    alias: str | None = None
    resolved_model: str

    @model_validator(mode="before")
    @classmethod
    def _normalize_empty_content(cls, data: Any) -> Any:
        """Treat empty response text as absent content."""
        if isinstance(data, Mapping) and data.get("content") == "":
            normalized = dict(data)
            normalized["content"] = None
            return normalized
        return data

    @model_validator(mode="after")
    def _enforce_non_empty_response(self) -> "LLMResponse":
        """Reject impossible empty responses while allowing provider stops."""
        allowed_empty_reasons = {"content_filter", "max_tokens", "unknown"}
        if self.finish_reason == "tool_calls" and not self.tool_calls:
            raise ValueError("`finish_reason='tool_calls'` requires tool calls.")
        if (
            self.content is None
            and not self.tool_calls
            and self.finish_reason not in allowed_empty_reasons
        ):
            raise ValueError(
                "`content` or `tool_calls` is required unless the provider "
                "stopped for content filtering, max tokens, or an unknown reason."
            )
        return self

    @property
    def has_tool_calls(self) -> bool:
        """Whether the response includes one or more tool-call intents."""
        return bool(self.tool_calls)

    @property
    def text(self) -> str:
        """Text content convenience alias that never returns ``None``."""
        return self.content or ""


_LLMUsage = LLMUsage


class _LLMRequest(BaseModel):
    """Normalized request payload used by `kitaru.llm()` internals."""

    prompt: str | list[dict[str, Any]]
    model: str | None = None
    system: str | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    tools: list[Any] | None = None
    tool_choice: Any = None
    call_name: str

    model_config = ConfigDict(arbitrary_types_allowed=True)


@dataclass(frozen=True)
class _ProviderTarget:
    """Parsed routing result for a resolved model string."""

    provider: Literal["openai", "anthropic", "ollama", "openrouter"]
    provider_model: str
    resolved_model: str


class _ProviderCallResult:
    """Normalized boundary between provider SDK response and Kitaru persistence.

    ``response_text`` and ``usage`` remain as compatibility properties for
    internal callers that only need text, such as memory compaction.
    """

    def __init__(
        self,
        response: LLMResponse | None = None,
        *,
        response_text: str | None = None,
        usage: LLMUsage | None = None,
    ) -> None:
        if response is None:
            if response_text is None:
                raise TypeError(
                    "_ProviderCallResult requires response or response_text."
                )
            response = LLMResponse(
                content=response_text,
                finish_reason="completed",
                usage=usage or LLMUsage(),
                resolved_model="unknown",
            )
        if not isinstance(response, LLMResponse):
            raise TypeError("_ProviderCallResult response must be an LLMResponse.")
        self.response = response

    @property
    def response_text(self) -> str:
        """Backward-compatible text view that requires actual text content."""
        if self.response.content is None:
            raise KitaruRuntimeError(
                "Provider returned no text content for a text-only internal caller."
            )
        return self.response.content

    @property
    def usage(self) -> LLMUsage:
        """Backward-compatible usage view of the normalized response."""
        return self.response.usage


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
# Request normalization
# ---------------------------------------------------------------------------


_TOOL_CHOICE_MODES = {"auto", "none", "required"}


def _require_text_content(message: Mapping[str, Any], role: str) -> str:
    """Read required text content from a canonical chat message."""
    content = message.get("content")
    if not isinstance(content, str) or not content:
        raise KitaruUsageError(f"`{role}` messages require non-empty text content.")
    return content


def _coerce_openai_message_tool_call(raw_tool_call: Mapping[str, Any]) -> LLMToolCall:
    """Strictly normalize one OpenAI-style assistant-history tool call."""
    tool_call_id = raw_tool_call.get("id")
    if not isinstance(tool_call_id, str) or not tool_call_id:
        raise KitaruUsageError("Assistant tool calls require a non-empty `id`.")

    function_payload = raw_tool_call.get("function")
    if not isinstance(function_payload, Mapping):
        raise KitaruUsageError("Assistant tool calls require a `function` object.")

    name = function_payload.get("name")
    if not isinstance(name, str) or not name:
        raise KitaruUsageError("Assistant tool calls require a non-empty name.")

    arguments_json = function_payload.get("arguments")
    if not isinstance(arguments_json, str):
        raise KitaruUsageError(
            "Assistant tool-call function arguments must be a JSON string."
        )
    if arguments_json == "":
        arguments_json = "{}"

    tool_call = LLMToolCall(
        id=tool_call_id,
        name=name,
        arguments_json=arguments_json,
    )
    return _validate_request_tool_call(tool_call)


def _validate_request_tool_call(tool_call: LLMToolCall) -> LLMToolCall:
    """Validate assistant-history tool calls before provider dispatch."""
    if not tool_call.id:
        raise KitaruUsageError("Assistant tool calls require a non-empty `id`.")
    if not isinstance(tool_call.name, str) or not tool_call.name:
        raise KitaruUsageError("Assistant tool calls require a non-empty name.")

    _, arguments, parse_error = _normalize_tool_arguments(tool_call.arguments_json)
    if parse_error is not None or arguments is None:
        raise KitaruUsageError(
            "Assistant tool-call arguments must be valid JSON objects."
        )
    return tool_call.model_copy(
        update={"arguments": arguments, "arguments_parse_error": None}
    )


def _coerce_message_tool_call(raw_tool_call: Any) -> LLMToolCall:
    """Normalize assistant-message tool call payloads into ``LLMToolCall``."""
    if isinstance(raw_tool_call, LLMToolCall):
        return _validate_request_tool_call(raw_tool_call)
    if isinstance(raw_tool_call, Mapping):
        if isinstance(raw_tool_call.get("function"), Mapping):
            return _coerce_openai_message_tool_call(raw_tool_call)
        try:
            tool_call = LLMToolCall.model_validate(dict(raw_tool_call))
        except ValidationError as exc:
            raise KitaruUsageError(
                "Assistant `tool_calls` entries must match the LLMToolCall shape."
            ) from exc
        return _validate_request_tool_call(tool_call)

    raise KitaruUsageError(
        "Assistant `tool_calls` entries must be LLMToolCall objects or "
        "dict-like payloads."
    )


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
                "Prompt message lists must contain dict-like items with `role` keys."
            )
        role = message.get("role")
        if not isinstance(role, str):
            raise KitaruUsageError("Each prompt message must contain a string `role`.")

        if role in {"system", "user"}:
            messages.append(
                {"role": role, "content": _require_text_content(message, role)}
            )
            continue

        if role == "assistant":
            normalized_message: dict[str, Any] = {"role": "assistant"}
            content = message.get("content")
            if content is not None:
                if not isinstance(content, str):
                    raise KitaruUsageError(
                        "Assistant message `content` must be text when provided."
                    )
                if content.strip():
                    normalized_message["content"] = content

            raw_tool_calls = message.get("tool_calls")
            if raw_tool_calls is not None:
                if not _is_non_string_sequence(raw_tool_calls):
                    raise KitaruUsageError(
                        "Assistant message `tool_calls` must be a list when provided."
                    )
                tool_calls = [
                    _coerce_message_tool_call(tool_call).model_dump()
                    for tool_call in raw_tool_calls
                ]
                if tool_calls:
                    normalized_message["tool_calls"] = tool_calls

            if (
                "content" not in normalized_message
                and "tool_calls" not in normalized_message
            ):
                raise KitaruUsageError(
                    "Assistant messages require non-empty `content`, "
                    "`tool_calls`, or both."
                )
            messages.append(normalized_message)
            continue

        if role == "tool":
            tool_call_id = message.get("tool_call_id")
            if not isinstance(tool_call_id, str) or not tool_call_id:
                raise KitaruUsageError(
                    "Tool messages require a non-empty `tool_call_id`."
                )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": _require_text_content(message, role),
                }
            )
            continue

        raise KitaruUsageError(
            "Unsupported message role "
            f"`{role}`. Supported roles are: system, user, assistant, tool."
        )

    if not messages:
        raise KitaruUsageError("Prompt message list cannot be empty.")
    return messages


def _normalize_tool_definition(tool: Any) -> dict[str, Any]:
    """Normalize one tool definition into OpenAI function-tool shape."""
    if isinstance(tool, LLMToolDefinition):
        function: dict[str, Any] = {
            "name": tool.name,
            "parameters": deepcopy(tool.parameters),
        }
        if tool.description is not None:
            function["description"] = tool.description
        tool_payload: dict[str, Any] = {"type": "function", "function": function}
    elif isinstance(tool, Mapping):
        tool_payload = dict(tool)
        if "type" not in tool_payload and "function" not in tool_payload:
            helper_keys = {"name", "description", "parameters"}
            if set(tool_payload).issubset(helper_keys) and "name" in tool_payload:
                function = {
                    "name": tool_payload["name"],
                    "parameters": deepcopy(
                        tool_payload.get(
                            "parameters",
                            {"type": "object", "properties": {}},
                        )
                    ),
                }
                if tool_payload.get("description") is not None:
                    function["description"] = tool_payload["description"]
                tool_payload = {"type": "function", "function": function}
    else:
        raise KitaruUsageError(
            "Tools must be OpenAI-style dicts or LLMToolDefinition objects."
        )

    if tool_payload.get("type") != "function":
        raise KitaruUsageError("Only function tools are supported.")
    function_payload_raw = tool_payload.get("function")
    if not isinstance(function_payload_raw, Mapping):
        raise KitaruUsageError("Function tools require a `function` object.")
    function_payload: dict[str, Any] = dict(
        cast(Mapping[Any, Any], function_payload_raw)
    )

    name = function_payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise KitaruUsageError("Function tools require a non-empty name.")

    normalized_function: dict[str, Any] = {"name": name.strip()}
    description = function_payload.get("description")
    if description is not None:
        if not isinstance(description, str):
            raise KitaruUsageError("Function tool descriptions must be text.")
        normalized_function["description"] = description

    parameters = function_payload.get(
        "parameters",
        {"type": "object", "properties": {}},
    )
    if not isinstance(parameters, Mapping):
        raise KitaruUsageError("Function tool parameters must be a JSON-schema object.")
    normalized_function["parameters"] = deepcopy(dict(parameters))
    return {"type": "function", "function": normalized_function}


def _normalize_tools(tools: list[Any] | None) -> list[dict[str, Any]] | None:
    """Validate and normalize request tools into canonical OpenAI shape."""
    if tools is None:
        return None
    if not _is_non_string_sequence(tools):
        raise KitaruUsageError("`tools` must be a list when provided.")

    normalized_tools = [_normalize_tool_definition(tool) for tool in tools]
    seen_names: set[str] = set()
    for tool in normalized_tools:
        name = tool["function"]["name"]
        if name in seen_names:
            raise KitaruUsageError(f"Duplicate tool name `{name}` is not allowed.")
        seen_names.add(name)
    return normalized_tools


def _normalize_tool_choice(
    tool_choice: Any,
    tools: list[dict[str, Any]] | None,
) -> str | dict[str, Any] | None:
    """Validate and normalize request ``tool_choice`` into canonical shape."""
    if tool_choice is None:
        return None

    tool_names = {tool["function"]["name"] for tool in tools or []}
    if isinstance(tool_choice, str):
        normalized_choice = tool_choice.strip()
        if not normalized_choice:
            raise KitaruUsageError("`tool_choice` cannot be empty.")
        if normalized_choice in _TOOL_CHOICE_MODES:
            if normalized_choice in {"auto", "required"} and not tool_names:
                raise KitaruUsageError(
                    f"`tool_choice='{normalized_choice}'` requires tools."
                )
            return normalized_choice
        if normalized_choice not in tool_names:
            raise KitaruUsageError(
                f"Named tool_choice `{normalized_choice}` does not match any tool."
            )
        return {"type": "function", "function": {"name": normalized_choice}}

    if not isinstance(tool_choice, Mapping):
        raise KitaruUsageError("`tool_choice` must be a string or dict when provided.")

    choice_type = tool_choice.get("type")
    if choice_type in _TOOL_CHOICE_MODES:
        if choice_type in {"auto", "required"} and not tool_names:
            raise KitaruUsageError(f"`tool_choice='{choice_type}'` requires tools.")
        return str(choice_type)
    if choice_type != "function":
        raise KitaruUsageError("Only function tool_choice dictionaries are supported.")

    function_payload = tool_choice.get("function")
    if not isinstance(function_payload, Mapping):
        raise KitaruUsageError("Function tool_choice requires a `function` object.")
    name = function_payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise KitaruUsageError("Function tool_choice requires a non-empty tool name.")
    if name.strip() not in tool_names:
        raise KitaruUsageError(
            f"Named tool_choice `{name.strip()}` does not match any tool."
        )
    return {"type": "function", "function": {"name": name.strip()}}


# ---------------------------------------------------------------------------
# Provider SDK helpers (lazy imports)
# ---------------------------------------------------------------------------


def _credential_value(env_overlay: Mapping[str, str], key: str) -> str | None:
    """Resolve a provider credential from an overlay first, then the env."""
    return env_overlay.get(key) or os.environ.get(key)


def _header_value(headers: Any, key: str) -> str | None:
    """Read a provider response header from common SDK header containers."""
    if headers is None:
        return None
    value: Any
    try:
        value = headers.get(key) or headers.get(key.lower()) or headers.get(key.title())
    except AttributeError:
        return None
    if value is None:
        return None
    return str(value)


def _retry_after_value(headers: Any) -> str | None:
    """Extract Retry-After information without guessing retry policy."""
    raw_value = _header_value(headers, "retry-after")
    if raw_value is None:
        return None
    try:
        return str(float(raw_value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError):
            return raw_value
        return str(max(0.0, (retry_at.timestamp() - time.time())))


def _provider_error_metadata(
    exc: Exception,
) -> tuple[int | None, str | None, str | None]:
    """Extract status code, request ID, and Retry-After from SDK exceptions."""
    response = getattr(exc, "response", None)
    headers = getattr(exc, "headers", None) or getattr(response, "headers", None)

    raw_status = getattr(exc, "status_code", None)
    if raw_status is None:
        raw_status = getattr(response, "status_code", None)
    try:
        status_code = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status_code = None

    request_id = (
        getattr(exc, "request_id", None)
        or getattr(exc, "x_request_id", None)
        or _header_value(headers, "x-request-id")
        or _header_value(headers, "request-id")
    )
    return (
        status_code,
        str(request_id) if request_id is not None else None,
        _retry_after_value(headers),
    )


def _with_provider_error_metadata(
    error: Exception,
    *,
    status_code: int | None,
    request_id: str | None,
) -> Exception:
    """Attach provider status/request metadata to a Kitaru exception."""
    if status_code is not None:
        error.__dict__["status_code"] = status_code
    if request_id is not None:
        error.__dict__["request_id"] = request_id
    return error


def _provider_error_message(
    *,
    provider_label: str,
    model: str,
    exc: Exception,
    status_code: int | None,
    request_id: str | None,
    error_id: str | None = None,
) -> str:
    """Build a provider error message that preserves useful SDK metadata."""
    parts = [
        f"kitaru.llm() failed while calling {provider_label} for model "
        f"`{provider_label}/{model}`: {exc}"
    ]
    if status_code is not None:
        parts.append(f"status_code={status_code}")
    if request_id is not None:
        parts.append(f"request_id={request_id}")
    if error_id is not None:
        parts.append(f"error_id={error_id}")
    return (
        " (".join([parts[0], ", ".join(parts[1:]) + ")"])
        if len(parts) > 1
        else parts[0]
    )


def _is_network_or_timeout_error(exc: Exception) -> bool:
    """Best-effort classifier for transport failures across provider SDKs."""
    class_name = type(exc).__name__.lower()
    module_name = type(exc).__module__.lower()
    text = f"{class_name} {module_name}"
    return any(
        hint in text
        for hint in (
            "timeout",
            "connection",
            "connect",
            "network",
            "transport",
            "apierror",
        )
    )


def _provider_exception_to_kitaru_error(
    exc: Exception,
    *,
    provider_label: str,
    model: str,
) -> Exception:
    """Map provider SDK exceptions into actionable Kitaru error classes."""
    status_code, request_id, retry_after = _provider_error_metadata(exc)

    if status_code in {401, 403}:
        error = KitaruRuntimeError(
            _provider_error_message(
                provider_label=provider_label,
                model=model,
                exc=exc,
                status_code=status_code,
                request_id=request_id,
            )
        )
    elif status_code == 429:
        error = KitaruLLMRateLimitError(
            _provider_error_message(
                provider_label=provider_label,
                model=model,
                exc=exc,
                status_code=status_code,
                request_id=request_id,
            ),
            retry_after=retry_after,
            status_code=status_code,
            request_id=request_id,
        )
    elif status_code == 400:
        error = KitaruUsageError(
            _provider_error_message(
                provider_label=provider_label,
                model=model,
                exc=exc,
                status_code=status_code,
                request_id=request_id,
            )
        )
    elif (
        status_code is not None and status_code >= 500
    ) or _is_network_or_timeout_error(exc):
        error = KitaruBackendError(
            _provider_error_message(
                provider_label=provider_label,
                model=model,
                exc=exc,
                status_code=status_code,
                request_id=request_id,
            )
        )
    else:
        error_id = uuid.uuid4().hex
        logger.exception(
            "Unknown LLM provider exception.",
            extra={
                "error_id": error_id,
                "provider": provider_label,
                "model": f"{provider_label}/{model}",
                "status_code": status_code,
                "request_id": request_id,
            },
        )
        error = KitaruBackendError(
            _provider_error_message(
                provider_label=provider_label,
                model=model,
                exc=exc,
                status_code=status_code,
                request_id=request_id,
                error_id=error_id,
            )
        )

    return _with_provider_error_metadata(
        error,
        status_code=status_code,
        request_id=request_id,
    )


def _tool_call_to_openai(tool_call_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Convert canonical assistant tool-call payload into OpenAI shape."""
    tool_call = _validate_request_tool_call(
        LLMToolCall.model_validate(dict(tool_call_payload))
    )
    openai_tool_call: dict[str, Any] = {
        "type": "function",
        "function": {
            "name": tool_call.name,
            "arguments": tool_call.arguments_json,
        },
    }
    openai_tool_call["id"] = tool_call.id
    return openai_tool_call


def _messages_to_openai(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert canonical chat messages into OpenAI-compatible shape."""
    openai_messages: list[dict[str, Any]] = []
    for message in messages:
        role = message["role"]
        if role in {"system", "user"}:
            openai_messages.append({"role": role, "content": message["content"]})
        elif role == "assistant":
            openai_message: dict[str, Any] = {"role": "assistant"}
            if "content" in message:
                openai_message["content"] = message["content"]
            elif "tool_calls" in message:
                openai_message["content"] = None
            if "tool_calls" in message:
                openai_message["tool_calls"] = [
                    _tool_call_to_openai(tool_call)
                    for tool_call in message["tool_calls"]
                ]
            openai_messages.append(openai_message)
        elif role == "tool":
            openai_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": message["tool_call_id"],
                    "content": message["content"],
                }
            )
        else:  # pragma: no cover - _normalize_messages catches this first.
            raise KitaruUsageError(f"Unsupported message role `{role}`.")
    return openai_messages


def _tools_to_anthropic(
    tools: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Convert canonical OpenAI-style tools into Anthropic tool definitions."""
    if not tools:
        return None

    anthropic_tools: list[dict[str, Any]] = []
    for tool in tools:
        function = tool["function"]
        anthropic_tool: dict[str, Any] = {
            "name": function["name"],
            "input_schema": deepcopy(function["parameters"]),
        }
        if "description" in function:
            anthropic_tool["description"] = function["description"]
        anthropic_tools.append(anthropic_tool)
    return anthropic_tools


def _tool_call_to_anthropic(tool_call_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Convert canonical assistant tool-call payload into Anthropic tool_use."""
    tool_call = _validate_request_tool_call(
        LLMToolCall.model_validate(dict(tool_call_payload))
    )
    arguments = tool_call.arguments
    tool_use: dict[str, Any] = {
        "type": "tool_use",
        "id": tool_call.id,
        "name": tool_call.name,
        "input": arguments,
    }
    if tool_use["id"] is None:
        raise KitaruUsageError("Anthropic assistant tool calls require an `id`.")
    return tool_use


def _messages_to_anthropic(
    messages: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert canonical chat messages into Anthropic system/messages shape."""
    system_parts: list[str] = []
    anthropic_messages: list[dict[str, Any]] = []
    seen_non_system = False

    def flush_tool_results(tool_results: list[dict[str, Any]]) -> None:
        if tool_results:
            anthropic_messages.append({"role": "user", "content": tool_results.copy()})
            tool_results.clear()

    pending_tool_results: list[dict[str, Any]] = []
    for message in messages:
        role = message["role"]
        if role == "system":
            if seen_non_system:
                raise KitaruUsageError(
                    "System messages must appear at the beginning of the "
                    "message list. Anthropic does not support interleaved "
                    "system messages."
                )
            system_parts.append(message["content"])
            continue

        seen_non_system = True
        if role == "user":
            flush_tool_results(pending_tool_results)
            anthropic_messages.append({"role": "user", "content": message["content"]})
        elif role == "assistant":
            flush_tool_results(pending_tool_results)
            content_blocks: list[dict[str, Any]] = []
            if message.get("content"):
                content_blocks.append({"type": "text", "text": message["content"]})
            content_blocks.extend(
                _tool_call_to_anthropic(tool_call)
                for tool_call in message.get("tool_calls", [])
            )
            anthropic_messages.append({"role": "assistant", "content": content_blocks})
        elif role == "tool":
            pending_tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": message["tool_call_id"],
                    "content": message["content"],
                },
            )
        else:  # pragma: no cover - _normalize_messages catches this first.
            raise KitaruUsageError(f"Unsupported message role `{role}`.")
    flush_tool_results(pending_tool_results)
    return system_parts, anthropic_messages


def _tool_choice_to_anthropic(
    tool_choice: str | dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Convert canonical tool choice into Anthropic's tool_choice shape."""
    if tool_choice is None or tool_choice == "none":
        return None
    if tool_choice == "auto":
        return {"type": "auto"}
    if tool_choice == "required":
        return {"type": "any"}
    if isinstance(tool_choice, dict):
        choice_type = tool_choice.get("type")
        if choice_type == "none":
            return None
        if choice_type == "auto":
            return {"type": "auto"}
        if choice_type == "required":
            return {"type": "any"}
        if choice_type == "function":
            function_payload_raw = tool_choice.get("function")
            if isinstance(function_payload_raw, Mapping):
                function_payload = dict(function_payload_raw)
                return {"type": "tool", "name": function_payload["name"]}
    raise KitaruUsageError("Unsupported tool_choice for Anthropic requests.")


def _call_openai(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float | None,
    max_tokens: int | None,
    env_overlay: Mapping[str, str],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | dict[str, Any] | None = None,
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

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": _messages_to_openai(messages),
    }
    if temperature is not None:
        kwargs["temperature"] = temperature
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    tools_disabled = tool_choice == "none" or (
        isinstance(tool_choice, dict) and tool_choice.get("type") == "none"
    )
    if tools and not tools_disabled:
        kwargs["tools"] = tools
    if tool_choice is not None:
        kwargs["tool_choice"] = tool_choice

    client_kwargs: dict[str, Any] = {}
    if base_url is not None:
        client_kwargs["base_url"] = base_url
    if api_key is not None:
        client_kwargs["api_key"] = api_key
    elif provider_label == "openai":
        openai_api_key = _credential_value(env_overlay, "OPENAI_API_KEY")
        if openai_api_key is not None:
            client_kwargs["api_key"] = openai_api_key

    try:
        client = OpenAI(**client_kwargs)
        response = client.chat.completions.create(**kwargs)
    except Exception as exc:
        raise _provider_exception_to_kitaru_error(
            exc,
            provider_label=provider_label,
            model=model,
        ) from exc

    return _ProviderCallResult(
        response=_parse_openai_compatible_response(
            response,
            resolved_model=f"{provider_label}/{model}",
        )
    )


def _call_anthropic(
    *,
    model: str,
    messages: list[dict[str, Any]],
    temperature: float | None,
    max_tokens: int | None,
    env_overlay: Mapping[str, str],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | dict[str, Any] | None = None,
) -> _ProviderCallResult:
    """Execute one Anthropic Messages API call."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise KitaruUsageError(
            f"Model 'anthropic/{model}' requires the anthropic package. "
            "Install with: pip install kitaru[anthropic]"
        ) from None

    system_parts, anthropic_messages = _messages_to_anthropic(messages)
    anthropic_tools = _tools_to_anthropic(tools)
    anthropic_tool_choice = _tool_choice_to_anthropic(tool_choice)
    tools_disabled = tool_choice == "none" or (
        isinstance(tool_choice, dict) and tool_choice.get("type") == "none"
    )

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
    if anthropic_tools is not None and not tools_disabled:
        kwargs["tools"] = anthropic_tools
    if anthropic_tool_choice is not None:
        kwargs["tool_choice"] = anthropic_tool_choice

    client_kwargs: dict[str, Any] = {}
    anthropic_api_key = _credential_value(env_overlay, "ANTHROPIC_API_KEY")
    if anthropic_api_key is not None:
        client_kwargs["api_key"] = anthropic_api_key

    try:
        client = Anthropic(**client_kwargs)
        response = client.messages.create(**kwargs)
    except Exception as exc:
        raise _provider_exception_to_kitaru_error(
            exc,
            provider_label="anthropic",
            model=model,
        ) from exc

    return _ProviderCallResult(
        response=_parse_anthropic_response(
            response,
            resolved_model=f"anthropic/{model}",
        )
    )


# ---------------------------------------------------------------------------
# Response parsing (provider-aware)
# ---------------------------------------------------------------------------


_OPENAI_FINISH_REASON_MAP: dict[str, LLMFinishReason] = {
    "stop": "completed",
    "length": "max_tokens",
    "tool_calls": "tool_calls",
    "content_filter": "content_filter",
}

_ANTHROPIC_FINISH_REASON_MAP: dict[str, LLMFinishReason] = {
    "end_turn": "completed",
    "max_tokens": "max_tokens",
    "tool_use": "tool_calls",
    "pause_turn": "pause",
}


def _read_field(payload: Any, key: str, default: Any = None) -> Any:
    """Read one field from a dict-like or object-like payload."""
    if payload is None:
        return default
    if isinstance(payload, Mapping):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _provider_finish_reason(raw_reason: Any) -> str | None:
    """Stringify provider finish reasons while preserving missing values."""
    if raw_reason is None:
        return None
    return str(raw_reason)


def _normalize_openai_finish_reason(raw_reason: Any) -> LLMFinishReason:
    """Normalize OpenAI-compatible finish reasons into Kitaru's taxonomy."""
    if raw_reason is None:
        return "unknown"
    return _OPENAI_FINISH_REASON_MAP.get(str(raw_reason), "unknown")


def _normalize_anthropic_finish_reason(raw_reason: Any) -> LLMFinishReason:
    """Normalize Anthropic stop reasons into Kitaru's taxonomy."""
    if raw_reason is None:
        return "unknown"
    return _ANTHROPIC_FINISH_REASON_MAP.get(str(raw_reason), "unknown")


def _is_non_string_sequence(value: Any) -> bool:
    """Return whether ``value`` is a sequence but not text/bytes."""
    return isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    )


def _normalize_text_content(content: Any) -> str | None:
    """Normalize provider text/content block shapes into a single string."""
    if isinstance(content, str):
        return content or None
    if not _is_non_string_sequence(content):
        return None

    text_parts: list[str] = []
    for part in content:
        if isinstance(part, str):
            text_parts.append(part)
            continue
        part_type = _read_field(part, "type")
        part_text = _read_field(part, "text")
        if (
            part_type in {None, "text", "output_text"}
            and isinstance(part_text, str)
            and part_text
        ):
            text_parts.append(part_text)
    return "\n".join(text_parts) if text_parts else None


def _normalize_tool_arguments(
    raw_arguments: Any,
) -> tuple[str, dict[str, Any] | None, str | None]:
    """Normalize tool arguments while preserving malformed raw JSON.

    Providers do not agree on argument shape: OpenAI-compatible chat
    completions usually return a JSON string, while Anthropic ``tool_use``
    blocks return a structured object. The normalized model keeps both the raw
    JSON representation and a parsed dict when parsing succeeds.
    """
    if isinstance(raw_arguments, str):
        if raw_arguments == "":
            return "{}", {}, None
        try:
            parsed_arguments = json.loads(raw_arguments)
        except json.JSONDecodeError as exc:
            return raw_arguments, None, str(exc)
        if isinstance(parsed_arguments, Mapping):
            return raw_arguments, dict(parsed_arguments), None
        return (
            raw_arguments,
            None,
            "Tool call arguments must decode to a JSON object; "
            f"got {type(parsed_arguments).__name__}.",
        )

    if isinstance(raw_arguments, Mapping):
        arguments = dict(raw_arguments)
        return (
            json.dumps(arguments, ensure_ascii=False, separators=(",", ":")),
            arguments,
            None,
        )

    if raw_arguments is None:
        return "", None, "Tool call arguments are missing."

    try:
        arguments_json = json.dumps(
            raw_arguments,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    except TypeError:
        arguments_json = repr(raw_arguments)
    return (
        arguments_json,
        None,
        "Tool call arguments must be a JSON object or JSON string; "
        f"got {type(raw_arguments).__name__}.",
    )


def _normalize_tool_call_name(raw_name: Any) -> tuple[Any, str | None]:
    """Preserve provider tool-call names while surfacing malformed values."""
    if isinstance(raw_name, str) and raw_name:
        return raw_name, None
    if raw_name is None:
        return None, "Tool call name is missing."
    try:
        json.dumps(raw_name)
        safe_raw_name = raw_name
    except TypeError:
        safe_raw_name = repr(raw_name)
    return (
        safe_raw_name,
        f"Tool call name must be a non-empty string; got {type(raw_name).__name__}.",
    )


def _parse_openai_tool_call(tool_call: Any) -> LLMToolCall:
    """Normalize one OpenAI-compatible function tool call."""
    function_payload = _read_field(tool_call, "function")
    name, name_parse_error = _normalize_tool_call_name(
        _read_field(function_payload, "name")
    )
    raw_arguments = _read_field(function_payload, "arguments")
    arguments_json, arguments, parse_error = _normalize_tool_arguments(raw_arguments)

    tool_call_id = _read_field(tool_call, "id")
    return LLMToolCall(
        id=str(tool_call_id) if tool_call_id is not None else None,
        name=name,
        name_parse_error=name_parse_error,
        arguments_json=arguments_json,
        arguments=arguments,
        arguments_parse_error=parse_error,
    )


def _parse_anthropic_tool_call(block: Any) -> LLMToolCall:
    """Normalize one Anthropic ``tool_use`` content block."""
    name, name_parse_error = _normalize_tool_call_name(_read_field(block, "name"))
    raw_arguments = _read_field(block, "input")
    arguments_json, arguments, parse_error = _normalize_tool_arguments(raw_arguments)

    tool_call_id = _read_field(block, "id")
    return LLMToolCall(
        id=str(tool_call_id) if tool_call_id is not None else None,
        name=name,
        name_parse_error=name_parse_error,
        arguments_json=arguments_json,
        arguments=arguments,
        arguments_parse_error=parse_error,
    )


def _build_provider_response(provider_label: str, **fields: Any) -> LLMResponse:
    """Build an LLMResponse and wrap provider-shape drift as Kitaru errors."""
    try:
        return LLMResponse(**fields)
    except ValidationError as exc:
        raise KitaruRuntimeError(
            f"{provider_label} returned an invalid LLM response shape: {exc}"
        ) from exc


def _parse_openai_compatible_response(
    raw_response: Any,
    *,
    resolved_model: str,
    requested_model: str | None = None,
    alias: str | None = None,
) -> LLMResponse:
    """Normalize an OpenAI-compatible Chat Completions response."""
    choices = _read_field(raw_response, "choices")
    if not _is_non_string_sequence(choices) or not choices:
        raise KitaruRuntimeError("OpenAI returned no response choices.")

    first_choice = choices[0]
    message = _read_field(first_choice, "message")
    if message is None:
        content = _read_field(first_choice, "text")
        tool_calls_payload = _read_field(first_choice, "tool_calls")
    else:
        content = _read_field(message, "content")
        tool_calls_payload = _read_field(message, "tool_calls")

    tool_calls: list[LLMToolCall] = []
    if _is_non_string_sequence(tool_calls_payload):
        tool_calls = [
            _parse_openai_tool_call(tool_call) for tool_call in tool_calls_payload
        ]

    raw_finish_reason = _read_field(first_choice, "finish_reason")
    return _build_provider_response(
        "OpenAI-compatible provider",
        content=_normalize_text_content(content),
        tool_calls=tool_calls,
        finish_reason=_normalize_openai_finish_reason(raw_finish_reason),
        provider_finish_reason=_provider_finish_reason(raw_finish_reason),
        usage=_extract_usage_openai(raw_response),
        requested_model=requested_model,
        alias=alias,
        resolved_model=resolved_model,
    )


def _parse_anthropic_response(
    raw_response: Any,
    *,
    resolved_model: str,
    requested_model: str | None = None,
    alias: str | None = None,
) -> LLMResponse:
    """Normalize an Anthropic Messages API response."""
    content_blocks = _read_field(raw_response, "content")
    if not _is_non_string_sequence(content_blocks) or not content_blocks:
        raise KitaruRuntimeError("Anthropic returned no response content.")

    text_blocks: list[str] = []
    tool_calls: list[LLMToolCall] = []
    for block in content_blocks:
        block_type = _read_field(block, "type")
        if block_type == "text":
            text = _read_field(block, "text")
            if isinstance(text, str) and text:
                text_blocks.append(text)
        elif block_type == "tool_use":
            tool_calls.append(_parse_anthropic_tool_call(block))

    raw_stop_reason = _read_field(raw_response, "stop_reason")
    return _build_provider_response(
        "Anthropic",
        content="\n".join(text_blocks) if text_blocks else None,
        tool_calls=tool_calls,
        finish_reason=_normalize_anthropic_finish_reason(raw_stop_reason),
        provider_finish_reason=_provider_finish_reason(raw_stop_reason),
        usage=_extract_usage_anthropic(raw_response),
        requested_model=requested_model,
        alias=alias,
        resolved_model=resolved_model,
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
    usage = _read_field(raw_response, "usage")
    return _LLMUsage(
        prompt_tokens=_read_usage_int(usage, "prompt_tokens"),
        completion_tokens=_read_usage_int(usage, "completion_tokens"),
        total_tokens=_read_usage_int(usage, "total_tokens"),
    )


def _extract_usage_anthropic(raw_response: Any) -> _LLMUsage:
    """Extract usage from an Anthropic Messages response."""
    usage = _read_field(raw_response, "usage")
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
# Rich response persistence helpers
# ---------------------------------------------------------------------------


def _response_payload(response: LLMResponse) -> dict[str, Any]:
    """Return the serializable artifact payload for a normalized response."""
    return response.model_dump(mode="json")


def _request_envelope(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
    tool_choice: str | dict[str, Any] | None,
    temperature: float | None,
    max_tokens: int | None,
) -> dict[str, Any]:
    """Return the serializable prompt artifact envelope for an LLM call."""
    return {
        "messages": messages,
        "tools": tools,
        "tool_choice": tool_choice,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }


def _finalize_response_metadata(
    response: LLMResponse,
    model_selection: ResolvedModelSelection,
) -> LLMResponse:
    """Attach request/model selection metadata to a parsed response."""
    return response.model_copy(
        update={
            "requested_model": model_selection.requested_model,
            "alias": model_selection.alias,
            "resolved_model": model_selection.resolved_model,
        }
    )


def _response_kind(response: LLMResponse) -> str:
    """Classify a normalized response for low-cardinality metadata."""
    has_content = bool(response.content)
    has_tools = bool(response.tool_calls)
    if has_content and has_tools:
        return "mixed"
    if has_content:
        return "text_only"
    if has_tools:
        return "tool_calls_only"
    return "empty"


def _structured_mock_response(raw_mock: str, resolved_model: str) -> LLMResponse:
    """Parse a structured mock response from JSON into ``LLMResponse``."""
    try:
        payload = json.loads(raw_mock)
    except json.JSONDecodeError as exc:
        raise KitaruUsageError(
            f"{_STRUCTURED_MOCK_RESPONSE_ENV} must contain valid JSON."
        ) from exc
    if not isinstance(payload, Mapping):
        raise KitaruUsageError(
            f"{_STRUCTURED_MOCK_RESPONSE_ENV} must contain a JSON object."
        )

    response_payload = dict(payload)
    allowed_keys = set(LLMResponse.model_fields)
    unknown_keys = set(response_payload) - allowed_keys
    if unknown_keys:
        formatted_keys = ", ".join(sorted(str(key) for key in unknown_keys))
        raise KitaruUsageError(
            f"{_STRUCTURED_MOCK_RESPONSE_ENV} contains unsupported keys: "
            f"{formatted_keys}."
        )
    response_payload.setdefault("finish_reason", "completed")
    response_payload.setdefault("resolved_model", resolved_model)
    try:
        return LLMResponse.model_validate(response_payload)
    except ValidationError as exc:
        raise KitaruUsageError(
            f"{_STRUCTURED_MOCK_RESPONSE_ENV} must match the LLMResponse shape."
        ) from exc


def _text_mock_response(mock_response: str, resolved_model: str) -> LLMResponse:
    """Build a text-only ``LLMResponse`` from the legacy mock env var."""
    return LLMResponse(
        content=mock_response,
        finish_reason="completed",
        provider_finish_reason="mock",
        usage=LLMUsage(),
        resolved_model=resolved_model,
    )


def _warn_if_mock_env_active() -> None:
    """Emit a one-shot process warning when mock LLM env vars hijack calls."""
    global _MOCK_ENV_WARNED
    if _MOCK_ENV_WARNED:
        return
    active_mock_vars = [
        name
        for name in (_STRUCTURED_MOCK_RESPONSE_ENV, _MOCK_RESPONSE_ENV)
        if os.environ.get(name) is not None
    ]
    if not active_mock_vars:
        return
    _MOCK_ENV_WARNED = True
    logger.warning(
        "Kitaru LLM mock environment variable(s) are active; provider calls "
        "will be skipped.",
        extra={"mock_env_vars": active_mock_vars},
    )


def _tool_parse_issue_metadata(response: LLMResponse) -> dict[str, Any]:
    """Summarize malformed tool-call fields from a provider response."""
    argument_error_ids = [
        tool_call.id
        for tool_call in response.tool_calls
        if tool_call.arguments_parse_error is not None
    ]
    name_error_ids = [
        tool_call.id
        for tool_call in response.tool_calls
        if tool_call.name_parse_error is not None
    ]
    return {
        "tool_arguments_parse_error_count": len(argument_error_ids),
        "tool_arguments_parse_error_ids": argument_error_ids,
        "tool_name_parse_error_count": len(name_error_ids),
        "tool_name_parse_error_ids": name_error_ids,
    }


def _warn_for_tool_parse_issues(
    *,
    call_name: str,
    response: LLMResponse,
    issue_metadata: Mapping[str, Any],
) -> None:
    """Log malformed tool-call fields without hiding the provider response."""
    argument_count = int(issue_metadata["tool_arguments_parse_error_count"])
    name_count = int(issue_metadata["tool_name_parse_error_count"])
    if argument_count:
        logger.warning(
            "LLM provider returned tool-call arguments that Kitaru could not parse.",
            extra={
                "call_name": call_name,
                "tool_call_ids": issue_metadata["tool_arguments_parse_error_ids"],
                "tool_call_count": argument_count,
            },
        )
    if name_count:
        logger.warning(
            "LLM provider returned tool-call names that Kitaru could not parse.",
            extra={
                "call_name": call_name,
                "tool_call_ids": issue_metadata["tool_name_parse_error_ids"],
                "tool_call_count": name_count,
                "raw_tool_names": [
                    tool_call.name
                    for tool_call in response.tool_calls
                    if tool_call.name_parse_error is not None
                ],
            },
        )


def _warn_once_for_llm_response_migration() -> None:
    """Emit the one-shot migration warning for the rich LLMResponse return."""
    global _LLM_DEPRECATION_WARNED
    if _LLM_DEPRECATION_WARNED:
        return
    _LLM_DEPRECATION_WARNED = True
    warnings.warn(
        "kitaru.llm() now returns LLMResponse instead of str. Use "
        "`response.text`/`response.content`, or `kitaru.llm_text()` for the "
        f"compatibility helper. See {_LLM_RESPONSE_DEPRECATION_DOCS_URL}.",
        DeprecationWarning,
        stacklevel=3,
    )


def _reserve_llm_call_name(name: str | None) -> str:
    """Normalize and reserve a call name in the active runtime scope."""
    if name is not None:
        call_name = _normalize_call_name(name)
        if not _register_llm_call_name(call_name):
            raise KitaruUsageError(
                f"Duplicate LLM call name `{call_name}` in the same runtime scope. "
                "Use unique `name=` values or omit `name` to let Kitaru "
                "auto-number calls."
            )
        return call_name

    while True:
        call_name = _normalize_call_name(None)
        if _register_llm_call_name(call_name):
            return call_name


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------


def _dispatch_provider_call(
    *,
    model_selection: ResolvedModelSelection,
    messages: list[dict[str, Any]],
    temperature: float | None,
    max_tokens: int | None,
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | dict[str, Any] | None = None,
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
            tools=tools,
            tool_choice=tool_choice,
            env_overlay=env_overlay,
        )
    if target.provider == "anthropic":
        return _call_anthropic(
            model=target.provider_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            tools=tools,
            tool_choice=tool_choice,
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
            tools=tools,
            tool_choice=tool_choice,
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
        "model": model_selection.resolved_model,  # dashboard compat alias
        "credential_source": credential_source,
        "mocked": mocked,
    }
    if extra_metadata is not None:
        metadata.update(
            {key: value for key, value in extra_metadata.items() if value is not None}
        )
    track(AnalyticsEvent.LLM_CALLED, metadata)


def _execute_llm_call(request: _LLMRequest) -> LLMResponse:
    """Execute one normalized LLM call and persist artifacts/metadata."""
    model_selection = resolve_model_selection(request.model)
    messages = _normalize_messages(request.prompt, system=request.system)
    tools = _normalize_tools(request.tools)
    tool_choice = _normalize_tool_choice(request.tool_choice, tools)
    request_envelope = _request_envelope(
        messages=messages,
        tools=tools,
        tool_choice=tool_choice,
        temperature=request.temperature,
        max_tokens=request.max_tokens,
    )

    # Mock short-circuit: skip credential resolution and provider SDK entirely
    structured_mock = os.environ.get(_STRUCTURED_MOCK_RESPONSE_ENV)
    text_mock = os.environ.get(_MOCK_RESPONSE_ENV)
    if structured_mock is not None:
        _warn_if_mock_env_active()
        result = _ProviderCallResult(
            response=_structured_mock_response(
                structured_mock,
                resolved_model=model_selection.resolved_model,
            )
        )
        credential_source = "environment"
        latency_ms = 0.0
        is_mocked = True
    elif text_mock is not None:
        _warn_if_mock_env_active()
        result = _ProviderCallResult(
            response=_text_mock_response(
                text_mock,
                resolved_model=model_selection.resolved_model,
            )
        )
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
            tools=tools,
            tool_choice=tool_choice,
            env_overlay=env_overlay,
        )
        latency_ms = round((time.perf_counter() - started_at) * 1000, 3)
        is_mocked = False

    response = _finalize_response_metadata(result.response, model_selection)
    usage = response.usage
    response_kind = _response_kind(response)
    tool_call_names = [tool_call.name for tool_call in response.tool_calls]
    parse_issue_metadata = _tool_parse_issue_metadata(response)
    _warn_for_tool_parse_issues(
        call_name=request.call_name,
        response=response,
        issue_metadata=parse_issue_metadata,
    )

    _safe_save(
        f"{request.call_name}_prompt",
        request_envelope,
        artifact_type="prompt",
        save_func=save,
    )
    _safe_save(
        f"{request.call_name}_response",
        _response_payload(response),
        artifact_type="response",
        save_func=save,
    )

    llm_metadata: dict[str, Any] = {
        "api_mode": "response",
        "requested_model": model_selection.requested_model,
        "alias": model_selection.alias,
        "resolved_model": model_selection.resolved_model,
        "credential_source": credential_source,
        "latency_ms": latency_ms,
        "tokens_input": usage.prompt_tokens,
        "tokens_output": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "finish_reason": response.finish_reason,
        "provider_finish_reason": response.provider_finish_reason,
        "response_kind": response_kind,
        "tool_call_count": len(response.tool_calls),
        "tool_call_names": tool_call_names,
        **parse_issue_metadata,
        "has_content": bool(response.content),
    }
    filtered_metadata = {
        key: value for key, value in llm_metadata.items() if value is not None
    }
    log(llm_calls={request.call_name: filtered_metadata})

    _track_llm_call_analytics(
        model_selection=model_selection,
        credential_source=credential_source,
        mocked=is_mocked,
        extra_metadata={
            "api_mode": "response",
            "tools_supplied": bool(tools),
            "tool_count": len(tools or []),
            "tool_calls_returned": bool(response.tool_calls),
            "tool_call_count": len(response.tool_calls),
            "tool_arguments_parse_error_count": parse_issue_metadata[
                "tool_arguments_parse_error_count"
            ],
            "tool_name_parse_error_count": parse_issue_metadata[
                "tool_name_parse_error_count"
            ],
            "finish_reason": response.finish_reason,
        },
    )

    return response


@checkpoint(type="llm_call")
def _llm_checkpoint_call(request: _LLMRequest) -> LLMResponse:
    """Synthetic checkpoint used for flow-body `kitaru.llm()` calls."""
    return _execute_llm_call(request)


def llm(
    prompt: str | list[dict[str, Any]],
    *,
    model: str | None = None,
    system: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    tools: list[LLMToolDefinition | dict[str, Any]] | None = None,
    tool_choice: str | dict[str, Any] | None = None,
    name: str | None = None,
) -> Any:
    """Make a tracked LLM call.

    ``kitaru.llm()`` has two concrete runtime shapes:

    - inside a ``@checkpoint`` body, it executes immediately and returns an
      :class:`LLMResponse`;
    - inside a ``@flow`` body but outside a checkpoint, it creates a synthetic
      durable checkpoint and returns that checkpoint's output handle. Call
      ``handle.load()`` to retrieve the :class:`LLMResponse`.

    Args:
        prompt: User prompt text or a chat-style message list.
        model: Model alias or provider/model identifier
            (e.g. ``openai/gpt-5-nano``).
        system: Optional system prompt.
        temperature: Optional sampling temperature.
        max_tokens: Optional maximum response tokens.
        tools: Optional OpenAI-style function tool definitions. These are
            passed through to OpenAI-compatible providers and translated for
            Anthropic.
        tool_choice: Optional tool-choice mode or named tool selection.
        name: Optional display name for this call.

    Returns:
        Inside a checkpoint, a concrete LLMResponse. Inside a flow body, a
        durable output handle whose `.load()` returns an LLMResponse; load it
        before inspecting `.content`, `.tool_calls`, `.usage`, or
        `.finish_reason`.

    Raises:
        KitaruContextError: If called outside a flow.
        KitaruUsageError: If prompt, model input, or provider is invalid,
            or if the required provider SDK is not installed.
        KitaruRuntimeError: If credentials or response content are invalid.
        KitaruBackendError: If secret retrieval or the provider call fails.
    """
    if not _is_inside_flow():
        raise KitaruContextError(_LLM_OUTSIDE_FLOW_ERROR)
    _warn_once_for_llm_response_migration()

    call_name = _reserve_llm_call_name(name)

    request = _LLMRequest(
        prompt=prompt,
        model=model,
        system=system,
        temperature=temperature,
        max_tokens=max_tokens,
        tools=tools,
        tool_choice=tool_choice,
        call_name=call_name,
    )

    if _is_inside_checkpoint():
        return _execute_llm_call(request)

    return _llm_checkpoint_call(request, id=request.call_name)


def llm_text(*args: Any, **kwargs: Any) -> str:
    """Compatibility helper that returns text from ``kitaru.llm()``.

    Inside a checkpoint this unwraps the immediate :class:`LLMResponse`. Inside
    a flow body this loads the synthetic checkpoint handle first. Tool-only
    responses return ``""``.
    """
    global _LLM_DEPRECATION_WARNED
    previous_warning_state = _LLM_DEPRECATION_WARNED
    _LLM_DEPRECATION_WARNED = True
    try:
        result = llm(*args, **kwargs)
    finally:
        _LLM_DEPRECATION_WARNED = previous_warning_state
    if isinstance(result, LLMResponse):
        return result.content or ""

    load = getattr(result, "load", None)
    if callable(load):
        loaded = load()
        if isinstance(loaded, LLMResponse):
            return loaded.content or ""
        content = getattr(loaded, "content", None)
        if content is None:
            return ""
        return str(content)

    content = getattr(result, "content", None)
    if content is None:
        return ""
    return str(content)
