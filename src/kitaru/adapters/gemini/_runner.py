"""Small bridge around Google/Gemini Interactions API calls."""

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib import metadata
from typing import Any, Literal, NoReturn, cast

from kitaru.errors import KitaruFeatureNotAvailableError, KitaruRuntimeError

from ._constants import INTERACTIONS_CONTRACT_ERROR_MESSAGE
from ._serialization import to_json_safe
from ._types import GeminiInteractionRequest, GeminiInteractionStepSummary
from ._utils import elapsed_ms

_STABLE_STATUSES = frozenset({"completed", "requires_action"})
_TERMINAL_FAILURE_STATUSES = frozenset(
    {"failed", "cancelled", "canceled", "incomplete", "budget_exceeded"}
)
_SAFE_ROLES = frozenset({"assistant", "model"})
_UNSAFE_ROLES = frozenset({"user", "tool", "function", "system", "developer"})
_SAFE_STEP_TYPES = frozenset(
    {
        "model_output",
        "model_response",
        "assistant_message",
        "assistant_output",
        "output_text",
    }
)
_OUTPUTS_COMPAT_SAFE_TYPES = frozenset({"text", "message", "output_text"})
_SAFE_NESTED_TEXT_TYPES = frozenset({"text", "output_text"})
_UNSAFE_TYPE_FRAGMENTS = (
    "user_input",
    "input",
    "tool_result",
    "tool_call",
    "tool",
    "function_result",
    "function_call",
    "function",
    "sandbox",
    "code",
    "shell",
    "terminal",
    "web",
    "mcp",
)
_CALL_ID_TYPE_FRAGMENTS = ("function_call", "tool_call")
StepSource = Literal["steps", "outputs"]


@dataclass(frozen=True)
class _StepTextSafety:
    """Cheap role/type classification for one Gemini timeline step."""

    normalized_type: str | None
    normalized_role: str | None
    safe_to_extract_text: bool


@dataclass(frozen=True)
class GeminiInteractionPayload:
    """Internal, pre-result payload extracted from one SDK interaction."""

    status: str
    interaction_id: str | None
    previous_interaction_id: str | None
    output_text: str | None
    model: str | None
    agent: str | None
    environment_id: str | None
    steps: list[GeminiInteractionStepSummary]
    raw_interaction: Any
    raw_steps: list[Any]
    usage: dict[str, Any] | None
    duration_ms: float
    poll_count: int = 0
    sdk_version: str = "unknown"
    warnings: list[str] = field(default_factory=list)


def google_genai_version() -> str:
    """Return the installed Google Gen AI SDK version when discoverable."""
    try:
        return metadata.version("google-genai")
    except metadata.PackageNotFoundError:
        return "unknown"


async def run_gemini_interaction(
    *,
    request: GeminiInteractionRequest,
    client: Any | None,
    client_factory: Callable[[], Any] | None,
    poll_interval_s: float,
) -> GeminiInteractionPayload:
    """Execute one Gemini Interactions API operation and normalize the result."""
    started_at = time.perf_counter()
    resolved_client = client
    if resolved_client is None:
        if client_factory is not None:
            resolved_client = client_factory()
        else:
            from google import genai

            resolved_client = genai.Client()

    interaction_resource = _validate_interactions_resource(resolved_client)
    if request.kind == "poll":
        interaction_id = cast(str, request.interaction_id)
        deadline = None
        get_kwargs = _build_get_kwargs(request)
        if request.timeout_s is not None:
            deadline = started_at + request.timeout_s
            get_kwargs["timeout"] = _remaining_timeout_s(deadline)
        interaction = await _maybe_await(
            interaction_resource.get(
                interaction_id,
                **get_kwargs,
            )
        )
        poll_count = 1
        if deadline is not None:
            interaction, poll_count = await _poll_until_stable_or_terminal(
                interaction_resource=interaction_resource,
                interaction=interaction,
                interaction_id=interaction_id,
                deadline=deadline,
                poll_interval_s=poll_interval_s,
                poll_count=poll_count,
            )
    else:
        interaction = await _maybe_await(
            interaction_resource.create(**_build_create_kwargs(request))
        )
        poll_count = 0
        status = _extract(interaction, "status")
        if (
            request.background
            and status not in _STABLE_STATUSES
            and status not in _TERMINAL_FAILURE_STATUSES
        ):
            # Do not create a second server job. Keep polling the returned id.
            interaction_id = _string_or_none(_extract(interaction, "id"))
            if interaction_id is not None and request.timeout_s is not None:
                interaction, poll_count = await _poll_until_stable_or_terminal(
                    interaction_resource=interaction_resource,
                    interaction=interaction,
                    interaction_id=interaction_id,
                    deadline=started_at + request.timeout_s,
                    poll_interval_s=poll_interval_s,
                    poll_count=poll_count,
                )
    payload = normalize_interaction(
        interaction,
        duration_ms=elapsed_ms(started_at),
        poll_count=poll_count,
    )
    if payload.status not in _STABLE_STATUSES:
        _raise_unstable_status(payload)
    return payload


async def _poll_until_stable_or_terminal(
    *,
    interaction_resource: Any,
    interaction: Any,
    interaction_id: str,
    deadline: float,
    poll_interval_s: float,
    poll_count: int,
) -> tuple[Any, int]:
    while True:
        status = _extract(interaction, "status")
        if status in _STABLE_STATUSES or status in _TERMINAL_FAILURE_STATUSES:
            break
        remaining_s = _remaining_timeout_s(deadline)
        if remaining_s <= 0:
            break
        await asyncio.sleep(min(poll_interval_s, remaining_s))
        remaining_s = _remaining_timeout_s(deadline)
        if remaining_s <= 0:
            break
        poll_count += 1
        interaction = await _maybe_await(
            interaction_resource.get(interaction_id, timeout=remaining_s)
        )
    return interaction, poll_count


def _remaining_timeout_s(deadline: float) -> float:
    return max(0.0, deadline - time.perf_counter())


def _raise_unstable_status(payload: GeminiInteractionPayload) -> NoReturn:
    interaction_id = payload.interaction_id or "(not reported by SDK)"
    stable_statuses = ", ".join(repr(status) for status in sorted(_STABLE_STATUSES))
    raise KitaruRuntimeError(
        "Gemini interaction "
        f"{interaction_id!r} returned non-stable status {payload.status!r}. "
        "Kitaru will not store this as a successful durable checkpoint because "
        "replay would treat an unfinished provider job as completed work. "
        f"Only statuses {stable_statuses} complete normally. "
        "For background jobs, use "
        f"GeminiInteractionRequest.poll(interaction_id={interaction_id!r}) "
        "to continue polling the same remote job instead of starting a "
        "duplicate job."
    )


def _build_create_kwargs(request: GeminiInteractionRequest) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"input": request.input}
    if request.model is not None:
        kwargs["model"] = request.model
    if request.agent is not None:
        kwargs["agent"] = request.agent
    if request.previous_interaction_id is not None:
        kwargs["previous_interaction_id"] = request.previous_interaction_id
    if request.background:
        kwargs["background"] = request.background
    kwargs["store"] = request.store
    if request.tools:
        kwargs["tools"] = request.tools
    if request.system_instruction is not None:
        kwargs["system_instruction"] = request.system_instruction
    if request.generation_config:
        kwargs["generation_config"] = request.generation_config
    if request.response_format is not None:
        kwargs["response_format"] = request.response_format
    if request.environment is not None:
        # The Interactions API exposes `environment` as a top-level request
        # field, but the current google-genai SDK does not expose it as a
        # first-class keyword yet. Keep it out of `agent_config`, which is for
        # typed agent configuration such as Deep Research settings.
        kwargs["extra_body"] = {"environment": request.environment}
    if request.timeout_s is not None:
        kwargs["timeout"] = request.timeout_s
    return kwargs


def _build_get_kwargs(request: GeminiInteractionRequest) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if request.timeout_s is not None:
        kwargs["timeout"] = request.timeout_s
    return kwargs


def normalize_interaction(
    interaction: Any,
    *,
    duration_ms: float,
    poll_count: int = 0,
) -> GeminiInteractionPayload:
    """Normalize a Gemini SDK interaction into adapter-local data."""
    warnings: list[str] = []
    raw_steps_source: StepSource = "steps"
    raw_steps = _sequence_or_empty(_extract(interaction, "steps"))
    if not raw_steps:
        raw_steps_source = "outputs"
        raw_steps = _sequence_or_empty(_extract(interaction, "outputs"))
        if raw_steps:
            warnings.append(
                "Gemini SDK response exposed outputs rather than `steps`; "
                "normalizing outputs as step summaries for compatibility."
            )
    text_safety = [
        _classify_step_text(value, source=raw_steps_source) for value in raw_steps
    ]
    safe_text_preview_index = _safe_final_output_step_index(
        raw_steps,
        text_safety=text_safety,
    )
    summaries = [
        _summarize_step(
            index=index,
            value=value,
            text_preview_allowed=index == safe_text_preview_index,
        )
        for index, value in enumerate(raw_steps)
    ]
    output_text = _string_or_none(_extract(interaction, "output_text"))
    if output_text is None:
        output_text = _extract_output_text(
            raw_steps,
            safe_index=safe_text_preview_index,
        )
    usage = _dict_or_none(_extract(interaction, "usage"))
    return GeminiInteractionPayload(
        status=str(_extract(interaction, "status") or "unknown"),
        interaction_id=_string_or_none(_extract(interaction, "id")),
        previous_interaction_id=_string_or_none(
            _extract(interaction, "previous_interaction_id")
        ),
        output_text=output_text,
        model=_string_or_none(_extract(interaction, "model")),
        agent=_string_or_none(_extract(interaction, "agent")),
        environment_id=_extract_environment_id(interaction),
        steps=summaries,
        raw_interaction=to_json_safe(interaction),
        raw_steps=[to_json_safe(step) for step in raw_steps],
        usage=usage,
        duration_ms=duration_ms,
        poll_count=poll_count,
        sdk_version=google_genai_version(),
        warnings=warnings,
    )


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _validate_interactions_resource(client: Any) -> Any:
    interaction_resource = _extract(client, "interactions")
    create = _extract(interaction_resource, "create")
    get = _extract(interaction_resource, "get")
    if interaction_resource is None or not callable(create) or not callable(get):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)
    return interaction_resource


def _extract(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _sequence_or_empty(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str | bytes | bytearray):
        return [value]
    if isinstance(value, Sequence):
        return list(value)
    return [value]


def _summarize_step(
    index: int,
    value: Any,
    *,
    text_preview_allowed: bool,
) -> GeminiInteractionStepSummary:
    raw = to_json_safe(value)
    raw_keys = sorted(str(key) for key in raw) if isinstance(raw, Mapping) else []
    type_value = _string_or_none(_extract(value, "type")) or type(value).__name__
    call_id = _extract_step_call_id(value)
    return GeminiInteractionStepSummary(
        index=index,
        step_id=_string_or_none(_extract(value, "id")),
        type=type_value,
        status=_string_or_none(_extract(value, "status")),
        call_id=call_id,
        tool_name=_string_or_none(_extract(value, "name")),
        text_preview=_text_preview(_extract_safe_text(value))
        if text_preview_allowed
        else None,
        raw_keys=raw_keys,
    )


def _extract_step_call_id(value: Any) -> str | None:
    explicit_call_id = _string_or_none(_extract(value, "call_id"))
    if explicit_call_id is not None:
        return explicit_call_id
    step_type = _normalized_step_type(value)
    if step_type is not None and any(
        fragment in step_type for fragment in _CALL_ID_TYPE_FRAGMENTS
    ):
        return _string_or_none(_extract(value, "id"))
    return None


def _extract_output_text(
    steps: list[Any],
    *,
    safe_index: int | None,
) -> str | None:
    if safe_index is None:
        return None
    text = _extract_safe_text(steps[safe_index])
    return text or None


def _safe_final_output_step_index(
    steps: list[Any],
    *,
    text_safety: list[_StepTextSafety],
) -> int | None:
    for index in range(len(steps) - 1, -1, -1):
        safety = text_safety[index]
        if safety.safe_to_extract_text:
            if _extract_safe_text(steps[index]) is not None:
                return index
            continue
        if _blocks_fallback_output_text(steps[index], safety=safety):
            return None
    return None


def _blocks_fallback_output_text(value: Any, *, safety: _StepTextSafety) -> bool:
    status = _normalized_token(_string_or_none(_extract(value, "status")))
    return (
        _has_unsafe_role_or_type_marker(
            role=safety.normalized_role,
            step_type=safety.normalized_type,
        )
        or status == "requires_action"
    )


def _classify_step_text(value: Any, *, source: StepSource) -> _StepTextSafety:
    role = _normalized_step_role(value)
    step_type = _normalized_step_type(value)
    unsafe = _has_unsafe_role_or_type_marker(role=role, step_type=step_type)
    safe = role in _SAFE_ROLES or step_type in _SAFE_STEP_TYPES
    if source == "outputs" and step_type in _OUTPUTS_COMPAT_SAFE_TYPES:
        safe = True
    return _StepTextSafety(
        normalized_type=step_type,
        normalized_role=role,
        safe_to_extract_text=safe and not unsafe,
    )


def _normalized_step_type(value: Any) -> str | None:
    step_type = _string_or_none(_extract(value, "type"))
    return _normalized_token(step_type)


def _normalized_step_role(value: Any) -> str | None:
    for key in ("role", "author", "speaker"):
        role = _extract(value, key)
        if isinstance(role, Mapping):
            for nested_key in ("role", "name", "type"):
                nested_role = _normalized_token(
                    _string_or_none(_extract(role, nested_key))
                )
                if nested_role:
                    return nested_role
        normalized_role = _normalized_token(_string_or_none(role))
        if normalized_role:
            return normalized_role
    return None


def _normalized_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = "_".join(value.strip().lower().replace("-", "_").split())
    return normalized or None


def _extract_safe_text(value: Any) -> str | None:
    text, is_safe = _extract_safe_text_candidate(value, nested=False)
    if not is_safe:
        return None
    return text


def _extract_safe_text_candidate(
    value: Any,
    *,
    nested: bool,
) -> tuple[str | None, bool]:
    if nested and _has_unsafe_text_marker(value):
        return None, False
    if nested:
        step_type = _normalized_step_type(value)
        if step_type not in _SAFE_NESTED_TEXT_TYPES:
            return None, False

    content = _extract(value, "content")
    if isinstance(content, Mapping):
        nested_text, nested_safe = _extract_safe_text_candidate(
            content,
            nested=True,
        )
        if not nested_safe:
            return None, False
        if nested_text:
            return nested_text, True
    if isinstance(content, Sequence) and not isinstance(
        content, str | bytes | bytearray
    ):
        text_parts: list[str] = []
        for item in content:
            nested_text, nested_safe = _extract_safe_text_candidate(
                item,
                nested=True,
            )
            if not nested_safe:
                return None, False
            if nested_text:
                text_parts.append(nested_text)
        text = "\n".join(text_parts)
        if text:
            return text, True

    direct = _extract(value, "text")
    if isinstance(direct, str):
        return direct, True
    if isinstance(content, str):
        return content, True
    result = _extract(value, "result")
    if isinstance(result, str):
        return result, True
    return None, True


def _has_unsafe_text_marker(value: Any) -> bool:
    return _has_unsafe_role_or_type_marker(
        role=_normalized_step_role(value),
        step_type=_normalized_step_type(value),
    )


def _has_unsafe_role_or_type_marker(
    *,
    role: str | None,
    step_type: str | None,
) -> bool:
    return role in _UNSAFE_ROLES or (
        step_type is not None
        and any(fragment in step_type for fragment in _UNSAFE_TYPE_FRAGMENTS)
    )


def _text_preview(value: str | None, *, limit: int = 160) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1]}…"


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if hasattr(value, "__dict__"):
        return {
            str(key): to_json_safe(nested)
            for key, nested in vars(value).items()
            if not str(key).startswith("_")
        }
    safe = to_json_safe(value)
    return safe if isinstance(safe, dict) else {"value": safe}


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _extract_environment_id(interaction: Any) -> str | None:
    for key in ("environment_id", "environment"):
        value = _string_or_none(_extract(interaction, key))
        if value:
            return value
    agent_config = _extract(interaction, "agent_config")
    if agent_config is not None:
        value = _string_or_none(_extract(agent_config, "environment_id"))
        if value:
            return value
    return None
