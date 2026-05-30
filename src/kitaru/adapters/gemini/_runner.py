"""Small bridge around Google/Gemini Interactions API calls."""

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from importlib import metadata
from typing import Any, cast

from ._serialization import to_json_safe
from ._types import GeminiInteractionRequest, GeminiInteractionStepSummary
from ._utils import elapsed_ms

_STABLE_STATUSES = frozenset({"completed", "requires_action"})


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

    interaction_resource = resolved_client.interactions
    if request.kind == "poll":
        interaction_id = cast(str, request.interaction_id)
        interaction = await _maybe_await(
            interaction_resource.get(
                interaction_id,
                **_build_get_kwargs(request),
            )
        )
        poll_count = 1
    else:
        interaction = await _maybe_await(
            interaction_resource.create(**_build_create_kwargs(request))
        )
        poll_count = 0
        if (
            request.background
            and _extract(interaction, "status") not in _STABLE_STATUSES
        ):
            # Do not create a second server job. Keep polling the returned id.
            interaction_id = _string_or_none(_extract(interaction, "id"))
            if interaction_id is not None and request.timeout_s is not None:
                deadline = time.perf_counter() + request.timeout_s
                while time.perf_counter() < deadline:
                    await asyncio.sleep(poll_interval_s)
                    poll_count += 1
                    interaction = await _maybe_await(
                        interaction_resource.get(interaction_id)
                    )
                    if _extract(interaction, "status") in _STABLE_STATUSES:
                        break
    payload = normalize_interaction(
        interaction,
        duration_ms=elapsed_ms(started_at),
        poll_count=poll_count,
    )
    if payload.status not in _STABLE_STATUSES:
        payload = replace(
            payload,
            warnings=[
                *payload.warnings,
                (
                    "Gemini interaction did not reach a v1 stable status. "
                    "Use GeminiInteractionRequest.poll(interaction_id=...) to "
                    "continue an existing background interaction instead of "
                    "starting a duplicate job."
                ),
            ],
        )
    return payload


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
    raw_steps = _sequence_or_empty(_extract(interaction, "steps"))
    if not raw_steps:
        raw_steps = _sequence_or_empty(_extract(interaction, "outputs"))
        if raw_steps:
            warnings.append(
                "Gemini SDK response exposed outputs rather than `steps`; "
                "normalizing outputs as step summaries for compatibility."
            )
    summaries = [
        _summarize_step(index=index, value=value)
        for index, value in enumerate(raw_steps)
    ]
    output_text = _extract_output_text(raw_steps)
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


def _summarize_step(index: int, value: Any) -> GeminiInteractionStepSummary:
    raw = to_json_safe(value)
    raw_keys = sorted(str(key) for key in raw) if isinstance(raw, Mapping) else []
    type_value = _string_or_none(_extract(value, "type")) or type(value).__name__
    call_id = _string_or_none(_extract(value, "call_id")) or _string_or_none(
        _extract(value, "id")
    )
    return GeminiInteractionStepSummary(
        index=index,
        step_id=_string_or_none(_extract(value, "id")),
        type=type_value,
        status=_string_or_none(_extract(value, "status")),
        call_id=call_id,
        tool_name=_string_or_none(_extract(value, "name")),
        text_preview=_text_preview(_extract_text(value)),
        raw_keys=raw_keys,
    )


def _extract_output_text(steps: list[Any]) -> str | None:
    parts = [_extract_text(step) for step in steps]
    text = "\n".join(part for part in parts if part)
    return text or None


def _extract_text(value: Any) -> str | None:
    direct = _extract(value, "text")
    if isinstance(direct, str):
        return direct
    content = _extract(value, "content")
    if isinstance(content, str):
        return content
    if isinstance(content, Sequence) and not isinstance(
        content, str | bytes | bytearray
    ):
        nested = [_extract_text(item) for item in content]
        text = "\n".join(item for item in nested if item)
        return text or None
    result = _extract(value, "result")
    if isinstance(result, str):
        return result
    return None


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
