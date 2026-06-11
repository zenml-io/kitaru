"""Small bridge around Google/Gemini Interactions API calls."""

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from importlib import metadata
from typing import Any, Literal, NoReturn, cast

from kitaru.errors import KitaruFeatureNotAvailableError, KitaruRuntimeError

from ._constants import INTERACTIONS_CONTRACT_ERROR_MESSAGE
from ._serialization import to_json_safe
from ._stream_accumulator import _StreamAccumulator
from ._stream_shapes import (
    CALL_ID_TYPE_FRAGMENTS,
    coerce_string,
    dict_or_none,
    environment_id,
    extract,
    has_unsafe_role_or_type_marker,
    normalize_token,
    normalized_token_contains_any,
    sequence_or_empty,
)
from ._types import GeminiInteractionRequest, GeminiInteractionStepSummary
from ._utils import elapsed_ms

_STABLE_STATUSES = frozenset({"completed", "requires_action"})
_TERMINAL_FAILURE_STATUSES = frozenset(
    {"failed", "cancelled", "canceled", "incomplete", "budget_exceeded"}
)
_SAFE_ROLES = frozenset({"assistant", "model"})
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
    stream_metadata: dict[str, Any] | None = None


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
    resolved_client = _resolve_client(client=client, client_factory=client_factory)
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
        status = extract(interaction, "status")
        if (
            request.background
            and status not in _STABLE_STATUSES
            and status not in _TERMINAL_FAILURE_STATUSES
        ):
            # Do not create a second server job. Keep polling the returned id.
            interaction_id = coerce_string(extract(interaction, "id"))
            if interaction_id is not None and request.timeout_s is not None:
                interaction, poll_count = await _poll_until_stable_or_terminal(
                    interaction_resource=interaction_resource,
                    interaction=interaction,
                    interaction_id=interaction_id,
                    deadline=started_at + request.timeout_s,
                    poll_interval_s=poll_interval_s,
                    poll_count=poll_count,
                )
    return _normalize_stable_interaction(
        interaction,
        duration_ms=elapsed_ms(started_at),
        poll_count=poll_count,
    )


async def run_gemini_interaction_streamed(
    *,
    request: GeminiInteractionRequest,
    client: Any | None,
    client_factory: Callable[[], Any] | None,
    on_event: Callable[[Any], Any] | None = None,
    allow_sync_stream: bool = False,
    poll_interval_s: float = 2.0,
) -> GeminiInteractionPayload:
    """Drain one Gemini Interactions stream and normalize its final result."""
    started_at = time.perf_counter()
    resolved_client = _resolve_client(client=client, client_factory=client_factory)
    interaction_resource = _validate_streaming_interactions_resource(
        _stream_interactions_resource(
            resolved_client,
            allow_sync_stream=allow_sync_stream,
        ),
        request=request,
    )

    if request.kind == "poll":
        return await _run_poll_stream(
            request=request,
            interaction_resource=interaction_resource,
            started_at=started_at,
            on_event=on_event,
            allow_sync_stream=allow_sync_stream,
        )
    if request.background:
        return await _run_background_stream(
            request=request,
            interaction_resource=interaction_resource,
            started_at=started_at,
            on_event=on_event,
            allow_sync_stream=allow_sync_stream,
            poll_interval_s=poll_interval_s,
        )
    return await _run_direct_create_stream(
        request=request,
        interaction_resource=interaction_resource,
        started_at=started_at,
        on_event=on_event,
        allow_sync_stream=allow_sync_stream,
    )


async def _run_direct_create_stream(
    *,
    request: GeminiInteractionRequest,
    interaction_resource: Any,
    started_at: float,
    on_event: Callable[[Any], Any] | None,
    allow_sync_stream: bool,
) -> GeminiInteractionPayload:
    stream = await _maybe_await(
        interaction_resource.create(
            **_build_create_kwargs(request),
            stream=True,
        )
    )
    accumulator = await _drain_stream(
        request=request,
        stream=stream,
        on_event=on_event,
        allow_sync_stream=allow_sync_stream,
    )
    return _normalize_streamed_interaction(
        accumulator.build_interaction(),
        started_at=started_at,
        poll_count=0,
        accumulator=accumulator,
        observation_mode="direct_create_stream",
        fallback_used=False,
    )


async def _run_poll_stream(
    *,
    request: GeminiInteractionRequest,
    interaction_resource: Any,
    started_at: float,
    on_event: Callable[[Any], Any] | None,
    allow_sync_stream: bool,
) -> GeminiInteractionPayload:
    interaction_id = cast(str, request.interaction_id)
    stream = await _maybe_await(
        interaction_resource.get(
            interaction_id,
            **_build_get_kwargs(request),
            stream=True,
        )
    )
    accumulator = await _drain_stream(
        request=request,
        stream=stream,
        on_event=on_event,
        allow_sync_stream=allow_sync_stream,
    )
    return _normalize_streamed_interaction(
        accumulator.build_interaction(),
        started_at=started_at,
        poll_count=1,
        accumulator=accumulator,
        observation_mode="poll_get_stream",
        fallback_used=False,
    )


async def _run_background_stream(
    *,
    request: GeminiInteractionRequest,
    interaction_resource: Any,
    started_at: float,
    on_event: Callable[[Any], Any] | None,
    allow_sync_stream: bool,
    poll_interval_s: float,
) -> GeminiInteractionPayload:
    deadline = started_at + request.timeout_s if request.timeout_s is not None else None
    create_kwargs = _build_create_kwargs(request)
    if deadline is not None:
        create_kwargs["timeout"] = _remaining_timeout_s(deadline)
    created_interaction = await _maybe_await(
        interaction_resource.create(**create_kwargs)
    )
    interaction_id = coerce_string(extract(created_interaction, "id"))
    if interaction_id is None:
        raise KitaruRuntimeError(
            "Gemini background streamed interaction was created without an "
            "interaction id. Kitaru cannot safely observe or poll it without "
            "risking duplicate provider work."
        )

    accumulator = _StreamAccumulator(request=request)
    accumulator.seed_interaction(created_interaction)

    created_status = extract(created_interaction, "status")
    if (
        created_status in _STABLE_STATUSES
        or created_status in _TERMINAL_FAILURE_STATUSES
    ):
        return _normalize_stable_interaction(
            created_interaction,
            duration_ms=elapsed_ms(started_at),
            poll_count=0,
            stream_metadata=_stream_metadata(
                accumulator,
                observation_mode="background_create_completed",
                fallback_used=False,
            ),
        )

    if deadline is None:
        return await _fallback_poll_background_stream(
            request=request,
            interaction_resource=interaction_resource,
            interaction=created_interaction,
            interaction_id=interaction_id,
            started_at=started_at,
            deadline=deadline,
            accumulator=accumulator,
            poll_interval_s=poll_interval_s,
        )
    if deadline is not None and _remaining_timeout_s(deadline) <= 0:
        return await _fallback_poll_background_stream(
            request=request,
            interaction_resource=interaction_resource,
            interaction=created_interaction,
            interaction_id=interaction_id,
            started_at=started_at,
            deadline=deadline,
            accumulator=accumulator,
            poll_interval_s=poll_interval_s,
        )
    try:
        stream = await _maybe_await(
            interaction_resource.get(
                interaction_id,
                **_build_background_get_kwargs(request, deadline=deadline),
                stream=True,
            )
        )
        accumulator = await _drain_stream(
            request=request,
            stream=stream,
            on_event=on_event,
            allow_sync_stream=allow_sync_stream,
            accumulator=accumulator,
        )
    except Exception:
        return await _fallback_poll_background_stream(
            request=request,
            interaction_resource=interaction_resource,
            interaction=created_interaction,
            interaction_id=interaction_id,
            started_at=started_at,
            deadline=deadline,
            accumulator=accumulator,
            poll_interval_s=poll_interval_s,
        )

    stream_interaction = accumulator.build_interaction()
    stream_status = extract(stream_interaction, "status")
    if stream_status in _STABLE_STATUSES or stream_status in _TERMINAL_FAILURE_STATUSES:
        return _normalize_streamed_interaction(
            stream_interaction,
            started_at=started_at,
            poll_count=1,
            accumulator=accumulator,
            observation_mode="background_create_then_get_stream",
            fallback_used=False,
        )
    return await _fallback_poll_background_stream(
        request=request,
        interaction_resource=interaction_resource,
        interaction=stream_interaction,
        interaction_id=interaction_id,
        started_at=started_at,
        deadline=deadline,
        accumulator=accumulator,
        poll_interval_s=poll_interval_s,
    )


async def _fallback_poll_background_stream(
    *,
    request: GeminiInteractionRequest,
    interaction_resource: Any,
    interaction: Any,
    interaction_id: str,
    started_at: float,
    deadline: float | None,
    accumulator: _StreamAccumulator,
    poll_interval_s: float,
) -> GeminiInteractionPayload:
    interaction, poll_count = await _poll_background_same_id_after_stream(
        interaction_resource=interaction_resource,
        interaction=interaction,
        interaction_id=interaction_id,
        deadline=deadline,
        poll_interval_s=poll_interval_s,
    )
    payload = _normalize_stable_interaction(
        interaction,
        duration_ms=elapsed_ms(started_at),
        poll_count=poll_count,
    )
    metadata = _stream_metadata(
        accumulator,
        observation_mode="background_create_then_get_stream",
        fallback_used=True,
    )
    metadata["final_status"] = payload.status
    return replace(
        payload,
        stream_metadata=metadata,
        warnings=[
            *payload.warnings,
            "Gemini background stream observation failed or ended before a "
            "stable result; Kitaru continued by polling the same interaction id.",
        ],
    )


async def _drain_stream(
    *,
    request: GeminiInteractionRequest,
    stream: Any,
    on_event: Callable[[Any], Any] | None,
    allow_sync_stream: bool,
    accumulator: _StreamAccumulator | None = None,
) -> _StreamAccumulator:
    accumulator = accumulator or _StreamAccumulator(request=request)
    async for event in _iter_stream(stream, allow_sync_stream=allow_sync_stream):
        if on_event is not None:
            with suppress(Exception):
                await _maybe_await(on_event(event))
        accumulator.record(event)
    return accumulator


def _normalize_streamed_interaction(
    interaction: Any,
    *,
    started_at: float,
    poll_count: int,
    accumulator: _StreamAccumulator,
    observation_mode: str,
    fallback_used: bool,
) -> GeminiInteractionPayload:
    payload = _normalize_stable_interaction(
        interaction,
        duration_ms=elapsed_ms(started_at),
        poll_count=poll_count,
        stream_metadata=_stream_metadata(
            accumulator,
            observation_mode=observation_mode,
            fallback_used=fallback_used,
        ),
    )
    if payload.interaction_id is None:
        raise KitaruRuntimeError(
            "Gemini streamed interaction ended without an interaction id. "
            "Kitaru will not store an incomplete streamed provider result."
        )
    return payload


def _stream_metadata(
    accumulator: _StreamAccumulator,
    *,
    observation_mode: str,
    fallback_used: bool,
) -> dict[str, Any]:
    metadata = accumulator.stream_metadata()
    metadata["observation_mode"] = observation_mode
    metadata["fallback_used"] = fallback_used
    return metadata


def _build_background_get_kwargs(
    request: GeminiInteractionRequest,
    *,
    deadline: float | None,
) -> dict[str, Any]:
    kwargs = _build_get_kwargs(request)
    if deadline is not None:
        kwargs["timeout"] = _remaining_timeout_s(deadline)
    return kwargs


async def _poll_background_same_id_after_stream(
    *,
    interaction_resource: Any,
    interaction: Any,
    interaction_id: str,
    deadline: float | None,
    poll_interval_s: float,
) -> tuple[Any, int]:
    kwargs: dict[str, Any] = {}
    if deadline is not None:
        remaining_s = _remaining_timeout_s(deadline)
        if remaining_s <= 0:
            return interaction, 0
        kwargs["timeout"] = remaining_s
    interaction = await _maybe_await(interaction_resource.get(interaction_id, **kwargs))
    poll_count = 1
    if deadline is None:
        return interaction, poll_count
    return await _poll_until_stable_or_terminal(
        interaction_resource=interaction_resource,
        interaction=interaction,
        interaction_id=interaction_id,
        deadline=deadline,
        poll_interval_s=poll_interval_s,
        poll_count=poll_count,
    )


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
        status = extract(interaction, "status")
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
    if request.agent_config is not None:
        kwargs["agent_config"] = request.agent_config
    if request.response_format is not None:
        kwargs["response_format"] = request.response_format
    if request.response_mime_type is not None:
        kwargs["response_mime_type"] = request.response_mime_type
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


def _normalize_stable_interaction(
    interaction: Any,
    *,
    duration_ms: float,
    poll_count: int = 0,
    stream_metadata: dict[str, Any] | None = None,
) -> GeminiInteractionPayload:
    payload = normalize_interaction(
        interaction,
        duration_ms=duration_ms,
        poll_count=poll_count,
    )
    if stream_metadata is not None:
        payload = replace(payload, stream_metadata=stream_metadata)
    if payload.status not in _STABLE_STATUSES:
        _raise_unstable_status(payload)
    return payload


def normalize_interaction(
    interaction: Any,
    *,
    duration_ms: float,
    poll_count: int = 0,
) -> GeminiInteractionPayload:
    """Normalize a Gemini SDK interaction into adapter-local data."""
    warnings: list[str] = []
    raw_steps_source: StepSource = "steps"
    raw_steps = sequence_or_empty(extract(interaction, "steps"))
    if not raw_steps:
        raw_steps_source = "outputs"
        raw_steps = sequence_or_empty(extract(interaction, "outputs"))
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
    output_text = coerce_string(extract(interaction, "output_text"))
    if output_text is None:
        output_text = _extract_output_text(
            raw_steps,
            safe_index=safe_text_preview_index,
        )
    usage = dict_or_none(extract(interaction, "usage"))
    return GeminiInteractionPayload(
        status=str(extract(interaction, "status") or "unknown"),
        interaction_id=coerce_string(extract(interaction, "id")),
        previous_interaction_id=coerce_string(
            extract(interaction, "previous_interaction_id")
        ),
        output_text=output_text,
        model=coerce_string(extract(interaction, "model")),
        agent=coerce_string(extract(interaction, "agent")),
        environment_id=environment_id(interaction),
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


def _resolve_client(
    *,
    client: Any | None,
    client_factory: Callable[[], Any] | None,
) -> Any:
    if client is not None:
        return client
    if client_factory is not None:
        return client_factory()
    from google import genai

    return genai.Client()


def _validate_interactions_resource(client: Any) -> Any:
    interaction_resource = extract(client, "interactions")
    create = extract(interaction_resource, "create")
    get = extract(interaction_resource, "get")
    if interaction_resource is None or not callable(create) or not callable(get):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)
    return interaction_resource


def _stream_interactions_resource(
    client: Any,
    *,
    allow_sync_stream: bool,
) -> Any:
    aio = extract(client, "aio")
    aio_resource = extract(aio, "interactions") if aio is not None else None
    if _is_interactions_resource(aio_resource):
        return aio_resource
    if not allow_sync_stream:
        raise KitaruFeatureNotAvailableError(
            "Gemini Interactions async streaming requires a google-genai client "
            "with `client.aio.interactions`. Kitaru refuses to start provider "
            "streaming work through the synchronous interactions resource from "
            "async `run_stream(...)` because draining it would block the event loop. "
            "Use `run_stream_sync(...)`, provide an async google-genai client, or "
            "upgrade `google-genai`."
        )
    return _validate_interactions_resource(client)


def _validate_streaming_interactions_resource(
    interaction_resource: Any,
    *,
    request: GeminiInteractionRequest,
) -> Any:
    create = extract(interaction_resource, "create")
    get = extract(interaction_resource, "get")
    if not _is_interactions_resource(interaction_resource):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)
    if request.background and request.timeout_s is None:
        return interaction_resource
    needs_get_stream = request.kind == "poll" or (
        request.background and request.timeout_s is not None
    )
    target = get if needs_get_stream else create
    target_name = "interactions.get" if needs_get_stream else "interactions.create"
    if not _call_accepts_keyword(target, "stream"):
        raise KitaruFeatureNotAvailableError(
            "Gemini Interactions streaming requires a google-genai SDK whose "
            f"{target_name}(...) method accepts `stream=True`. Install a newer "
            "`kitaru[gemini]` / `google-genai` version to use run_stream()."
        )
    return interaction_resource


def _is_interactions_resource(value: Any) -> bool:
    return callable(extract(value, "create")) and callable(extract(value, "get"))


def _call_accepts_keyword(function: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return True
    return keyword in signature.parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )


async def _iter_stream(stream: Any, *, allow_sync_stream: bool) -> AsyncIterator[Any]:
    stream = await _maybe_await(stream)
    if hasattr(stream, "__aiter__"):
        async for item in stream:
            yield await _maybe_await(item)
        return
    if isinstance(stream, str | bytes | bytearray):
        raise KitaruRuntimeError(
            "Gemini streamed interaction did not return an event iterator."
        )
    if not allow_sync_stream:
        raise KitaruFeatureNotAvailableError(
            "Gemini Interactions streaming returned a synchronous event iterator "
            "for async `run_stream(...)`. Use `run_stream_sync(...)`, provide an "
            "async google-genai interactions client, or upgrade `google-genai` "
            "to a version that returns AsyncStream for async interactions."
        )
    try:
        iterator = iter(stream)
    except TypeError as exc:
        raise KitaruRuntimeError(
            "Gemini streamed interaction did not return an event iterator."
        ) from exc
    for item in iterator:
        yield await _maybe_await(item)


def _summarize_step(
    index: int,
    value: Any,
    *,
    text_preview_allowed: bool,
) -> GeminiInteractionStepSummary:
    raw = to_json_safe(value)
    raw_keys = sorted(str(key) for key in raw) if isinstance(raw, Mapping) else []
    type_value = coerce_string(extract(value, "type")) or type(value).__name__
    call_id = _extract_step_call_id(value)
    return GeminiInteractionStepSummary(
        index=index,
        step_id=coerce_string(extract(value, "id")),
        type=type_value,
        status=coerce_string(extract(value, "status")),
        call_id=call_id,
        tool_name=coerce_string(extract(value, "name")),
        text_preview=_text_preview(_extract_safe_text(value))
        if text_preview_allowed
        else None,
        raw_keys=raw_keys,
    )


def _extract_step_call_id(value: Any) -> str | None:
    explicit_call_id = coerce_string(extract(value, "call_id"))
    if explicit_call_id is not None:
        return explicit_call_id
    step_type = _normalized_step_type(value)
    if step_type is not None and normalized_token_contains_any(
        step_type, CALL_ID_TYPE_FRAGMENTS
    ):
        return coerce_string(extract(value, "id"))
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
    status = normalize_token(coerce_string(extract(value, "status")))
    return (
        has_unsafe_role_or_type_marker(
            role=safety.normalized_role,
            step_type=safety.normalized_type,
        )
        or status == "requires_action"
    )


def _classify_step_text(value: Any, *, source: StepSource) -> _StepTextSafety:
    role = _normalized_step_role(value)
    step_type = _normalized_step_type(value)
    unsafe = has_unsafe_role_or_type_marker(role=role, step_type=step_type)
    safe = role in _SAFE_ROLES or step_type in _SAFE_STEP_TYPES
    if source == "outputs" and step_type in _OUTPUTS_COMPAT_SAFE_TYPES:
        safe = True
    return _StepTextSafety(
        normalized_type=step_type,
        normalized_role=role,
        safe_to_extract_text=safe and not unsafe,
    )


def _normalized_step_type(value: Any) -> str | None:
    step_type = coerce_string(extract(value, "type"))
    return normalize_token(step_type)


def _normalized_step_role(value: Any) -> str | None:
    for key in ("role", "author", "speaker"):
        role = extract(value, key)
        if isinstance(role, Mapping):
            for nested_key in ("role", "name", "type"):
                nested_role = normalize_token(coerce_string(extract(role, nested_key)))
                if nested_role:
                    return nested_role
        normalized_role = normalize_token(coerce_string(role))
        if normalized_role:
            return normalized_role
    return None


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

    content = extract(value, "content")
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

    direct = extract(value, "text")
    if isinstance(direct, str):
        return direct, True
    if isinstance(content, str):
        return content, True
    result = extract(value, "result")
    if isinstance(result, str):
        return result, True
    return None, True


def _has_unsafe_text_marker(value: Any) -> bool:
    return has_unsafe_role_or_type_marker(
        role=_normalized_step_role(value),
        step_type=_normalized_step_type(value),
    )


def _text_preview(value: str | None, *, limit: int = 160) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 1]}…"
