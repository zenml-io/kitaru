"""Best-effort live events for PydanticAI streaming."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import kitaru.events as kitaru_events
from kitaru.adapters._streaming_utils import (
    BaseStreamPublisher,
    clip_stream_text,
    first_string,
    safe_int,
    safe_string,
)

from ._tracking import normalize_agent_name

PYDANTIC_AI_STREAM_STARTED = "pydantic_ai.stream.started"
PYDANTIC_AI_STREAM_EVENT = "pydantic_ai.stream.event"
PYDANTIC_AI_STREAM_COMPLETED = "pydantic_ai.stream.completed"
PYDANTIC_AI_STREAM_FAILED = "pydantic_ai.stream.failed"
PYDANTIC_AI_STREAM_EVENT_KINDS = (
    PYDANTIC_AI_STREAM_STARTED,
    PYDANTIC_AI_STREAM_EVENT,
    PYDANTIC_AI_STREAM_COMPLETED,
    PYDANTIC_AI_STREAM_FAILED,
)
PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS = (
    PYDANTIC_AI_STREAM_COMPLETED,
    PYDANTIC_AI_STREAM_FAILED,
)

_MAX_DISPLAY_CHARS = 240
_MAX_TEXT_DELTA_CHARS = 240
_MAX_MESSAGE_CHARS = 500

_MODEL_STREAM_SUPPRESSED: ContextVar[bool] = ContextVar(
    "kitaru_pydantic_ai_model_stream_suppressed", default=False
)
_CURRENT_STREAM_SURFACE: ContextVar[str | None] = ContextVar(
    "kitaru_pydantic_ai_stream_surface", default=None
)


class PydanticAIStreamPublisher(BaseStreamPublisher):
    """Normalize PydanticAI stream events and publish Kitaru live events.

    Live events are radio chatter, not durability. Every publish is best effort:
    a missing checkpoint scope, disabled streaming backend, or normalization bug
    must never change the PydanticAI run result.
    """

    _started_kind = PYDANTIC_AI_STREAM_STARTED
    _completed_kind = PYDANTIC_AI_STREAM_COMPLETED
    _failed_kind = PYDANTIC_AI_STREAM_FAILED
    _normalization_failed_kind = PYDANTIC_AI_STREAM_EVENT
    _started_display = "PydanticAI stream started"
    _completed_display_prefix = "PydanticAI stream completed"
    _failed_display_prefix = "PydanticAI stream failed"
    _normalization_failed_display = "PydanticAI stream event"
    _error_message_limit = _MAX_MESSAGE_CHARS

    def __init__(
        self,
        *,
        agent_name: str,
        surface: str,
        source: str,
        include_content: bool,
        enabled: bool = True,
    ) -> None:
        super().__init__(
            publish=lambda *args, **kwargs: kitaru_events.publish(*args, **kwargs),
            enabled=enabled,
        )
        self._agent_name = normalize_agent_name(agent_name)
        self._surface = surface
        self._source = source
        self._include_content = include_content

    def event(self, event: object) -> None:
        self._publish_stream_item(event)

    def completed(
        self, *, status: str = "completed", event_count: int | None = None
    ) -> None:
        fields: dict[str, Any] = {}
        if event_count is not None:
            fields["event_count"] = event_count
        self._publish_completed(status=status, **fields)

    def failed(self, error: BaseException) -> None:
        self._publish_failed(error)

    def _normalize_publishable(self, item: Any) -> tuple[str, dict[str, Any]]:
        return PYDANTIC_AI_STREAM_EVENT, self.normalize_event(item)

    def _started_fields(self) -> dict[str, Any]:
        return {"status": "started"}

    def _failed_fields(self, error: BaseException) -> dict[str, Any]:
        return {
            "status": "cancelled"
            if isinstance(error, asyncio.CancelledError)
            else "failed"
        }

    def normalize_event(self, event: object) -> dict[str, Any]:
        event_kind = safe_string(getattr(event, "event_kind", None))
        event_type = type(event).__name__
        category = event_kind or safe_string(getattr(event, "kind", None)) or event_type
        payload = self._base_payload(
            category=category,
            display=_display_for_event(
                event,
                category=category,
                include_content=self._include_content,
            ),
            event_type=event_type,
        )
        if event_kind is not None:
            payload["event_kind"] = event_kind
        if (index := safe_int(getattr(event, "index", None))) is not None:
            payload["index"] = index

        _add_tool_metadata(payload, event)
        _add_part_metadata(payload, getattr(event, "part", None))
        _add_delta_metadata(
            payload, getattr(event, "delta", None), self._include_content
        )
        _add_tool_metadata(payload, getattr(event, "result", None))
        return payload

    def _base_payload(
        self, *, category: str, display: str, **fields: Any
    ) -> dict[str, Any]:
        return {
            "adapter": "pydantic_ai",
            "agent_name": self._agent_name,
            "surface": self._surface,
            "source": self._source,
            "category": category,
            "display": clip_stream_text(display, _MAX_DISPLAY_CHARS),
            **fields,
        }


@contextmanager
def suppress_model_stream_live_events() -> Iterator[None]:
    """Temporarily skip model-stream live publishes to avoid duplicates."""
    token = _MODEL_STREAM_SUPPRESSED.set(True)
    try:
        yield
    finally:
        _MODEL_STREAM_SUPPRESSED.reset(token)


def model_stream_live_events_suppressed() -> bool:
    """Return whether the current context is already observing stream events."""
    return _MODEL_STREAM_SUPPRESSED.get()


@contextmanager
def stream_surface(surface: str) -> Iterator[None]:
    """Label downstream model-stream events with the public user surface."""
    token = _CURRENT_STREAM_SURFACE.set(surface)
    try:
        yield
    finally:
        _CURRENT_STREAM_SURFACE.reset(token)


def current_stream_surface(default: str) -> str:
    """Return the current public stream surface label."""
    return _CURRENT_STREAM_SURFACE.get() or default


def _display_for_event(event: object, *, category: str, include_content: bool) -> str:
    if category == "part_delta":
        delta = getattr(event, "delta", None)
        text_delta = _text_delta(delta)
        if text_delta:
            return text_delta if include_content else "Text delta"
    if category == "part_start":
        part_kind = _part_kind(getattr(event, "part", None))
        return f"Part started: {part_kind}" if part_kind else "Part started"
    if category == "part_end":
        part_kind = _part_kind(getattr(event, "part", None))
        return f"Part ended: {part_kind}" if part_kind else "Part ended"
    if category == "final_result":
        return "Final result detected"
    if category == "function_tool_call":
        return _tool_display("Function tool call", event)
    if category == "function_tool_result":
        return _tool_display("Function tool result", event)
    if category == "builtin_tool_call":
        return _tool_display("Built-in tool call", event)
    if category == "builtin_tool_result":
        return _tool_display("Built-in tool result", event)
    return category.replace("_", " ").strip().capitalize() or type(event).__name__


def _tool_display(prefix: str, event: object) -> str:
    tool_name = first_string(
        getattr(event, "tool_name", None),
        getattr(getattr(event, "part", None), "tool_name", None),
        getattr(getattr(event, "result", None), "tool_name", None),
    )
    return f"{prefix}: {tool_name}" if tool_name else prefix


def _add_part_metadata(payload: dict[str, Any], part: object | None) -> None:
    if part is None:
        return
    if (part_kind := _part_kind(part)) is not None:
        payload["part_kind"] = part_kind
    _add_tool_metadata(payload, part)


def _add_delta_metadata(
    payload: dict[str, Any], delta: object | None, include_content: bool
) -> None:
    if delta is None:
        return
    if (delta_kind := safe_string(getattr(delta, "part_delta_kind", None))) is not None:
        payload["delta_kind"] = delta_kind
    if include_content and delta_kind == "text":
        text_delta = _text_delta(delta)
        if text_delta is not None:
            payload["text_delta"] = clip_stream_text(text_delta, _MAX_TEXT_DELTA_CHARS)


def _add_tool_metadata(payload: dict[str, Any], value: object | None) -> None:
    if value is None:
        return
    tool_name = safe_string(getattr(value, "tool_name", None))
    tool_call_id = first_string(
        getattr(value, "tool_call_id", None),
        getattr(value, "call_id", None),
    )
    if tool_name is not None:
        payload["tool_name"] = clip_stream_text(tool_name, _MAX_DISPLAY_CHARS)
    if tool_call_id is not None:
        payload["tool_call_id"] = clip_stream_text(tool_call_id, _MAX_DISPLAY_CHARS)


def _part_kind(part: object | None) -> str | None:
    if part is None:
        return None
    return safe_string(getattr(part, "part_kind", None)) or safe_string(
        getattr(part, "kind", None)
    )


def _text_delta(delta: object | None) -> str | None:
    if delta is None:
        return None
    return safe_string(getattr(delta, "content_delta", None)) or safe_string(
        getattr(delta, "delta", None)
    )


__all__ = [
    "PYDANTIC_AI_STREAM_COMPLETED",
    "PYDANTIC_AI_STREAM_EVENT",
    "PYDANTIC_AI_STREAM_EVENT_KINDS",
    "PYDANTIC_AI_STREAM_FAILED",
    "PYDANTIC_AI_STREAM_STARTED",
    "PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS",
    "PydanticAIStreamPublisher",
    "current_stream_surface",
    "model_stream_live_events_suppressed",
    "stream_surface",
    "suppress_model_stream_live_events",
]
