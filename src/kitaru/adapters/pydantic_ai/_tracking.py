"""Runtime-local child-event tracking for the PydanticAI adapter."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any


def normalize_agent_name(agent_name: str | None) -> str:
    """Normalize an agent name for artifact and metadata keys."""
    raw = (agent_name or "agent").strip()
    if not raw:
        raw = "agent"
    normalized = re.sub(r"\W+", "_", raw).strip("_")
    return normalized or "agent"


@dataclass(frozen=True)
class ModelEventContext:
    """Tracker-derived metadata for one model child event."""

    sequence_index: int
    turn_index: int
    parent_event_ids: list[str]
    fan_in_from: list[str]


@dataclass(frozen=True)
class ToolEventContext:
    """Tracker-derived metadata for one tool child event."""

    sequence_index: int
    turn_index: int
    parent_event_ids: list[str]
    fan_out_from: str | None


@dataclass
class ChildEventTracker:
    """Track model/tool child-event lineage for one agent run."""

    agent_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _counter: int = 0
    _turn_index: int = 0
    _current_turn_index: int = 0
    _current_model_event_id: str | None = None
    _pending_tool_event_ids: list[str] = field(default_factory=list)
    _event_ids_in_order: list[str] = field(default_factory=list)
    _model_call_count: int = 0
    _tool_call_count: int = 0
    _event_stream_handler_call_count: int = 0
    _event_stream_handler_duration_ms: float = 0.0
    _started_at: float = field(default_factory=time.perf_counter)

    def __post_init__(self) -> None:
        """Normalize adapter-visible naming fields."""
        self.agent_name = normalize_agent_name(self.agent_name)

    @property
    def current_model_event_id(self) -> str | None:
        """Return the most recently completed model event ID."""
        return self._current_model_event_id

    def _next_event_id(self, event_type: str) -> tuple[str, int]:
        """Allocate the next child-event ID and sequence index."""
        normalized_type = event_type.strip().lower()
        if normalized_type not in {"llm_call", "tool_call"}:
            raise ValueError(f"Unsupported child event type: {event_type!r}.")

        self._counter += 1
        event_id = (
            f"{self.agent_name}_{self.run_label}_{normalized_type}_{self._counter}"
        )
        self._event_ids_in_order.append(event_id)
        return event_id, self._counter

    def start_model_event(self) -> tuple[str, ModelEventContext]:
        """Start model event tracking and return ID plus lineage metadata."""
        event_id, sequence_index = self._next_event_id("llm_call")
        self._model_call_count += 1
        self._turn_index += 1
        self._current_turn_index = self._turn_index

        fan_in_from = list(self._pending_tool_event_ids)
        self._pending_tool_event_ids.clear()

        context = ModelEventContext(
            sequence_index=sequence_index,
            turn_index=self._current_turn_index,
            parent_event_ids=fan_in_from,
            fan_in_from=fan_in_from,
        )
        return event_id, context

    def complete_model_event(self, event_id: str) -> None:
        """Record a completed model event."""
        self._current_model_event_id = event_id

    def fail_model_event(self, event_id: str) -> None:
        """Record a failed model event and keep prior lineage anchor."""
        if self._current_model_event_id == event_id:
            self._current_model_event_id = None

    def start_tool_event(self) -> tuple[str, ToolEventContext]:
        """Start tool event tracking and return ID plus lineage metadata."""
        event_id, sequence_index = self._next_event_id("tool_call")
        self._tool_call_count += 1

        fan_out_from = self._current_model_event_id
        parent_event_ids = [fan_out_from] if fan_out_from is not None else []

        context = ToolEventContext(
            sequence_index=sequence_index,
            turn_index=self._current_turn_index,
            parent_event_ids=parent_event_ids,
            fan_out_from=fan_out_from,
        )
        return event_id, context

    def complete_tool_event(self, event_id: str) -> None:
        """Record a completed tool event for fan-in on the next model call."""
        self._pending_tool_event_ids.append(event_id)

    def fail_tool_event(self, event_id: str) -> None:
        """Record a failed tool event without adding it to pending fan-in."""
        if event_id in self._pending_tool_event_ids:
            self._pending_tool_event_ids.remove(event_id)

    def record_event_stream_handler(self, duration_ms: float) -> int:
        """Track one event-stream handler invocation."""
        self._event_stream_handler_call_count += 1
        self._event_stream_handler_duration_ms = round(
            self._event_stream_handler_duration_ms + duration_ms,
            3,
        )
        return self._event_stream_handler_call_count

    def build_run_summary(
        self,
        *,
        status: str,
        error: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build run-level summary metadata for one wrapped agent invocation."""
        summary: dict[str, Any] = {
            "agent_name": self.agent_name,
            "run_label": self.run_label,
            "status": status,
            "model_call_count": self._model_call_count,
            "tool_call_count": self._tool_call_count,
            "total_events": self._counter,
            "turn_count": self._turn_index,
            "event_ids_in_order": list(self._event_ids_in_order),
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
        }
        if self._event_stream_handler_call_count > 0:
            summary["event_stream_handler"] = {
                "call_count": self._event_stream_handler_call_count,
                "duration_ms": self._event_stream_handler_duration_ms,
            }
        if error is not None:
            summary["error"] = error
        return summary


_CURRENT_TRACKER: ContextVar[ChildEventTracker | None] = ContextVar(
    "kitaru_pydantic_ai_child_event_tracker",
    default=None,
)


@contextmanager
def tracker_scope(agent_name: str | None) -> Iterator[ChildEventTracker]:
    """Install a child-event tracker for one wrapped agent run."""
    tracker = ChildEventTracker(agent_name=agent_name or "agent")
    tracker_token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    finally:
        _CURRENT_TRACKER.reset(tracker_token)


def get_current_tracker() -> ChildEventTracker | None:
    """Return the active run-local child-event tracker, if any."""
    return _CURRENT_TRACKER.get()
