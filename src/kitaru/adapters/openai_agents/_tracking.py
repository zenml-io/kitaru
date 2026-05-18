"""Lightweight event tracking for OpenAI Agents SDK runs."""

import re
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Literal

import kitaru

from ._constants import (
    OPENAI_AGENTS_EVENTS_METADATA_KEY,
    OPENAI_AGENTS_RUN_SUMMARIES_METADATA_KEY,
)
from ._events import (
    EventStatus,
    OpenAIAdapterEvent,
    dump_openai_events,
    error_from_exception,
)
from ._kitaru_internal import (
    get_current_checkpoint_id,
    get_current_checkpoint_name,
    is_inside_checkpoint,
    is_inside_flow,
)

_NON_WORD_PATTERN = re.compile(r"\W+")
_EVENT_ID_DISPLAY_PATTERN = re.compile(r"^(.+)_(llm_call|tool_call)_(\d+)$")
ArtifactKind = Literal["input", "result", "response", "usage", "error"]


def normalize_agent_name(agent_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (agent_name or "").strip()).strip("_")
    return normalized or "agent"


def artifact_name(event_id: str, kind: ArtifactKind) -> str:
    match = _EVENT_ID_DISPLAY_PATTERN.match(event_id)
    if match is None:
        return f"{event_id}_{kind}"
    namespace, event_kind, sequence_index = match.groups()
    return f"{event_kind}_{sequence_index}_{kind}__{namespace}"


@dataclass(frozen=True)
class EventContext:
    sequence_index: int
    checkpoint_id: str | None = None
    checkpoint_name: str | None = None


def _current_checkpoint_identity() -> tuple[str | None, str | None]:
    """Return active checkpoint identity without letting observability fail calls."""
    try:
        if not is_inside_checkpoint():
            return None, None
        checkpoint_id = get_current_checkpoint_id()
        checkpoint_name = get_current_checkpoint_name()
        return (
            str(checkpoint_id) if checkpoint_id is not None else None,
            str(checkpoint_name) if checkpoint_name is not None else None,
        )
    except Exception:
        return None, None


@dataclass(frozen=True)
class _ReservedToolEvent:
    tool_call_id: str
    event_id: str
    sequence_index: int


@dataclass
class EventTracker:
    """Tracks model/tool events for one OpenAI runner call."""

    agent_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _counter: int = 0
    _events: list[OpenAIAdapterEvent] = field(default_factory=list)
    _status: EventStatus = "completed"
    _error: BaseException | None = None
    _model_call_count: int = 0
    _tool_call_count: int = 0
    _tool_checkpoint_sequence: int = 0
    _started_at: float = field(default_factory=time.perf_counter)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _reserved_tool_events_by_call_id: dict[str, list[_ReservedToolEvent]] = field(
        default_factory=dict, init=False, repr=False
    )
    _active_tool_event_ids: set[str] = field(
        default_factory=set, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.agent_name = normalize_agent_name(self.agent_name)

    @property
    def events(self) -> Sequence[OpenAIAdapterEvent]:
        with self._lock:
            return tuple(self._ordered_events())

    @property
    def event_log_artifact_name(self) -> str:
        return f"event_log__{self.agent_name}_{self.run_label}"

    @property
    def run_summary_artifact_name(self) -> str:
        return f"run_summary__{self.agent_name}_{self.run_label}"

    def _next_event_id(self, event_kind: str) -> tuple[str, int]:
        self._counter += 1
        event_id = f"{self.agent_name}_{self.run_label}_{event_kind}_{self._counter}"
        return event_id, self._counter

    def _ordered_events(self) -> list[OpenAIAdapterEvent]:
        return sorted(self._events, key=lambda event: event.sequence_index)

    def _clear_reserved_tool_events_unlocked(self) -> None:
        self._reserved_tool_events_by_call_id.clear()

    def start_llm_event(self) -> tuple[str, EventContext]:
        with self._lock:
            if not self._active_tool_event_ids:
                self._clear_reserved_tool_events_unlocked()
            event_id, sequence_index = self._next_event_id("llm_call")
            checkpoint_id, checkpoint_name = _current_checkpoint_identity()
            self._model_call_count += 1
            return event_id, EventContext(
                sequence_index=sequence_index,
                checkpoint_id=checkpoint_id,
                checkpoint_name=checkpoint_name,
            )

    def reserve_tool_call_order(
        self,
        tool_call_ids: Sequence[str],
    ) -> None:
        """Reserve event slots for assistant-emitted local tool calls.

        The model response is the only point where Kitaru sees the assistant's
        intended tool-call order before the SDK may start local tools in
        parallel. Reservations are numbered parking spaces: they become real
        events only if a matching tool callback actually starts.
        """
        with self._lock:
            for tool_call_id in tool_call_ids:
                if not tool_call_id:
                    continue
                event_id, sequence_index = self._next_event_id("tool_call")
                reserved = _ReservedToolEvent(
                    tool_call_id=tool_call_id,
                    event_id=event_id,
                    sequence_index=sequence_index,
                )
                self._reserved_tool_events_by_call_id.setdefault(
                    tool_call_id, []
                ).append(reserved)

    def next_tool_checkpoint_sequence(self) -> int:
        """Return a per-run deterministic fallback tool call sequence."""
        with self._lock:
            self._tool_checkpoint_sequence += 1
            return self._tool_checkpoint_sequence

    def start_tool_event(
        self,
        *,
        tool_call_id: str | None = None,
    ) -> tuple[str, EventContext]:
        with self._lock:
            reservation: _ReservedToolEvent | None = None
            reservations = (
                self._reserved_tool_events_by_call_id.get(tool_call_id)
                if tool_call_id
                else None
            )
            if tool_call_id and reservations:
                reservation = reservations.pop(0)
                if not reservations:
                    del self._reserved_tool_events_by_call_id[tool_call_id]

            if reservation is None:
                event_id, sequence_index = self._next_event_id("tool_call")
            else:
                event_id = reservation.event_id
                sequence_index = reservation.sequence_index

            checkpoint_id, checkpoint_name = _current_checkpoint_identity()
            self._tool_call_count += 1
            self._active_tool_event_ids.add(event_id)
            return event_id, EventContext(
                sequence_index=sequence_index,
                checkpoint_id=checkpoint_id,
                checkpoint_name=checkpoint_name,
            )

    def record_event(
        self,
        event_id: str,
        context: EventContext,
        *,
        kind: Literal["llm_call", "tool_call"],
        status: EventStatus,
        duration_ms: float,
        artifacts: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        error: BaseException | None = None,
        checkpoint_id: str | None = None,
        checkpoint_name: str | None = None,
    ) -> None:
        with self._lock:
            if kind == "tool_call":
                self._active_tool_event_ids.discard(event_id)
            self._events.append(
                OpenAIAdapterEvent(
                    event_id=event_id,
                    kind=kind,
                    status=status,
                    sequence_index=context.sequence_index,
                    run_label=self.run_label,
                    agent_name=self.agent_name,
                    checkpoint_id=checkpoint_id
                    or getattr(context, "checkpoint_id", None),
                    checkpoint_name=checkpoint_name
                    or getattr(context, "checkpoint_name", None),
                    duration_ms=duration_ms,
                    metadata=metadata or {},
                    artifacts=artifacts or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def set_run_error(self, error: BaseException) -> None:
        with self._lock:
            self._status = "failed"
            self._error = error

    def _build_run_summary_unlocked(
        self,
        ordered_events: list[OpenAIAdapterEvent],
    ) -> dict[str, object]:
        return {
            "agent_name": self.agent_name,
            "run_label": self.run_label,
            "status": self._status,
            "model_call_count": self._model_call_count,
            "tool_call_count": self._tool_call_count,
            "total_events": len(ordered_events),
            "event_ids_in_order": [event.event_id for event in ordered_events],
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "error": (
                error_from_exception(self._error).model_dump(mode="json")
                if self._error is not None
                else None
            ),
        }

    def build_run_summary(self) -> dict[str, object]:
        with self._lock:
            return self._build_run_summary_unlocked(self._ordered_events())

    def persist(self) -> None:
        if not (is_inside_flow() or is_inside_checkpoint()):
            return
        run_label = self.run_label
        with self._lock:
            ordered_events = self._ordered_events()
            events_dump = dump_openai_events(ordered_events)
            summary_dump = self._build_run_summary_unlocked(ordered_events)
        kitaru.log(
            **{
                OPENAI_AGENTS_EVENTS_METADATA_KEY: {run_label: events_dump},
                OPENAI_AGENTS_RUN_SUMMARIES_METADATA_KEY: {run_label: summary_dump},
            }
        )


_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_openai_agents_event_tracker",
    default=None,
)
_TRACKING_ACTIVE: ContextVar[bool] = ContextVar(
    "kitaru_openai_agents_tracking_active",
    default=False,
)


@contextmanager
def tracker_scope(agent_name: str | None) -> Iterator[EventTracker]:
    if _TRACKING_ACTIVE.get():
        existing = _CURRENT_TRACKER.get()
        if existing is None:
            yield EventTracker(agent_name=agent_name or "agent")
        else:
            yield existing
        return

    tracker = EventTracker(agent_name=agent_name or "agent")
    active_token = _TRACKING_ACTIVE.set(True)
    tracker_token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    except Exception as error:
        tracker.set_run_error(error)
        raise
    finally:
        try:
            tracker.persist()
        finally:
            _CURRENT_TRACKER.reset(tracker_token)
            _TRACKING_ACTIVE.reset(active_token)


def get_current_tracker() -> EventTracker | None:
    return _CURRENT_TRACKER.get()
