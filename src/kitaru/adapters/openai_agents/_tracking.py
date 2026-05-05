"""Lightweight event tracking for OpenAI Agents SDK runs."""

import re
import time
import uuid
from collections.abc import Iterator
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
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow

_NON_WORD_PATTERN = re.compile(r"\W+")
ArtifactKind = Literal["input", "result", "response", "usage", "error"]


def normalize_agent_name(agent_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (agent_name or "").strip()).strip("_")
    return normalized or "agent"


def artifact_name(event_id: str, kind: ArtifactKind) -> str:
    return f"{event_id}_{kind}"


@dataclass(frozen=True)
class EventContext:
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

    def __post_init__(self) -> None:
        self.agent_name = normalize_agent_name(self.agent_name)

    @property
    def event_log_artifact_name(self) -> str:
        return f"{self.agent_name}_{self.run_label}_event_log"

    @property
    def run_summary_artifact_name(self) -> str:
        return f"{self.agent_name}_{self.run_label}_run_summary"

    def _next_event_id(self, event_kind: str) -> tuple[str, int]:
        self._counter += 1
        event_id = f"{self.agent_name}_{self.run_label}_{event_kind}_{self._counter}"
        return event_id, self._counter

    def start_llm_event(self) -> tuple[str, EventContext]:
        event_id, sequence_index = self._next_event_id("llm_call")
        self._model_call_count += 1
        return event_id, EventContext(sequence_index=sequence_index)

    def next_tool_checkpoint_sequence(self) -> int:
        """Return a per-run deterministic fallback tool call sequence."""
        self._tool_checkpoint_sequence += 1
        return self._tool_checkpoint_sequence

    def start_tool_event(self) -> tuple[str, EventContext]:
        event_id, sequence_index = self._next_event_id("tool_call")
        self._tool_call_count += 1
        return event_id, EventContext(sequence_index=sequence_index)

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
    ) -> None:
        self._events.append(
            OpenAIAdapterEvent(
                event_id=event_id,
                kind=kind,
                status=status,
                sequence_index=context.sequence_index,
                run_label=self.run_label,
                agent_name=self.agent_name,
                duration_ms=duration_ms,
                metadata=metadata or {},
                artifacts=artifacts or {},
                error=error_from_exception(error) if error is not None else None,
            )
        )

    def set_run_error(self, error: BaseException) -> None:
        self._status = "failed"
        self._error = error

    def build_run_summary(self) -> dict[str, object]:
        return {
            "agent_name": self.agent_name,
            "run_label": self.run_label,
            "status": self._status,
            "model_call_count": self._model_call_count,
            "tool_call_count": self._tool_call_count,
            "total_events": self._counter,
            "event_ids_in_order": [event.event_id for event in self._events],
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "error": (
                error_from_exception(self._error).model_dump(mode="json")
                if self._error is not None
                else None
            ),
        }

    def persist(self) -> None:
        if not (is_inside_flow() or is_inside_checkpoint()):
            return
        events_dump = dump_openai_events(self._events)
        summary_dump = self.build_run_summary()
        kitaru.log(
            **{
                OPENAI_AGENTS_EVENTS_METADATA_KEY: {self.run_label: events_dump},
                OPENAI_AGENTS_RUN_SUMMARIES_METADATA_KEY: {
                    self.run_label: summary_dump
                },
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
