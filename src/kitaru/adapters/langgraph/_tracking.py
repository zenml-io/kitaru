"""Lightweight graph-level event tracking for LangGraph adapter runs."""

import re
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

import kitaru

from ._constants import (
    LANGGRAPH_EVENTS_METADATA_KEY,
    LANGGRAPH_RUN_SUMMARIES_METADATA_KEY,
)
from ._events import (
    EventStatus,
    LangGraphAdapterEvent,
    LangGraphEventKind,
    dump_langgraph_events,
    error_from_exception,
)
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow

_NON_WORD_PATTERN = re.compile(r"\W+")


def normalize_graph_name(graph_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (graph_name or "").strip()).strip("_")
    return normalized or "graph"


@dataclass
class EventTracker:
    """Tracks graph-level events for one LangGraph invocation."""

    graph_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _counter: int = 0
    _events: list[LangGraphAdapterEvent] = field(default_factory=list)
    _status: EventStatus = "completed"
    _error: BaseException | None = None
    _has_failure_event: bool = False
    _started_at: float = field(default_factory=time.perf_counter)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.graph_name = normalize_graph_name(self.graph_name)

    @property
    def events(self) -> Sequence[LangGraphAdapterEvent]:
        with self._lock:
            return tuple(self._events)

    @property
    def event_log_artifact_name(self) -> str:
        return f"{self.graph_name}_{self.run_label}_event_log"

    @property
    def run_summary_artifact_name(self) -> str:
        return f"{self.graph_name}_{self.run_label}_run_summary"

    @property
    def has_failure_event(self) -> bool:
        with self._lock:
            return self._has_failure_event

    def record(
        self,
        kind: LangGraphEventKind,
        *,
        status: EventStatus = "completed",
        metadata: dict[str, object] | None = None,
        error: BaseException | None = None,
        duration_ms: float | None = None,
    ) -> None:
        with self._lock:
            self._counter += 1
            if status != "completed":
                self._status = status
            if status == "failed":
                self._has_failure_event = True
            if error is not None:
                self._error = error
            self._events.append(
                LangGraphAdapterEvent(
                    event_id=(
                        f"{self.graph_name}_{self.run_label}_{kind}_{self._counter}"
                    ),
                    kind=kind,
                    status=status,
                    sequence_index=self._counter,
                    run_label=self.run_label,
                    graph_name=self.graph_name,
                    duration_ms=duration_ms,
                    metadata=metadata or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def build_run_summary(self) -> dict[str, object]:
        with self._lock:
            return self._build_run_summary_unlocked()

    def _build_run_summary_unlocked(self) -> dict[str, object]:
        return {
            "adapter": "langgraph",
            "graph_name": self.graph_name,
            "run_label": self.run_label,
            "status": self._status,
            "total_events": len(self._events),
            "event_ids_in_order": [event.event_id for event in self._events],
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "error": (
                error_from_exception(self._error).model_dump(mode="json")
                if self._error is not None
                else None
            ),
        }

    def persist(self, extra_summary: dict[str, object] | None = None) -> None:
        if not (is_inside_flow() or is_inside_checkpoint()):
            return
        run_label = self.run_label
        with self._lock:
            events_dump = dump_langgraph_events(self._events)
            summary_dump = self._build_run_summary_unlocked()
        if extra_summary:
            summary_dump.update(extra_summary)
        kitaru.log(
            **{
                LANGGRAPH_EVENTS_METADATA_KEY: {run_label: events_dump},
                LANGGRAPH_RUN_SUMMARIES_METADATA_KEY: {run_label: summary_dump},
            }
        )


_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_langgraph_event_tracker",
    default=None,
)


@contextmanager
def tracker_scope(graph_name: str | None) -> Iterator[EventTracker]:
    tracker = EventTracker(graph_name=graph_name or "graph")
    tracker_token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    except Exception as error:
        if not tracker.has_failure_event:
            tracker.record("graph_call_failed", status="failed", error=error)
        raise
    finally:
        _CURRENT_TRACKER.reset(tracker_token)


def get_current_tracker() -> EventTracker | None:
    return _CURRENT_TRACKER.get()
