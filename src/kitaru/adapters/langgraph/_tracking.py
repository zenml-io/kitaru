"""Lightweight graph-level event tracking for LangGraph adapter runs."""

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
from kitaru.errors import KitaruRuntimeError

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
ArtifactKind = Literal["event_log", "run_summary"]
EventPersistenceOperation = Literal[
    "save_event_log",
    "save_run_summary",
    "log_metadata",
]


def normalize_graph_name(graph_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (graph_name or "").strip()).strip("_")
    return normalized or "graph"


def artifact_name(graph_name: str, run_label: str, kind: ArtifactKind) -> str:
    return f"{kind}__{normalize_graph_name(graph_name)}_{run_label}"


@dataclass(frozen=True)
class EventPersistenceFailure:
    """Structured failure from best-effort event/log persistence."""

    operation: EventPersistenceOperation
    artifact_name: str | None
    exception_type: str
    message: str

    @classmethod
    def from_exception(
        cls,
        *,
        operation: EventPersistenceOperation,
        artifact_name: str | None,
        error: BaseException,
    ) -> "EventPersistenceFailure":
        return cls(
            operation=operation,
            artifact_name=artifact_name,
            exception_type=type(error).__name__,
            message=str(error),
        )

    def as_metadata(self) -> dict[str, str | None]:
        return {
            "operation": self.operation,
            "artifact_name": self.artifact_name,
            "exception_type": self.exception_type,
            "message": self.message,
        }


def _save_artifact(
    failures: list[EventPersistenceFailure],
    *,
    operation: EventPersistenceOperation,
    artifact_name: str,
    value: object,
) -> None:
    try:
        kitaru.save(artifact_name, value, type="context")
    except Exception as error:
        failures.append(
            EventPersistenceFailure.from_exception(
                operation=operation,
                artifact_name=artifact_name,
                error=error,
            )
        )


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
        return artifact_name(self.graph_name, self.run_label, "event_log")

    @property
    def run_summary_artifact_name(self) -> str:
        return artifact_name(self.graph_name, self.run_label, "run_summary")

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

    def persist(
        self,
        extra_summary: dict[str, object] | None = None,
        *,
        fail_on_error: bool = False,
    ) -> list[EventPersistenceFailure]:
        failures: list[EventPersistenceFailure] = []
        if not (is_inside_flow() or is_inside_checkpoint()):
            return failures
        run_label = self.run_label
        with self._lock:
            events_dump = dump_langgraph_events(self._events)
            summary_dump = self._build_run_summary_unlocked()
        if extra_summary:
            summary_dump.update(extra_summary)
        inside_checkpoint = is_inside_checkpoint()
        if inside_checkpoint:
            _save_artifact(
                failures,
                operation="save_event_log",
                artifact_name=self.event_log_artifact_name,
                value=events_dump,
            )
            _save_artifact(
                failures,
                operation="save_run_summary",
                artifact_name=self.run_summary_artifact_name,
                value=summary_dump,
            )
        events_metadata, summary_metadata = self._metadata_payloads(
            events_dump,
            summary_dump,
            failures=failures,
            lightweight=inside_checkpoint,
        )
        try:
            kitaru.log(
                **{
                    LANGGRAPH_EVENTS_METADATA_KEY: {run_label: events_metadata},
                    LANGGRAPH_RUN_SUMMARIES_METADATA_KEY: {run_label: summary_metadata},
                }
            )
        except Exception as error:
            failures.append(
                EventPersistenceFailure.from_exception(
                    operation="log_metadata",
                    artifact_name=None,
                    error=error,
                )
            )
        if failures and fail_on_error:
            failure_summary = "; ".join(
                f"{failure.operation}: {failure.exception_type}: {failure.message}"
                for failure in failures
            )
            raise KitaruRuntimeError(
                f"LangGraph event/log persistence failed: {failure_summary}"
            )
        return failures

    def _metadata_payloads(
        self,
        events_dump: list[dict[str, object]],
        summary_dump: dict[str, object],
        *,
        failures: Sequence[EventPersistenceFailure],
        lightweight: bool,
    ) -> tuple[object, dict[str, object]]:
        if not lightweight:
            if failures:
                summary_dump = {
                    **summary_dump,
                    "persistence_failures": [
                        failure.as_metadata() for failure in failures
                    ],
                }
            return events_dump, summary_dump
        failure_metadata = [failure.as_metadata() for failure in failures]
        event_ids = [str(event.get("event_id")) for event in events_dump]
        events_metadata: dict[str, object] = {
            "artifact_name": self.event_log_artifact_name,
            "event_count": len(events_dump),
            "event_ids_in_order": event_ids,
        }
        summary_metadata: dict[str, object] = {
            "artifact_name": self.run_summary_artifact_name,
            "graph_name": summary_dump.get("graph_name", self.graph_name),
            "run_label": self.run_label,
            "status": summary_dump.get("status"),
            "total_events": summary_dump.get("total_events", len(events_dump)),
            "thread_id": summary_dump.get("thread_id"),
            "latest_checkpoint_id": summary_dump.get("latest_checkpoint_id"),
        }
        if failure_metadata:
            events_metadata["persistence_failures"] = failure_metadata
            summary_metadata["persistence_failures"] = failure_metadata
        return events_metadata, summary_metadata


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
