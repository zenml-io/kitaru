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
from ._kitaru_internal import (
    get_current_checkpoint_id,
    get_current_checkpoint_name,
    is_inside_checkpoint,
    is_inside_flow,
)
from ._policy import LangGraphCallCheckpointPolicy, LangGraphCapturePolicy

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


@dataclass(frozen=True)
class EventContext:
    """Reserved event identity and ordering information."""

    sequence_index: int
    parent_event_ids: tuple[str, ...] = ()
    checkpoint_id: str | None = None
    checkpoint_name: str | None = None


@dataclass(frozen=True)
class _ReservedToolEvent:
    tool_call_id: str
    event_id: str
    sequence_index: int
    parent_event_ids: tuple[str, ...]


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
    _model_call_count: int = 0
    _tool_call_count: int = 0
    _checkpoint_sequence: int = 0
    _tool_checkpoint_sequence: int = 0
    _current_model_event_id: str | None = None
    _pending_tool_event_ids: list[str] = field(default_factory=list)
    _started_at: float = field(default_factory=time.perf_counter)
    _lock: threading.RLock = field(
        default_factory=threading.RLock, init=False, repr=False
    )
    _reserved_tool_events_by_call_id: dict[str, list[_ReservedToolEvent]] = field(
        default_factory=dict, init=False, repr=False
    )
    _event_sequence_by_id: dict[str, int] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.graph_name = normalize_graph_name(self.graph_name)

    @property
    def events(self) -> Sequence[LangGraphAdapterEvent]:
        with self._lock:
            return tuple(self._ordered_events())

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

    def _next_event_id(self, event_kind: str) -> tuple[str, int]:
        self._counter += 1
        event_id = f"{self.graph_name}_{self.run_label}_{event_kind}_{self._counter}"
        self._event_sequence_by_id[event_id] = self._counter
        return event_id, self._counter

    def _ordered_events(self) -> list[LangGraphAdapterEvent]:
        return sorted(self._events, key=lambda event: event.sequence_index)

    def _event_sequence_index(self, event_id: str) -> int:
        return self._event_sequence_by_id.get(event_id, self._counter + 1)

    def _clear_reserved_tool_events_unlocked(self) -> None:
        for reservations in self._reserved_tool_events_by_call_id.values():
            for reservation in reservations:
                self._event_sequence_by_id.pop(reservation.event_id, None)
        self._reserved_tool_events_by_call_id.clear()

    def _current_checkpoint_identity(self) -> tuple[str | None, str | None]:
        try:
            if not is_inside_checkpoint():
                return None, None
            return get_current_checkpoint_id(), get_current_checkpoint_name()
        except Exception:
            return None, None

    def _update_status_unlocked(
        self,
        *,
        status: EventStatus,
        error: BaseException | None,
    ) -> None:
        if status != "completed":
            self._status = status
        if status == "failed":
            self._has_failure_event = True
        if error is not None:
            self._error = error

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
            event_id, sequence_index = self._next_event_id(kind)
            self._update_status_unlocked(status=status, error=error)
            self._events.append(
                LangGraphAdapterEvent(
                    event_id=event_id,
                    kind=kind,
                    status=status,
                    sequence_index=sequence_index,
                    run_label=self.run_label,
                    graph_name=self.graph_name,
                    duration_ms=duration_ms,
                    metadata=metadata or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def start_model_event(self) -> tuple[str, EventContext]:
        """Reserve a model-call event slot and return its identity."""
        with self._lock:
            parent_event_ids = tuple(
                sorted(self._pending_tool_event_ids, key=self._event_sequence_index)
            )
            self._pending_tool_event_ids.clear()
            self._clear_reserved_tool_events_unlocked()
            event_id, sequence_index = self._next_event_id("model_call")
            checkpoint_id, checkpoint_name = self._current_checkpoint_identity()
            self._model_call_count += 1
            return event_id, EventContext(
                sequence_index=sequence_index,
                parent_event_ids=parent_event_ids,
                checkpoint_id=checkpoint_id,
                checkpoint_name=checkpoint_name,
            )

    def reserve_tool_call_order(
        self,
        *,
        parent_model_event_id: str,
        tool_call_ids: Sequence[str],
    ) -> None:
        """Reserve future tool-call event slots in model-declared order."""
        with self._lock:
            parent_event_ids = (parent_model_event_id,)
            for tool_call_id in tool_call_ids:
                if not tool_call_id:
                    continue
                event_id, sequence_index = self._next_event_id("tool_call")
                reserved = _ReservedToolEvent(
                    tool_call_id=tool_call_id,
                    event_id=event_id,
                    sequence_index=sequence_index,
                    parent_event_ids=parent_event_ids,
                )
                self._reserved_tool_events_by_call_id.setdefault(
                    tool_call_id, []
                ).append(reserved)

    def record_model_event(
        self,
        event_id: str,
        context: EventContext,
        *,
        status: EventStatus,
        duration_ms: float,
        artifacts: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        error: BaseException | None = None,
        checkpoint_id: str | None = None,
        checkpoint_name: str | None = None,
        model_name: str | None = None,
        node_name: str | None = None,
        source: str | None = None,
        checkpoint_mode: Literal["true", "metadata_only"] | None = None,
    ) -> None:
        with self._lock:
            if status == "completed":
                self._current_model_event_id = event_id
            elif self._current_model_event_id == event_id:
                self._current_model_event_id = None
            self._update_status_unlocked(status=status, error=error)
            self._events.append(
                LangGraphAdapterEvent(
                    event_id=event_id,
                    kind="model_call",
                    status=status,
                    sequence_index=context.sequence_index,
                    run_label=self.run_label,
                    graph_name=self.graph_name,
                    duration_ms=duration_ms,
                    checkpoint_id=checkpoint_id or context.checkpoint_id,
                    checkpoint_name=checkpoint_name or context.checkpoint_name,
                    parent_event_ids=list(context.parent_event_ids),
                    model_name=model_name,
                    node_name=node_name,
                    source=source,
                    checkpoint_mode=checkpoint_mode,
                    metadata=metadata or {},
                    artifacts=artifacts or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def start_tool_event(
        self,
        *,
        tool_call_id: str | None = None,
    ) -> tuple[str, EventContext]:
        """Reserve or claim a tool-call event slot."""
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
                parent_event_ids = (
                    (self._current_model_event_id,)
                    if self._current_model_event_id is not None
                    else ()
                )
            else:
                event_id = reservation.event_id
                sequence_index = reservation.sequence_index
                parent_event_ids = reservation.parent_event_ids

            checkpoint_id, checkpoint_name = self._current_checkpoint_identity()
            self._tool_call_count += 1
            return event_id, EventContext(
                sequence_index=sequence_index,
                parent_event_ids=parent_event_ids,
                checkpoint_id=checkpoint_id,
                checkpoint_name=checkpoint_name,
            )

    def record_tool_event(
        self,
        event_id: str,
        context: EventContext,
        *,
        status: EventStatus,
        duration_ms: float,
        artifacts: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        error: BaseException | None = None,
        checkpoint_id: str | None = None,
        checkpoint_name: str | None = None,
        tool_name: str | None = None,
        tool_call_id: str | None = None,
        node_name: str | None = None,
        source: str | None = None,
        checkpoint_mode: Literal["true", "metadata_only"] | None = None,
    ) -> None:
        with self._lock:
            if status == "completed":
                self._pending_tool_event_ids.append(event_id)
            elif event_id in self._pending_tool_event_ids:
                self._pending_tool_event_ids.remove(event_id)
            self._update_status_unlocked(status=status, error=error)
            self._events.append(
                LangGraphAdapterEvent(
                    event_id=event_id,
                    kind="tool_call",
                    status=status,
                    sequence_index=context.sequence_index,
                    run_label=self.run_label,
                    graph_name=self.graph_name,
                    duration_ms=duration_ms,
                    checkpoint_id=checkpoint_id or context.checkpoint_id,
                    checkpoint_name=checkpoint_name or context.checkpoint_name,
                    parent_event_ids=list(context.parent_event_ids),
                    tool_name=tool_name,
                    tool_call_id=tool_call_id,
                    node_name=node_name,
                    source=source,
                    checkpoint_mode=checkpoint_mode,
                    metadata=metadata or {},
                    artifacts=artifacts or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def next_checkpoint_sequence(self) -> int:
        """Return a deterministic per-run fallback checkpoint sequence."""
        with self._lock:
            self._checkpoint_sequence += 1
            return self._checkpoint_sequence

    def next_tool_checkpoint_sequence(self) -> int:
        """Return a deterministic per-run fallback tool checkpoint sequence."""
        with self._lock:
            self._tool_checkpoint_sequence += 1
            return self._tool_checkpoint_sequence

    def build_run_summary(self) -> dict[str, object]:
        with self._lock:
            return self._build_run_summary_unlocked(self._ordered_events())

    def _build_run_summary_unlocked(
        self,
        ordered_events: Sequence[LangGraphAdapterEvent],
    ) -> dict[str, object]:
        return {
            "adapter": "langgraph",
            "graph_name": self.graph_name,
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
            ordered_events = self._ordered_events()
            events_dump = dump_langgraph_events(ordered_events)
            summary_dump = self._build_run_summary_unlocked(ordered_events)
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
_ACTIVE_CALL_CHECKPOINT_POLICY: ContextVar[LangGraphCallCheckpointPolicy | None] = (
    ContextVar("kitaru_langgraph_call_checkpoint_policy", default=None)
)
_ACTIVE_CAPTURE_POLICY: ContextVar[LangGraphCapturePolicy | None] = ContextVar(
    "kitaru_langgraph_capture_policy",
    default=None,
)


@contextmanager
def tracker_scope(
    graph_name: str | None,
    *,
    call_checkpoint_policy: LangGraphCallCheckpointPolicy | None = None,
    capture: LangGraphCapturePolicy | None = None,
) -> Iterator[EventTracker]:
    tracker = EventTracker(graph_name=graph_name or "graph")
    tracker_token = _CURRENT_TRACKER.set(tracker)
    policy_token = _ACTIVE_CALL_CHECKPOINT_POLICY.set(call_checkpoint_policy)
    capture_token = _ACTIVE_CAPTURE_POLICY.set(capture)
    try:
        yield tracker
    except Exception as error:
        if not tracker.has_failure_event:
            tracker.record("graph_call_failed", status="failed", error=error)
        raise
    finally:
        _ACTIVE_CAPTURE_POLICY.reset(capture_token)
        _ACTIVE_CALL_CHECKPOINT_POLICY.reset(policy_token)
        _CURRENT_TRACKER.reset(tracker_token)


def get_current_tracker() -> EventTracker | None:
    return _CURRENT_TRACKER.get()


def get_active_call_checkpoint_policy() -> LangGraphCallCheckpointPolicy | None:
    """Return the runner-installed calls-mode checkpoint policy, if any."""
    return _ACTIVE_CALL_CHECKPOINT_POLICY.get()


def get_active_capture_policy() -> LangGraphCapturePolicy | None:
    """Return the runner-installed LangGraph capture policy, if any."""
    return _ACTIVE_CAPTURE_POLICY.get()


def resolve_active_call_checkpoint_policy(
    fallback: LangGraphCallCheckpointPolicy | None = None,
) -> LangGraphCallCheckpointPolicy:
    """Resolve active runner policy, then fallback policy, then defaults."""
    return (
        get_active_call_checkpoint_policy()
        or fallback
        or LangGraphCallCheckpointPolicy()
    )


def resolve_active_capture_policy(
    fallback: LangGraphCapturePolicy | None = None,
) -> LangGraphCapturePolicy:
    """Resolve active runner capture settings, then fallback settings, then defaults."""
    return get_active_capture_policy() or fallback or LangGraphCapturePolicy()
