"""Lightweight event tracking for Gemini interactions."""

import threading
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal

import kitaru
from kitaru.errors import KitaruRuntimeError

from ._constants import GEMINI_EVENTS_METADATA_KEY, GEMINI_RUN_SUMMARIES_METADATA_KEY
from ._events import (
    EventStatus,
    GeminiAdapterEvent,
    dump_gemini_events,
    error_from_exception,
)
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._utils import normalize_identifier

ArtifactKind = Literal[
    "input",
    "request_manifest",
    "raw_interaction",
    "steps",
    "output",
    "usage",
    "event_log",
    "run_summary",
]
ArtifactNameByKind = dict[ArtifactKind, str]
EventPersistenceOperation = Literal[
    "save_event_log",
    "save_run_summary",
    "log_metadata",
]


def normalize_runner_name(runner_name: str | None) -> str:
    return normalize_identifier(runner_name, fallback="gemini_runner")


def artifact_name(runner_name: str, run_label: str, kind: ArtifactKind) -> str:
    return f"{kind}__{normalize_runner_name(runner_name)}_{run_label}"


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

    def warning(self) -> str:
        target = (
            f" artifact {self.artifact_name}" if self.artifact_name is not None else ""
        )
        return (
            "Gemini event/log persistence failed for "
            f"{self.operation}{target}: "
            f"{self.exception_type}: {self.message}"
        )

    @staticmethod
    def as_error(failures: list["EventPersistenceFailure"]) -> KitaruRuntimeError:
        failure_summary = "; ".join(failure.warning() for failure in failures)
        return KitaruRuntimeError(
            f"{failure_summary}. Gemini already succeeded; retrying may duplicate "
            "the interaction."
        )


@dataclass
class EventTracker:
    """Tracks the single interaction event for one Gemini runner call."""

    runner_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _events: list[GeminiAdapterEvent] = field(default_factory=list)
    _status: EventStatus = "completed"
    _error: BaseException | None = None
    _started_at: float = field(default_factory=time.perf_counter)
    _interaction_id: str | None = None
    _warnings: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.runner_name = normalize_runner_name(self.runner_name)

    @property
    def events(self) -> Sequence[GeminiAdapterEvent]:
        with self._lock:
            return tuple(self._events)

    @property
    def event_log_artifact_name(self) -> str:
        return artifact_name(self.runner_name, self.run_label, "event_log")

    @property
    def run_summary_artifact_name(self) -> str:
        return artifact_name(self.runner_name, self.run_label, "run_summary")

    def artifact_name(self, kind: ArtifactKind) -> str:
        return artifact_name(self.runner_name, self.run_label, kind)

    def record_interaction(
        self,
        *,
        status: EventStatus,
        duration_ms: float,
        interaction_id: str | None = None,
        artifacts: Mapping[ArtifactKind, str] | None = None,
        metadata: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            if status == "failed" and error is not None:
                self._status = "failed"
                self._error = error
            self._interaction_id = interaction_id
            self._warnings = list(warnings or [])
            event_id = f"{self.runner_name}_{self.run_label}_interaction_1"
            self._events = [
                GeminiAdapterEvent(
                    event_id=event_id,
                    status=status,
                    sequence_index=1,
                    run_label=self.run_label,
                    runner_name=self.runner_name,
                    duration_ms=duration_ms,
                    interaction_id=interaction_id,
                    metadata=metadata or {},
                    artifacts={
                        str(key): value for key, value in (artifacts or {}).items()
                    },
                    error=error_from_exception(error) if error is not None else None,
                )
            ]

    def _build_run_summary_unlocked(
        self,
        ordered_events: list[GeminiAdapterEvent],
    ) -> dict[str, Any]:
        return {
            "runner_name": self.runner_name,
            "run_label": self.run_label,
            "status": self._status,
            "interaction_count": len(ordered_events),
            "event_ids_in_order": [event.event_id for event in ordered_events],
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "interaction_id": self._interaction_id,
            "warnings": self._warnings,
            "error": (
                error_from_exception(self._error).model_dump(mode="json")
                if self._error is not None
                else None
            ),
        }

    def build_run_summary(self) -> dict[str, Any]:
        with self._lock:
            return self._build_run_summary_unlocked(list(self._events))

    def persist(self, *, fail_on_error: bool = False) -> list[EventPersistenceFailure]:
        failures: list[EventPersistenceFailure] = []
        if not self._events:
            return failures
        if not (is_inside_flow() or is_inside_checkpoint()):
            return failures
        run_label = self.run_label
        with self._lock:
            ordered_events = list(self._events)
            events_dump = dump_gemini_events(ordered_events)
            summary_dump = self._build_run_summary_unlocked(ordered_events)
        if is_inside_checkpoint():
            event_log_name = self.event_log_artifact_name
            try:
                kitaru.save(event_log_name, events_dump, type="context")
            except Exception as error:
                failures.append(
                    EventPersistenceFailure.from_exception(
                        operation="save_event_log",
                        artifact_name=event_log_name,
                        error=error,
                    )
                )
            run_summary_name = self.run_summary_artifact_name
            try:
                kitaru.save(run_summary_name, summary_dump, type="context")
            except Exception as error:
                failures.append(
                    EventPersistenceFailure.from_exception(
                        operation="save_run_summary",
                        artifact_name=run_summary_name,
                        error=error,
                    )
                )
        try:
            kitaru.log(
                **{
                    GEMINI_EVENTS_METADATA_KEY: {run_label: events_dump},
                    GEMINI_RUN_SUMMARIES_METADATA_KEY: {run_label: summary_dump},
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
            raise EventPersistenceFailure.as_error(failures)
        return failures


_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_gemini_interactions_event_tracker",
    default=None,
)
_TRACKING_ACTIVE: ContextVar[bool] = ContextVar(
    "kitaru_gemini_interactions_tracking_active",
    default=False,
)


@contextmanager
def tracker_scope(
    runner_name: str | None,
    *,
    persist_on_exit: bool = True,
    fail_on_persist_error: bool = False,
) -> Iterator[EventTracker]:
    """Create a tracker for a Gemini runner call."""
    parent = _CURRENT_TRACKER.get()
    if parent is not None and _TRACKING_ACTIVE.get():
        yield parent
        return
    tracker = EventTracker(runner_name=runner_name or "gemini_runner")
    tracker_token = _CURRENT_TRACKER.set(tracker)
    active_token = _TRACKING_ACTIVE.set(True)
    try:
        yield tracker
    finally:
        _TRACKING_ACTIVE.reset(active_token)
        _CURRENT_TRACKER.reset(tracker_token)
        if persist_on_exit:
            tracker.persist(fail_on_error=fail_on_persist_error)
