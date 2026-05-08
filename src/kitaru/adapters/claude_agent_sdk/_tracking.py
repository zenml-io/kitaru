"""Lightweight event tracking for Claude Agent SDK invocations."""

import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal

import kitaru

from ._constants import (
    CLAUDE_AGENT_SDK_EVENTS_METADATA_KEY,
    CLAUDE_AGENT_SDK_RUN_SUMMARIES_METADATA_KEY,
)
from ._events import (
    ClaudeAdapterEvent,
    EventStatus,
    dump_claude_events,
    error_from_exception,
)
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._utils import normalize_identifier

ArtifactKind = Literal[
    "messages",
    "transcript",
    "options_manifest",
    "output",
    "usage",
    "event_log",
    "run_summary",
]


def normalize_runner_name(runner_name: str | None) -> str:
    return normalize_identifier(runner_name, fallback="claude_runner")


def artifact_name(runner_name: str, run_label: str, kind: ArtifactKind) -> str:
    return f"{normalize_runner_name(runner_name)}_{run_label}_{kind}"


@dataclass
class EventTracker:
    """Tracks the single invocation event for one Claude runner call."""

    runner_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _events: list[ClaudeAdapterEvent] = field(default_factory=list)
    _status: EventStatus = "completed"
    _error: BaseException | None = None
    _started_at: float = field(default_factory=time.perf_counter)
    _session_id: str | None = None
    _transcript_path: str | None = None
    _warnings: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.runner_name = normalize_runner_name(self.runner_name)

    @property
    def events(self) -> Sequence[ClaudeAdapterEvent]:
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

    def record_invocation(
        self,
        *,
        status: EventStatus,
        duration_ms: float,
        session_id: str | None = None,
        artifacts: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        transcript_path: str | None = None,
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            if status == "failed" and error is not None:
                self._status = "failed"
                self._error = error
            self._session_id = session_id
            self._transcript_path = transcript_path
            self._warnings = list(warnings or [])
            event_id = f"{self.runner_name}_{self.run_label}_invocation_1"
            self._events = [
                ClaudeAdapterEvent(
                    event_id=event_id,
                    status=status,
                    sequence_index=1,
                    run_label=self.run_label,
                    runner_name=self.runner_name,
                    duration_ms=duration_ms,
                    session_id=session_id,
                    metadata=metadata or {},
                    artifacts=artifacts or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            ]

    def set_run_error(self, error: BaseException) -> None:
        with self._lock:
            self._status = "failed"
            self._error = error

    def _build_run_summary_unlocked(
        self,
        ordered_events: list[ClaudeAdapterEvent],
    ) -> dict[str, Any]:
        return {
            "runner_name": self.runner_name,
            "run_label": self.run_label,
            "status": self._status,
            "invocation_count": len(ordered_events),
            "event_ids_in_order": [event.event_id for event in ordered_events],
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "session_id": self._session_id,
            "transcript_path": self._transcript_path,
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

    def persist(self) -> None:
        if not self._events:
            return
        if not (is_inside_flow() or is_inside_checkpoint()):
            return
        run_label = self.run_label
        with self._lock:
            ordered_events = list(self._events)
            events_dump = dump_claude_events(ordered_events)
            summary_dump = self._build_run_summary_unlocked(ordered_events)
        if is_inside_checkpoint():
            kitaru.save(self.event_log_artifact_name, events_dump, type="context")
            kitaru.save(self.run_summary_artifact_name, summary_dump, type="context")
        kitaru.log(
            **{
                CLAUDE_AGENT_SDK_EVENTS_METADATA_KEY: {run_label: events_dump},
                CLAUDE_AGENT_SDK_RUN_SUMMARIES_METADATA_KEY: {run_label: summary_dump},
            }
        )


_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_claude_agent_sdk_event_tracker",
    default=None,
)
_TRACKING_ACTIVE: ContextVar[bool] = ContextVar(
    "kitaru_claude_agent_sdk_tracking_active",
    default=False,
)


@contextmanager
def tracker_scope(runner_name: str | None) -> Iterator[EventTracker]:
    if _TRACKING_ACTIVE.get():
        existing = _CURRENT_TRACKER.get()
        if existing is None:
            raise RuntimeError(
                "Claude tracker_scope invariant violated: tracking is active "
                "without a current tracker."
            )
        yield existing
        return

    tracker = EventTracker(runner_name=runner_name or "claude_runner")
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
