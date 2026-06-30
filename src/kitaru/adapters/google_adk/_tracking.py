"""Lightweight event tracking for Google ADK runs."""

from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from ._constants import (
    GOOGLE_ADK_EVENTS_METADATA_KEY,
    GOOGLE_ADK_RUN_SUMMARIES_METADATA_KEY,
)
from ._events import (
    ADKAdapterEvent,
    ADKEventKind,
    ADKEventStatus,
    dump_adk_events,
    error_from_exception,
)
from ._kitaru_internal import (
    get_current_checkpoint_id,
    get_current_checkpoint_name,
    is_inside_checkpoint,
)
from ._policy import ADKCallCheckpointPolicy, ADKCapturePolicy

logger = logging.getLogger(__name__)

_NON_WORD_PATTERN = re.compile(r"\W+")

_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_google_adk_tracker", default=None
)
_CURRENT_CAPTURE_POLICY: ContextVar[ADKCapturePolicy | None] = ContextVar(
    "kitaru_google_adk_capture_policy", default=None
)
_CURRENT_CALL_POLICY: ContextVar[ADKCallCheckpointPolicy | None] = ContextVar(
    "kitaru_google_adk_call_policy", default=None
)


def normalize_runner_name(runner_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (runner_name or "").strip()).strip("_")
    return normalized or "adk_runner"


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
        return get_current_checkpoint_id(), get_current_checkpoint_name()
    except Exception:
        logger.debug("Could not resolve current checkpoint identity", exc_info=True)
        return None, None


@dataclass
class EventTracker:
    """Tracks ADK events for one runner turn."""

    runner_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _counter: int = 0
    _events: list[ADKAdapterEvent] = field(default_factory=list)
    _started_at: float = field(default_factory=time.perf_counter)
    _status: ADKEventStatus = "completed"
    _error: BaseException | None = None
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self.runner_name = normalize_runner_name(self.runner_name)

    @property
    def events(self) -> Sequence[ADKAdapterEvent]:
        with self._lock:
            return tuple(sorted(self._events, key=lambda event: event.sequence_index))

    @property
    def event_log_artifact_name(self) -> str:
        return f"event_log__{self.runner_name}_{self.run_label}"

    @property
    def run_summary_artifact_name(self) -> str:
        return f"run_summary__{self.runner_name}_{self.run_label}"

    def _next_event_id(self, event_kind: str) -> tuple[str, int]:
        self._counter += 1
        event_id = f"{self.runner_name}_{self.run_label}_{event_kind}_{self._counter}"
        return event_id, self._counter

    def start_event(self, kind: ADKEventKind) -> tuple[str, EventContext]:
        with self._lock:
            event_id, sequence_index = self._next_event_id(kind)
            checkpoint_id, checkpoint_name = _current_checkpoint_identity()
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
        kind: ADKEventKind,
        status: ADKEventStatus,
        duration_ms: float | None = None,
        model_name: str | None = None,
        tool_name: str | None = None,
        tool_call_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        artifacts: dict[str, str] | None = None,
        error: BaseException | None = None,
        checkpoint_id: str | None = None,
        checkpoint_name: str | None = None,
    ) -> None:
        with self._lock:
            if status == "failed":
                self._status = "failed"
                self._error = error
            self._events.append(
                ADKAdapterEvent(
                    event_id=event_id,
                    kind=kind,
                    status=status,
                    sequence_index=context.sequence_index,
                    run_label=self.run_label,
                    runner_name=self.runner_name,
                    checkpoint_id=checkpoint_id
                    if checkpoint_id is not None
                    else context.checkpoint_id,
                    checkpoint_name=(
                        checkpoint_name
                        if checkpoint_name is not None
                        else context.checkpoint_name
                    ),
                    model_name=model_name,
                    tool_name=tool_name,
                    tool_call_id=tool_call_id,
                    duration_ms=duration_ms,
                    metadata=metadata or {},
                    artifacts=artifacts or {},
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def record_metadata_event(
        self,
        *,
        kind: ADKEventKind,
        metadata: dict[str, Any] | None = None,
        model_name: str | None = None,
        tool_name: str | None = None,
        tool_call_id: str | None = None,
        status: ADKEventStatus = "metadata_only",
    ) -> None:
        event_id, context = self.start_event(kind)
        self.record_event(
            event_id,
            context,
            kind=kind,
            status=status,
            model_name=model_name,
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            metadata=metadata,
        )

    def mark_failed(self, error: BaseException) -> None:
        with self._lock:
            self._status = "failed"
            self._error = error

    def summary(self) -> dict[str, Any]:
        events = list(self.events)
        return {
            "runner_name": self.runner_name,
            "run_label": self.run_label,
            "status": self._status,
            "duration_ms": round((time.perf_counter() - self._started_at) * 1000, 3),
            "event_count": len(events),
            "model_call_count": sum(
                1 for event in events if event.kind == "model_call"
            ),
            "tool_call_count": sum(1 for event in events if event.kind == "tool_call"),
            "error": str(self._error) if self._error is not None else None,
        }

    def persisted_metadata(self) -> dict[str, Any]:
        """Return metadata payloads that callers can attach to a checkpoint."""
        return {
            GOOGLE_ADK_EVENTS_METADATA_KEY: dump_adk_events(list(self.events)),
            GOOGLE_ADK_RUN_SUMMARIES_METADATA_KEY: [self.summary()],
        }


@contextmanager
def tracker_scope(
    tracker: EventTracker,
    *,
    capture: ADKCapturePolicy,
    call_policy: ADKCallCheckpointPolicy,
) -> Iterator[EventTracker]:
    tracker_token = _CURRENT_TRACKER.set(tracker)
    capture_token = _CURRENT_CAPTURE_POLICY.set(capture)
    policy_token = _CURRENT_CALL_POLICY.set(call_policy)
    try:
        yield tracker
    finally:
        _CURRENT_CALL_POLICY.reset(policy_token)
        _CURRENT_CAPTURE_POLICY.reset(capture_token)
        _CURRENT_TRACKER.reset(tracker_token)


def current_tracker() -> EventTracker | None:
    return _CURRENT_TRACKER.get()


def current_capture_policy() -> ADKCapturePolicy | None:
    return _CURRENT_CAPTURE_POLICY.get()


def current_call_policy() -> ADKCallCheckpointPolicy | None:
    return _CURRENT_CALL_POLICY.get()
