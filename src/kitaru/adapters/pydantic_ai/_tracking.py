import re
import threading
import time
import uuid
import weakref
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast

import kitaru
from kitaru._llm_usage import (
    LLMBillingEffect,
    LLMCacheStatus,
    build_usage_record,
    usage_records_metadata,
)

from pydantic_ai.usage import RequestUsage

from ._logging import logger
from ._events import (
    AgentEvent,
    DeferredEvent,
    DeferredKind,
    EventError,
    EventStatus,
    EventStreamHandlerSummary,
    ModelEvent,
    RunSummary,
    StreamEvent,
    ToolEvent,
    ToolsetKind,
    dump_agent_events,
    error_from_exception,
)
from ._kitaru_internal import (
    get_current_checkpoint,
    get_current_checkpoint_name,
    get_current_execution_id,
    is_inside_checkpoint,
)
from ._policy import CaptureMode

_NON_WORD_PATTERN = re.compile(r"\W+")
_EVENT_ID_DISPLAY_PATTERN = re.compile(
    r"_(llm_call|tool_call|event_stream|deferred)_(\d+)$"
)

ArtifactKind = Literal[
    "args", "result", "prompt", "response", "stream_transcript", "context"
]


CheckpointKey = tuple[str | None, str | None, str | None, int]


class _CheckpointLike(Protocol):
    execution_id: str | None
    checkpoint_id: str | None
    name: str


@dataclass
class _ArtifactNamespaceState:
    checkpoint_key: CheckpointKey
    tracker_count: int = 0


_ARTIFACT_NAMESPACE_LOCK = threading.Lock()
_ARTIFACT_NAMESPACE_COUNTS: dict[CheckpointKey, int] = {}
_ARTIFACT_NAMESPACE_FINALIZERS: dict[CheckpointKey, weakref.finalize] = {}
_CONTEXT_ARTIFACT_NAMESPACE_STATE: ContextVar[_ArtifactNamespaceState | None] = (
    ContextVar(
        "kitaru_pydantic_ai_context_artifact_namespace_state",
        default=None,
    )
)


def _request_usage_payload(usage: RequestUsage) -> dict[str, Any]:
    """Return a JSON-like mapping for Pydantic AI usage objects."""
    model_dump = getattr(usage, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="json")
        if isinstance(dumped, dict):
            return cast(dict[str, Any], dumped)
    if hasattr(usage, "__dict__"):
        return dict(vars(usage))
    return {"value": repr(usage)}


def normalize_agent_name(agent_name: str | None) -> str:
    normalized = _NON_WORD_PATTERN.sub("_", (agent_name or "").strip()).strip("_")
    return normalized or "agent"


def _with_namespace(name: str, namespace: str | None) -> str:
    if namespace is None:
        return name
    return f"{name}__{namespace}"


def _event_artifact_base_name(event_id: str, kind: ArtifactKind) -> str:
    match = _EVENT_ID_DISPLAY_PATTERN.search(event_id)
    if match is None:
        return f"{event_id}_{kind}"
    event_kind, sequence_index = match.groups()
    return f"{event_kind}_{sequence_index}_{kind}"


def artifact_name(event_id: str, kind: ArtifactKind) -> str:
    return _event_artifact_base_name(event_id, kind)


def _namespaced_artifact_name(
    event_id: str, kind: ArtifactKind, *, namespace: str | None
) -> str:
    return _with_namespace(artifact_name(event_id, kind), namespace)


def _checkpoint_key(checkpoint: _CheckpointLike) -> CheckpointKey:
    return (
        checkpoint.execution_id,
        checkpoint.checkpoint_id,
        checkpoint.name,
        id(checkpoint),
    )


def _reset_artifact_namespace_state(checkpoint: _CheckpointLike | None = None) -> None:
    checkpoint_key = _checkpoint_key(checkpoint) if checkpoint is not None else None
    with _ARTIFACT_NAMESPACE_LOCK:
        keys = (
            [checkpoint_key]
            if checkpoint_key is not None
            else list(
                set(_ARTIFACT_NAMESPACE_COUNTS) | set(_ARTIFACT_NAMESPACE_FINALIZERS)
            )
        )
        for key in keys:
            if key is None:
                continue
            _ARTIFACT_NAMESPACE_COUNTS.pop(key, None)
            finalizer = _ARTIFACT_NAMESPACE_FINALIZERS.pop(key, None)
            if finalizer is not None:
                finalizer.detach()
    _CONTEXT_ARTIFACT_NAMESPACE_STATE.set(None)


def _clear_artifact_namespace_count(checkpoint_key: CheckpointKey) -> None:
    _reset_artifact_namespace_state_for_key(checkpoint_key, detach_finalizer=False)


def _reset_artifact_namespace_state_for_key(
    checkpoint_key: CheckpointKey, *, detach_finalizer: bool
) -> None:
    with _ARTIFACT_NAMESPACE_LOCK:
        _ARTIFACT_NAMESPACE_COUNTS.pop(checkpoint_key, None)
        finalizer = _ARTIFACT_NAMESPACE_FINALIZERS.pop(checkpoint_key, None)
        if detach_finalizer and finalizer is not None:
            finalizer.detach()


def _tracker_artifact_namespace(
    agent_name: str, run_label: str, tracker_count: int
) -> str:
    return f"{agent_name}_{run_label}_tracker_{tracker_count}"


def _context_artifact_namespace(
    agent_name: str, run_label: str, checkpoint_key: CheckpointKey
) -> str:
    state = _CONTEXT_ARTIFACT_NAMESPACE_STATE.get()
    if state is None or state.checkpoint_key != checkpoint_key:
        state = _ArtifactNamespaceState(checkpoint_key=checkpoint_key)
        _CONTEXT_ARTIFACT_NAMESPACE_STATE.set(state)

    state.tracker_count += 1
    return _tracker_artifact_namespace(agent_name, run_label, state.tracker_count)


def _allocate_artifact_namespace(agent_name: str, run_label: str) -> str | None:
    checkpoint = get_current_checkpoint()
    if checkpoint is None:
        return _tracker_artifact_namespace(agent_name, run_label, 1)
    checkpoint_key = _checkpoint_key(checkpoint)

    with _ARTIFACT_NAMESPACE_LOCK:
        if checkpoint_key not in _ARTIFACT_NAMESPACE_FINALIZERS:
            try:
                _ARTIFACT_NAMESPACE_FINALIZERS[checkpoint_key] = weakref.finalize(
                    checkpoint,
                    _clear_artifact_namespace_count,
                    checkpoint_key,
                )
            except TypeError:
                return _context_artifact_namespace(
                    agent_name, run_label, checkpoint_key
                )
        tracker_count = _ARTIFACT_NAMESPACE_COUNTS.get(checkpoint_key, 0) + 1
        _ARTIFACT_NAMESPACE_COUNTS[checkpoint_key] = tracker_count

    return _tracker_artifact_namespace(agent_name, run_label, tracker_count)


@dataclass(frozen=True)
class ModelEventContext:
    sequence_index: int
    turn_index: int
    fan_in_from: list[str]


@dataclass(frozen=True)
class ToolEventContext:
    sequence_index: int
    turn_index: int
    fan_out_from: str | None


@dataclass(frozen=True)
class _ReservedToolEvent:
    tool_call_id: str
    event_id: str
    sequence_index: int
    turn_index: int
    fan_out_from: str | None


@dataclass
class EventTracker:
    agent_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    checkpoint_name: str | None = field(default=None, init=False)
    checkpoint_id: str | None = field(default=None, init=False)
    exec_id: str | None = field(default=None, init=False)
    artifact_namespace: str | None = field(default=None, init=False)
    _counter: int = 0
    _turn_index: int = 0
    _current_model_event_id: str | None = None
    _pending_tool_event_ids: list[str] = field(default_factory=list)
    _events: list[AgentEvent] = field(default_factory=list)
    _status: EventStatus = "completed"
    _error: EventError | None = None
    _model_call_count: int = 0
    _tool_call_count: int = 0
    _event_stream_handler_call_count: int = 0
    _event_stream_handler_duration_ms: float = 0.0
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
        self.agent_name = normalize_agent_name(self.agent_name)
        checkpoint = get_current_checkpoint()
        self.checkpoint_name = get_current_checkpoint_name()
        self.checkpoint_id = (
            checkpoint.checkpoint_id if checkpoint is not None else None
        )
        self.exec_id = get_current_execution_id()
        self.artifact_namespace = _allocate_artifact_namespace(
            self.agent_name,
            self.run_label,
        )

    @property
    def events(self) -> Sequence[AgentEvent]:
        with self._lock:
            return tuple(self._ordered_events())

    @property
    def event_log_artifact_name(self) -> str:
        return _with_namespace("event_log", self.artifact_namespace)

    @property
    def run_summary_artifact_name(self) -> str:
        return _with_namespace("run_summary", self.artifact_namespace)

    def artifact_name(self, event_id: str, kind: ArtifactKind) -> str:
        return _namespaced_artifact_name(
            event_id,
            kind,
            namespace=self.artifact_namespace,
        )

    def _next_event_id(self, event_kind: str) -> tuple[str, int]:
        self._counter += 1
        event_id = f"{self.agent_name}_{self.run_label}_{event_kind}_{self._counter}"
        self._event_sequence_by_id[event_id] = self._counter
        return event_id, self._counter

    def _ordered_events(self) -> list[AgentEvent]:
        return sorted(self._events, key=lambda event: event.sequence_index)

    def _event_sequence_index(self, event_id: str) -> int:
        return self._event_sequence_by_id.get(event_id, self._counter + 1)

    def _current_checkpoint_refs(self) -> tuple[str | None, str | None]:
        checkpoint = get_current_checkpoint()
        return (
            get_current_checkpoint_name(),
            checkpoint.checkpoint_id if checkpoint is not None else None,
        )

    def _clear_reserved_tool_events_unlocked(self) -> None:
        for reservations in self._reserved_tool_events_by_call_id.values():
            for reservation in reservations:
                self._event_sequence_by_id.pop(reservation.event_id, None)
        self._reserved_tool_events_by_call_id.clear()

    def set_run_error(self, error: BaseException) -> None:
        with self._lock:
            self._status = "failed"
            self._error = error_from_exception(error)

    def start_model_event(self) -> tuple[str, ModelEventContext]:
        with self._lock:
            fan_in_from = sorted(
                self._pending_tool_event_ids,
                key=self._event_sequence_index,
            )
            self._pending_tool_event_ids.clear()
            self._clear_reserved_tool_events_unlocked()
            event_id, sequence_index = self._next_event_id("llm_call")
            self._model_call_count += 1
            self._turn_index += 1
            return event_id, ModelEventContext(
                sequence_index=sequence_index,
                turn_index=self._turn_index,
                fan_in_from=fan_in_from,
            )

    def reserve_tool_call_order(
        self,
        *,
        parent_model_event_id: str,
        turn_index: int,
        tool_call_ids: Sequence[str],
    ) -> None:
        with self._lock:
            for tool_call_id in tool_call_ids:
                event_id, sequence_index = self._next_event_id("tool_call")
                reserved = _ReservedToolEvent(
                    tool_call_id=tool_call_id,
                    event_id=event_id,
                    sequence_index=sequence_index,
                    turn_index=turn_index,
                    fan_out_from=parent_model_event_id,
                )
                self._reserved_tool_events_by_call_id.setdefault(
                    tool_call_id, []
                ).append(reserved)

    def record_model_event(
        self,
        event_id: str,
        event_context: ModelEventContext,
        *,
        status: EventStatus,
        duration_ms: float,
        artifacts: dict[str, str],
        model_name: str | None = None,
        usage: RequestUsage | None = None,
        stream_event_count: int | None = None,
        error: BaseException | None = None,
        checkpoint_name: str | None = None,
        checkpoint_id: str | None = None,
        billing_effect: LLMBillingEffect = "incurred",
        cache_status: LLMCacheStatus = "executed",
    ) -> None:
        with self._lock:
            if status == "completed":
                self._current_model_event_id = event_id
            elif self._current_model_event_id == event_id:
                self._current_model_event_id = None

            current_checkpoint_name, current_checkpoint_id = (
                self._current_checkpoint_refs()
            )
            self._events.append(
                ModelEvent(
                    event_id=event_id,
                    status=status,
                    sequence_index=event_context.sequence_index,
                    turn_index=event_context.turn_index,
                    parent_event_ids=event_context.fan_in_from,
                    checkpoint_name=checkpoint_name or current_checkpoint_name,
                    checkpoint_id=checkpoint_id or current_checkpoint_id,
                    fan_in_from=event_context.fan_in_from,
                    duration_ms=duration_ms,
                    artifacts=artifacts,
                    model_name=model_name,
                    usage=usage,
                    billing_effect=billing_effect,
                    cache_status=cache_status,
                    stream_event_count=stream_event_count,
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def start_tool_event(
        self,
        *,
        tool_call_id: str | None = None,
    ) -> tuple[str, ToolEventContext]:
        with self._lock:
            reservation: _ReservedToolEvent | None = None
            if isinstance(tool_call_id, str) and tool_call_id:
                reservations = self._reserved_tool_events_by_call_id.get(tool_call_id)
                if reservations:
                    reservation = reservations.pop(0)
                    if not reservations:
                        self._reserved_tool_events_by_call_id.pop(tool_call_id, None)

            if reservation is None:
                event_id, sequence_index = self._next_event_id("tool_call")
                turn_index = self._turn_index
                fan_out_from = self._current_model_event_id
            else:
                event_id = reservation.event_id
                sequence_index = reservation.sequence_index
                turn_index = reservation.turn_index
                fan_out_from = reservation.fan_out_from

            self._tool_call_count += 1
            return event_id, ToolEventContext(
                sequence_index=sequence_index,
                turn_index=turn_index,
                fan_out_from=fan_out_from,
            )

    def record_tool_event(
        self,
        event_id: str,
        event_context: ToolEventContext,
        *,
        status: EventStatus,
        name: str,
        toolset_kind: ToolsetKind,
        capture_mode: CaptureMode,
        duration_ms: float,
        hitl: bool,
        artifacts: dict[str, str],
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            if status == "completed":
                self._pending_tool_event_ids.append(event_id)
            elif event_id in self._pending_tool_event_ids:
                self._pending_tool_event_ids.remove(event_id)

            parent_event_ids = (
                [event_context.fan_out_from]
                if event_context.fan_out_from is not None
                else []
            )
            checkpoint_name, checkpoint_id = self._current_checkpoint_refs()
            self._events.append(
                ToolEvent(
                    event_id=event_id,
                    status=status,
                    sequence_index=event_context.sequence_index,
                    turn_index=event_context.turn_index,
                    parent_event_ids=parent_event_ids,
                    checkpoint_name=checkpoint_name,
                    checkpoint_id=checkpoint_id,
                    tool_name=name,
                    toolset_kind=toolset_kind,
                    hitl=hitl,
                    capture_mode=capture_mode,
                    fan_out_from=event_context.fan_out_from,
                    duration_ms=duration_ms,
                    artifacts=artifacts,
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def record_deferred_event(
        self,
        *,
        tool_name: str,
        deferred_kind: DeferredKind,
        wait_name: str,
        metadata: dict[str, object] | None,
        approved: bool | None = None,
    ) -> None:
        with self._lock:
            event_id, sequence_index = self._next_event_id("deferred")
            parent_event_ids = (
                [self._current_model_event_id] if self._current_model_event_id else []
            )
            checkpoint_name, checkpoint_id = self._current_checkpoint_refs()
            self._events.append(
                DeferredEvent(
                    event_id=event_id,
                    status="completed",
                    sequence_index=sequence_index,
                    turn_index=self._turn_index,
                    parent_event_ids=parent_event_ids,
                    checkpoint_name=checkpoint_name,
                    checkpoint_id=checkpoint_id,
                    tool_name=tool_name,
                    deferred_kind=deferred_kind,
                    wait_name=wait_name,
                    metadata=metadata,
                    approved=approved,
                )
            )

    def record_stream_event(
        self, *, duration_ms: float, error: BaseException | None
    ) -> None:
        with self._lock:
            self._event_stream_handler_call_count += 1
            self._event_stream_handler_duration_ms = round(
                self._event_stream_handler_duration_ms + duration_ms, 3
            )
            event_id, sequence_index = self._next_event_id("event_stream")
            checkpoint_name, checkpoint_id = self._current_checkpoint_refs()
            self._events.append(
                StreamEvent(
                    event_id=event_id,
                    status="failed" if error is not None else "completed",
                    sequence_index=sequence_index,
                    turn_index=self._turn_index,
                    parent_event_ids=[],
                    checkpoint_name=checkpoint_name,
                    checkpoint_id=checkpoint_id,
                    index=self._event_stream_handler_call_count,
                    duration_ms=duration_ms,
                    error=error_from_exception(error) if error is not None else None,
                )
            )

    def _build_run_summary_unlocked(
        self,
        ordered_events: list[AgentEvent],
    ) -> RunSummary:
        event_stream_handler = None
        if self._event_stream_handler_call_count:
            event_stream_handler = EventStreamHandlerSummary(
                call_count=self._event_stream_handler_call_count,
                duration_ms=round(self._event_stream_handler_duration_ms, 3),
            )
        return RunSummary(
            agent_name=self.agent_name,
            run_label=self.run_label,
            status=self._status,
            model_call_count=self._model_call_count,
            tool_call_count=self._tool_call_count,
            total_events=len(ordered_events),
            turn_count=self._turn_index,
            event_ids_in_order=[event.event_id for event in ordered_events],
            duration_ms=round((time.perf_counter() - self._started_at) * 1000, 3),
            checkpoint_name=self.checkpoint_name,
            checkpoint_id=self.checkpoint_id,
            exec_id=self.exec_id,
            event_stream_handler=event_stream_handler,
            error=self._error,
        )

    def build_run_summary(self) -> RunSummary:
        with self._lock:
            return self._build_run_summary_unlocked(self._ordered_events())

    def persist(self) -> None:
        with self._lock:
            ordered_events = self._ordered_events()
            events_dump = dump_agent_events(ordered_events)
            summary_dump = self._build_run_summary_unlocked(ordered_events).model_dump(
                mode="json"
            )
            event_log_artifact_name = self.event_log_artifact_name
            run_summary_artifact_name = self.run_summary_artifact_name
            run_label = self.run_label
            agent_name = self.agent_name

        if is_inside_checkpoint():
            kitaru.save(event_log_artifact_name, events_dump, type="context")
            kitaru.save(run_summary_artifact_name, summary_dump, type="context")
        else:
            logger.debug(
                "Persisting PydanticAI tracker outside a checkpoint; emitting flow-level metadata only.",
                extra={"agent_name": agent_name, "run_label": run_label},
            )
        usage_records = []
        for event in ordered_events:
            if not isinstance(event, ModelEvent) or event.usage is None:
                continue
            usage_payload = _request_usage_payload(event.usage)
            usage_records.append(
                build_usage_record(
                    adapter="pydantic_ai",
                    surface="model_call",
                    call_name=agent_name,
                    event_id=event.event_id,
                    record_id=event.event_id,
                    checkpoint_id=event.checkpoint_id,
                    checkpoint_name=event.checkpoint_name,
                    model=event.model_name,
                    usage=usage_payload,
                    latency_ms=event.duration_ms,
                    status=event.status,
                    billing_effect=event.billing_effect,
                    cache_status=event.cache_status,
                )
            )
        kitaru.log(
            pydantic_ai_events={run_label: events_dump},
            pydantic_ai_run_summaries={run_label: summary_dump},
            **usage_records_metadata(usage_records),
        )


_CURRENT_TRACKER: ContextVar[EventTracker | None] = ContextVar(
    "kitaru_pydantic_ai_event_tracker",
    default=None,
)


@contextmanager
def tracker_scope(agent_name: str | None) -> Iterator[EventTracker]:
    tracker = EventTracker(agent_name=agent_name or "agent")
    token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    except Exception as error:
        tracker.set_run_error(error)
        raise
    finally:
        try:
            try:
                tracker.persist()
            except Exception:
                logger.warning(
                    "Failed to persist PydanticAI tracker state.",
                    exc_info=True,
                    extra={
                        "agent_name": tracker.agent_name,
                        "run_label": tracker.run_label,
                    },
                )
        finally:
            _CURRENT_TRACKER.reset(token)


def get_current_tracker() -> EventTracker | None:
    return _CURRENT_TRACKER.get()
