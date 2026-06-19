"""Internal runtime support for the Kitaru SDK.

This module provides shared utilities used across the SDK implementation.
It is not part of the public API surface.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Lock
from typing import Any, NoReturn, cast
from uuid import uuid4

from zenml.execution.pipeline.dynamic.run_context import DynamicPipelineRunContext
from zenml.steps.step_context import StepContext

from kitaru._source_aliases import normalize_flow_name as _shared_normalize_flow_name
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError


@dataclass(frozen=True)
class _FlowScope:
    """Internal runtime context for the currently executing flow."""

    name: str | None
    flow_id: str | None = None
    execution_id: str | None = None


@dataclass(frozen=True)
class _CheckpointScope:
    """Internal runtime context for the currently executing checkpoint."""

    name: str
    type: str | None
    execution_id: str | None = None
    checkpoint_id: str | None = None
    event_correlation_id: str | None = None


_CURRENT_FLOW_SCOPE: ContextVar[_FlowScope | None] = ContextVar(
    "kitaru_current_flow_scope",
    default=None,
)
_CURRENT_CHECKPOINT_SCOPE: ContextVar[_CheckpointScope | None] = ContextVar(
    "kitaru_current_checkpoint_scope",
    default=None,
)
_LLM_CALL_COUNTER: ContextVar[int] = ContextVar("kitaru_llm_call_counter", default=0)


class _CheckpointEventCounterState:
    """Shared checkpoint-local state for live event index reservation."""

    def __init__(self) -> None:
        self._next_index = 0
        self._lock = Lock()

    def reserve(self, index: int | None = None) -> int:
        """Reserve one event index while holding the shared state lock."""
        with self._lock:
            next_index = self._next_index
            if index is None:
                self._next_index = next_index + 1
                return next_index

            if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                raise KitaruUsageError("Event index must be a non-negative integer.")
            if index < next_index:
                raise KitaruUsageError(
                    f"Event index {index} is lower than the next available checkpoint "
                    f"event index {next_index}."
                )

            self._next_index = index + 1
            return index


_CHECKPOINT_EVENT_COUNTER: ContextVar[_CheckpointEventCounterState | None] = ContextVar(
    "kitaru_checkpoint_event_counter",
    default=None,
)


def _to_optional_str(value: Any) -> str | None:
    """Convert IDs from ZenML objects into optional strings."""
    if value is None:
        return None
    return str(value)


def _get_zenml_execution_id() -> str | None:
    """Resolve the active execution ID from ZenML runtime contexts."""
    if step_context := StepContext.get():
        return _to_optional_str(getattr(step_context.pipeline_run, "id", None))

    if run_context := DynamicPipelineRunContext.get():
        return _to_optional_str(getattr(run_context.run, "id", None))

    return None


def _get_zenml_checkpoint_id() -> str | None:
    """Resolve the active checkpoint invocation ID from ZenML step context."""
    if step_context := StepContext.get():
        return _to_optional_str(getattr(step_context.step_run, "id", None))

    return None


def _new_checkpoint_event_correlation_id(checkpoint_name: str) -> str:
    """Return a non-sensitive event correlation ID for one checkpoint run."""
    return f"kitaru.checkpoint:{checkpoint_name}:{uuid4().hex}"


def _get_zenml_flow_name() -> str | None:
    """Resolve the active flow name from ZenML runtime contexts, if available."""
    if step_context := StepContext.get():
        pipeline = getattr(step_context.pipeline_run, "pipeline", None)
        if flow_name := _shared_normalize_flow_name(getattr(pipeline, "name", None)):
            return flow_name

    if run_context := DynamicPipelineRunContext.get():
        run_pipeline = getattr(getattr(run_context, "run", None), "pipeline", None)
        if flow_name := _shared_normalize_flow_name(
            getattr(run_pipeline, "name", None)
        ):
            return flow_name

        if flow_name := _shared_normalize_flow_name(
            getattr(run_context.pipeline, "name", None)
        ):
            return flow_name

    return None


def _get_zenml_flow_id() -> str | None:
    """Resolve the active flow ID from ZenML runtime contexts, if available."""
    if step_context := StepContext.get():
        pipeline = getattr(step_context.pipeline_run, "pipeline", None)
        if flow_id := _to_optional_str(getattr(pipeline, "id", None)):
            return flow_id

    if run_context := DynamicPipelineRunContext.get():
        run_pipeline = getattr(getattr(run_context, "run", None), "pipeline", None)
        if flow_id := _to_optional_str(getattr(run_pipeline, "id", None)):
            return flow_id

        if flow_id := _to_optional_str(getattr(run_context.pipeline, "id", None)):
            return flow_id

    return None


@contextmanager
def _flow_scope(
    *,
    name: str | None,
    flow_id: str | None = None,
    execution_id: str | None = None,
) -> Iterator[None]:
    """Set flow runtime scope for the active execution context."""
    resolved_flow_id = flow_id if flow_id is not None else _get_zenml_flow_id()
    resolved_execution_id = (
        execution_id if execution_id is not None else _get_zenml_execution_id()
    )
    flow_token = _CURRENT_FLOW_SCOPE.set(
        _FlowScope(
            name=name,
            flow_id=resolved_flow_id,
            execution_id=resolved_execution_id,
        )
    )
    llm_counter_token = _LLM_CALL_COUNTER.set(0)
    try:
        yield
    finally:
        _LLM_CALL_COUNTER.reset(llm_counter_token)
        _CURRENT_FLOW_SCOPE.reset(flow_token)


@contextmanager
def _checkpoint_scope(
    *,
    name: str,
    checkpoint_type: str | None,
    execution_id: str | None = None,
    checkpoint_id: str | None = None,
    event_correlation_id: str | None = None,
) -> Iterator[None]:
    """Set checkpoint runtime scope for the active execution context."""
    resolved_execution_id = (
        execution_id if execution_id is not None else _get_zenml_execution_id()
    )
    resolved_checkpoint_id = (
        checkpoint_id if checkpoint_id is not None else _get_zenml_checkpoint_id()
    )
    checkpoint_token = _CURRENT_CHECKPOINT_SCOPE.set(
        _CheckpointScope(
            name=name,
            type=checkpoint_type,
            execution_id=resolved_execution_id,
            checkpoint_id=resolved_checkpoint_id,
            event_correlation_id=event_correlation_id
            or _new_checkpoint_event_correlation_id(name),
        )
    )
    llm_counter_token = _LLM_CALL_COUNTER.set(0)
    event_counter_token = _CHECKPOINT_EVENT_COUNTER.set(_CheckpointEventCounterState())
    try:
        yield
    finally:
        _CHECKPOINT_EVENT_COUNTER.reset(event_counter_token)
        _LLM_CALL_COUNTER.reset(llm_counter_token)
        _CURRENT_CHECKPOINT_SCOPE.reset(checkpoint_token)


@contextmanager
def _suspend_checkpoint_scope() -> Iterator[None]:
    """Temporarily clear checkpoint scope while keeping flow scope active.

    This internal helper exists for low-level runtime operations that need to
    step outside checkpoint-local bookkeeping. It does not make arbitrary
    checkpoint-contained waits part of the public contract: ``kitaru.wait()``
    should still be called from flow scope, before or after checkpoint calls.

    Both Kitaru's checkpoint scope and ZenML's ``StepContext`` are suspended
    because some internal operations need to hide all step-local state, not
    only Kitaru's own scope marker.
    """
    checkpoint_token = _CURRENT_CHECKPOINT_SCOPE.set(None)
    step_context_var = _get_step_context_var()
    # Setting to None mirrors the "unset" default used by BaseContext.get();
    # is_active() returns False as a result.
    step_context_token = step_context_var.set(None)
    try:
        yield
    finally:
        step_context_var.reset(step_context_token)
        _CURRENT_CHECKPOINT_SCOPE.reset(checkpoint_token)


def _get_step_context_var() -> ContextVar[StepContext | None]:
    """Return ZenML's internal `StepContext` ContextVar with a hard failure.

    Runtime helpers occasionally need to clear ZenML's step-local state while
    preserving Kitaru flow scope. Keep the private-attribute reach-through in
    one place so a ZenML rename fails loudly and contract tests can pin the
    dependency.
    """
    context_var = getattr(StepContext, "__context_var__", None)
    if not isinstance(context_var, ContextVar):
        raise KitaruFeatureNotAvailableError(
            "Installed ZenML build does not expose StepContext context state "
            "required for adapter-managed waits."
        )
    return cast(ContextVar[StepContext | None], context_var)


@contextmanager
def _suspend_step_context() -> Iterator[None]:
    """Temporarily clear ZenML's step-local context."""
    step_context_var = _get_step_context_var()
    step_ctx_token = step_context_var.set(None)
    try:
        yield
    finally:
        step_context_var.reset(step_ctx_token)


def _get_current_flow() -> _FlowScope | None:
    """Get the currently active flow scope, if any."""
    return _CURRENT_FLOW_SCOPE.get()


def _get_current_flow_id() -> str | None:
    """Get the durable ID for the active flow, if any."""
    if (flow_scope := _get_current_flow()) and flow_scope.flow_id is not None:
        return flow_scope.flow_id
    return None


def _is_inside_flow() -> bool:
    """Check whether code is currently running inside a flow."""
    return _get_current_flow() is not None


def _get_current_checkpoint() -> _CheckpointScope | None:
    """Get the currently active checkpoint scope, if any."""
    return _CURRENT_CHECKPOINT_SCOPE.get()


def _get_current_checkpoint_name() -> str | None:
    """Get the current checkpoint name from active Kitaru scope."""
    if (checkpoint_scope := _get_current_checkpoint()) and checkpoint_scope.name:
        return checkpoint_scope.name
    return None


def _is_inside_checkpoint() -> bool:
    """Check whether code is currently running inside a checkpoint."""
    return _get_current_checkpoint() is not None


def _get_current_execution_id() -> str | None:
    """Get the current execution ID from active Kitaru scopes, if available."""
    if (
        checkpoint_scope := _get_current_checkpoint()
    ) and checkpoint_scope.execution_id:
        return checkpoint_scope.execution_id

    if (flow_scope := _get_current_flow()) and flow_scope.execution_id:
        return flow_scope.execution_id

    return None


def current_execution_id() -> str | None:
    """Return the active Kitaru execution ID, or None outside a Kitaru scope.

    Example:
        execution_id = kitaru.current_execution_id() or "local"
        resource_name = f"worker_{execution_id}"
    """
    return _get_current_execution_id()


def _get_current_checkpoint_id() -> str | None:
    """Get the current checkpoint invocation ID from active Kitaru scope."""
    if (
        checkpoint_scope := _get_current_checkpoint()
    ) and checkpoint_scope.checkpoint_id:
        return checkpoint_scope.checkpoint_id

    return None


def _get_current_checkpoint_event_correlation_id() -> str | None:
    """Get the active checkpoint's default live-event correlation ID."""
    if (
        checkpoint_scope := _get_current_checkpoint()
    ) and checkpoint_scope.event_correlation_id:
        return checkpoint_scope.event_correlation_id

    return None


def _next_checkpoint_event_index(index: int | None = None) -> int:
    """Reserve the next checkpoint event index.

    Explicit indexes are allowed only when they are at or above the next
    automatic index. When accepted, they advance the counter so the next
    automatic event cannot collide with the caller-provided index.
    """
    state = _CHECKPOINT_EVENT_COUNTER.get()
    if state is None:
        state = _CheckpointEventCounterState()
        _CHECKPOINT_EVENT_COUNTER.set(state)
    return state.reserve(index)


def _next_llm_call_name(prefix: str = "llm") -> str:
    """Return the next runtime-local sequential LLM call name."""
    normalized_prefix = prefix.strip() or "llm"
    next_index = _LLM_CALL_COUNTER.get() + 1
    _LLM_CALL_COUNTER.set(next_index)
    return f"{normalized_prefix}_{next_index}"


def _not_implemented(name: str) -> NoReturn:
    """Raise a consistent feature-availability error for stubbed APIs."""
    raise KitaruFeatureNotAvailableError(
        f"kitaru.{name}() is not yet implemented. "
        "The Kitaru SDK is under active development."
    )
