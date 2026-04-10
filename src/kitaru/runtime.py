"""Internal runtime support for the Kitaru SDK.

This module provides shared utilities used across the SDK implementation.
It is not part of the public API surface.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, NoReturn

from zenml.execution.pipeline.dynamic.run_context import DynamicPipelineRunContext
from zenml.steps.step_context import StepContext

from kitaru._source_aliases import normalize_flow_name as _shared_normalize_flow_name
from kitaru.errors import KitaruFeatureNotAvailableError


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


_CURRENT_FLOW_SCOPE: ContextVar[_FlowScope | None] = ContextVar(
    "kitaru_current_flow_scope",
    default=None,
)
_CURRENT_CHECKPOINT_SCOPE: ContextVar[_CheckpointScope | None] = ContextVar(
    "kitaru_current_checkpoint_scope",
    default=None,
)
_LLM_CALL_COUNTER: ContextVar[int] = ContextVar("kitaru_llm_call_counter", default=0)


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
        )
    )
    llm_counter_token = _LLM_CALL_COUNTER.set(0)
    try:
        yield
    finally:
        _LLM_CALL_COUNTER.reset(llm_counter_token)
        _CURRENT_CHECKPOINT_SCOPE.reset(checkpoint_token)


@contextmanager
def _suspend_checkpoint_scope() -> Iterator[None]:
    """Temporarily clear checkpoint scope while keeping flow scope active.

    This internal helper is used by framework adapters that need to trigger
    flow-level operations (for example `kitaru.wait()`) during framework-internal
    execution that otherwise runs inside a checkpoint.
    """
    checkpoint_token = _CURRENT_CHECKPOINT_SCOPE.set(None)
    try:
        yield
    finally:
        _CURRENT_CHECKPOINT_SCOPE.reset(checkpoint_token)


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


def _get_current_checkpoint_id() -> str | None:
    """Get the current checkpoint invocation ID from active Kitaru scope."""
    if (
        checkpoint_scope := _get_current_checkpoint()
    ) and checkpoint_scope.checkpoint_id:
        return checkpoint_scope.checkpoint_id

    return None


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
