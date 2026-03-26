"""Checkpoint decorator for durable work boundaries.

A checkpoint is a unit of work inside a flow whose outcome is persisted.
Successful outputs become artifacts; failures are recorded for retry.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from functools import update_wrapper, wraps
from typing import Any, cast, overload

from zenml.config.retry_config import StepRetryConfig
from zenml.enums import StepRuntime, StepType
from zenml.execution.pipeline.dynamic.run_context import DynamicPipelineRunContext
from zenml.pipelines.compilation_context import PipelineCompilationContext
from zenml.steps.step_context import StepContext
from zenml.steps.step_decorator import step

from kitaru._source_aliases import (
    build_checkpoint_registration_name,
    build_checkpoint_source_alias,
    callable_name,
)
from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.futures import (
    KitaruMapFuture,
    KitaruStepFuture,
    unwrap_kitaru_futures,
)
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_checkpoint_id,
    _get_current_execution_id,
    _get_current_flow,
    _get_zenml_checkpoint_id,
    _get_zenml_execution_id,
    _get_zenml_flow_name,
    _is_inside_flow,
)

_CHECKPOINT_OUTSIDE_FLOW_ERROR = "Checkpoints can only run inside a @flow."
_CHECKPOINT_NESTED_ERROR = (
    "Nested checkpoint calls are not supported in the Kitaru MVP."
)
_CHECKPOINT_CONCURRENT_OUTSIDE_FLOW_ERROR = (
    "Concurrent checkpoint execution is only available inside a running @flow."
)


def _register_checkpoint_source_alias(
    *,
    func: Callable[..., Any],
    alias: str,
    step_obj: Any,
) -> None:
    """Register the ZenML step object under a module-level alias."""
    module = sys.modules.get(func.__module__)
    if module is None:
        return
    setattr(module, alias, step_obj)


def _normalize_retries(retries: int) -> int:
    """Validate and normalize checkpoint retries."""
    if retries < 0:
        raise KitaruUsageError("Checkpoint retries must be >= 0.")
    return retries


def _to_retry_config(retries: int) -> StepRetryConfig | None:
    """Convert retry count to ZenML retry config."""
    if retries == 0:
        return None
    return StepRetryConfig(max_retries=retries)


def _build_checkpoint_extra(checkpoint_type: str | None) -> dict[str, Any]:
    """Build namespaced step metadata for dashboard rendering."""
    payload: dict[str, Any] = {"boundary": "checkpoint"}
    if checkpoint_type is not None:
        payload["type"] = checkpoint_type
    return {"kitaru": payload}


_KNOWN_STEP_TYPES: dict[str, StepType] = {
    "llm_call": StepType.LLM_CALL,
    "tool_call": StepType.TOOL_CALL,
}


def _to_step_type(checkpoint_type: str | None) -> StepType | None:
    """Map well-known checkpoint types to ZenML's StepType enum."""
    if checkpoint_type is None:
        return None
    return _KNOWN_STEP_TYPES.get(checkpoint_type)


_KNOWN_STEP_RUNTIMES: dict[str, StepRuntime] = {
    "inline": StepRuntime.INLINE,
    "isolated": StepRuntime.ISOLATED,
}
_RUNTIME_OPTIONS = ", ".join(_KNOWN_STEP_RUNTIMES)


def _runtime_error(value: object) -> KitaruUsageError:
    return KitaruUsageError(
        f"Unsupported checkpoint runtime {value!r}. "
        f"Expected one of: {_RUNTIME_OPTIONS}."
    )


def _normalize_runtime(runtime: StepRuntime | str | None) -> StepRuntime | None:
    """Validate and normalize checkpoint runtime input.

    Accepts ``None``, a :class:`StepRuntime` enum member, or a case-insensitive
    string (``"inline"`` / ``"isolated"``).
    """
    if runtime is None:
        return None
    if isinstance(runtime, StepRuntime):
        return runtime
    if not isinstance(runtime, str):
        raise _runtime_error(runtime)
    normalized = runtime.strip().lower()
    resolved = _KNOWN_STEP_RUNTIMES.get(normalized)
    if resolved is None:
        raise _runtime_error(runtime)
    return resolved


def _wrap_entrypoint(
    func: Callable[..., Any],
    *,
    checkpoint_type: str | None,
) -> Callable[..., Any]:
    """Wrap the user function with Kitaru checkpoint runtime scope."""

    checkpoint_name = callable_name(func)

    @wraps(func)
    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        current_flow = _get_current_flow()
        execution_id = _get_current_execution_id() or _get_zenml_execution_id()
        checkpoint_id = _get_current_checkpoint_id() or _get_zenml_checkpoint_id()

        with ExitStack() as scope_stack:
            if current_flow is None:
                scope_stack.enter_context(
                    _flow_scope(
                        name=_get_zenml_flow_name(),
                        execution_id=execution_id,
                    )
                )

            scope_stack.enter_context(
                _checkpoint_scope(
                    name=checkpoint_name,
                    checkpoint_type=checkpoint_type,
                    execution_id=execution_id,
                    checkpoint_id=checkpoint_id,
                )
            )
            return func(*args, **kwargs)

    return _wrapped


class _CheckpointDefinition:
    """Callable wrapper returned by `@checkpoint`."""

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        retries: int,
        checkpoint_type: str | None,
        runtime: StepRuntime | str | None,
    ) -> None:
        """Initialize a Kitaru checkpoint wrapper."""
        self._func = func
        self._checkpoint_type = checkpoint_type
        self._default_retries = _normalize_retries(retries)
        self._runtime = _normalize_runtime(runtime)

        wrapped_entrypoint = _wrap_entrypoint(
            func,
            checkpoint_type=checkpoint_type,
        )
        func_name = callable_name(func)
        registration_name = build_checkpoint_registration_name(func_name)
        source_alias = build_checkpoint_source_alias(func_name)
        aliasable_entrypoint = cast(Any, wrapped_entrypoint)
        aliasable_entrypoint.__name__ = source_alias
        aliasable_entrypoint.__qualname__ = source_alias

        self._step = step(
            name=registration_name,
            retry=_to_retry_config(self._default_retries),
            extra=_build_checkpoint_extra(checkpoint_type),
            step_type=_to_step_type(checkpoint_type),
            runtime=self._runtime,
        )(wrapped_entrypoint)
        _register_checkpoint_source_alias(
            func=func,
            alias=source_alias,
            step_obj=self._step,
        )

        update_wrapper(self, func)

    def _assert_call_allowed(self) -> None:
        """Validate that checkpoint call semantics match Kitaru MVP rules."""
        if PipelineCompilationContext.is_active() and _is_inside_flow():
            return

        if StepContext.is_active():
            raise KitaruContextError(_CHECKPOINT_NESTED_ERROR)

        if DynamicPipelineRunContext.is_active() and _is_inside_flow():
            return

        raise KitaruContextError(_CHECKPOINT_OUTSIDE_FLOW_ERROR)

    def _assert_submit_allowed(self) -> None:
        """Validate that checkpoint submission is legal in the current context."""
        if StepContext.is_active():
            raise KitaruContextError(_CHECKPOINT_NESTED_ERROR)

        if not DynamicPipelineRunContext.is_active() or not _is_inside_flow():
            raise KitaruContextError(_CHECKPOINT_CONCURRENT_OUTSIDE_FLOW_ERROR)

    def __call__(
        self,
        *args: Any,
        id: str | None = None,
        after: Any | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Call the checkpoint with context guardrails."""
        self._assert_call_allowed()
        return self._step(
            *unwrap_kitaru_futures(args),
            id=id,
            after=unwrap_kitaru_futures(after),
            **unwrap_kitaru_futures(kwargs),
        )

    def submit(
        self,
        *args: Any,
        id: str | None = None,
        after: Any | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> KitaruStepFuture:
        """Submit the checkpoint concurrently inside a running flow."""
        self._assert_submit_allowed()
        native = self._step.submit(
            *unwrap_kitaru_futures(args),
            id=id,
            after=unwrap_kitaru_futures(after),
            **unwrap_kitaru_futures(kwargs),
        )
        return KitaruStepFuture(native)

    def map(
        self,
        *args: Any,
        after: Any | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> KitaruMapFuture:
        """Map checkpoint invocations inside a running flow."""
        self._assert_submit_allowed()
        native = self._step.map(
            *unwrap_kitaru_futures(args),
            after=unwrap_kitaru_futures(after),
            **unwrap_kitaru_futures(kwargs),
        )
        return KitaruMapFuture(native)

    def product(
        self,
        *args: Any,
        after: Any | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> KitaruMapFuture:
        """Map checkpoint invocations as a cartesian product in a running flow."""
        self._assert_submit_allowed()
        native = self._step.product(
            *unwrap_kitaru_futures(args),
            after=unwrap_kitaru_futures(after),
            **unwrap_kitaru_futures(kwargs),
        )
        return KitaruMapFuture(native)


@overload
def checkpoint(func: Callable[..., Any], /) -> _CheckpointDefinition: ...


@overload
def checkpoint(
    *,
    retries: int = 0,
    type: str | None = None,
    runtime: str | None = None,
) -> Callable[[Callable[..., Any]], _CheckpointDefinition]: ...


def checkpoint(
    func: Callable[..., Any] | None = None,
    *,
    retries: int = 0,
    type: str | None = None,
    runtime: str | None = None,
) -> _CheckpointDefinition | Callable[[Callable[..., Any]], _CheckpointDefinition]:
    """Mark a function as a durable checkpoint.

    Can be used as a bare decorator or with arguments::

        from kitaru import checkpoint

        @checkpoint
        def my_step(): ...

        @checkpoint(retries=3, type="llm_call")
        def my_step(): ...

        @checkpoint(runtime="isolated")
        def heavy_step(): ...

    Args:
        func: Optional function for bare decorator use.
        retries: Number of checkpoint-level retries on failure.
        type: Checkpoint type for dashboard visualization.
        runtime: Execution runtime for this checkpoint. Accepts ``"inline"``
            or ``"isolated"`` (case-insensitive). When set to ``"isolated"``,
            the checkpoint runs in its own container on remote orchestrators
            that support it. ``None`` (the default) lets the orchestrator
            decide.

    Returns:
        The wrapped checkpoint object or a decorator that returns it.
    """
    checkpoint_type = type

    def _decorate(target: Callable[..., Any]) -> _CheckpointDefinition:
        return _CheckpointDefinition(
            target,
            retries=retries,
            checkpoint_type=checkpoint_type,
            runtime=runtime,
        )

    if func is not None:
        return _decorate(func)

    return _decorate
