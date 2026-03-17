"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import update_wrapper, wraps
from typing import Any, cast, overload

from zenml.client import Client
from zenml.config.constants import DOCKER_SETTINGS_KEY
from zenml.config.docker_settings import DockerSettings
from zenml.config.retry_config import StepRetryConfig
from zenml.enums import ExecutionStatus
from zenml.models import PipelineRunResponse
from zenml.pipelines.pipeline_decorator import pipeline
from zenml.pipelines.pipeline_definition import Pipeline

from kitaru._source_aliases import (
    build_pipeline_registration_name,
    build_pipeline_source_alias,
    callable_name,
)
from kitaru.analytics import track
from kitaru.config import (
    ImageInput,
    ImageSettings,
    KitaruConfig,
    build_frozen_execution_spec,
    image_settings_to_docker_settings,
    persist_frozen_execution_spec,
    resolve_connection_config,
    resolve_execution_config,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    classify_failure_origin,
    execution_error_from_failure,
    traceback_last_line,
)
from kitaru.replay import build_replay_plan
from kitaru.runtime import _flow_scope

ImageSetting = ImageInput
_STACK_BINDING_LOCK = threading.RLock()


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack for one flow invocation.

    Args:
        stack_name_or_id: Optional stack name or ID. When ``None``, the
            currently active ZenML stack is used unchanged.
    """
    with _STACK_BINDING_LOCK:
        if not stack_name_or_id:
            yield
            return

        client = Client()
        old_stack_id = client.active_stack_model.id
        client.activate_stack(stack_name_or_id)
        try:
            yield
        finally:
            client.activate_stack(old_stack_id)


def _register_pipeline_source_alias(
    *,
    func: Callable[..., Any],
    alias: str,
    pipeline_obj: Pipeline,
) -> None:
    """Register the ZenML pipeline object under a module-level alias.

    ZenML dynamic runs reload pipelines from their source import path. Kitaru
    wraps ZenML pipelines, so we expose the underlying pipeline object under a
    dedicated alias and point source resolution there.

    Args:
        func: User flow function.
        alias: Module-level alias name.
        pipeline_obj: Underlying ZenML pipeline object.
    """
    module = sys.modules.get(func.__module__)
    if module is None:
        return
    setattr(module, alias, pipeline_obj)


def _wrap_flow_entrypoint(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a flow entrypoint with Kitaru flow runtime scope."""

    flow_name = callable_name(func)

    @wraps(func)
    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        with _flow_scope(name=flow_name):
            return func(*args, **kwargs)

    return _wrapped


def _normalize_retries(retries: int) -> int:
    """Validate and normalize flow retries.

    Args:
        retries: Retry count.

    Raises:
        KitaruUsageError: If retries is negative.

    Returns:
        The normalized retry count.
    """
    if retries < 0:
        raise KitaruUsageError("Flow retries must be >= 0.")
    return retries


def _to_retry_config(retries: int) -> StepRetryConfig | None:
    """Convert a retry count to ZenML retry config.

    Args:
        retries: Retry count.

    Returns:
        A ZenML retry config, or `None` when retries are disabled.
    """
    if retries == 0:
        return None
    return StepRetryConfig(max_retries=retries)


def _build_settings(
    image: ImageSettings | None,
) -> dict[str, DockerSettings]:
    """Build ZenML settings payload for flow execution.

    Kitaru is always included in the Docker requirements so that remote
    containers have the SDK available at runtime.

    Args:
        image: Optional image configuration.

    Returns:
        Pipeline settings dictionary.
    """
    return {DOCKER_SETTINGS_KEY: image_settings_to_docker_settings(image)}


def _build_execution_overrides(
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> KitaruConfig:
    """Build a partial execution config from flow/run/deploy overrides."""
    values: dict[str, Any] = {}
    if stack is not None:
        values["stack"] = stack
    if image is not None:
        values["image"] = image
    if cache is not None:
        values["cache"] = cache
    if retries is not None:
        values["retries"] = retries
    return KitaruConfig.model_validate(values)


def _extract_values_from_output_specs(run: PipelineRunResponse) -> list[Any]:
    """Extract return values using explicit pipeline output specs."""
    hydrated_run = run.get_hydrated_version()

    snapshot = hydrated_run.snapshot
    pipeline_spec = snapshot.pipeline_spec if snapshot else None
    output_specs = pipeline_spec.outputs if pipeline_spec else []
    if not output_specs:
        return []

    step_runs = hydrated_run.steps
    values: list[Any] = []
    for output_spec in output_specs:
        step_run = step_runs.get(output_spec.step_name)
        if step_run is None:
            raise KitaruRuntimeError(
                f"Execution {hydrated_run.id} is missing step output metadata "
                f"for '{output_spec.step_name}'."
            )

        artifact = step_run.regular_outputs.get(output_spec.output_name)
        if artifact is None:
            raise KitaruRuntimeError(
                f"Execution {hydrated_run.id} is missing output "
                f"'{output_spec.output_name}' on step '{output_spec.step_name}'."
            )

        values.append(artifact.load())

    return values


def _extract_values_from_terminal_steps(run: PipelineRunResponse) -> list[Any]:
    """Extract return values from terminal step outputs as a fallback.

    This fallback is intentionally conservative to avoid returning values in an
    incorrect order when ZenML pipeline-level output specs are unavailable.
    """
    hydrated_run = run.get_hydrated_version()
    step_runs = hydrated_run.steps
    if not step_runs:
        return []

    upstream_step_names: set[str] = set()
    for step_run in step_runs.values():
        step_spec = getattr(step_run, "spec", None)
        if step_spec is None:
            continue
        upstream_step_names.update(getattr(step_spec, "upstream_steps", []) or [])

    terminal_step_names = sorted(
        step_name for step_name in step_runs if step_name not in upstream_step_names
    )
    if not terminal_step_names:
        return []
    if len(terminal_step_names) > 1:
        raise KitaruRuntimeError(
            "Execution output metadata is missing and fallback extraction is "
            "ambiguous because multiple terminal steps were found."
        )

    terminal_step_name = terminal_step_names[0]
    terminal_step = step_runs[terminal_step_name]
    if not terminal_step.regular_outputs:
        raise KitaruRuntimeError(
            f"Execution {hydrated_run.id} has no regular outputs on terminal "
            f"step '{terminal_step_name}'."
        )
    if len(terminal_step.regular_outputs) > 1:
        raise KitaruRuntimeError(
            "Execution output metadata is missing and fallback extraction is "
            "ambiguous because the terminal step has multiple outputs."
        )

    output_name = next(iter(terminal_step.regular_outputs))
    artifact = terminal_step.regular_outputs[output_name]
    return [artifact.load()]


def _extract_flow_result(run: PipelineRunResponse) -> Any:
    """Extract user-facing flow return value from a finished pipeline run.

    Args:
        run: The pipeline run.

    Raises:
        KitaruRuntimeError: If run output metadata is missing or ambiguous.

    Returns:
        The flow result (`None`, a single value, or a tuple of values).
    """
    values = _extract_values_from_output_specs(run)
    if not values:
        values = _extract_values_from_terminal_steps(run)

    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return tuple(values)


def _raise_for_unsuccessful_run(run: PipelineRunResponse) -> None:
    """Raise a typed Kitaru execution error with run failure context."""
    details = [f"Execution {run.id} finished with status '{run.status.value}'."]

    run_body = run.get_body() if hasattr(run, "get_body") else run
    status_reason = getattr(run_body, "status_reason", None)
    if status_reason:
        details.append(status_reason)

    traceback_text: str | None = None
    if run.exception_info is not None:
        traceback_text = run.exception_info.traceback

    traceback_tail = traceback_last_line(traceback_text)
    if traceback_tail:
        details.append(traceback_tail)

    default_origin = (
        FailureOrigin.USER_CODE if traceback_text is not None else FailureOrigin.UNKNOWN
    )
    failure_origin = classify_failure_origin(
        status_reason=status_reason,
        traceback=traceback_text,
        default=default_origin,
    )
    raise execution_error_from_failure(
        " ".join(details),
        exec_id=str(run.id),
        status=run.status.value,
        origin=failure_origin,
    )


class FlowHandle:
    """Handle for a running or finished flow execution."""

    def __init__(self, run: PipelineRunResponse) -> None:
        """Initialize a flow handle.

        Args:
            run: Initial pipeline run response.
        """
        self._run = run
        self._run_id = run.id

    @property
    def exec_id(self) -> str:
        """Execution identifier for this flow run."""
        return str(self._run_id)

    @property
    def status(self) -> ExecutionStatus:
        """Current execution status."""
        return self._refresh().status

    def wait(self) -> Any:
        """Block until execution finishes and return its result.

        Raises:
            KitaruExecutionError: If the run finishes unsuccessfully.
            KitaruRuntimeError: If result extraction fails after completion.

        Returns:
            The flow return value.
        """
        while True:
            run = self._refresh()
            if run.status.is_finished:
                if not run.status.is_successful:
                    _raise_for_unsuccessful_run(run)
                return _extract_flow_result(run)
            time.sleep(1)

    def get(self) -> Any:
        """Get the flow result without waiting.

        Raises:
            KitaruStateError: If the run is still unfinished.
            KitaruExecutionError: If the run finished unsuccessfully.
            KitaruRuntimeError: If result extraction fails after completion.

        Returns:
            The flow return value.
        """
        run = self._refresh()
        if not run.status.is_finished:
            raise KitaruStateError(
                f"Execution {run.id} is still running (status: {run.status.value})."
            )
        if not run.status.is_successful:
            _raise_for_unsuccessful_run(run)
        return _extract_flow_result(run)

    def _refresh(self) -> PipelineRunResponse:
        """Refresh the cached run model from the server."""
        try:
            self._run = Client().get_pipeline_run(
                self._run_id,
                allow_name_prefix_match=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to refresh execution {self._run_id}: {exc}"
            ) from exc
        return self._run


class _FlowDefinition:
    """Flow wrapper returned by `@flow`."""

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        stack: str | None,
        image: ImageSetting | None,
        cache: bool | None,
        retries: int | None,
    ) -> None:
        """Initialize a Kitaru flow wrapper.

        Args:
            func: User flow function.
            stack: Default stack override.
            image: Default image settings.
            cache: Default cache behavior.
            retries: Default retry count.
        """
        self._func = func
        self._decorator_config = _build_execution_overrides(
            stack=stack,
            image=image,
            cache=cache,
            retries=retries,
        )

        wrapped_entrypoint = _wrap_flow_entrypoint(func)
        func_name = callable_name(func)
        registration_name = build_pipeline_registration_name(func_name)
        source_alias = build_pipeline_source_alias(func_name)
        aliasable_entrypoint = cast(Any, wrapped_entrypoint)
        aliasable_entrypoint.__name__ = source_alias
        aliasable_entrypoint.__qualname__ = source_alias

        self._pipeline: Pipeline = pipeline(
            dynamic=True,
            name=registration_name,
        )(wrapped_entrypoint)
        _register_pipeline_source_alias(
            func=func,
            alias=source_alias,
            pipeline_obj=self._pipeline,
        )
        update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Raise a friendly error — direct flow calls are not supported."""
        raise KitaruUsageError(
            "Direct flow calls are not supported. Use:\n"
            "  handle = my_flow.run(...)        # returns FlowHandle\n"
            "  result = my_flow.run(...).wait()  # blocks until complete\n"
            "  handle = my_flow.deploy(...)      # remote execution"
        )

    def run(
        self,
        *args: Any,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        **kwargs: Any,
    ) -> FlowHandle:
        """Run a flow execution and return a handle.

        Args:
            *args: Flow input args.
            stack: Optional stack override.
            image: Optional image override.
            cache: Optional cache override.
            retries: Optional retry override.
            **kwargs: Flow input kwargs.

        Returns:
            A handle for the started execution.
        """
        return self._submit(
            args=args,
            kwargs=kwargs,
            invocation_overrides=_build_execution_overrides(
                stack=stack,
                image=image,
                cache=cache,
                retries=retries,
            ),
        )

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        **flow_inputs: Any,
    ) -> FlowHandle:
        """Replay a prior execution from a checkpoint boundary.

        Args:
            exec_id: Source execution ID.
            from_: Checkpoint selector (name, invocation ID, or call ID).
            overrides: Optional `checkpoint.*` override map.
            stack: Optional stack override for the replay run.
            image: Optional image override for the replay run.
            cache: Optional cache override for the replay run.
            retries: Optional retry override for the replay run.
            **flow_inputs: Optional flow input overrides.

        Returns:
            A handle for the replayed execution.
        """
        resolved_connection = resolve_connection_config(validate_for_use=True)

        try:
            original_run = Client().get_pipeline_run(
                name_id_or_prefix=exec_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load source execution '{exec_id}' for replay: {exc}"
            ) from exc

        replay_plan = build_replay_plan(
            run=original_run,
            from_=from_,
            overrides=overrides,
            flow_inputs=flow_inputs,
        )

        resolved_execution = resolve_execution_config(
            decorator_overrides=self._decorator_config,
            invocation_overrides=_build_execution_overrides(
                stack=stack,
                image=image,
                cache=cache,
                retries=retries,
            ),
        )
        frozen_execution_spec = build_frozen_execution_spec(
            resolved_execution=resolved_execution,
            flow_defaults=self._decorator_config,
            connection=resolved_connection,
        )
        configured_pipeline = self._pipeline.with_options(
            enable_cache=resolved_execution.cache,
            retry=_to_retry_config(_normalize_retries(resolved_execution.retries)),
            settings=_build_settings(resolved_execution.image),
        )

        with _temporary_active_stack(resolved_execution.stack):
            try:
                replayed_run = configured_pipeline.replay(
                    pipeline_run=original_run.id,
                    skip=replay_plan.steps_to_skip,
                    skip_successful_steps=False,
                    input_overrides=replay_plan.input_overrides or None,
                    step_input_overrides=replay_plan.step_input_overrides or None,
                )
            except Exception as exc:
                failure_origin = classify_failure_origin(
                    status_reason=str(exc),
                    traceback=None,
                    default=FailureOrigin.BACKEND,
                )
                if failure_origin == FailureOrigin.DIVERGENCE:
                    raise execution_error_from_failure(
                        f"Replay diverged for execution '{exec_id}': {exc}",
                        exec_id=str(original_run.id),
                        status="failed",
                        origin=failure_origin,
                    ) from exc
                raise KitaruBackendError(
                    f"Failed to replay execution '{exec_id}': {exc}"
                ) from exc

        if replayed_run is None:
            raise KitaruRuntimeError("Replay did not produce a pipeline run.")

        persist_frozen_execution_spec(
            run_id=replayed_run.id,
            frozen_execution_spec=frozen_execution_spec,
        )

        track("Kitaru flow replayed", {"execution_id": str(replayed_run.id)})
        return FlowHandle(replayed_run)

    def _submit(
        self,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        invocation_overrides: KitaruConfig,
    ) -> FlowHandle:
        """Submit an execution using resolved runtime settings.

        Args:
            args: Flow input args.
            kwargs: Flow input kwargs.
            invocation_overrides: Invocation-time execution overrides.

        Returns:
            A handle for the started execution.
        """
        resolved_execution = resolve_execution_config(
            decorator_overrides=self._decorator_config,
            invocation_overrides=invocation_overrides,
        )
        frozen_execution_spec = build_frozen_execution_spec(
            resolved_execution=resolved_execution,
            flow_defaults=self._decorator_config,
            connection=resolve_connection_config(validate_for_use=True),
        )
        configured_pipeline = self._pipeline.with_options(
            enable_cache=resolved_execution.cache,
            retry=_to_retry_config(_normalize_retries(resolved_execution.retries)),
            settings=_build_settings(resolved_execution.image),
        )

        with _temporary_active_stack(resolved_execution.stack):
            run = configured_pipeline(*args, **kwargs)

        if run is None:
            raise KitaruRuntimeError("Flow execution did not produce a pipeline run.")

        persist_frozen_execution_spec(
            run_id=run.id,
            frozen_execution_spec=frozen_execution_spec,
        )
        return FlowHandle(run)

    def deploy(
        self,
        *args: Any,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        **kwargs: Any,
    ) -> FlowHandle:
        """Run a flow execution, signaling remote/deployment intent.

        This is semantic sugar for `.run(..., stack=...)`.

        Args:
            *args: Flow input args.
            stack: Optional stack override.
            image: Optional image override.
            cache: Optional cache override.
            retries: Optional retry override.
            **kwargs: Flow input kwargs.

        Returns:
            A handle for the started execution.
        """
        handle = self.run(
            *args,
            stack=stack,
            image=image,
            cache=cache,
            retries=retries,
            **kwargs,
        )
        track("Kitaru flow deployed", {"flow_name": callable_name(self._func)})
        return handle


@overload
def flow(func: Callable[..., Any], /) -> _FlowDefinition: ...


@overload
def flow(
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> Callable[[Callable[..., Any]], _FlowDefinition]: ...


def flow(
    func: Callable[..., Any] | None = None,
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> _FlowDefinition | Callable[[Callable[..., Any]], _FlowDefinition]:
    """Mark a function as a durable flow.

    Can be used as a bare decorator or with arguments::

        @flow
        def my_flow(...):
            ...

        @flow(stack="prod", retries=2)
        def my_other_flow(...):
            ...

    Args:
        func: Optional function for bare decorator use.
        stack: Default execution stack.
        image: Default image settings.
        cache: Optional cache override (when omitted, lower-precedence config
            sources apply and eventually default to ``True``).
        retries: Optional retry override (when omitted, lower-precedence config
            sources apply and eventually default to ``0``).

    Returns:
        The wrapped flow object or a decorator that returns it.
    """

    def _decorate(target: Callable[..., Any]) -> _FlowDefinition:
        return _FlowDefinition(
            target,
            stack=stack,
            image=image,
            cache=cache,
            retries=retries,
        )

    if func is not None:
        return _decorate(func)
    return _decorate
