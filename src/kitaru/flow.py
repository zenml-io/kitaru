"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import re
import sys
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
from kitaru.runtime import _flow_scope

ImageSetting = ImageInput


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack for one flow invocation.

    Args:
        stack_name_or_id: Optional stack name or ID.
    """
    if not stack_name_or_id or stack_name_or_id == "local":
        # "local" is Kitaru's implicit built-in default mode.
        # We don't force-activate a named stack for it.
        yield
        return

    client = Client()
    old_stack_id = client.active_stack_model.id
    client.activate_stack(stack_name_or_id)
    try:
        yield
    finally:
        client.activate_stack(old_stack_id)


def _pipeline_source_alias_name(func: Callable[..., Any]) -> str:
    """Build a stable module-level alias for ZenML source loading.

    Args:
        func: User flow function.

    Returns:
        Alias name used to expose the ZenML pipeline object.
    """
    flow_name = getattr(func, "__name__", func.__class__.__name__)
    normalized_name = re.sub(r"\W", "_", flow_name)
    if not normalized_name:
        normalized_name = "flow"
    if normalized_name[0].isdigit():
        normalized_name = f"flow_{normalized_name}"
    return f"__kitaru_pipeline_source_{normalized_name}"


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

    flow_name = getattr(func, "__name__", func.__class__.__name__)

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
        ValueError: If retries is negative.

    Returns:
        The normalized retry count.
    """
    if retries < 0:
        raise ValueError("Flow retries must be >= 0.")
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
) -> dict[str, DockerSettings] | None:
    """Build ZenML settings payload for flow execution.

    Args:
        image: Optional image configuration.

    Returns:
        Pipeline settings dictionary or `None`.
    """
    docker_settings = image_settings_to_docker_settings(image)
    if docker_settings is None:
        return None
    return {DOCKER_SETTINGS_KEY: docker_settings}


def _build_execution_overrides(
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> KitaruConfig:
    """Build a partial execution config from flow/deploy/start overrides."""
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
            raise RuntimeError(
                f"Execution {hydrated_run.id} is missing step output metadata "
                f"for '{output_spec.step_name}'."
            )

        artifact = step_run.regular_outputs.get(output_spec.output_name)
        if artifact is None:
            raise RuntimeError(
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
        raise RuntimeError(
            "Execution output metadata is missing and fallback extraction is "
            "ambiguous because multiple terminal steps were found."
        )

    terminal_step_name = terminal_step_names[0]
    terminal_step = step_runs[terminal_step_name]
    if not terminal_step.regular_outputs:
        raise RuntimeError(
            f"Execution {hydrated_run.id} has no regular outputs on terminal "
            f"step '{terminal_step_name}'."
        )
    if len(terminal_step.regular_outputs) > 1:
        raise RuntimeError(
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
        RuntimeError: If run output metadata is missing.

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
    """Raise a runtime error with helpful run failure context.

    Args:
        run: A finished but unsuccessful run.

    Raises:
        RuntimeError: Always.
    """
    details = [f"Execution {run.id} finished with status '{run.status.value}'."]

    run_body = run.get_body() if hasattr(run, "get_body") else run
    status_reason = getattr(run_body, "status_reason", None)
    if status_reason:
        details.append(status_reason)
    if run.exception_info and run.exception_info.traceback:
        details.append(run.exception_info.traceback.splitlines()[-1])
    raise RuntimeError(" ".join(details))


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
            RuntimeError: If the run finishes unsuccessfully.

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
            RuntimeError: If the run is unfinished or unsuccessful.

        Returns:
            The flow return value.
        """
        run = self._refresh()
        if not run.status.is_finished:
            raise RuntimeError(
                f"Execution {run.id} is still running (status: {run.status.value})."
            )
        if not run.status.is_successful:
            _raise_for_unsuccessful_run(run)
        return _extract_flow_result(run)

    def _refresh(self) -> PipelineRunResponse:
        """Refresh the cached run model from the server."""
        self._run = Client().get_pipeline_run(
            self._run_id,
            allow_name_prefix_match=False,
        )
        return self._run


class _FlowDefinition:
    """Callable wrapper returned by `@kitaru.flow`."""

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
        source_alias = _pipeline_source_alias_name(func)
        aliasable_entrypoint = cast(Any, wrapped_entrypoint)
        aliasable_entrypoint.__name__ = source_alias
        aliasable_entrypoint.__qualname__ = source_alias

        self._pipeline: Pipeline = pipeline(dynamic=True)(wrapped_entrypoint)
        _register_pipeline_source_alias(
            func=func,
            alias=source_alias,
            pipeline_obj=self._pipeline,
        )
        update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run flow synchronously and return the final result."""
        handle = self._submit(
            args=args,
            kwargs=kwargs,
            invocation_overrides=KitaruConfig(),
        )
        return handle.wait()

    def start(
        self,
        *args: Any,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        **kwargs: Any,
    ) -> FlowHandle:
        """Start a flow execution and return a handle.

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
            connection=resolve_connection_config(),
        )
        configured_pipeline = self._pipeline.with_options(
            enable_cache=resolved_execution.cache,
            retry=_to_retry_config(_normalize_retries(resolved_execution.retries)),
            settings=_build_settings(resolved_execution.image),
        )

        with _temporary_active_stack(resolved_execution.stack):
            run = configured_pipeline(*args, **kwargs)

        if run is None:
            raise RuntimeError("Flow execution did not produce a pipeline run.")

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
        """Start a flow execution, signaling remote/deployment intent.

        This is semantic sugar for `.start(..., stack=...)`.

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
        return self.start(
            *args,
            stack=stack,
            image=image,
            cache=cache,
            retries=retries,
            **kwargs,
        )


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

        @kitaru.flow
        def my_flow(...):
            ...

        @kitaru.flow(stack="prod", retries=2)
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
