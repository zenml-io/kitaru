"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from functools import update_wrapper
from typing import Any, overload

from zenml.client import Client
from zenml.config.constants import DOCKER_SETTINGS_KEY
from zenml.config.docker_settings import DockerSettings
from zenml.config.retry_config import StepRetryConfig
from zenml.enums import ExecutionStatus
from zenml.models import PipelineRunResponse
from zenml.pipelines.pipeline_decorator import pipeline
from zenml.pipelines.pipeline_definition import Pipeline

ImageSetting = str | DockerSettings | Mapping[str, Any]


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack for one flow invocation.

    Args:
        stack_name_or_id: Optional stack name or ID.
    """
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


def _normalize_image(image: ImageSetting | None) -> DockerSettings | None:
    """Normalize image input into ZenML Docker settings.

    Args:
        image: Optional image configuration.

    Returns:
        Docker settings for ZenML, or `None`.
    """
    if image is None:
        return None
    if isinstance(image, DockerSettings):
        return image
    if isinstance(image, str):
        return DockerSettings(parent_image=image)
    return DockerSettings.model_validate(image)


def _build_settings(image: ImageSetting | None) -> dict[str, DockerSettings] | None:
    """Build ZenML settings payload for flow execution.

    Args:
        image: Optional image configuration.

    Returns:
        Pipeline settings dictionary or `None`.
    """
    docker_settings = _normalize_image(image)
    if docker_settings is None:
        return None
    return {DOCKER_SETTINGS_KEY: docker_settings}


def _extract_flow_result(run: PipelineRunResponse) -> Any:
    """Extract user-facing flow return value from a finished pipeline run.

    Args:
        run: The pipeline run.

    Raises:
        RuntimeError: If run output metadata is missing.

    Returns:
        The flow result (`None`, a single value, or a tuple of values).
    """
    hydrated_run = run.get_hydrated_version()

    snapshot = hydrated_run.snapshot
    pipeline_spec = snapshot.pipeline_spec if snapshot else None
    output_specs = pipeline_spec.outputs if pipeline_spec else []

    if not output_specs:
        return None

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
    status_reason = run.get_body().status_reason
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
        cache: bool,
        retries: int,
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
        self._default_stack = stack
        self._default_image = image
        self._default_cache = cache
        self._default_retries = _normalize_retries(retries)
        self._pipeline: Pipeline = pipeline(dynamic=True)(func)
        update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Run flow synchronously and return the final result."""
        handle = self._submit(
            args=args,
            kwargs=kwargs,
            stack=self._default_stack,
            image=self._default_image,
            cache=self._default_cache,
            retries=self._default_retries,
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
        resolved_stack = stack if stack is not None else self._default_stack
        resolved_image = image if image is not None else self._default_image
        resolved_cache = cache if cache is not None else self._default_cache
        resolved_retries = retries if retries is not None else self._default_retries

        return self._submit(
            args=args,
            kwargs=kwargs,
            stack=resolved_stack,
            image=resolved_image,
            cache=resolved_cache,
            retries=resolved_retries,
        )

    def _submit(
        self,
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        stack: str | None,
        image: ImageSetting | None,
        cache: bool,
        retries: int,
    ) -> FlowHandle:
        """Submit an execution using resolved runtime settings.

        Args:
            args: Flow input args.
            kwargs: Flow input kwargs.
            stack: Resolved stack.
            image: Resolved image settings.
            cache: Resolved cache behavior.
            retries: Resolved retry count.

        Returns:
            A handle for the started execution.
        """
        resolved_retries = _normalize_retries(retries)
        configured_pipeline = self._pipeline.with_options(
            enable_cache=cache,
            retry=_to_retry_config(resolved_retries),
            settings=_build_settings(image),
        )

        with _temporary_active_stack(stack):
            run = configured_pipeline(*args, **kwargs)

        if run is None:
            raise RuntimeError("Flow execution did not produce a pipeline run.")

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
    cache: bool = True,
    retries: int = 0,
) -> Callable[[Callable[..., Any]], _FlowDefinition]: ...


def flow(
    func: Callable[..., Any] | None = None,
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool = True,
    retries: int = 0,
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
        cache: Whether checkpoint caching is enabled.
        retries: Number of automatic retries for flow steps.

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
