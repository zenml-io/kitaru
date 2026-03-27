"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import update_wrapper, wraps
from typing import Any, cast, overload

from zenml.client import Client
from zenml.models import PipelineRunResponse

from kitaru._source_aliases import (
    build_pipeline_registration_name,
    build_pipeline_source_alias,
    callable_name,
)
from kitaru.analytics import AnalyticsEvent, track
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ImageInput,
    ImageSettings,
    KitaruConfig,
    ModelRegistryConfig,
    _read_env_model_registry,
    _read_model_registry_config,
    build_frozen_execution_spec,
    detect_explicit_execution_overrides,
    resolve_connection_config,
    resolve_execution_config,
)
from kitaru.engines import get_engine_backend
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
logger = logging.getLogger(__name__)


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
    source_object: Any,
) -> None:
    """Register the backend flow object under a module-level alias.

    Backend engines reload flow definitions from their source import path.
    We expose the underlying backend object under a dedicated alias
    and point source resolution there.

    Args:
        func: User flow function.
        alias: Module-level alias name.
        source_object: Backend-native flow object.
    """
    module = sys.modules.get(func.__module__)
    if module is None:
        return
    setattr(module, alias, source_object)


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


def _inject_model_registry_env(
    image: ImageSettings | None,
    *,
    read_local_registry: Callable[[], ModelRegistryConfig],
) -> tuple[ImageSettings, ModelRegistryConfig, bool]:
    """Return image settings with a transported model-registry snapshot."""
    existing_environment = (
        dict(image.environment) if image and image.environment else {}
    )
    existing_registry = _read_env_model_registry(
        environ=existing_environment,
        source_label="image environment",
    )
    if existing_registry is not None:
        transport_image = (
            image.model_copy()
            if image is not None
            else ImageSettings(environment=existing_environment)
        )
        return transport_image, existing_registry, False

    local_registry = read_local_registry()
    transport_environment = dict(existing_environment)
    transport_environment[KITARU_MODEL_REGISTRY_ENV] = local_registry.model_dump_json(
        exclude_none=True
    )
    if image is None:
        return (
            ImageSettings(environment=transport_environment),
            local_registry,
            True,
        )
    return (
        image.model_copy(update={"environment": transport_environment}),
        local_registry,
        True,
    )


def _prepare_model_registry_transport(
    image: ImageSettings | None,
) -> tuple[ImageSettings, ModelRegistryConfig]:
    """Inject the model registry into image env and log the outcome."""
    transport_image, effective_model_registry, did_inject_registry = (
        _inject_model_registry_env(
            image,
            read_local_registry=_read_model_registry_config,
        )
    )
    if did_inject_registry:
        logger.debug(
            "Transporting %d model aliases to remote environment.",
            len(effective_model_registry.aliases),
        )
    else:
        logger.debug(
            "Using preconfigured transported model registry with %d model aliases.",
            len(effective_model_registry.aliases),
        )
    return transport_image, effective_model_registry


def _build_execution_overrides(
    *,
    stack: str | None = None,
    image: ImageSetting | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> KitaruConfig:
    """Build a partial execution config from flow and invocation overrides."""
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


_DAPR_FINISHED_STATUSES = frozenset({"completed", "failed", "cancelled"})


def _raise_for_unsuccessful_dapr_execution(execution: Any) -> None:
    """Raise a typed Kitaru execution error from a Dapr Execution model."""
    status_value = execution.status.value
    details = [f"Execution {execution.exec_id} finished with status '{status_value}'."]

    if execution.status_reason:
        details.append(execution.status_reason)

    traceback_text: str | None = None
    failure = execution.failure
    if failure is not None:
        if failure.message and failure.message not in details:
            details.append(failure.message)
        traceback_text = failure.traceback
        failure_origin = failure.origin
    else:
        failure_origin = FailureOrigin.UNKNOWN

    traceback_tail = traceback_last_line(traceback_text)
    if traceback_tail and traceback_tail not in details:
        details.append(traceback_tail)

    if failure_origin == FailureOrigin.UNKNOWN:
        failure_origin = classify_failure_origin(
            status_reason=execution.status_reason,
            traceback=traceback_text,
            default=FailureOrigin.UNKNOWN,
        )

    raise execution_error_from_failure(
        " ".join(details),
        exec_id=execution.exec_id,
        status=execution.status.value,
        origin=failure_origin,
    )


class FlowHandle:
    """Handle for a running or finished flow execution.

    Supports both ZenML (``PipelineRunResponse``) and Dapr
    (``DaprFlowRunHandle``) backends. The backend is detected
    automatically from the object passed to the constructor.
    """

    def __init__(self, run_or_handle: Any) -> None:
        """Initialize a flow handle.

        Args:
            run_or_handle: A ZenML ``PipelineRunResponse`` or a
                ``DaprFlowRunHandle``.
        """
        from kitaru.engines.dapr.backend import DaprFlowRunHandle

        if isinstance(run_or_handle, DaprFlowRunHandle):
            self._backend_kind = "dapr"
            self._exec_id = run_or_handle.exec_id
            self._run: PipelineRunResponse | None = None
            self._cached_dapr_client: Any = None
        else:
            self._backend_kind = "zenml"
            self._run = run_or_handle
            self._exec_id = str(run_or_handle.id)
            self._cached_dapr_client = None

    @property
    def exec_id(self) -> str:
        """Execution identifier for this flow run."""
        return self._exec_id

    @property
    def status(self) -> Any:
        """Current execution status (backend-specific enum)."""
        if self._backend_kind == "dapr":
            return self._dapr_get_execution().status
        return self._refresh().status

    def wait(self) -> Any:
        """Block until execution finishes and return its result.

        Raises:
            KitaruExecutionError: If the run finishes unsuccessfully.
            KitaruRuntimeError: If result extraction fails after completion.

        Returns:
            The flow return value.
        """
        if self._backend_kind == "dapr":
            return self._dapr_wait()

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
        if self._backend_kind == "dapr":
            return self._dapr_get()

        run = self._refresh()
        if not run.status.is_finished:
            raise KitaruStateError(
                f"Execution {run.id} is still running (status: {run.status.value})."
            )
        if not run.status.is_successful:
            _raise_for_unsuccessful_run(run)
        return _extract_flow_result(run)

    # -- ZenML internals ----------------------------------------------------

    def _refresh(self) -> PipelineRunResponse:
        """Refresh the cached run model from the server."""
        try:
            self._run = Client().get_pipeline_run(
                self._exec_id,
                allow_name_prefix_match=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to refresh execution {self._exec_id}: {exc}"
            ) from exc
        assert self._run is not None
        return self._run

    # -- Dapr internals -----------------------------------------------------

    def _dapr_client(self) -> Any:
        if self._cached_dapr_client is None:
            from kitaru.client import KitaruClient

            self._cached_dapr_client = KitaruClient()
        return self._cached_dapr_client

    def _dapr_get_execution(self) -> Any:
        """Fetch the current Dapr execution state."""
        try:
            return self._dapr_client().executions.get(self._exec_id)
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to refresh execution {self._exec_id}: {exc}"
            ) from exc

    def _dapr_wait(self) -> Any:
        """Poll a Dapr execution until it finishes and return the result."""
        from kitaru._client._models import ExecutionStatus as DaprStatus

        while True:
            execution = self._dapr_get_execution()
            if execution.status.value in _DAPR_FINISHED_STATUSES:
                if execution.status == DaprStatus.COMPLETED:
                    return self._dapr_client()._load_execution_result(self._exec_id)
                _raise_for_unsuccessful_dapr_execution(execution)
            time.sleep(1)

    def _dapr_get(self) -> Any:
        """Get the Dapr execution result without waiting."""
        from kitaru._client._models import ExecutionStatus as DaprStatus

        execution = self._dapr_get_execution()
        if execution.status.value not in _DAPR_FINISHED_STATUSES:
            raise KitaruStateError(
                f"Execution {self._exec_id} is still running "
                f"(status: {execution.status.value})."
            )
        if execution.status == DaprStatus.COMPLETED:
            return self._dapr_client()._load_execution_result(self._exec_id)
        _raise_for_unsuccessful_dapr_execution(execution)


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

        backend = get_engine_backend()
        self._backend = backend
        self._engine_definition = backend.create_flow_definition(
            entrypoint=wrapped_entrypoint,
            registration_name=registration_name,
        )
        _register_pipeline_source_alias(
            func=func,
            alias=source_alias,
            source_object=self._engine_definition.source_object,
        )
        update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Raise a friendly error — direct flow calls are not supported."""
        raise KitaruUsageError(
            "Direct flow calls are not supported. Use:\n"
            "  handle = my_flow.run(...)        # returns FlowHandle\n"
            "  result = my_flow.run(...).wait()  # blocks until complete"
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
        # Fail early if the backend does not support flow-level replay
        self._backend.validate_flow_replay_support()

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
            snapshot=get_engine_backend().execution_graph_from_run(original_run),
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
        transport_image, effective_model_registry = _prepare_model_registry_transport(
            resolved_execution.image
        )
        frozen_execution_spec = build_frozen_execution_spec(
            resolved_execution=resolved_execution,
            flow_defaults=self._decorator_config,
            connection=resolved_connection,
            model_registry=effective_model_registry,
        )
        with _temporary_active_stack(resolved_execution.stack):
            try:
                replayed_run = self._engine_definition.replay(
                    source_run_id=original_run.id,
                    cache=resolved_execution.cache,
                    retries=_normalize_retries(resolved_execution.retries),
                    image=transport_image,
                    steps_to_skip=replay_plan.steps_to_skip,
                    input_overrides=replay_plan.input_overrides or None,
                    step_input_overrides=replay_plan.step_input_overrides or None,
                    frozen_execution_spec=frozen_execution_spec,
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

        track(AnalyticsEvent.FLOW_REPLAYED, {"execution_id": str(replayed_run.id)})
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
        # Validate backend capabilities before any side effects
        explicit = detect_explicit_execution_overrides(
            decorator_overrides=self._decorator_config,
            invocation_overrides=invocation_overrides,
        )
        self._backend.validate_flow_run_options(explicit)

        resolved_execution = resolve_execution_config(
            decorator_overrides=self._decorator_config,
            invocation_overrides=invocation_overrides,
        )
        resolved_connection = resolve_connection_config(validate_for_use=True)
        transport_image, effective_model_registry = _prepare_model_registry_transport(
            resolved_execution.image
        )
        frozen_execution_spec = build_frozen_execution_spec(
            resolved_execution=resolved_execution,
            flow_defaults=self._decorator_config,
            connection=resolved_connection,
            model_registry=effective_model_registry,
        )
        with _temporary_active_stack(resolved_execution.stack):
            run = self._engine_definition.run(
                args=args,
                kwargs=kwargs,
                cache=resolved_execution.cache,
                retries=_normalize_retries(resolved_execution.retries),
                image=transport_image,
                frozen_execution_spec=frozen_execution_spec,
            )

        if run is None:
            raise KitaruRuntimeError("Flow execution did not produce a pipeline run.")

        return FlowHandle(run)


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
