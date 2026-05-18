"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import inspect
import logging
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from functools import update_wrapper, wraps
from typing import Any, cast, overload
from urllib.parse import quote
from uuid import uuid4

from pydantic import ConfigDict, create_model
from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.config.constants import DOCKER_SETTINGS_KEY
from zenml.config.docker_settings import DockerSettings
from zenml.config.global_config import GlobalConfiguration
from zenml.config.retry_config import StepRetryConfig
from zenml.constants import DEFAULT_STACK_AND_COMPONENT_NAME
from zenml.enums import ArtifactType
from zenml.execution.pipeline.dynamic.outputs import OutputArtifact
from zenml.models import PipelineRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.pipelines.pipeline_decorator import pipeline
from zenml.pipelines.pipeline_definition import Pipeline

from kitaru._client._deployments import DEFAULT_DEPLOYMENT_TAG
from kitaru._client._mappers import _to_public_status
from kitaru._client._models import ExecutionStatus
from kitaru._config._active_context import (
    ActiveConfigSelectionProvenance,
    collect_active_context_provenance,
    stringify_config_id,
    with_resolved_selection,
)
from kitaru._interface_deployments import (
    Deployment,
    ensure_stack_is_server_runnable,
    resolve_deployment_selector,
    validate_deployment_selector,
)
from kitaru._source_aliases import (
    build_pipeline_registration_name,
    build_pipeline_source_alias,
    callable_name,
)
from kitaru._telemetry import (
    deployment_metadata_for_stack as _deployment_metadata_for_stack,
)
from kitaru.analytics import AnalyticsEvent, track
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ImageInput,
    ImageSettings,
    KitaruConfig,
    ModelRegistryConfig,
    ResolvedExecutionConfig,
    _read_env_model_registry,
    _read_model_registry_config,
    build_frozen_execution_spec,
    image_settings_to_docker_settings,
    persist_frozen_execution_spec,
    resolve_connection_config,
    resolve_execution_config,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruAmbiguousFlowResultError,
    KitaruBackendError,
    KitaruDeploymentInputValuesError,
    KitaruRuntimeError,
    KitaruStackIntegrationDependencyError,
    KitaruStateError,
    KitaruUsageError,
    classify_failure_origin,
    execution_error_from_failure,
    format_recovery_hint,
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


def _preflight_active_stack_implementation_hydration(
    *,
    client_factory: Callable[[], Any] | None = None,
) -> None:
    """Verify that the active stack can be loaded as implementation objects."""
    client = (client_factory or Client)()
    try:
        _ = client.active_stack
    except ImportError as exc:
        zenml_guidance = str(exc).strip()
        message = (
            "Cannot submit this Kitaru flow because the active stack could not "
            "be loaded in this Python environment.\n\n"
            "A stack integration dependency appears to be missing. Install the "
            "missing ZenML integration or stack requirements, then retry."
        )
        if zenml_guidance:
            message = f"{message}\n\nZenML guidance:\n\n{zenml_guidance}"
        raise KitaruStackIntegrationDependencyError(message) from None


def _capture_active_stack_provenance_for_guard() -> (
    ActiveConfigSelectionProvenance | None
):
    """Capture raw active stack provenance before Client sanitizes it."""
    try:
        stack_provenance, _ = collect_active_context_provenance(GlobalConfiguration())
    except Exception:
        logger.debug("Could not collect active stack provenance", exc_info=True)
        return None
    return stack_provenance


def _guard_implicit_active_stack_fallback(
    *,
    operation: str,
    resolved_execution: ResolvedExecutionConfig,
    raw_active_stack_provenance: ActiveConfigSelectionProvenance | None,
    client_factory: Callable[[], Any] | None = None,
) -> None:
    """Fail closed when an implicit stale active stack would run on default."""
    # Status/info diagnose any raw-vs-resolved active context mismatch. Runtime
    # blocking is deliberately narrower: only the high-risk case where Kitaru
    # would implicitly inherit a stale active stack that resolved to `default`.
    if resolved_execution.stack_source != "zenml_active_stack":
        return
    if resolved_execution.stack != DEFAULT_STACK_AND_COMPONENT_NAME:
        return
    if (
        raw_active_stack_provenance is None
        or raw_active_stack_provenance.effective_id is None
    ):
        return
    if raw_active_stack_provenance.effective_source not in {
        "repo-local config",
        "global config",
    }:
        return

    client = (client_factory or Client)()
    active_stack_model = client.active_stack_model
    resolved_name = str(getattr(active_stack_model, "name", "") or "")
    if resolved_name != DEFAULT_STACK_AND_COMPONENT_NAME:
        return

    resolved_provenance = with_resolved_selection(
        raw_active_stack_provenance,
        resolved_id=stringify_config_id(getattr(active_stack_model, "id", None)),
        resolved_name=resolved_name,
    )
    if (
        resolved_provenance is None
        or resolved_provenance.resolved_id is None
        or resolved_provenance.effective_id == resolved_provenance.resolved_id
    ):
        return

    source = resolved_provenance.effective_source
    if resolved_provenance.effective_source_detail:
        source = f"{source} ({resolved_provenance.effective_source_detail})"
    raise KitaruUsageError(
        "\n".join(
            [
                f"Kitaru refused to {operation} because the saved active "
                "stack appears stale and resolved to fallback stack "
                f"`{DEFAULT_STACK_AND_COMPONENT_NAME}` implicitly.",
                f"Configured active stack from {source}: "
                f"{resolved_provenance.effective_id}",
                f"Resolved active stack: {DEFAULT_STACK_AND_COMPONENT_NAME} "
                f"({resolved_provenance.resolved_id})",
                "Choose a stack explicitly before running this workflow. "
                "For example: pass `stack=...`, set `KITARU_STACK=...`, "
                "configure `[tool.kitaru].stack`, or run "
                "`kitaru stack use <stack>` once the saved active stack is "
                "correct again.",
            ]
        )
    )


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


_FLOW_RESULT_ARTIFACT_NAME = "kitaru_flow_result"
_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME = "kitaru_flow_result_tuple_metadata"
_FLOW_RESULT_TUPLE_METADATA_MARKER = "kitaru_flow_result_tuple_v1"
_FLOW_RESULT_COERCION_ENABLED: ContextVar[bool] = ContextVar(
    "kitaru_flow_result_coercion_enabled",
    default=True,
)


@contextmanager
def _suspend_flow_return_coercion() -> Iterator[None]:
    """Temporarily skip return-value artifact persistence.

    Deployment snapshot preparation may execute the dynamic pipeline function with
    representative inputs. That phase is only compiling a saved snapshot, not
    producing a real user-invoked execution, so it must not create durable flow
    result artifacts.
    """
    token = _FLOW_RESULT_COERCION_ENABLED.set(False)
    try:
        yield
    finally:
        _FLOW_RESULT_COERCION_ENABLED.reset(token)


def _is_zenml_pipeline_output_artifact(value: Any) -> bool:
    """Return whether ``value`` is already valid as a ZenML pipeline output."""
    return isinstance(value, ArtifactVersionResponse | OutputArtifact)


def _save_flow_result_artifact(value: Any, *, name: str) -> ArtifactVersionResponse:
    """Persist one plain flow result value as a ZenML artifact."""
    try:
        return save_artifact(
            data=value,
            name=name,
            artifact_type=ArtifactType.DATA,
            user_metadata={"kitaru_artifact_type": "output"},
        )
    except Exception as exc:
        raise KitaruRuntimeError(
            "Kitaru could not persist the flow return value as a ZenML "
            "artifact. The user flow returned successfully, but the backend "
            f"artifact save failed: {exc}"
        ) from exc


def _flow_result_tuple_metadata(length: int) -> dict[str, Any]:
    """Return hidden metadata that marks expanded pipeline outputs as a tuple."""
    return {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": length,
    }


def _is_flow_result_tuple_metadata(value: Any) -> bool:
    """Return whether a loaded output value is Kitaru tuple metadata."""
    return (
        isinstance(value, Mapping)
        and value.get("kitaru_artifact_type") == _FLOW_RESULT_TUPLE_METADATA_MARKER
        and value.get("version") == 1
        and isinstance(value.get("length"), int)
    )


def _coerce_flow_return_for_zenml(value: Any) -> Any:
    """Convert a user flow return value into a ZenML 0.94.4-compatible output.

    ZenML 0.94.4 validates dynamic pipeline return values and only accepts
    artifact references (or tuples of artifact references). Kitaru flows expose
    normal Python return values, so plain values need to be persisted manually
    before they are handed back to ZenML's pipeline finalizer.
    """
    if value is None:
        return None
    if _is_zenml_pipeline_output_artifact(value):
        return value
    if isinstance(value, tuple) and any(
        _is_zenml_pipeline_output_artifact(item) for item in value
    ):
        if type(value) is not tuple:
            raise KitaruUsageError(
                "Kitaru cannot preserve tuple subclass return values that "
                "contain checkpoint output handles. Return a plain tuple, or "
                "wrap the structured value in a final @checkpoint so ZenML "
                "materializes it as one artifact."
            )

        coerced_items = tuple(
            item
            if _is_zenml_pipeline_output_artifact(item)
            else _save_flow_result_artifact(
                item,
                name=f"{_FLOW_RESULT_ARTIFACT_NAME}_{index}",
            )
            for index, item in enumerate(value)
        )
        metadata = _save_flow_result_artifact(
            _flow_result_tuple_metadata(len(value)),
            name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
        )
        return (*coerced_items, metadata)

    return _save_flow_result_artifact(value, name=_FLOW_RESULT_ARTIFACT_NAME)


def _wrap_flow_entrypoint(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a flow entrypoint with Kitaru flow runtime scope."""

    flow_name = callable_name(func)

    @wraps(func)
    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        with _flow_scope(name=flow_name):
            result = func(*args, **kwargs)
            if not _FLOW_RESULT_COERCION_ENABLED.get():
                return result
            return _coerce_flow_return_for_zenml(result)

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


def _build_pipeline_options(
    *,
    resolved_execution: ResolvedExecutionConfig,
    transport_image: ImageSettings | None,
) -> dict[str, Any]:
    """Build kwargs for ``Pipeline.with_options(...)``.

    ``enable_cache`` is only forwarded when the user explicitly configured
    cache at some execution layer. Passing a concrete bool here makes ZenML's
    compiler overwrite every step's ``enable_cache``, which would clobber any
    ``@checkpoint(cache=...)`` overrides.

    ``secrets`` is forwarded only when non-empty so ZenML does not overwrite
    its own defaults with an empty list. Secret references intentionally
    bypass Docker settings so values never enter image/build metadata.
    """
    options: dict[str, Any] = {
        "retry": _to_retry_config(_normalize_retries(resolved_execution.retries)),
        "settings": _build_settings(transport_image),
    }
    if resolved_execution.cache is not None:
        options["enable_cache"] = resolved_execution.cache
    if transport_image is not None and transport_image.secret_environment_from:
        options["secrets"] = list(transport_image.secret_environment_from)
    return options


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


def _flow_input_schema(
    func: Callable[..., Any],
    *,
    default_values: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Derive a best-effort JSON schema for SDK deployment inputs."""
    try:
        signature = inspect.signature(func)
        fields: dict[str, tuple[Any, Any]] = {}
        for name, parameter in signature.parameters.items():
            if parameter.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                return None
            annotation = (
                Any
                if parameter.annotation is inspect.Parameter.empty
                else parameter.annotation
            )
            if default_values is not None and name in default_values:
                default = default_values[name]
            else:
                default = (
                    ...
                    if parameter.default is inspect.Parameter.empty
                    else parameter.default
                )
            fields[name] = (annotation, default)

        input_model = create_model(
            f"{build_pipeline_registration_name(callable_name(func))}DeploymentInput",
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **cast(Any, fields),
        )
        return input_model.model_json_schema()
    except Exception:
        logger.debug(
            "Failed to derive deployment input schema for flow %s.",
            callable_name(func),
            exc_info=True,
        )
        return None


def _deployment_extra_metadata(
    *,
    func: Callable[..., Any],
    stack: str | None,
    default_values: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build best-effort metadata stored on Kitaru deployment snapshots."""
    metadata: dict[str, Any] = {
        "schema": _flow_input_schema(func, default_values=default_values)
    }
    if stack:
        metadata["stack"] = stack
    return {key: value for key, value in metadata.items() if value is not None}


def _extract_run_pipeline_id(run: PipelineRunResponse) -> str | None:
    """Extract the Kitaru flow/pipeline ID from structured run data."""
    candidate_paths = (
        ("pipeline_id",),
        ("pipeline", "id"),
        ("snapshot", "pipeline_id"),
        ("snapshot", "pipeline", "id"),
        ("resources", "pipeline", "id"),
    )
    for path in candidate_paths:
        try:
            value: Any = run
            for attr in path:
                value = getattr(value, attr, None)
                if value is None:
                    break
            if value is None:
                continue
            pipeline_id = str(value).strip()
            if pipeline_id:
                return pipeline_id
        except Exception:
            logger.debug(
                "Failed to read pipeline ID from run %s via %s.",
                getattr(run, "id", "<unknown>"),
                ".".join(path),
                exc_info=True,
            )
    return None


def _build_kitaru_execution_url(
    run: PipelineRunResponse,
    *,
    server_url: str | None,
) -> str | None:
    """Build the Kitaru-native execution detail URL for a run if possible."""
    if server_url is None or not str(server_url).strip():
        return None

    execution_id_value = getattr(run, "id", None)
    if execution_id_value is None:
        return None
    execution_id = str(execution_id_value).strip()
    if not execution_id:
        return None

    flow_id = _extract_run_pipeline_id(run)
    if flow_id is None:
        return None

    base_url = str(server_url).strip().rstrip("/")
    flow_segment = quote(flow_id, safe="")
    execution_segment = quote(execution_id, safe="")
    return f"{base_url}/flows/{flow_segment}/executions/{execution_segment}"


def _emit_kitaru_execution_url(
    run: PipelineRunResponse,
    *,
    server_url: str | None,
) -> None:
    """Log a Kitaru-native execution URL without risking flow execution."""
    try:
        url = _build_kitaru_execution_url(run, server_url=server_url)
    except Exception:
        logger.debug(
            "Failed to build Kitaru execution URL for run %s.",
            getattr(run, "id", "<unknown>"),
            exc_info=True,
        )
        return

    if url is None:
        logger.debug(
            "Skipping Kitaru execution URL for run %s because structured URL "
            "data is incomplete.",
            getattr(run, "id", "<unknown>"),
        )
        return

    logger.info("Execution URL: %s", url)


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


class _MultipleTerminalStepsOutputError(KitaruAmbiguousFlowResultError):
    """Raised when fallback result extraction sees several terminal steps.

    Subclasses :class:`KitaruAmbiguousFlowResultError` so external callers can
    catch the public ambiguity error, while internal adapters (e.g. the
    Pydantic AI auto-flow path) can detect this specific subtype to recover
    via in-memory results instead of re-raising.
    """


def _is_multiple_terminal_steps_output_error(error: BaseException) -> bool:
    """Return whether ``error`` came from ambiguous terminal-step extraction."""
    return isinstance(error, _MultipleTerminalStepsOutputError)


def _ambiguous_terminal_message(execution_id: str, *, reason: str) -> str:
    """Build a discoverable message when terminal-step extraction can't pick.

    Common in agent-style flows where each model/tool call produces its own
    checkpoint with no DAG sink — there is no single artifact that represents
    "the" flow result, but the per-checkpoint artifacts are still visible in
    the Kitaru UI and retrievable via ``KitaruClient``.
    """
    lines = [
        f"This flow's return value cannot be extracted automatically because {reason}.",
        "",
        "This typically happens when a flow's checkpoints fan out into "
        "parallel branches without a single sink — for example, when an "
        "agent adapter creates one checkpoint per model or tool call. The "
        "per-checkpoint artifacts ARE persisted and visible:",
        f"  - View artifacts in the Kitaru UI for execution {execution_id}",
        f"  - Retrieve via the client: "
        f"`KitaruClient().executions.get('{execution_id}')` and inspect "
        "checkpoint outputs",
        "",
        "To get a clean `.wait()` return value, give the flow a single "
        "sink: wrap the agent call in an explicit `@checkpoint`, or add a "
        "final checkpoint that consumes the upstream result(s) and returns "
        "the value you want.",
    ]
    return "\n".join(lines)


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
    execution_id = str(hydrated_run.id)
    if len(terminal_step_names) > 1:
        raise _MultipleTerminalStepsOutputError(
            _ambiguous_terminal_message(
                execution_id,
                reason=(
                    f"multiple terminal checkpoints were found "
                    f"({len(terminal_step_names)}): "
                    f"{', '.join(terminal_step_names)}"
                ),
            )
        )

    terminal_step_name = terminal_step_names[0]
    terminal_step = step_runs[terminal_step_name]
    if not terminal_step.regular_outputs:
        raise KitaruRuntimeError(
            f"Execution {execution_id} has no regular outputs on terminal "
            f"step '{terminal_step_name}'."
        )
    if len(terminal_step.regular_outputs) > 1:
        output_names = ", ".join(sorted(terminal_step.regular_outputs))
        raise KitaruAmbiguousFlowResultError(
            _ambiguous_terminal_message(
                execution_id,
                reason=(
                    f"terminal checkpoint '{terminal_step_name}' has "
                    f"{len(terminal_step.regular_outputs)} outputs: "
                    f"{output_names}"
                ),
            )
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

    maybe_tuple_metadata = values[-1]
    if len(values) > 1 and _is_flow_result_tuple_metadata(maybe_tuple_metadata):
        tuple_values = values[:-1]
        expected_length = maybe_tuple_metadata["length"]
        if len(tuple_values) != expected_length:
            raise KitaruRuntimeError(
                "Execution flow result tuple metadata did not match the "
                "loaded output count."
            )
        return tuple(tuple_values)

    if len(values) == 1:
        return values[0]
    return tuple(values)


def _extract_run_failure_context(
    run: PipelineRunResponse,
) -> tuple[str | None, str | None]:
    """Return (status_reason, traceback_text) from a run response."""
    run_body = run.get_body() if hasattr(run, "get_body") else run
    status_reason = getattr(run_body, "status_reason", None)
    traceback_text: str | None = None
    if run.exception_info is not None:
        traceback_text = run.exception_info.traceback
    return status_reason, traceback_text


def _classify_run_failure(run: PipelineRunResponse) -> FailureOrigin:
    """Classify the failure origin for an unsuccessful run without raising."""
    status_reason, traceback_text = _extract_run_failure_context(run)
    default_origin = (
        FailureOrigin.USER_CODE if traceback_text is not None else FailureOrigin.UNKNOWN
    )
    return classify_failure_origin(
        status_reason=status_reason,
        traceback=traceback_text,
        default=default_origin,
    )


def _safe_classify_run_failure(run: PipelineRunResponse) -> FailureOrigin:
    """Classify failure origin for analytics without crashing.

    Falls back to UNKNOWN if classification itself raises, so analytics
    never masks the user's real execution error.
    """
    try:
        return _classify_run_failure(run)
    except Exception:
        logger.debug(
            "Failed to classify failure origin for run %s; defaulting to UNKNOWN",
            run.id,
            exc_info=True,
        )
        return FailureOrigin.UNKNOWN


def _duration_metadata_from_run(
    run: PipelineRunResponse,
    *,
    observed_started_at: float | None,
) -> dict[str, Any]:
    """Return best-effort terminal duration metadata for a run."""
    start_time = getattr(run, "start_time", None)
    end_time = getattr(run, "end_time", None)
    if isinstance(start_time, datetime) and isinstance(end_time, datetime):
        duration_seconds = max((end_time - start_time).total_seconds(), 0.0)
        return {
            "duration_seconds": round(duration_seconds, 3),
            "duration_source": "backend_timestamps",
        }

    if observed_started_at is None:
        return {}

    duration_seconds = max(time.perf_counter() - observed_started_at, 0.0)
    return {
        "duration_seconds": round(duration_seconds, 3),
        "duration_source": "sdk_observed",
    }


def _checkpoint_count_from_run(run: PipelineRunResponse) -> int | None:
    """Return a best-effort count of hydrated run steps as checkpoint proxy."""
    try:
        hydrated_run = run.get_hydrated_version()
        steps = getattr(hydrated_run, "steps", None)
        if isinstance(steps, Mapping):
            return len(steps)
    except Exception:
        logger.debug(
            "Failed to derive checkpoint count for terminal analytics.",
            exc_info=True,
        )
    return None


def _raise_for_unsuccessful_run(
    run: PipelineRunResponse,
    *,
    failure_origin: FailureOrigin | None = None,
) -> None:
    """Raise a typed Kitaru execution error with run failure context."""
    details = [f"Execution {run.id} finished with status '{run.status.value}'."]

    status_reason, traceback_text = _extract_run_failure_context(run)
    if status_reason:
        details.append(status_reason)

    traceback_tail = traceback_last_line(traceback_text)
    if traceback_tail:
        details.append(traceback_tail)

    message = " ".join(details)

    hint = format_recovery_hint(str(run.id), status=run.status.value)
    if hint:
        message = f"{message}\n\n{hint}"

    if failure_origin is None:
        failure_origin = _safe_classify_run_failure(run)
    raise execution_error_from_failure(
        message,
        exec_id=str(run.id),
        status=_to_public_status(run.status),
        origin=failure_origin,
    )


class FlowHandle:
    """Handle for a running or finished flow execution."""

    def __init__(
        self,
        run: PipelineRunResponse,
        *,
        observed_started_at: float | None = None,
        analytics_metadata: dict[str, Any] | None = None,
        track_terminal_if_finished: bool = False,
    ) -> None:
        """Initialize a flow handle.

        Args:
            run: Initial pipeline run response.
            observed_started_at: SDK-observed start time from ``time.perf_counter``.
            analytics_metadata: Privacy-safe metadata captured at submission time.
            track_terminal_if_finished: Emit terminal analytics immediately when
                the initial run is already terminal.
        """
        self._run = run
        self._run_id = run.id
        self._terminal_event_emitted = False
        self._observed_started_at = (
            observed_started_at
            if observed_started_at is not None
            else time.perf_counter()
        )
        self._analytics_metadata = dict(analytics_metadata or {})

        if track_terminal_if_finished and run.status.is_finished:
            if not run.status.is_successful:
                origin = _safe_classify_run_failure(run)
                self._track_terminal_once(run, failure_origin=origin)
            else:
                self._track_terminal_once(run)

    @property
    def exec_id(self) -> str:
        """Execution identifier for this flow run."""
        return str(self._run_id)

    @property
    def status(self) -> ExecutionStatus:
        """Current execution status."""
        return _to_public_status(self._refresh().status)

    def _track_terminal_once(
        self,
        run: PipelineRunResponse,
        *,
        failure_origin: FailureOrigin | None = None,
    ) -> None:
        """Emit FLOW_TERMINAL at most once per handle instance."""
        if self._terminal_event_emitted:
            return
        self._terminal_event_emitted = True
        metadata: dict[str, Any] = dict(self._analytics_metadata)
        metadata["status"] = run.status.value
        if failure_origin is not None:
            metadata["failure_origin"] = failure_origin.value

        try:
            metadata.update(
                _duration_metadata_from_run(
                    run,
                    observed_started_at=self._observed_started_at,
                )
            )
        except Exception:
            logger.debug(
                "Failed to derive duration metadata for terminal analytics.",
                exc_info=True,
            )

        checkpoint_count = _checkpoint_count_from_run(run)
        if checkpoint_count is not None:
            metadata["checkpoint_count"] = checkpoint_count
            metadata["checkpoint_count_source"] = "hydrated_run_steps"

        track(AnalyticsEvent.FLOW_TERMINAL, metadata)

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
                    origin = _safe_classify_run_failure(run)
                    self._track_terminal_once(run, failure_origin=origin)
                    _raise_for_unsuccessful_run(run, failure_origin=origin)
                self._track_terminal_once(run)
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
            origin = _safe_classify_run_failure(run)
            self._track_terminal_once(run, failure_origin=origin)
            _raise_for_unsuccessful_run(run, failure_origin=origin)
        self._track_terminal_once(run)
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

    def _deployments_api_and_flow_name(self) -> tuple[Any, str]:
        """Return the client deployments API and this flow's registration name."""
        from kitaru.client import KitaruClient

        return (
            KitaruClient().deployments,
            build_pipeline_registration_name(callable_name(self._func)),
        )

    def deploy(
        self,
        *args: Any,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        tags: dict[str, bool] | None = None,
        publish_default_on_first_deploy: bool = True,
        **kwargs: Any,
    ) -> Deployment:
        """Create a versioned deployment snapshot for this flow.

        Positional and keyword flow inputs are accepted as deployment-time
        defaults when ZenML needs concrete parameters to compile a dynamic
        snapshot. Later invocations can override those values by passing flow
        inputs to ``invoke(...)``.
        """
        raw_active_stack_provenance = _capture_active_stack_provenance_for_guard()
        resolved_execution = resolve_execution_config(
            decorator_overrides=self._decorator_config,
            invocation_overrides=_build_execution_overrides(
                stack=stack,
                image=image,
                cache=cache,
                retries=retries,
            ),
        )
        _guard_implicit_active_stack_fallback(
            operation="deploy this flow",
            resolved_execution=resolved_execution,
            raw_active_stack_provenance=raw_active_stack_provenance,
        )
        transport_image, _ = _prepare_model_registry_transport(resolved_execution.image)
        configured_pipeline = self._pipeline.with_options(
            **_build_pipeline_options(
                resolved_execution=resolved_execution,
                transport_image=transport_image,
            )
        )
        deployments_api, flow_name = self._deployments_api_and_flow_name()
        source_name = f"kitaru-source::{flow_name}::{uuid4().hex}"

        with _temporary_active_stack(resolved_execution.stack):
            stack_client = Client()
            ensure_stack_is_server_runnable(
                zen_store=stack_client.zen_store,
                stack=stack_client.active_stack_model,
                operation="deploy",
                flow=flow_name,
            )
            _preflight_active_stack_implementation_hydration()
            try:
                with _suspend_flow_return_coercion():
                    configured_pipeline.prepare(*args, **kwargs)
            except (RuntimeError, ValueError) as exc:
                raise KitaruDeploymentInputValuesError(
                    "Unable to create this deployment because Kitaru needs concrete "
                    "input values to prepare the saved deployment snapshot. Pass "
                    "representative input values when calling flow.deploy(...), then "
                    "override them later when invoking it."
                ) from exc

            metadata = _deployment_extra_metadata(
                func=self._func,
                stack=resolved_execution.stack,
                default_values=getattr(configured_pipeline, "_parameters", None),
            )
            try:
                source_snapshot = configured_pipeline._create_snapshot(
                    skip_schedule_registration=True,
                    name=source_name,
                    replace=False,
                    extra={"kitaru_deployment": metadata},
                    **configured_pipeline._run_args,
                )
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to create deployment source snapshot for flow "
                    f"{flow_name!r}: {exc}"
                ) from exc

        create_kwargs: dict[str, Any] = {
            "flow": flow_name,
            "source_snapshot": source_snapshot,
            "tags": tags,
        }
        if not publish_default_on_first_deploy:
            create_kwargs["publish_default_on_first_deploy"] = False

        return deployments_api.create(**create_kwargs)

    def deployments(self) -> list[Deployment]:
        """List deployment versions for this flow."""
        deployments_api, flow_name = self._deployments_api_and_flow_name()
        return deployments_api.list(flow=flow_name)

    def deployment(
        self,
        *,
        version: int | None = None,
        tag: str | None = None,
    ) -> Deployment:
        """Get one deployment version by version or tag."""
        version, tag = validate_deployment_selector(
            version=version, tag=tag, default_tag=DEFAULT_DEPLOYMENT_TAG
        )
        deployments_api, flow_name = self._deployments_api_and_flow_name()
        return deployments_api.get(flow=flow_name, version=version, tag=tag)

    def invoke(
        self,
        *,
        version: int | None = None,
        tag: str | None = None,
        **flow_inputs: Any,
    ) -> FlowHandle:
        """Invoke a deployed flow snapshot and return an execution handle."""
        selector = resolve_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        deployments_api, flow_name = self._deployments_api_and_flow_name()
        return deployments_api.invoke(
            flow=flow_name,
            version=selector.version,
            tag=selector.tag,
            selector_source=selector.source,
            inputs=flow_inputs,
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
        raw_active_stack_provenance = _capture_active_stack_provenance_for_guard()
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
        _guard_implicit_active_stack_fallback(
            operation="replay this flow",
            resolved_execution=resolved_execution,
            raw_active_stack_provenance=raw_active_stack_provenance,
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
        configured_pipeline = self._pipeline.with_options(
            **_build_pipeline_options(
                resolved_execution=resolved_execution,
                transport_image=transport_image,
            )
        )

        with _temporary_active_stack(resolved_execution.stack):
            _preflight_active_stack_implementation_hydration()
            deployment_metadata = _deployment_metadata_for_stack(
                resolved_execution.stack
            )
            replay_metadata = {
                "from_checkpoint": from_,
                "replay_path": "flow_wrapper",
                **deployment_metadata,
            }
            track(AnalyticsEvent.REPLAY_REQUESTED, replay_metadata)

            observed_started_at = time.perf_counter()
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
                track(
                    AnalyticsEvent.REPLAY_FAILED,
                    {
                        **replay_metadata,
                        "error_type": type(exc).__name__,
                        "failure_origin": failure_origin.value,
                    },
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
            track(
                AnalyticsEvent.REPLAY_FAILED,
                {
                    **replay_metadata,
                    "error_type": "KitaruRuntimeError",
                    "failure_origin": FailureOrigin.RUNTIME.value,
                },
            )
            raise KitaruRuntimeError("Replay did not produce a pipeline run.")

        _emit_kitaru_execution_url(
            replayed_run,
            server_url=getattr(resolved_connection, "server_url", None),
        )
        persist_frozen_execution_spec(
            run_id=replayed_run.id,
            frozen_execution_spec=frozen_execution_spec,
        )

        track(
            AnalyticsEvent.FLOW_REPLAYED,
            {"replay_path": "flow_wrapper", **deployment_metadata},
        )
        return FlowHandle(
            replayed_run,
            observed_started_at=observed_started_at,
            analytics_metadata=deployment_metadata,
            track_terminal_if_finished=True,
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
        raw_active_stack_provenance = _capture_active_stack_provenance_for_guard()
        resolved_execution = resolve_execution_config(
            decorator_overrides=self._decorator_config,
            invocation_overrides=invocation_overrides,
        )
        _guard_implicit_active_stack_fallback(
            operation="run this flow",
            resolved_execution=resolved_execution,
            raw_active_stack_provenance=raw_active_stack_provenance,
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
        configured_pipeline = self._pipeline.with_options(
            **_build_pipeline_options(
                resolved_execution=resolved_execution,
                transport_image=transport_image,
            )
        )

        with _temporary_active_stack(resolved_execution.stack):
            _preflight_active_stack_implementation_hydration()
            deployment_metadata = _deployment_metadata_for_stack(
                resolved_execution.stack
            )
            observed_started_at = time.perf_counter()
            run = configured_pipeline(*args, **kwargs)

        if run is None:
            raise KitaruRuntimeError("Flow execution did not produce a pipeline run.")

        _emit_kitaru_execution_url(
            run,
            server_url=getattr(resolved_connection, "server_url", None),
        )
        track(AnalyticsEvent.FLOW_SUBMITTED, deployment_metadata)
        persist_frozen_execution_spec(
            run_id=run.id,
            frozen_execution_spec=frozen_execution_spec,
        )
        return FlowHandle(
            run,
            observed_started_at=observed_started_at,
            analytics_metadata=deployment_metadata,
            track_terminal_if_finished=True,
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

    Can be used as a bare decorator or with arguments:

    ```python
    @flow
    def my_flow(...):
        ...

    @flow(stack="prod", retries=2)
    def my_other_flow(...):
        ...
    ```

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
