"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the top-level
function whose execution becomes durable, replayable, and observable.
"""

from __future__ import annotations

import inspect
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from functools import update_wrapper, wraps
from typing import Any, Literal, cast, overload
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

from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    parse_deployment_snapshot_name,
)
from kitaru._client._mappers import _list_pending_wait_conditions, _to_public_status
from kitaru._client._models import ExecutionStatus
from kitaru._config._active_context import (
    ActiveConfigSelectionProvenance,
    collect_active_context_provenance,
    stringify_config_id,
    with_resolved_selection,
)
from kitaru._env import ZENML_ACTIVE_PROJECT_ID_ENV, _temporary_env
from kitaru._interface_deployments import (
    Deployment,
    ensure_stack_is_server_runnable,
    resolve_deployment_selector,
    validate_deployment_selector,
)
from kitaru._run_identity import extract_run_project_identity
from kitaru._source_aliases import (
    build_pipeline_registration_name,
    build_pipeline_source_alias,
    callable_name,
)
from kitaru._telemetry import (
    deployment_metadata_for_stack as _deployment_metadata_for_stack,
)
from kitaru._terminal_hooks import aggregate_llm_usage_on_run_end
from kitaru._terminal_usage import (
    _persist_terminal_llm_usage_metadata as _shared_persist_terminal_llm_usage_metadata,
)
from kitaru._terminal_usage import _safe_persist_terminal_llm_usage_metadata
from kitaru._ui_urls import (
    UiUrlContext,
    build_execution_url_from_context,
    resolve_ui_url_context,
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
from kitaru.replay import (
    ReplayFailureRow,
    ReplayPlan,
    ReplayResultRow,
    ReplaySkippedRow,
    ReplaySubmission,
    build_replay_plan,
    build_replay_request_document,
    new_replay_submission_id,
    replay_at_skip_reason,
    replay_at_status,
    safe_compare_url_for_executions,
    safe_persist_replay_submission_metadata,
)
from kitaru.replay_context import (
    KITARU_REPLAY_CONTEXT_ENV,
    get_replay_runtime_context,
)
from kitaru.runtime import _flow_scope, _get_current_execution_id

ImageSetting = ImageInput
_ACTIVE_ZENML_STATE_LOCK = threading.RLock()
_REPLAY_RUNTIME_CONTEXT_LOCK = threading.RLock()
logger = logging.getLogger(__name__)


def _connection_project(resolved_connection: Any) -> str | None:
    """Return the project selected by a resolved connection object."""
    return cast(str | None, getattr(resolved_connection, "project", None))


@contextmanager
def _temporary_active_project(project_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a project for one ZenML write operation.

    ZenML writes runs and snapshots into its active project. Kitaru resolves a
    project independently, so this helper briefly makes ZenML's active project
    match Kitaru's resolved project and then restores the previous state.

    Args:
        project_name_or_id: Optional project name or ID. When ``None`` or blank,
            the currently active ZenML project is used unchanged.
    """
    with _ACTIVE_ZENML_STATE_LOCK:
        if not project_name_or_id or not str(project_name_or_id).strip():
            yield
            return

        client = Client()
        previous_project_id: str | None = None

        try:
            previous_project = client.active_project
            previous_project_id = str(previous_project.id)
        except Exception:
            logger.debug("Could not capture previous active project", exc_info=True)

        try:
            target_project = client.get_project(
                str(project_name_or_id).strip(),
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to activate project {project_name_or_id!r}: {exc}"
            ) from exc

        target_project_id = str(target_project.id)
        with _temporary_env({ZENML_ACTIVE_PROJECT_ID_ENV: target_project_id}):
            try:
                # ZenML persists this active-project change; the restore below is
                # best-effort if the process exits before the context manager unwinds.
                client.set_active_project(target_project_id)
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to activate project {project_name_or_id!r}: {exc}"
                ) from exc

            body_error: BaseException | None = None
            try:
                yield
            except BaseException as exc:
                body_error = exc
                raise
            finally:
                restore_error: BaseException | None = None
                try:
                    if previous_project_id is not None:
                        client.set_active_project(previous_project_id)
                except BaseException as exc:
                    restore_error = exc
                    logger.warning(
                        "Failed to restore previous active project after Kitaru write.",
                        exc_info=True,
                    )

                if restore_error is not None and body_error is None:
                    raise KitaruBackendError(
                        "Failed to restore the previous active project after "
                        f"the Kitaru write: {restore_error}"
                    ) from restore_error


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack for one flow invocation.

    Args:
        stack_name_or_id: Optional stack name or ID. When ``None``, the
            currently active ZenML stack is used unchanged.
    """
    with _ACTIVE_ZENML_STATE_LOCK:
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
#: Execution-metadata key linking a saved plain-value flow result back to its run.
#: ZenML dynamic pipelines can leave Kitaru's explicit ``kitaru_flow_result``
#: artifact unavailable through run outputs or pipeline output specs. The link
#: lets `.wait()` recover that explicit return value before Kitaru guesses from
#: terminal checkpoint outputs.
_FLOW_RESULT_REF_METADATA_KEY = "kitaru_flow_result_ref_v1"
_FLOW_RESULT_NONE_METADATA_KEY = "kitaru_flow_result_none_v1"
_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME = "kitaru_flow_result_tuple_metadata"
_FLOW_RESULT_TUPLE_METADATA_MARKER = "kitaru_flow_result_tuple_v1"
_FLOW_RESULT_ROLE_METADATA_KEY = "kitaru_flow_result_role"
_FLOW_RESULT_TUPLE_METADATA_ROLE = "tuple_metadata"
_KITARU_EXTRA_NAMESPACE = "kitaru"
_FLOW_RESULT_CANDIDATE_KEY = "flow_result_candidate"
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


def _save_flow_result_artifact(
    value: Any,
    *,
    name: str,
    user_metadata: Mapping[str, Any] | None = None,
) -> ArtifactVersionResponse:
    """Persist one plain flow result value as a ZenML artifact."""
    metadata: dict[str, Any] = {"kitaru_artifact_type": "output"}
    if user_metadata is not None:
        metadata.update(user_metadata)

    try:
        return save_artifact(
            data=value,
            name=name,
            artifact_type=ArtifactType.DATA,
            user_metadata=metadata,
        )
    except Exception as exc:
        raise KitaruRuntimeError(
            "Kitaru could not persist the flow return value as a ZenML "
            "artifact. The user flow returned successfully, but the backend "
            "artifact save failed after user code returned. If ZenML retries "
            "this flow body, non-idempotent side effects in the flow may run "
            f"again: {exc}"
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
    if not isinstance(value, Mapping):
        return False

    length = value.get("length")
    return (
        value.get("kitaru_artifact_type") == _FLOW_RESULT_TUPLE_METADATA_MARKER
        and value.get("version") == 1
        and isinstance(length, int)
        and not isinstance(length, bool)
        and length > 0
    )


def _coerce_flow_return_for_zenml(value: Any) -> Any:
    """Convert a user flow return value into a ZenML 0.94.4-compatible output.

    ZenML 0.94.4 validates dynamic pipeline return values and only accepts
    artifact references (or tuples of artifact references). Kitaru flows expose
    normal Python return values, so plain values need to be persisted manually
    before they are handed back to ZenML's pipeline finalizer.
    """
    if value is None:
        _record_flow_result_none()
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
            user_metadata={
                _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE,
            },
        )
        return (*coerced_items, metadata)

    saved = _save_flow_result_artifact(value, name=_FLOW_RESULT_ARTIFACT_NAME)
    _record_flow_result_reference(saved)
    return saved


def _record_flow_result_reference(artifact: ArtifactVersionResponse) -> None:
    """Link a saved plain-value flow result to the running execution.

    Best-effort: the value was already saved and returned to ZenML, so a failed
    metadata write must not break the flow. Recording the artifact id in
    execution metadata is what lets `.wait()` recover the value when terminal-step
    inference is ambiguous.
    """
    try:
        from kitaru.logging import log as _log_flow_metadata

        _log_flow_metadata(**{_FLOW_RESULT_REF_METADATA_KEY: str(artifact.id)})
    except Exception:
        logger.debug("Could not record flow result reference metadata.", exc_info=True)


def _record_flow_result_none() -> None:
    """Record that the running execution explicitly returned ``None``."""
    if _get_current_execution_id() is None:
        return

    try:
        from kitaru.logging import log as _log_flow_metadata

        _log_flow_metadata(**{_FLOW_RESULT_NONE_METADATA_KEY: True})
    except Exception as exc:
        raise KitaruRuntimeError(
            "Kitaru could not record that the flow returned None. The user "
            "flow returned successfully, but Kitaru could not persist a "
            "metadata marker for that explicit None result. Without that "
            "marker, later result extraction could incorrectly infer a "
            f"terminal checkpoint output instead: {exc}"
        ) from exc


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


def _inject_replay_context_env(
    image: ImageSettings | None,
    *,
    replay_context_json: str,
) -> ImageSettings:
    """Attach replay runtime context to the transport image environment."""
    existing_environment = (
        dict(image.environment) if image and image.environment else {}
    )
    existing_environment[KITARU_REPLAY_CONTEXT_ENV] = replay_context_json
    if image is None:
        return ImageSettings(environment=existing_environment)
    return image.model_copy(update={"environment": existing_environment})


@contextmanager
def _scoped_replay_runtime_context(
    replay_context_json: str,
) -> Iterator[None]:
    """Expose replay runtime context to local replay code for one replay call."""
    with _REPLAY_RUNTIME_CONTEXT_LOCK:
        previous_value = os.environ.get(KITARU_REPLAY_CONTEXT_ENV)
        had_previous_value = KITARU_REPLAY_CONTEXT_ENV in os.environ
        os.environ[KITARU_REPLAY_CONTEXT_ENV] = replay_context_json
        get_replay_runtime_context.cache_clear()
        try:
            yield
        finally:
            if had_previous_value and previous_value is not None:
                os.environ[KITARU_REPLAY_CONTEXT_ENV] = previous_value
            else:
                os.environ.pop(KITARU_REPLAY_CONTEXT_ENV, None)
            get_replay_runtime_context.cache_clear()


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


def _resolve_execution_flow_version(run: PipelineRunResponse) -> str:
    """Resolve the Kitaru deployment version for an execution URL."""
    snapshot_name_getters = (
        lambda: getattr(getattr(run, "source_snapshot", None), "name", None),
        lambda: getattr(
            getattr(getattr(run, "resources", None), "source_snapshot", None),
            "name",
            None,
        ),
    )
    for getter in snapshot_name_getters:
        try:
            parsed = parse_deployment_snapshot_name(getter())
        except Exception:
            logger.debug(
                "Failed to read deployment snapshot name from run %s.",
                getattr(run, "id", "<unknown>"),
                exc_info=True,
            )
            continue
        if parsed is not None:
            return str(parsed.version)

    try:
        metadata = getattr(run, "run_metadata", None)
    except Exception:
        logger.debug(
            "Failed to read deployment metadata from run %s.",
            getattr(run, "id", "<unknown>"),
            exc_info=True,
        )
        metadata = None
    metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
    nested = metadata_mapping.get("kitaru_deployment")
    nested_mapping = nested if isinstance(nested, Mapping) else {}
    for key in ("deployment_version", "kitaru_deployment_version"):
        value = metadata_mapping.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    nested_version = nested_mapping.get("version")
    if nested_version is not None and str(nested_version).strip():
        return str(nested_version).strip()

    return "local"


def _build_kitaru_execution_url(
    run: PipelineRunResponse,
    *,
    ui_context: UiUrlContext | None = None,
    server_url: str | None = None,
) -> str | None:
    """Build the Kitaru-native execution detail URL for a run if possible."""
    if ui_context is None:
        if server_url is None or not str(server_url).strip():
            return None
        ui_context = UiUrlContext(
            base_url=str(server_url).strip().rstrip("/"),
            route_kind="legacy",
            source="connection_config",
        )

    execution_id_value = getattr(run, "id", None)
    if execution_id_value is None:
        return None
    execution_id = str(execution_id_value).strip()
    if not execution_id:
        return None

    flow_id = _extract_run_pipeline_id(run)
    if flow_id is None:
        return None

    return build_execution_url_from_context(
        ui_context,
        flow_id=flow_id,
        execution_id=execution_id,
        project_name_or_id=extract_run_project_identity(
            run,
            logger=logger,
            allow_lazy_project_lookup=True,
        ).name_or_id,
        version=_resolve_execution_flow_version(run),
    )


def _emit_kitaru_execution_url(
    run: PipelineRunResponse,
) -> None:
    """Log a Kitaru-native execution URL without risking flow execution."""
    try:
        url = _build_kitaru_execution_url(
            run,
            ui_context=resolve_ui_url_context(),
        )
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


@dataclass(frozen=True)
class _FlowResultOutput:
    """Loaded flow-output value plus the artifact identity it came from."""

    provenance: Literal["run_output", "step_output"]
    step_name: str | None
    output_name: str
    artifact: Any
    value: Any


def _loaded_flow_result_output(
    *,
    provenance: Literal["run_output", "step_output"],
    step_name: str | None,
    output_name: str,
    artifact: Any,
) -> _FlowResultOutput:
    """Load one artifact and wrap it with result-extraction metadata."""
    return _FlowResultOutput(
        provenance=provenance,
        step_name=step_name,
        output_name=output_name,
        artifact=artifact,
        value=artifact.load(),
    )


def _extract_outputs_from_output_specs(
    run: PipelineRunResponse,
) -> list[_FlowResultOutput]:
    """Extract return outputs using explicit pipeline output specs."""
    hydrated_run = run.get_hydrated_version()

    snapshot = hydrated_run.snapshot
    pipeline_spec = snapshot.pipeline_spec if snapshot else None
    output_specs = pipeline_spec.outputs if pipeline_spec else []
    if not output_specs:
        return []

    step_runs = hydrated_run.steps
    outputs: list[_FlowResultOutput] = []
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

        outputs.append(
            _loaded_flow_result_output(
                provenance="step_output",
                step_name=output_spec.step_name,
                output_name=output_spec.output_name,
                artifact=artifact,
            )
        )

    return outputs


def _extract_outputs_from_run_outputs(
    run: PipelineRunResponse,
) -> list[_FlowResultOutput]:
    """Extract return outputs from persisted run-level output artifacts."""
    run_outputs = getattr(run, "outputs", None)
    if not run_outputs:
        return []
    if not isinstance(run_outputs, Mapping):
        return []

    outputs: list[_FlowResultOutput] = []
    # ZenML hydrates PipelineRunResponse.outputs in output_index order, so the
    # mapping iteration order is the persisted flow return order.
    for output_name, artifact in run_outputs.items():
        if artifact is None:
            raise KitaruRuntimeError(
                f"Execution {run.id} is missing run output artifact '{output_name}'."
            )
        outputs.append(
            _loaded_flow_result_output(
                provenance="run_output",
                step_name=None,
                output_name=str(output_name),
                artifact=artifact,
            )
        )
    return outputs


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


def _iter_step_kitaru_extras(step_run: Any) -> Iterator[Mapping[str, Any]]:
    """Yield Kitaru step metadata from a hydrated step run, if present."""
    for owner_name in ("config", "spec"):
        owner = getattr(step_run, owner_name, None)
        raw_extra = getattr(owner, "extra", None)
        if not isinstance(raw_extra, Mapping):
            continue
        raw_kitaru = raw_extra.get(_KITARU_EXTRA_NAMESPACE)
        if isinstance(raw_kitaru, Mapping):
            yield raw_kitaru


def _is_flow_result_candidate_step(step_run: Any) -> bool:
    """Return whether a terminal step can be used as the fallback flow result."""
    for raw_kitaru in _iter_step_kitaru_extras(step_run):
        if _FLOW_RESULT_CANDIDATE_KEY in raw_kitaru:
            return raw_kitaru[_FLOW_RESULT_CANDIDATE_KEY] is not False
    return True


def _extract_single_terminal_step_output(
    *,
    execution_id: str,
    terminal_step_name: str,
    terminal_step: Any,
) -> list[_FlowResultOutput]:
    """Extract the output for one selected terminal step."""
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
    return [
        _loaded_flow_result_output(
            provenance="step_output",
            step_name=terminal_step_name,
            output_name=output_name,
            artifact=artifact,
        )
    ]


def _extract_outputs_from_terminal_steps(
    run: PipelineRunResponse,
) -> list[_FlowResultOutput]:
    """Extract return outputs from terminal step outputs as a fallback.

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
        eligible_terminal_step_names: list[str] = []
        non_candidate_terminal_step_names: list[str] = []
        for step_name in terminal_step_names:
            if _is_flow_result_candidate_step(step_runs[step_name]):
                eligible_terminal_step_names.append(step_name)
            else:
                non_candidate_terminal_step_names.append(step_name)

        if len(eligible_terminal_step_names) == 1:
            terminal_step_name = eligible_terminal_step_names[0]
            return _extract_single_terminal_step_output(
                execution_id=execution_id,
                terminal_step_name=terminal_step_name,
                terminal_step=step_runs[terminal_step_name],
            )

        reason = (
            f"multiple terminal checkpoints were found "
            f"({len(terminal_step_names)}): "
            f"{', '.join(terminal_step_names)}"
        )
        if eligible_terminal_step_names:
            reason += (
                ". Terminal checkpoints still eligible as flow results: "
                f"{', '.join(eligible_terminal_step_names)}."
            )
        if non_candidate_terminal_step_names:
            reason += (
                " Terminal checkpoints marked as adapter-created/non-result: "
                f"{', '.join(non_candidate_terminal_step_names)}. These steps "
                "also ended the graph, so Kitaru cannot safely ignore them; "
                "add a final checkpoint that consumes all branches and returns "
                "the value you want."
            )
        raise _MultipleTerminalStepsOutputError(
            _ambiguous_terminal_message(
                execution_id,
                reason=reason,
            )
        )

    terminal_step_name = terminal_step_names[0]
    return _extract_single_terminal_step_output(
        execution_id=execution_id,
        terminal_step_name=terminal_step_name,
        terminal_step=step_runs[terminal_step_name],
    )


def _safe_artifact_name(artifact: Any) -> str | None:
    """Best-effort artifact name lookup that tolerates lazy/test doubles."""
    try:
        name = getattr(artifact, "name", None)
    except Exception:
        return None
    if name is None:
        return None
    return str(name)


def _metadata_value(value: Any) -> Any:
    """Unwrap ZenML metadata wrappers when present."""
    if hasattr(value, "value"):
        try:
            return value.value
        except Exception:
            return value
    return value


def _metadata_mapping(metadata: Any) -> Mapping[str, Any]:
    """Return plain metadata values from a ZenML metadata mapping."""
    if not isinstance(metadata, Mapping):
        return {}
    return {str(key): _metadata_value(value) for key, value in metadata.items()}


def _artifact_metadata_value(artifact: Any, key: str) -> Any:
    """Return one metadata value from a ZenML artifact if available."""
    for attr_name in ("run_metadata", "user_metadata", "metadata"):
        try:
            metadata = getattr(artifact, attr_name, None)
        except Exception:
            continue
        if isinstance(metadata, Mapping) and key in metadata:
            return _metadata_value(metadata[key])
    return None


def _tuple_metadata_length_from_output(output: _FlowResultOutput) -> int | None:
    """Return tuple length only for Kitaru's reserved metadata artifact."""
    if (
        _safe_artifact_name(output.artifact)
        != _FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME
    ):
        return None

    role = _artifact_metadata_value(output.artifact, _FLOW_RESULT_ROLE_METADATA_KEY)
    if role is None:
        return None
    if role != _FLOW_RESULT_TUPLE_METADATA_ROLE:
        raise KitaruRuntimeError(
            "Execution flow result tuple metadata artifact has an unexpected "
            f"role: {role!r}."
        )

    if not _is_flow_result_tuple_metadata(output.value):
        raise KitaruRuntimeError(
            "Execution flow result tuple metadata artifact did not contain "
            "valid Kitaru tuple metadata."
        )
    return cast(int, output.value["length"])


_FLOW_RESULT_NOT_FOUND = object()


def _load_referenced_flow_result(
    run: PipelineRunResponse,
    *,
    project: str | None = None,
) -> Any:
    """Load the flow result linked via execution metadata, if present.

    Returns ``_FLOW_RESULT_NOT_FOUND`` when the run has no linked result (e.g. it
    predates this linkage, or returned a checkpoint handle handled elsewhere).
    """
    metadata = _metadata_mapping(getattr(run, "run_metadata", None))
    artifact_id = metadata.get(_FLOW_RESULT_REF_METADATA_KEY)
    if isinstance(artifact_id, str) and artifact_id:
        try:
            return Client().get_artifact_version(artifact_id, project=project).load()
        except Exception as exc:
            raise KitaruRuntimeError(
                f"Could not load the linked flow result artifact {artifact_id!r}: {exc}"
            ) from exc
    if metadata.get(_FLOW_RESULT_NONE_METADATA_KEY) is True:
        return None
    return _FLOW_RESULT_NOT_FOUND


def _extract_flow_result(
    run: PipelineRunResponse,
    *,
    project: str | None = None,
) -> Any:
    """Extract user-facing flow return value from a finished pipeline run.

    Args:
        run: The pipeline run.

    Raises:
        KitaruRuntimeError: If run output metadata is missing or ambiguous.

    Returns:
        The flow result (`None`, a single value, or a tuple of values).
    """
    outputs = _extract_outputs_from_run_outputs(run)
    if not outputs:
        outputs = _extract_outputs_from_output_specs(run)
    if not outputs:
        referenced = _load_referenced_flow_result(run, project=project)
        if referenced is not _FLOW_RESULT_NOT_FOUND:
            return referenced
    if not outputs:
        outputs = _extract_outputs_from_terminal_steps(run)

    if not outputs:
        return None

    metadata_outputs: list[tuple[_FlowResultOutput, int]] = []
    for output in outputs:
        length = _tuple_metadata_length_from_output(output)
        if length is not None:
            metadata_outputs.append((output, length))

    if len(metadata_outputs) > 1:
        raise KitaruRuntimeError(
            "Execution flow result contained multiple Kitaru tuple metadata artifacts."
        )

    if metadata_outputs:
        metadata_output, expected_length = metadata_outputs[0]
        tuple_outputs = [output for output in outputs if output is not metadata_output]
        if len(tuple_outputs) != expected_length:
            raise KitaruRuntimeError(
                "Execution flow result tuple metadata did not match the "
                "loaded output count."
            )
        return tuple(output.value for output in tuple_outputs)

    values = [output.value for output in outputs]
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


def _persist_terminal_llm_usage_metadata(
    run: PipelineRunResponse,
    *,
    zenml_client: Any | None = None,
) -> bool:
    """Compatibility wrapper for terminal LLM usage aggregation."""
    return _shared_persist_terminal_llm_usage_metadata(
        run,
        zenml_client=zenml_client,
    )


def _flow_submission_attempt_metadata() -> dict[str, Any]:
    """Return privacy-safe metadata for direct SDK flow submission attempts."""
    return {"submission_path": "flow_wrapper"}


def _track_flow_submission_failure(
    exc: Exception,
    *,
    deployment_metadata: Mapping[str, Any] | None,
    failure_origin: FailureOrigin | None = None,
) -> None:
    """Emit privacy-safe analytics for failures before a run exists."""
    origin = failure_origin or classify_failure_origin(
        status_reason=str(exc),
        traceback=None,
        default=FailureOrigin.BACKEND,
    )
    metadata: dict[str, Any] = _flow_submission_attempt_metadata()
    if deployment_metadata is not None:
        metadata.update(deployment_metadata)
    metadata["error_type"] = type(exc).__name__
    metadata["failure_origin"] = origin.value
    track(AnalyticsEvent.FLOW_FAILED, metadata)


@dataclass(frozen=True)
class _FlowHandleWaitConditionClient:
    """Provide the client shape needed to list wait conditions."""

    zenml_client: Any
    _project: str | None

    def _client(self) -> Any:
        return self.zenml_client


class FlowHandle:
    """Handle for a running or finished flow execution."""

    def __init__(
        self,
        run: PipelineRunResponse,
        *,
        project: str | None = None,
        observed_started_at: float | None = None,
        analytics_metadata: dict[str, Any] | None = None,
        track_terminal_if_finished: bool = False,
    ) -> None:
        """Initialize a flow handle.

        Args:
            run: Initial pipeline run response.
            project: Kitaru project where this run was created, if known.
            observed_started_at: SDK-observed start time from ``time.perf_counter``.
            analytics_metadata: Privacy-safe metadata captured at submission time.
            track_terminal_if_finished: Emit terminal analytics immediately when
                the initial run is already terminal.
        """
        self._run = run
        self._run_id = run.id
        self._project = project
        self._terminal_event_emitted = False
        self._terminal_llm_usage_metadata_persisted = False
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
                self._persist_terminal_llm_usage_once(run)
            else:
                self._track_terminal_once(run)
                self._persist_terminal_llm_usage_once(run)

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

    def _persist_terminal_llm_usage_once(self, run: PipelineRunResponse) -> None:
        if self._terminal_llm_usage_metadata_persisted:
            return

        try:
            zenml_client = Client()
            aggregation_run = zenml_client.get_pipeline_run(
                run.id,
                allow_name_prefix_match=False,
                project=self._project,
            )
        except Exception:
            logger.debug(
                "Failed to refresh execution metadata before terminal LLM "
                "usage aggregation.",
                exc_info=True,
            )
            return
        self._run = aggregation_run

        if _safe_persist_terminal_llm_usage_metadata(
            aggregation_run,
            zenml_client=zenml_client,
        ):
            self._terminal_llm_usage_metadata_persisted = True

    def wait(self) -> Any:
        """Block until execution finishes and return its result.

        Raises:
            KitaruStateError: If the execution is waiting for input or paused.
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
                    self._persist_terminal_llm_usage_once(run)
                    _raise_for_unsuccessful_run(run, failure_origin=origin)
                self._track_terminal_once(run)
                self._persist_terminal_llm_usage_once(run)
                return _extract_flow_result(run, project=self._project)

            if _to_public_status(run.status) == ExecutionStatus.WAITING:
                wait_client = _FlowHandleWaitConditionClient(Client(), self._project)
                try:
                    pending_waits = _list_pending_wait_conditions(
                        run=run,
                        client=cast(Any, wait_client),
                    )
                except KitaruBackendError as exc:
                    raise KitaruStateError(
                        f"Execution '{run.id}' is paused/waiting, but Kitaru "
                        "could not determine whether it has pending wait input: "
                        f"{exc}\n\n"
                        "If input is still pending, resolve it with:\n\n"
                        f"  kitaru executions input {run.id} --value '<json>'\n\n"
                        "If all wait input is already resolved, resume it with:\n\n"
                        f"  kitaru executions resume {run.id}"
                    ) from exc

                if pending_waits:
                    raise KitaruStateError(
                        f"Execution '{run.id}' is waiting for input. "
                        "`FlowHandle.wait()` cannot return a result until the "
                        "pending wait is resolved.\n\n"
                        "Provide the wait input with:\n\n"
                        f"  kitaru executions input {run.id} --value '<json>'"
                    )

                raise KitaruStateError(
                    f"Execution '{run.id}' is paused, but Kitaru found no "
                    "pending wait input to resolve. `FlowHandle.wait()` cannot "
                    "resume it automatically.\n\n"
                    "Resume the execution with:\n\n"
                    f"  kitaru executions resume {run.id}"
                )

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
            self._persist_terminal_llm_usage_once(run)
            _raise_for_unsuccessful_run(run, failure_origin=origin)
        self._track_terminal_once(run)
        self._persist_terminal_llm_usage_once(run)
        return _extract_flow_result(run, project=self._project)

    def _refresh(self) -> PipelineRunResponse:
        """Refresh the cached run model from the server."""
        try:
            self._run = Client().get_pipeline_run(
                self._run_id,
                allow_name_prefix_match=False,
                project=self._project,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to refresh execution {self._run_id}: {exc}"
            ) from exc
        return self._run


class _FlowDefinition:
    """Flow wrapper returned by `@flow`."""

    _kitaru_replay_flow_wrapper = True

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
            on_end=aggregate_llm_usage_on_run_end,
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
            retries: Optional retry override. Retries rerun the whole flow body;
                if an internal result-artifact save fails after user code
                returns, ZenML may replay any side effects in the flow.
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
        resolved_connection = resolve_connection_config(validate_for_use=True)
        resolved_project = _connection_project(resolved_connection)
        transport_image, _ = _prepare_model_registry_transport(resolved_execution.image)
        configured_pipeline = self._pipeline.with_options(
            **_build_pipeline_options(
                resolved_execution=resolved_execution,
                transport_image=transport_image,
            )
        )
        deployments_api, flow_name = self._deployments_api_and_flow_name()
        source_name = f"kitaru-source::{flow_name}::{uuid4().hex}"

        with (
            _temporary_active_project(resolved_project),
            _temporary_active_stack(resolved_execution.stack),
        ):
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
                    "Unable to create this deployment because Kitaru needs "
                    "concrete input values to prepare the saved deployment "
                    "snapshot. Pass representative input values when calling "
                    "flow.deploy(...), then override them later when invoking it."
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

    def _replay_one_handle(
        self,
        execution: str,
        *,
        at: str,
        flow_overrides: Mapping[str, Any] | None = None,
        checkpoint_overrides: Mapping[str, Any] | None = None,
        invocation_overrides: Mapping[str, Any] | None = None,
        skip: Sequence[str] | None = None,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
        resolved_connection: Any | None = None,
        original_run: PipelineRunResponse | None = None,
        replay_submission_id: str | None = None,
        replay_tag: str | None = None,
    ) -> tuple[FlowHandle, PipelineRunResponse, ReplayPlan]:
        """Submit one replay child and return the live handle plus its plan."""
        raw_active_stack_provenance = _capture_active_stack_provenance_for_guard()
        if resolved_connection is None:
            resolved_connection = resolve_connection_config(validate_for_use=True)

        if original_run is None:
            try:
                original_run = Client().get_pipeline_run(
                    name_id_or_prefix=execution,
                    allow_name_prefix_match=False,
                    hydrate=True,
                    project=_connection_project(resolved_connection),
                )
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to load source execution '{execution}' for replay: {exc}"
                ) from exc

        replay_plan = build_replay_plan(
            run=original_run,
            at=at,
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
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
        transport_image = _inject_replay_context_env(
            transport_image,
            replay_context_json=replay_plan.runtime_context.to_json(),
        )
        resolved_execution = resolved_execution.model_copy(
            update={"image": transport_image}
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

        resolved_project = _connection_project(resolved_connection)
        with (
            _temporary_active_project(resolved_project),
            _temporary_active_stack(resolved_execution.stack),
        ):
            _preflight_active_stack_implementation_hydration()
            deployment_metadata = _deployment_metadata_for_stack(
                resolved_execution.stack
            )
            replay_metadata = {
                "at_checkpoint": at,
                "replay_path": "flow_wrapper",
                **deployment_metadata,
            }
            track(AnalyticsEvent.REPLAY_REQUESTED, replay_metadata)

            observed_started_at = time.perf_counter()
            try:
                with _scoped_replay_runtime_context(
                    replay_plan.runtime_context.to_json()
                ):
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
                        f"Replay diverged for execution '{execution}': {exc}",
                        exec_id=str(original_run.id),
                        status="failed",
                        origin=failure_origin,
                    ) from exc
                raise KitaruBackendError(
                    f"Failed to replay execution '{execution}': {exc}"
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
            persist_frozen_execution_spec(
                run_id=replayed_run.id,
                frozen_execution_spec=frozen_execution_spec,
            )
            safe_persist_replay_submission_metadata(
                replay_exec_id=str(replayed_run.id),
                original_exec_id=str(original_run.id),
                submission_id=replay_submission_id or new_replay_submission_id(),
                tag=replay_tag,
                steps_to_skip=replay_plan.steps_to_skip,
            )

        _emit_kitaru_execution_url(replayed_run)

        track(
            AnalyticsEvent.FLOW_REPLAYED,
            {"replay_path": "flow_wrapper", **deployment_metadata},
        )
        handle = FlowHandle(
            replayed_run,
            observed_started_at=observed_started_at,
            project=resolved_project,
            analytics_metadata=deployment_metadata,
            track_terminal_if_finished=True,
        )
        return handle, original_run, replay_plan

    def replay(
        self,
        execution: str | Sequence[str],
        *,
        at: str,
        flow_overrides: Mapping[str, Any] | None = None,
        checkpoint_overrides: Mapping[str, Any] | None = None,
        invocation_overrides: Mapping[str, Any] | None = None,
        skip: Sequence[str] | None = None,
        tag: str | None = None,
        wait: bool | None = None,
        on_error: Literal["collect", "fail"] | None = None,
        stack: str | None = None,
        image: ImageSetting | None = None,
        cache: bool | None = None,
        retries: int | None = None,
    ) -> ReplaySubmission:
        """Replay one or more explicit executions with unified overrides."""
        from kitaru.cohort import coerce_exec_ids

        exec_ids = (
            [execution] if isinstance(execution, str) else coerce_exec_ids(execution)
        )
        if not exec_ids:
            raise KitaruUsageError("Pass at least one execution ID to replay.")
        resolved_wait = (len(exec_ids) == 1) if wait is None else wait
        resolved_on_error = on_error or ("fail" if len(exec_ids) == 1 else "collect")
        if resolved_on_error not in {"collect", "fail"}:
            raise KitaruUsageError("`on_error` must be 'collect' or 'fail'.")
        validated_connection = resolve_connection_config(validate_for_use=True)

        submission_id = new_replay_submission_id()
        request_document = build_replay_request_document(
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
        )
        plan_document = request_document
        results: list[ReplayResultRow] = []
        failures: list[ReplayFailureRow] = []
        skipped_rows: list[ReplaySkippedRow] = []
        original_and_replay_ids: list[str] = []
        client = Client()

        for exec_ref in exec_ids:
            original_id: str | None = None
            try:
                original_run = client.get_pipeline_run(
                    name_id_or_prefix=exec_ref,
                    allow_name_prefix_match=False,
                    hydrate=True,
                    project=_connection_project(validated_connection),
                )
                original_id = str(original_run.id)
                if resolved_on_error == "collect":
                    at_status = replay_at_status(run=original_run, at=at)
                    if at_status in {"missing", "no_checkpoints"}:
                        skipped_rows.append(
                            ReplaySkippedRow(
                                original_exec_ref=exec_ref,
                                original_exec_id=original_id,
                                reason=replay_at_skip_reason(run=original_run, at=at),
                            )
                        )
                        continue
                    if at_status == "ambiguous":
                        failures.append(
                            ReplayFailureRow(
                                original_exec_ref=exec_ref,
                                original_exec_id=original_id,
                                reason=replay_at_skip_reason(run=original_run, at=at),
                            )
                        )
                        continue

                handle, loaded_original_run, replay_plan = self._replay_one_handle(
                    exec_ref,
                    at=at,
                    flow_overrides=flow_overrides,
                    checkpoint_overrides=checkpoint_overrides,
                    invocation_overrides=invocation_overrides,
                    skip=skip,
                    stack=stack,
                    image=image,
                    cache=cache,
                    retries=retries,
                    resolved_connection=validated_connection,
                    original_run=original_run,
                    replay_submission_id=submission_id,
                    replay_tag=tag,
                )
                original_id = str(loaded_original_run.id)
                plan_document = replay_plan.document
                replay_exec_id = str(handle.exec_id)
                row_status: Literal["submitted", "completed", "failed"] = "submitted"
                if resolved_wait:
                    handle.wait()
                    row_status = "completed"
                row_compare_url = safe_compare_url_for_executions(
                    [original_id, replay_exec_id]
                )
                original_and_replay_ids.extend([original_id, replay_exec_id])
                results.append(
                    ReplayResultRow(
                        original_exec_ref=exec_ref,
                        original_exec_id=original_id,
                        replay_exec_id=replay_exec_id,
                        status=row_status,
                        compare_url=row_compare_url,
                        handle=None if resolved_wait else handle,
                    )
                )
            except Exception as exc:
                if resolved_on_error == "fail":
                    raise
                failures.append(
                    ReplayFailureRow(
                        original_exec_ref=exec_ref,
                        original_exec_id=original_id,
                        reason=str(exc),
                    )
                )

        compare_url = safe_compare_url_for_executions(original_and_replay_ids)
        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=tag,
            at=at,
            wait=resolved_wait,
            plan=plan_document,
            results=results,
            failures=failures,
            skipped=skipped_rows,
            compare_url=compare_url,
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
        track(AnalyticsEvent.FLOW_ATTEMPTED, _flow_submission_attempt_metadata())
        deployment_metadata: dict[str, Any] | None = None
        failure_origin: FailureOrigin | None = None
        # Submission has succeeded the moment a run object exists. Failures after
        # that point (URL emission, spec persistence) must not be tracked as
        # submission failures, while every setup failure before it must be.
        submitted = False
        try:
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
            resolved_project = _connection_project(resolved_connection)
            transport_image, effective_model_registry = (
                _prepare_model_registry_transport(resolved_execution.image)
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

            # The resolved project stays active across both the run submission and
            # the post-submit spec persistence so ZenML writes them into the same
            # project. Activating it can fail (deleted/mistyped project); that is a
            # pre-submission setup failure and is tracked by the handler below.
            with _temporary_active_project(resolved_project):
                with _temporary_active_stack(resolved_execution.stack):
                    _preflight_active_stack_implementation_hydration()
                    deployment_metadata = _deployment_metadata_for_stack(
                        resolved_execution.stack
                    )
                    observed_started_at = time.perf_counter()
                    run = configured_pipeline(*args, **kwargs)

                if run is None:
                    failure_origin = FailureOrigin.RUNTIME
                    raise KitaruRuntimeError(
                        "Flow execution did not produce a pipeline run."
                    )

                submitted = True
                _emit_kitaru_execution_url(run)
                track(AnalyticsEvent.FLOW_SUBMITTED, deployment_metadata)
                persist_frozen_execution_spec(
                    run_id=run.id,
                    frozen_execution_spec=frozen_execution_spec,
                )
        except Exception as exc:
            if not submitted:
                _track_flow_submission_failure(
                    exc,
                    deployment_metadata=deployment_metadata,
                    failure_origin=failure_origin,
                )
            raise

        return FlowHandle(
            run,
            observed_started_at=observed_started_at,
            project=resolved_project,
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
            sources apply and eventually default to ``0``). Retries rerun the
            whole flow body, including any side effects that happened before a
            post-return internal result-artifact save failure.

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
