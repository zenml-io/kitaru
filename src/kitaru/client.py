"""Kitaru client for execution and artifact management.

`KitaruClient` provides a programmatic API for inspecting and managing
executions and artifacts outside flow bodies.

Example:
    ```python
    from kitaru import KitaruClient

    client = KitaruClient()
    execution = client.executions.get("exec-123")
    print(execution.status)
    ```
"""

from __future__ import annotations

import builtins
import importlib
import logging
import sys
import threading
import warnings
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Literal, NoReturn, Protocol, cast
from uuid import UUID

from pydantic import ValidationError
from zenml.client import Client
from zenml.config.pipeline_run_configuration import PipelineRunConfiguration
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.exceptions import EntityExistsError
from zenml.login.credentials_store import get_credentials_store
from zenml.models import PipelineRunResponse, PipelineRunUpdate
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.utils.pagination_utils import depaginate_stream
from zenml.utils.run_utils import stop_run
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    build_deployment_snapshot_name,
    deployment_native_tags,
    deployment_public_tag,
    deployment_snapshot_marker_tag,
    map_deployment_snapshot,
    next_deployment_version,
    resolve_deployment_exclusive,
    validate_deployment_flow,
    validate_deployment_tag,
    validate_deployment_version,
)
from kitaru._client._events import (
    StreamingStore,
    open_rest_sse_stream,
    watch_execution_events,
)
from kitaru._client._imports import ImportsAPI
from kitaru._client._logs import (
    _coerce_log_level,
    _coerce_log_lineno,
    _coerce_log_text,
    _is_empty_log_result_error,
    _is_log_endpoint_version_skew_error,
    _is_otel_log_retrieval_error,
    _log_sort_key,
    _map_runtime_log_entry,
    _normalize_log_source,
    _parse_log_timestamp,
    _sort_log_entries,
    _step_log_fetch_order_key,
)
from kitaru._client._mappers import (
    _CHECKPOINT_SOURCE_ALIAS_PREFIX,
    _PIPELINE_SOURCE_ALIAS_PREFIX,
    _RAW_STATUSES_BY_PUBLIC_STATUS,
    _WAIT_CONDITION_STATUS_PENDING,
    _backend_filter_value,
    _checkpoint_lineage_key,
    _coerce_status_filter,
    _first_pending_wait,
    _get_active_wait_condition,
    _list_checkpoint_attempts_for_run,
    _list_pending_wait_conditions,
    _list_run_wait_conditions,
    _map_artifact_ref,
    _map_checkpoint_attempt,
    _map_checkpoint_call,
    _map_execution,
    _map_failure_info,
    _map_pending_wait,
    _parse_frozen_execution_spec,
    _select_pending_wait_condition,
    _status_filter_value,
    _to_plain_dict,
    _to_public_status,
)
from kitaru._client._models import (
    ArtifactRef,
    AuthAPIKey,
    AuthAPIKeyWithValue,
    AuthServiceAccount,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionEvent,
    ExecutionStatistics,
    ExecutionStatisticsDimension,
    ExecutionStatisticsGroup,
    ExecutionStatisticsGrouping,
    ExecutionStatisticsMetric,
    ExecutionStatisticsMetricAggregation,
    ExecutionStatisticsMetricSource,
    ExecutionStatisticsTimeGranularity,
    ExecutionStatus,
    FailureInfo,
    LogEntry,
    PendingWait,
    ScoreFilter,
)
from kitaru._client._models import (
    Deployment as DeploymentRecord,
)
from kitaru._client._statistics import (
    get_execution_statistics,
    normalize_execution_statistics_groupings,
    normalize_execution_statistics_metrics,
)
from kitaru._experiments import (
    Experiment,
    ExperimentRunLookup,
    experiment_targets_execution,
)
from kitaru._import_contract import raise_if_imported_execution
from kitaru._interface_deployments import (
    Deployment,
    DeploymentSelectorSource,
    deployment_tags_for_create,
    ensure_stack_is_server_runnable,
    is_deployment_known,
    mark_deployment_known,
    validate_deployment_selector,
    validate_remove_deployment_tag,
    warn_if_deployment_drifted,
)
from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru._source_aliases import normalize_flow_name as _normalize_flow_name
from kitaru._telemetry import deployment_metadata_for_stack_model
from kitaru._terminal_usage import _safe_persist_terminal_llm_usage_metadata
from kitaru.analytics import AnalyticsEvent, track
from kitaru.config import (
    AgentCreateResult,
    AgentDeleteResult,
    AgentInfo,
    ProjectCreateResult,
    ProjectDeleteResult,
    ProjectInfo,
    active_stack_log_store,
    resolve_connection_config,
    resolve_log_store,
)
from kitaru.config import (
    create_agent as _create_agent,
)
from kitaru.config import (
    create_project as _create_project,
)
from kitaru.config import (
    current_agent as _current_agent,
)
from kitaru.config import (
    current_project as _current_project,
)
from kitaru.config import (
    delete_agent as _delete_agent,
)
from kitaru.config import (
    delete_project as _delete_project,
)
from kitaru.config import (
    get_agent as _get_agent,
)
from kitaru.config import (
    get_project as _get_project,
)
from kitaru.config import (
    list_agents as _list_agents,
)
from kitaru.config import (
    list_projects as _list_projects,
)
from kitaru.config import (
    use_agent as _use_agent,
)
from kitaru.config import (
    use_project as _use_project,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruLogRetrievalError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    KitaruWaitValidationError,
    classify_failure_origin,
    execution_error_from_failure,
)
from kitaru.replay import (
    EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY,
    ReplayFailureRow,
    ReplayResultRow,
    ReplaySkippedRow,
    ReplaySubmission,
    build_replay_plan,
    build_replay_request_document,
    new_replay_submission_id,
    plan_requires_runtime_transport,
    replay_at_skip_reason,
    replay_at_status,
    safe_compare_url_for_executions,
    safe_persist_replay_submission_metadata,
)
from kitaru.scoring import (
    ObservationQuery,
    ScoreObservation,
    ScoreObservationStatus,
)
from kitaru.scoring._evaluation import ScoreEvaluationService

logger = logging.getLogger(__name__)

_WAIT_CONDITION_RESOLUTION_CONTINUE = "continue"
_WAIT_CONDITION_RESOLUTION_ABORT = "abort"
_RETRY_RESUMING_REASON = "Manual retry requested by user."
_RETRY_ROLLBACK_REASON = "Retry submission failed."
_RESUME_RESUMING_REASON = "Manual resume requested by user."
_RESUME_ROLLBACK_REASON = "Manual resume failed."
_DUPLICATE_WAIT_CONDITION_CONFIGURATION_ERROR = (
    "A run wait condition with this name already exists for the run, "
    "but with different configuration."
)
_OPERATIONAL_RESUME_FAILURE_MARKERS = (
    "Additionally failed to roll back execution status",
    "Could not verify whether the execution is still RESUMING",
    "The execution may remain RESUMING",
    "Additionally failed to restore the previous active Kitaru stack",
    "restoring the previous active Kitaru stack failed",
    "The execution may continue",
)
_REPLAY_IMPORT_LOCK = threading.RLock()


class _DeploymentStackModel(Protocol):
    """Hydrated stack model shape needed by deployment validation/telemetry."""

    id: object
    name: str
    components: Mapping[Any, Any]


class _ReplayImportDependencyError(KitaruRuntimeError):
    """Replay source import failed because one of its dependencies is missing."""


@dataclass(frozen=True)
class _ReplayLink:
    """Lightweight native linkage discovered from an unhydrated run row."""

    exec_id: str
    original_exec_id: str


# The direct imports above preserve `kitaru.client.*` patch targets; this tuple
# simply keeps intentionally re-exported private names alive for linting.
_CLIENT_FACADE_LINT_ANCHOR = (
    _CHECKPOINT_SOURCE_ALIAS_PREFIX,
    _WAIT_CONDITION_STATUS_PENDING,
    _checkpoint_lineage_key,
    _coerce_log_level,
    _coerce_log_lineno,
    _coerce_log_text,
    _coerce_status_filter,
    _first_pending_wait,
    _get_active_wait_condition,
    _is_empty_log_result_error,
    _is_otel_log_retrieval_error,
    _list_checkpoint_attempts_for_run,
    _list_run_wait_conditions,
    _log_sort_key,
    _map_checkpoint_attempt,
    _map_checkpoint_call,
    _map_failure_info,
    _map_pending_wait,
    _parse_frozen_execution_spec,
    _parse_log_timestamp,
    _to_plain_dict,
    _to_public_status,
)


def _validate_non_empty_auth_value(value: str, *, name: str) -> str:
    """Validate a required auth-management string argument."""
    if not isinstance(value, str) or not value.strip():
        raise KitaruUsageError(f"`{name}` must be a non-empty string.")
    return value


def _is_strict_int(value: Any) -> bool:
    """Return True only for plain `int` values (rejects `bool`, `str`, `float`)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_auth_pagination(
    *,
    limit: int | None,
    page: int | None,
    size: int | None,
) -> tuple[int, int]:
    """Validate shared auth list pagination and return backend page/size."""
    if limit is not None:
        if not _is_strict_int(limit) or limit < 1:
            raise KitaruUsageError("`limit` must be an integer >= 1 when provided.")
        if page is not None or size is not None:
            raise KitaruUsageError("`limit` cannot be combined with `page` or `size`.")
        return 1, limit
    if page is not None and (not _is_strict_int(page) or page < 1):
        raise KitaruUsageError("`page` must be an integer >= 1 when provided.")
    if size is not None and (not _is_strict_int(size) or size < 1):
        raise KitaruUsageError("`size` must be an integer >= 1 when provided.")
    if page is not None and size is None:
        raise KitaruUsageError("`size` is required when `page` is provided.")
    if size is None:
        return 1, 20
    return page or 1, size


def _map_auth_service_account(service_account: Any) -> AuthServiceAccount:
    """Map a ZenML service-account response to the public Kitaru DTO."""
    return AuthServiceAccount(
        service_account_id=str(service_account.id),
        name=str(service_account.name),
        full_name=str(getattr(service_account, "full_name", "") or ""),
        description=str(getattr(service_account, "description", "") or ""),
        active=bool(getattr(service_account, "active", False)),
        created_at=getattr(service_account, "created", None),
        updated_at=getattr(service_account, "updated", None),
        avatar_url=getattr(service_account, "avatar_url", None),
    )


def _map_auth_api_key(api_key: Any) -> AuthAPIKey:
    """Map a ZenML API-key response to metadata-only public Kitaru DTO."""
    service_account = getattr(api_key, "service_account", None)
    return AuthAPIKey(
        api_key_id=str(api_key.id),
        name=str(api_key.name),
        service_account_id=str(getattr(service_account, "id", "")),
        service_account_name=str(getattr(service_account, "name", "")),
        description=str(getattr(api_key, "description", "") or ""),
        active=bool(getattr(api_key, "active", False)),
        created_at=getattr(api_key, "created", None),
        updated_at=getattr(api_key, "updated", None),
        last_login=getattr(api_key, "last_login", None),
        last_rotated=getattr(api_key, "last_rotated", None),
        retain_period_minutes=int(getattr(api_key, "retain_period_minutes", 0) or 0),
    )


def _map_auth_api_key_with_value(api_key: Any) -> AuthAPIKeyWithValue:
    """Map a create/rotate API-key response including its one-time raw value."""
    raw_key = getattr(api_key, "key", None)
    if not isinstance(raw_key, str) or not raw_key:
        raise KitaruBackendError(
            "The server did not return the one-time API key value for this "
            "create/rotate operation."
        )
    return AuthAPIKeyWithValue(
        api_key=_map_auth_api_key(api_key),
        key=raw_key,
    )


@dataclass(frozen=True)
class _LocalCredentialSnapshot:
    """Best-effort snapshot of the previous persisted API-key credential."""

    server_url: str | None
    previous_api_key: str | None = field(default=None, repr=False)
    reason_unavailable: str | None = None

    @property
    def previous_api_key_available(self) -> bool:
        """Whether this snapshot contains a rollback candidate."""
        return bool(self.previous_api_key)


def _redact_auth_error_text(text: str, *, secrets: list[str]) -> str:
    """Return error text without leaking known sensitive credential values."""
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[redacted]")
    return redacted


def _sanitize_local_key_activation_error(
    exc: Exception,
    *,
    raw_key: str,
    previous_key: str | None = None,
) -> str:
    """Return local activation error text without leaking API-key values."""
    message = str(exc) or type(exc).__name__
    return _redact_auth_error_text(
        message,
        secrets=[raw_key, previous_key or ""],
    )


def _capture_previous_local_api_key(zenml_client: Any) -> _LocalCredentialSnapshot:
    """Return the previous persisted API key if it can be restored safely."""
    server_url = getattr(getattr(zenml_client, "zen_store", None), "url", None)
    if not isinstance(server_url, str) or not server_url:
        return _LocalCredentialSnapshot(
            server_url=None,
            reason_unavailable=(
                "Kitaru could not determine the active remote server URL, so "
                "there was no previous persisted credential to restore."
            ),
        )

    try:
        credentials_store = get_credentials_store()
        previous_api_key = credentials_store.get_api_key(server_url=server_url)
    except Exception as exc:
        return _LocalCredentialSnapshot(
            server_url=server_url,
            reason_unavailable=(
                "Kitaru could not read the previous persisted local API key for "
                f"{server_url!r}, so rollback was not possible: {exc}"
            ),
        )

    if not previous_api_key:
        return _LocalCredentialSnapshot(
            server_url=server_url,
            reason_unavailable=(
                "No previous persisted local API key was available to restore. "
                "Environment credentials, if any, were not modified."
            ),
        )

    return _LocalCredentialSnapshot(
        server_url=server_url,
        previous_api_key=previous_api_key,
    )


def _with_local_key_activation_status(
    result: AuthAPIKeyWithValue,
    *,
    succeeded: bool,
    error: str | None = None,
    rollback_attempted: bool = False,
    rollback_succeeded: bool | None = None,
    rollback_error: str | None = None,
    rollback_reason: str | None = None,
) -> AuthAPIKeyWithValue:
    """Return an API-key result annotated with local activation status."""
    return AuthAPIKeyWithValue(
        api_key=result.api_key,
        key=result.key,
        local_key_activation_requested=True,
        local_key_activation_succeeded=succeeded,
        local_key_activation_error=error,
        local_key_rollback_attempted=rollback_attempted,
        local_key_rollback_succeeded=rollback_succeeded,
        local_key_rollback_error=rollback_error,
        local_key_rollback_reason=rollback_reason,
    )


def _attempt_local_key_activation(
    result: AuthAPIKeyWithValue,
    *,
    zenml_client: Any,
    operation: Literal["create", "rotate"],
) -> AuthAPIKeyWithValue:
    """Best-effort local activation that never discards the one-time key."""
    previous_credential = _capture_previous_local_api_key(zenml_client)
    try:
        zenml_client.set_api_key(key=result.key)
    except Exception as exc:
        action = "created" if operation == "create" else "rotated"
        sanitized_error = _sanitize_local_key_activation_error(
            exc,
            raw_key=result.key,
            previous_key=previous_credential.previous_api_key,
        )
        base_error = (
            f"API key was {action}, but Kitaru could not set it as the "
            f"active local credential: {sanitized_error}."
        )
        if not previous_credential.previous_api_key_available:
            rollback_reason = previous_credential.reason_unavailable or (
                "No previous persisted local API key was available to restore."
            )
            return _with_local_key_activation_status(
                result,
                succeeded=False,
                error=f"{base_error} Rollback was not possible: {rollback_reason}",
                rollback_attempted=False,
                rollback_succeeded=None,
                rollback_reason=rollback_reason,
            )

        assert previous_credential.previous_api_key is not None
        try:
            zenml_client.set_api_key(key=previous_credential.previous_api_key)
        except Exception as rollback_exc:
            rollback_error = _sanitize_local_key_activation_error(
                rollback_exc,
                raw_key=result.key,
                previous_key=previous_credential.previous_api_key,
            )
            return _with_local_key_activation_status(
                result,
                succeeded=False,
                error=(
                    f"{base_error} Kitaru also tried to restore the previous "
                    "local credential, but that rollback failed. The server-side "
                    f"API key was still {action}; local credentials may need "
                    "manual repair."
                ),
                rollback_attempted=True,
                rollback_succeeded=False,
                rollback_error=rollback_error,
            )

        return _with_local_key_activation_status(
            result,
            succeeded=False,
            error=(
                f"{base_error} Kitaru restored the previous local credential, "
                "so this machine should still be using the credential that was "
                "active before the attempted activation."
            ),
            rollback_attempted=True,
            rollback_succeeded=True,
        )
    return _with_local_key_activation_status(result, succeeded=True)


class _ReplayFlowLike(Protocol):
    """Flow wrapper protocol used by client-side replay resolution."""

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
    ) -> Any: ...


_KITARU_REPLAY_FLOW_WRAPPER_MARKER = "_kitaru_replay_flow_wrapper"


def _is_replay_flow_wrapper(candidate: Any) -> bool:
    """Return whether ``candidate`` is a real Kitaru flow wrapper.

    A plain ZenML ``Pipeline`` also has a ``replay`` method, but it does not
    accept Kitaru's unified replay arguments such as ``at`` and
    ``flow_overrides``.  Runtime-checkable protocols only verify attribute
    presence, not signatures, so replay resolution uses an explicit marker set
    by Kitaru's ``@flow`` wrapper before delegating to the wrapper path.
    """
    return bool(getattr(candidate, _KITARU_REPLAY_FLOW_WRAPPER_MARKER, False))


def _snapshot_source_parts(run: PipelineRunResponse) -> tuple[str, str | None]:
    """Return `(module, attribute)` from a run snapshot source."""
    snapshot = run.snapshot
    pipeline_spec = getattr(snapshot, "pipeline_spec", None)
    source = getattr(pipeline_spec, "source", None)
    if source is None:
        raise KitaruRuntimeError(
            "Replay requires pipeline source metadata on the source execution."
        )

    module = getattr(source, "module", None)
    attribute = getattr(source, "attribute", None)

    import_path = getattr(source, "import_path", None)
    if isinstance(import_path, str) and import_path:
        import_module, _, import_attribute = import_path.rpartition(".")
        if not module and import_module:
            module = import_module
        if attribute is None and import_attribute:
            attribute = import_attribute

    if not isinstance(module, str) or not module:
        raise KitaruRuntimeError(
            "Replay source metadata is missing a module import path."
        )

    if attribute is not None and not isinstance(attribute, str):
        attribute = None

    return module, attribute


def _import_module_for_replay(module_name: str, run_id: str | Any) -> Any:
    """Import a module by name, falling back to local and in-memory matches.

    ZenML records the pipeline source module relative to the archived source
    root (e.g. ``replay_with_overrides``), but in the running process the
    module may be loaded under a different path. Fallback order:

    1. Direct ``importlib.import_module`` (exact match).
    2. Search ``sys.modules`` for a suffix match (e.g. the module is loaded
       as ``examples.features.replay.replay_with_overrides``).
    3. Return a matching ``__main__`` module when the current process is
       already executing the replay source via ``python -m`` or as a script.
    4. Temporarily prepend the current working directory to ``sys.path`` and
       retry import when a matching local module/package exists there.
    """
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if not _is_retryable_replay_import_error(exc, module_name):
            raise _missing_replay_dependency_error(module_name, run_id, exc) from exc

    loaded_module = _find_loaded_replay_module(module_name)
    if loaded_module is not None:
        return loaded_module

    main_module = _matching_main_module(module_name)
    if main_module is not None:
        return main_module

    cwd_module = _import_module_from_cwd(module_name, run_id)
    if cwd_module is not None:
        return cwd_module

    cwd = Path.cwd()
    raise KitaruRuntimeError(
        f"Failed to import replay source module '{module_name}' for "
        f"execution '{run_id}': tried direct import, loaded-module lookup, "
        f"matching '__main__', and a temporary cwd import from '{cwd}'. "
        "Run replay from the project directory or set PYTHONPATH when the "
        "source module is local."
    )


def _missing_replay_dependency_error(
    module_name: str, run_id: str | Any, exc: ModuleNotFoundError
) -> _ReplayImportDependencyError:
    """Build a domain error for source modules with missing dependencies."""
    missing_name = getattr(exc, "name", None)
    dependency = (
        f" '{missing_name}'" if isinstance(missing_name, str) and missing_name else ""
    )
    return _ReplayImportDependencyError(
        f"Failed to import replay source module '{module_name}' for "
        f"execution '{run_id}': the module imports missing dependency"
        f"{dependency}. Install the dependency or run replay in an environment "
        "that matches the original execution."
    )


def _is_retryable_replay_import_error(
    exc: ModuleNotFoundError, module_name: str
) -> bool:
    """Return whether a replay import error is for the target module itself."""
    missing_name = getattr(exc, "name", None)
    if not isinstance(missing_name, str) or not missing_name:
        return False

    module_parts = module_name.split(".")
    retryable_names = {
        ".".join(module_parts[:index]) for index in range(1, len(module_parts) + 1)
    }
    return missing_name in retryable_names


def _module_name_matches(candidate_name: str | None, module_name: str) -> bool:
    """Return whether a candidate module identity matches the replay module."""
    if not candidate_name:
        return False
    return candidate_name == module_name or candidate_name.endswith(f".{module_name}")


def _find_loaded_replay_module(module_name: str) -> Any | None:
    """Return an already-loaded module whose name matches the replay source."""
    for loaded_name, loaded_module in list(sys.modules.items()):
        if _module_name_matches(loaded_name, module_name) and loaded_module is not None:
            return loaded_module
    return None


def _matching_main_module(module_name: str) -> Any | None:
    """Return ``__main__`` only when it matches the recorded replay module."""
    main_module = sys.modules.get("__main__")
    if main_module is None:
        return None

    main_spec = getattr(main_module, "__spec__", None)
    spec_name = getattr(main_spec, "name", None)
    if isinstance(spec_name, str) and _module_name_matches(spec_name, module_name):
        return main_module

    if "." in module_name:
        return None

    main_file = getattr(main_module, "__file__", None)
    if not isinstance(main_file, str) or not main_file:
        return None

    expected_stem = module_name.rsplit(".", 1)[-1]
    if Path(main_file).stem == expected_stem:
        return main_module

    return None


def _module_candidate_paths(module_name: str, cwd: Path) -> tuple[Path, Path]:
    """Return module and package candidate paths under the current directory."""
    module_parts = module_name.split(".")
    module_path = cwd.joinpath(*module_parts).with_suffix(".py")
    package_path = cwd.joinpath(*module_parts, "__init__.py")
    return module_path, package_path


@contextmanager
def _temporary_sys_path_prepend(path: str) -> Iterator[None]:
    """Temporarily prepend one entry to ``sys.path`` for a scoped import."""
    with _REPLAY_IMPORT_LOCK:
        original_path_count = sys.path.count(path)
        inserted = False
        if not sys.path or sys.path[0] != path:
            sys.path.insert(0, path)
            inserted = True

        try:
            yield
        finally:
            if inserted:
                if sys.path and sys.path[0] == path:
                    sys.path.pop(0)
                elif sys.path.count(path) > original_path_count:
                    for index, entry in enumerate(sys.path):
                        if entry == path:
                            del sys.path[index]
                            break


def _import_module_from_cwd(module_name: str, run_id: str | Any) -> Any | None:
    """Retry replay module import from the current directory when justified."""
    cwd = Path.cwd()
    module_path, package_path = _module_candidate_paths(module_name, cwd)
    if not module_path.exists() and not package_path.exists():
        return None

    with _temporary_sys_path_prepend(str(cwd)):
        try:
            return importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if not _is_retryable_replay_import_error(exc, module_name):
                raise _missing_replay_dependency_error(
                    module_name, run_id, exc
                ) from exc
            return None


def _resolve_flow_for_replay(run: PipelineRunResponse) -> _ReplayFlowLike:
    """Resolve the original flow wrapper object for a replay source run."""
    module_name, source_attribute = _snapshot_source_parts(run)
    module = _import_module_for_replay(module_name, run.id)

    selectors: list[str] = []
    if run.pipeline is not None:
        flow_name = _normalize_flow_name(run.pipeline.name)
        if flow_name:
            selectors.append(flow_name)

    if source_attribute and source_attribute.startswith(_PIPELINE_SOURCE_ALIAS_PREFIX):
        selectors.append(source_attribute.removeprefix(_PIPELINE_SOURCE_ALIAS_PREFIX))

    if source_attribute:
        selectors.append(source_attribute)

    deduped_selectors = list(
        dict.fromkeys(selector for selector in selectors if selector)
    )
    for selector in deduped_selectors:
        candidate = getattr(module, selector, None)
        if _is_replay_flow_wrapper(candidate):
            return cast(_ReplayFlowLike, candidate)

    tried_selectors = ", ".join(deduped_selectors) or "none"
    raise KitaruRuntimeError(
        "Unable to resolve a replay-capable flow object from source module "
        f"'{module_name}' for execution '{run.id}'. "
        f"Tried: {tried_selectors}."
    )


def _resolve_pipeline_for_replay(run: PipelineRunResponse) -> Any:
    """Resolve the underlying pipeline object for replay fallback."""
    module_name, source_attribute = _snapshot_source_parts(run)
    if not source_attribute:
        raise KitaruRuntimeError(
            "Replay fallback could not determine pipeline source attribute for "
            f"execution '{run.id}'."
        )

    module = _import_module_for_replay(module_name, run.id)

    pipeline_obj = getattr(module, source_attribute, None)
    if pipeline_obj is None or not hasattr(pipeline_obj, "replay"):
        raise KitaruRuntimeError(
            "Replay fallback expected a pipeline object with `.replay(...)` at "
            f"'{module_name}.{source_attribute}'."
        )
    return pipeline_obj


def _raise_if_running_source(run: PipelineRunResponse, execution: str) -> None:
    run_status_value = _run_status_value(run)
    if run_status_value in _RAW_STATUSES_BY_PUBLIC_STATUS[ExecutionStatus.RUNNING]:
        raise KitaruStateError(
            "Replay requires a non-running source execution. "
            f"Execution '{execution}' is currently '{run_status_value}'."
        )


def _run_status_value(run: PipelineRunResponse) -> str:
    """Return a pipeline run status as a plain string."""
    return str(getattr(run.status, "value", run.status))


def _exception_chain_contains(exc: BaseException, markers: tuple[str, ...]) -> bool:
    """Return whether an exception, cause, or context contains any marker."""
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop()
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)

        if any(marker in str(current) for marker in markers):
            return True

        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return False


def _is_duplicate_wait_condition_configuration_error(exc: BaseException) -> bool:
    """Return whether an exception chain contains ZenML's duplicate wait error."""
    return _exception_chain_contains(
        exc,
        (_DUPLICATE_WAIT_CONDITION_CONFIGURATION_ERROR,),
    )


def _has_operational_resume_failure_context(exc: BaseException) -> bool:
    """Return whether resume failure text includes operational recovery warnings."""
    return _exception_chain_contains(exc, _OPERATIONAL_RESUME_FAILURE_MARKERS)


def _duplicate_wait_condition_resume_message(exec_id: str) -> str:
    """Build the duplicate wait-condition resume failure message."""
    return (
        f"Unable to resume execution '{exec_id}' because the resumed run "
        "re-entered an existing wait condition, but the execution backend "
        "reported that the wait condition now has different configuration. "
        "Keep wait `name`, "
        "`question`, `type`, and `schema`/`data_schema` stable across resume. "
        "If the execution is still waiting for unresolved input, resolve that "
        "input first with:\n\n"
        f"  kitaru executions input {exec_id} --value '<json>'"
    )


def _rollback_reopened_run(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    operation_name: str,
    original_error: Exception,
    rollback_status: ZenMLExecutionStatus,
    rollback_reason: str,
) -> NoReturn:
    """Try to restore a run status after reopening it failed."""
    try:
        client._client().zen_store.update_run(
            run_id=run.id,
            run_update=PipelineRunUpdate(
                status=rollback_status,
                status_reason=rollback_reason,
            ),
        )
    except Exception as rollback_error:
        raise KitaruBackendError(
            f"Failed to {operation_name} execution '{run.id}': {original_error}. "
            "Additionally failed to roll back execution status to "
            f"'{rollback_status.value}': {rollback_error}. "
            "The execution may remain RESUMING."
        ) from original_error

    raise KitaruBackendError(
        f"Failed to {operation_name} execution '{run.id}': {original_error}"
    ) from original_error


def _repair_reopened_run_after_resume_failure(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    operation_name: str,
    original_error: Exception,
    rollback_status: ZenMLExecutionStatus,
    rollback_reason: str,
) -> NoReturn:
    """Repair a reopened run if ZenML did not move it out of RESUMING."""
    try:
        latest_run = client._get_pipeline_run(str(run.id), hydrate=False)
    except Exception as refresh_error:
        raise KitaruBackendError(
            f"Failed to {operation_name} execution '{run.id}': {original_error}. "
            "Could not verify whether the execution is still RESUMING: "
            f"{refresh_error}. The execution may remain RESUMING."
        ) from original_error

    if _run_status_value(latest_run) != ZenMLExecutionStatus.RESUMING.value:
        raise KitaruBackendError(
            f"Failed to {operation_name} execution '{run.id}': {original_error}"
        ) from original_error

    _rollback_reopened_run(
        run=run,
        client=client,
        operation_name=operation_name,
        original_error=original_error,
        rollback_status=rollback_status,
        rollback_reason=rollback_reason,
    )


def _restore_previous_active_stack(
    *,
    client: Client,
    old_stack_id: str | UUID,
) -> Exception | None:
    """Restore the active Kitaru stack and return the error if it fails."""
    try:
        client.activate_stack(old_stack_id)
    except Exception as exc:
        return exc
    return None


def _raise_with_restore_failure_context(
    *,
    error: KitaruBackendError,
    original_error: Exception,
    restoration_error: Exception | None,
) -> NoReturn:
    """Raise an operation failure, adding Kitaru stack restore context if needed."""
    if restoration_error is None:
        raise error
    raise KitaruBackendError(
        f"{error}. Additionally failed to restore the previous active Kitaru "
        f"stack: {restoration_error}."
    ) from original_error


def _restart_run_from_snapshot(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    operation_name: str,
    resuming_reason: str,
    rollback_status: ZenMLExecutionStatus,
    rollback_reason: str,
) -> None:
    """Restart an execution from its stored snapshot metadata."""
    snapshot = run.snapshot
    if snapshot is None:
        raise KitaruRuntimeError(
            f"Unable to {operation_name} execution because snapshot metadata "
            "is missing."
        )
    if snapshot.stack is None:
        raise KitaruRuntimeError(
            f"Unable to {operation_name} execution because snapshot stack "
            "metadata is missing."
        )
    snapshot_stack_id = getattr(snapshot.stack, "id", None)
    if not snapshot_stack_id:
        raise KitaruRuntimeError(
            f"Unable to {operation_name} execution because snapshot stack "
            "id is missing."
        )

    try:
        reopened_run = client._client().zen_store.update_run(
            run_id=run.id,
            run_update=PipelineRunUpdate(
                status=ZenMLExecutionStatus.RESUMING,
                status_reason=resuming_reason,
            ),
        )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to reopen execution '{run.id}' for {operation_name}: {exc}"
        ) from exc

    try:
        stack_client = Client()
        old_stack_id = cast(str | UUID, stack_client.active_stack_model.id)
    except Exception as exc:
        _rollback_reopened_run(
            run=reopened_run,
            client=client,
            operation_name=operation_name,
            original_error=exc,
            rollback_status=rollback_status,
            rollback_reason=rollback_reason,
        )

    try:
        stack_client.activate_stack(str(snapshot_stack_id))
    except Exception as exc:
        _rollback_reopened_run(
            run=reopened_run,
            client=client,
            operation_name=operation_name,
            original_error=exc,
            rollback_status=rollback_status,
            rollback_reason=rollback_reason,
        )

    try:
        active_stack = stack_client.active_stack
        orchestrator = cast(Any, active_stack.orchestrator)
    except Exception as exc:
        restoration_error = _restore_previous_active_stack(
            client=stack_client,
            old_stack_id=old_stack_id,
        )
        try:
            _rollback_reopened_run(
                run=reopened_run,
                client=client,
                operation_name=operation_name,
                original_error=exc,
                rollback_status=rollback_status,
                rollback_reason=rollback_reason,
            )
        except KitaruBackendError as operation_error:
            _raise_with_restore_failure_context(
                error=operation_error,
                original_error=exc,
                restoration_error=restoration_error,
            )

    try:
        orchestrator.resume_run(
            snapshot=snapshot,
            run=reopened_run,
            stack=active_stack,
        )
    except Exception as exc:
        restoration_error = _restore_previous_active_stack(
            client=stack_client,
            old_stack_id=old_stack_id,
        )
        try:
            _repair_reopened_run_after_resume_failure(
                run=reopened_run,
                client=client,
                operation_name=operation_name,
                original_error=exc,
                rollback_status=rollback_status,
                rollback_reason=rollback_reason,
            )
        except KitaruBackendError as operation_error:
            _raise_with_restore_failure_context(
                error=operation_error,
                original_error=exc,
                restoration_error=restoration_error,
            )

    restoration_error = _restore_previous_active_stack(
        client=stack_client,
        old_stack_id=old_stack_id,
    )
    if restoration_error is not None:
        raise KitaruBackendError(
            f"The {operation_name} request for execution '{run.id}' was submitted, "
            "but restoring the previous active Kitaru stack failed: "
            f"{restoration_error}. The execution may continue; inspect its latest "
            "status before retrying."
        ) from restoration_error


def _validate_non_empty_string_list(
    values: Sequence[str] | None,
    *,
    name: str,
) -> builtins.list[str]:
    """Normalize a sequence containing only non-empty strings."""
    if values is None:
        return []
    if isinstance(values, (str, bytes)):
        raise KitaruUsageError(f"`{name}` must be a list of non-empty strings.")
    if not isinstance(values, Sequence):
        raise KitaruUsageError(f"`{name}` must be a list of non-empty strings.")

    normalized: builtins.list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise KitaruUsageError(f"`{name}` values must be strings.")
        text = value.strip()
        if not text:
            raise KitaruUsageError(f"`{name}` values must be non-empty strings.")
        normalized.append(text)
    return normalized


def _validate_event_kind_filter_values(
    values: Sequence[str] | None,
) -> builtins.list[str]:
    """Validate event-kind filters for execution event watching."""
    normalized = _validate_non_empty_string_list(values, name="kinds")
    for kind in normalized:
        if "\n" in kind or "\r" in kind:
            raise KitaruUsageError("`kinds` values cannot contain newline characters.")
    return normalized


def _validate_optional_event_filter_value(
    value: str | None,
    *,
    name: str,
) -> str | None:
    """Validate an optional single string filter for execution events."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise KitaruUsageError(f"`{name}` must be a string when provided.")
    normalized = value.strip()
    if not normalized:
        raise KitaruUsageError(f"`{name}` must be non-empty when provided.")
    return normalized


def _run_has_complete_step_list(run: Any) -> bool:
    """Return whether run steps are stable enough for server-side filters."""
    return _to_public_status(run.status).is_finished


def _pipeline_name_filter_value(flow: str) -> str:
    """Return the backend filter value for both stored names of a Kitaru flow."""
    candidates = [flow, f"{_PIPELINE_SOURCE_ALIAS_PREFIX}{flow}"]
    return _backend_filter_value(candidates)


def _list_status_filter_value(public_status: ExecutionStatus) -> str:
    """Return the safest backend status filter for execution listing."""
    if public_status in {ExecutionStatus.RUNNING, ExecutionStatus.WAITING}:
        raw_statuses = (
            *_RAW_STATUSES_BY_PUBLIC_STATUS[ExecutionStatus.RUNNING],
            *_RAW_STATUSES_BY_PUBLIC_STATUS[ExecutionStatus.WAITING],
        )
        return _backend_filter_value(raw_statuses)
    status_value = _status_filter_value(public_status)
    assert status_value is not None
    return status_value


class _ExecutionsAPI:
    """Namespace for execution lifecycle and inspection operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def _require_rest_store(self, unavailable_error: Exception) -> RestZenStore:
        """Return the active REST store or raise the caller-specific error."""
        zen_store = self._client_ref._client().zen_store
        if isinstance(zen_store, RestZenStore):
            return zen_store
        raise unavailable_error

    def _rest_store(self) -> RestZenStore:
        """Return a REST-backed zen store required for runtime log retrieval."""
        return self._require_rest_store(
            KitaruLogRetrievalError(
                "Runtime log retrieval requires a server-backed connection. "
                "Local database mode does not expose execution log endpoints."
            )
        )

    def _event_rest_store(self) -> RestZenStore:
        """Return a REST-backed zen store required for event watching."""
        return self._require_rest_store(
            KitaruFeatureNotAvailableError(
                "Execution event watching requires a server-backed Kitaru "
                "connection. Local database mode does not expose live event "
                "streams."
            )
        )

    def _resolve_log_endpoint_hint(self) -> str | None:
        """Resolve a best-effort endpoint hint for log-retrieval errors."""
        active_log_store = active_stack_log_store()
        if active_log_store is not None and active_log_store.endpoint:
            return active_log_store.endpoint

        try:
            preferred_log_store = resolve_log_store()
        except ValueError:
            return None

        return preferred_log_store.endpoint

    def _fetch_log_payload(
        self,
        *,
        path: str,
        source: str,
    ) -> builtins.list[Mapping[str, Any]]:
        """Call a log endpoint and normalize the response payload shape."""
        store = self._rest_store()

        try:
            payload = store.get(path, params={"source": source})
        except Exception as exc:
            error_message = str(exc)
            if _is_empty_log_result_error(error_message):
                return []

            if _is_otel_log_retrieval_error(error_message):
                endpoint_hint = self._resolve_log_endpoint_hint()
                message = (
                    "Logs for this execution are stored in an OTEL backend and "
                    "cannot be fetched via the Kitaru log retrieval API."
                )
                if endpoint_hint:
                    message += f" View them in your OTEL backend at: {endpoint_hint}."
                raise KitaruLogRetrievalError(message) from exc

            if _is_log_endpoint_version_skew_error(error_message):
                raise KitaruLogRetrievalError(
                    "Unable to retrieve runtime logs because the server log "
                    "endpoint is incompatible with this Kitaru client. This "
                    "usually means the client and server are running different "
                    "Kitaru versions. Upgrade the server runtime or align the "
                    "client and server versions, then retry `kitaru executions "
                    "logs`."
                ) from exc

            raise KitaruLogRetrievalError(
                f"Failed to retrieve runtime logs for source '{source}': {exc}"
            ) from exc

        if not isinstance(payload, list):
            raise KitaruLogRetrievalError(
                "Unexpected response while retrieving runtime logs: "
                "expected a list payload."
            )

        normalized_payload: builtins.list[Mapping[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise KitaruLogRetrievalError(
                    "Unexpected log entry payload type returned by the server."
                )
            normalized_payload.append(entry)

        return normalized_payload

    def logs(
        self,
        exec_id: str,
        *,
        checkpoint: str | None = None,
        source: str = "step",
        limit: int | None = None,
    ) -> builtins.list[LogEntry]:
        """Fetch runtime log entries for an execution."""
        normalized_source = _normalize_log_source(source)
        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        normalized_checkpoint: str | None = None
        if checkpoint is not None:
            normalized_checkpoint = checkpoint.strip()
            if not normalized_checkpoint:
                raise KitaruUsageError("`checkpoint` must be non-empty when provided.")

        if normalized_source == "runner" and normalized_checkpoint is not None:
            raise KitaruUsageError(
                "`checkpoint` cannot be combined with `source='runner'`."
            )

        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)

        if normalized_source == "runner":
            run_payload = self._fetch_log_payload(
                path=f"/runs/{run.id}/logs",
                source=normalized_source,
            )
            run_entries = [
                _map_runtime_log_entry(
                    raw_entry,
                    source=normalized_source,
                    checkpoint_name=None,
                )
                for raw_entry in run_payload
            ]
            sorted_run_entries = _sort_log_entries(run_entries)
            if limit is not None:
                return sorted_run_entries[:limit]
            return sorted_run_entries

        step_runs = sorted(run.steps.values(), key=_step_log_fetch_order_key)
        if normalized_checkpoint is not None:
            step_runs = [
                step
                for step in step_runs
                if _normalize_checkpoint_name(step.name) == normalized_checkpoint
            ]

        if not step_runs:
            return []

        entries: list[LogEntry] = []
        for step in step_runs:
            checkpoint_name = _normalize_checkpoint_name(step.name)
            step_payload = self._fetch_log_payload(
                path=f"/steps/{step.id}/logs",
                source=normalized_source,
            )
            entries.extend(
                _map_runtime_log_entry(
                    raw_entry,
                    source=normalized_source,
                    checkpoint_name=checkpoint_name,
                )
                for raw_entry in step_payload
            )

            if limit is not None and len(entries) >= limit:
                break

        sorted_entries = _sort_log_entries(entries)
        if limit is not None:
            return sorted_entries[:limit]
        return sorted_entries

    def events(
        self,
        exec_id: str,
        *,
        kinds: builtins.list[str] | None = None,
        checkpoint: str | None = None,
        correlation_ids: builtins.list[str] | None = None,
        since: str | None = None,
        reconnect: bool = True,
    ) -> Iterator[ExecutionEvent]:
        """Watch live events for an execution.

        The stream uses the backend SSE cursor for reconnects. Event indexes and
        correlation IDs describe event identity/order, but they are never used
        as network resume positions.
        """
        normalized_kinds = _validate_event_kind_filter_values(kinds)
        normalized_correlation_ids = _validate_non_empty_string_list(
            correlation_ids,
            name="correlation_ids",
        )
        normalized_checkpoint = _validate_optional_event_filter_value(
            checkpoint,
            name="checkpoint",
        )
        normalized_since = _validate_optional_event_filter_value(since, name="since")

        store = self._event_rest_store()
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=False)

        params: list[tuple[str, str]] = []
        if normalized_since is not None:
            params.append(("since", normalized_since))
        params.extend(("kinds", kind) for kind in normalized_kinds)
        params.extend(
            ("correlation_ids", correlation_id)
            for correlation_id in normalized_correlation_ids
        )

        if normalized_checkpoint is not None and _run_has_complete_step_list(run):
            hydrated_run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
            step_names = [
                step.name
                for step in hydrated_run.steps.values()
                if _normalize_checkpoint_name(step.name) == normalized_checkpoint
            ]
            params.extend(("step_names", step_name) for step_name in step_names)

        path = f"/runs/{run.id}/events/stream"

        def _open_stream(last_event_id: str | None) -> Any:
            return open_rest_sse_stream(
                cast(StreamingStore, store),
                path=path,
                params=params,
                last_event_id=last_event_id,
            )

        return watch_execution_events(
            open_stream=_open_stream,
            fallback_exec_id=str(run.id),
            checkpoint=normalized_checkpoint,
            reconnect=reconnect,
        )

    def pending_waits(self, exec_id: str) -> builtins.list[PendingWait]:
        """List all pending wait conditions for an execution."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        return [_map_pending_wait(condition) for condition in conditions]

    def _resolve_wait_condition(
        self,
        exec_id: str,
        *,
        wait: str,
        resolution: str,
        value: Any | None = None,
    ) -> Execution:
        """Resolve a pending wait condition with the given resolution."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        pending_conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        if not pending_conditions:
            raise KitaruStateError(
                f"Execution '{exec_id}' has no pending waits to resolve."
            )

        condition = _select_pending_wait_condition(
            run=run,
            wait=wait,
            pending_conditions=pending_conditions,
        )

        try:
            cast(Any, self._client_ref._client()).resolve_run_wait_condition(
                run_wait_condition_id=condition.id,
                resolution=cast(Any, resolution),
                result=value,
            )
        except (ValidationError, TypeError, ValueError) as exc:
            raise KitaruWaitValidationError(
                "Wait input failed validation for "
                f"'{condition.name}' on execution '{exec_id}': {exc}"
            ) from exc
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to resolve wait condition "
                f"'{condition.name}' for execution '{exec_id}': {exc}"
            ) from exc

        track(
            AnalyticsEvent.WAIT_RESOLVED,
            {
                "resolution": resolution,
            },
        )

        return self.get(exec_id)

    def input(self, exec_id: str, *, wait: str, value: Any) -> Execution:
        """Provide input to a waiting execution."""
        return self._resolve_wait_condition(
            exec_id,
            wait=wait,
            resolution=_WAIT_CONDITION_RESOLUTION_CONTINUE,
            value=value,
        )

    def abort_wait(self, exec_id: str, *, wait: str) -> Execution:
        """Abort a pending wait condition on an execution."""
        return self._resolve_wait_condition(
            exec_id,
            wait=wait,
            resolution=_WAIT_CONDITION_RESOLUTION_ABORT,
        )

    def retry(self, exec_id: str) -> Execution:
        """Retry a failed execution as same-execution recovery."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        raise_if_imported_execution(run, "retried")
        run_status_value = _run_status_value(run)
        if run_status_value != ZenMLExecutionStatus.FAILED.value:
            raise KitaruStateError(
                "Only failed executions can be retried. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        _restart_run_from_snapshot(
            run=run,
            client=self._client_ref,
            operation_name="retry",
            resuming_reason=_RETRY_RESUMING_REASON,
            rollback_status=ZenMLExecutionStatus.FAILED,
            rollback_reason=_RETRY_ROLLBACK_REASON,
        )
        track(AnalyticsEvent.EXECUTION_RETRIED, {})
        return self.get(exec_id)

    def resume(self, exec_id: str) -> Execution:
        """Resume a paused execution after all waits are resolved."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        raise_if_imported_execution(run, "resumed")
        pending_conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        if pending_conditions:
            raise KitaruStateError(
                f"Resolve pending wait input before resuming execution '{exec_id}'."
            )

        run_status_value = _run_status_value(run)
        if run_status_value != ZenMLExecutionStatus.PAUSED.value:
            raise KitaruStateError(
                "Only paused executions can be resumed. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        try:
            _restart_run_from_snapshot(
                run=run,
                client=self._client_ref,
                operation_name="resume",
                resuming_reason=_RESUME_RESUMING_REASON,
                rollback_status=ZenMLExecutionStatus.PAUSED,
                rollback_reason=_RESUME_ROLLBACK_REASON,
            )
        except KitaruBackendError as exc:
            if _has_operational_resume_failure_context(exc):
                raise
            if _is_duplicate_wait_condition_configuration_error(exc):
                raise KitaruStateError(
                    _duplicate_wait_condition_resume_message(exec_id)
                ) from exc
            raise
        track(AnalyticsEvent.EXECUTION_RESUMED, {})
        return self.get(exec_id)

    def _await_replay_completion(self, handle_or_run: Any) -> str:
        """Block until a replay finishes and roll up terminal LLM usage metadata."""
        from kitaru.flow import FlowHandle

        if isinstance(handle_or_run, FlowHandle) or callable(
            getattr(handle_or_run, "wait", None)
        ):
            handle = handle_or_run
        else:
            handle = FlowHandle(handle_or_run, project=self._client_ref._project)
        handle.wait()
        return str(handle.exec_id)

    def _persist_replay_terminal_llm_usage_if_terminal(self, exec_id: str) -> None:
        """Best-effort terminal LLM rollup after replay metadata exists."""
        try:
            run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        except Exception:
            logger.debug(
                "Failed to refresh replay execution before terminal LLM usage "
                "aggregation.",
                exc_info=True,
            )
            return
        if not getattr(getattr(run, "status", None), "is_finished", False):
            return
        try:
            zenml_client = self._client_ref._client()
        except Exception:
            logger.debug(
                "Failed to create ZenML client for replay terminal LLM usage "
                "aggregation.",
                exc_info=True,
            )
            return
        _safe_persist_terminal_llm_usage_metadata(
            run,
            zenml_client=zenml_client,
        )

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
    ) -> ReplaySubmission:
        """Replay one or more explicit executions from a checkpoint cut point."""
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

        first_run = self._client_ref._get_pipeline_run(exec_ids[0], hydrate=True)
        raise_if_imported_execution(first_run, "replayed")
        _raise_if_running_source(first_run, exec_ids[0])

        replay_flow: _ReplayFlowLike | None = None
        try:
            replay_flow = _resolve_flow_for_replay(first_run)
        except _ReplayImportDependencyError:
            raise
        except KitaruRuntimeError:
            replay_flow = None

        if replay_flow is not None:
            result = replay_flow.replay(
                exec_ids[0] if len(exec_ids) == 1 else exec_ids,
                at=at,
                flow_overrides=flow_overrides,
                checkpoint_overrides=checkpoint_overrides,
                invocation_overrides=invocation_overrides,
                skip=skip,
                tag=tag,
                wait=resolved_wait,
                on_error=resolved_on_error,
            )
            if isinstance(result, ReplaySubmission):
                return result
            # Compatibility with any locally imported old flow wrapper.
            replay_exec_id = (
                self._await_replay_completion(result)
                if resolved_wait
                else str(result.exec_id)
            )
            execution_obj = self.get(replay_exec_id) if resolved_wait else None
            return ReplaySubmission.create(
                tag=tag,
                at=at,
                wait=resolved_wait,
                plan=build_replay_request_document(flow_overrides=flow_overrides),
                results=[
                    ReplayResultRow(
                        original_exec_ref=exec_ids[0],
                        original_exec_id=str(first_run.id),
                        replay_exec_id=replay_exec_id,
                        status=(
                            "completed" if execution_obj is not None else "submitted"
                        ),
                        compare_url=safe_compare_url_for_executions(
                            [str(first_run.id), replay_exec_id]
                        ),
                        handle=None if resolved_wait else result,
                    )
                ],
            )

        return self._replay_via_pipeline_fallback(
            exec_ids,
            at=at,
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
            tag=tag,
            wait=resolved_wait,
            on_error=resolved_on_error,
            prefetched_runs={exec_ids[0]: first_run},
        )

    def _replay_via_pipeline_fallback(
        self,
        executions: Sequence[str],
        *,
        at: str,
        flow_overrides: Mapping[str, Any] | None,
        checkpoint_overrides: Mapping[str, Any] | None,
        invocation_overrides: Mapping[str, Any] | None,
        skip: Sequence[str] | None,
        tag: str | None,
        wait: bool,
        on_error: Literal["collect", "fail"],
        prefetched_runs: Mapping[str, PipelineRunResponse] | None = None,
    ) -> ReplaySubmission:
        """Replay through ZenML pipeline fallback when no Kitaru flow wrapper exists."""
        from kitaru.flow import FlowHandle, _temporary_active_project

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
        compare_ids: list[str] = []

        for exec_ref in executions:
            original_id: str | None = None
            try:
                source_run = (prefetched_runs or {}).get(exec_ref)
                if source_run is None:
                    source_run = self._client_ref._get_pipeline_run(
                        exec_ref, hydrate=True
                    )
                raise_if_imported_execution(source_run, "replayed")
                _raise_if_running_source(source_run, exec_ref)
                original_id = str(source_run.id)
                if on_error == "collect":
                    at_status = replay_at_status(run=source_run, at=at)
                    if at_status in {"missing", "no_checkpoints"}:
                        skipped_rows.append(
                            ReplaySkippedRow(
                                original_exec_ref=exec_ref,
                                original_exec_id=original_id,
                                reason=replay_at_skip_reason(run=source_run, at=at),
                            )
                        )
                        continue
                    if at_status == "ambiguous":
                        failures.append(
                            ReplayFailureRow(
                                original_exec_ref=exec_ref,
                                original_exec_id=original_id,
                                reason=replay_at_skip_reason(run=source_run, at=at),
                            )
                        )
                        continue

                replay_pipeline = _resolve_pipeline_for_replay(source_run)
                replay_plan = build_replay_plan(
                    run=source_run,
                    at=at,
                    flow_overrides=flow_overrides,
                    checkpoint_overrides=checkpoint_overrides,
                    invocation_overrides=invocation_overrides,
                    skip=skip,
                )
                if plan_requires_runtime_transport(replay_plan):
                    raise KitaruRuntimeError(
                        "Replay request includes runtime-only overrides (`code`, "
                        "targeted `model`, or adapter `input`), but Kitaru could "
                        "not resolve the flow wrapper needed to transport "
                        "KITARU_REPLAY_CONTEXT. Run replay from the project "
                        "directory or remove those overrides."
                    )
                plan_document = replay_plan.document

                replay_metadata: dict[str, Any] = {
                    "at_checkpoint": at,
                    "replay_path": "pipeline_fallback",
                }
                track(AnalyticsEvent.REPLAY_REQUESTED, replay_metadata)

                try:
                    with _temporary_active_project(self._client_ref._project):
                        replayed_run = replay_pipeline.replay(
                            pipeline_run=source_run.id,
                            skip=replay_plan.steps_to_skip,
                            skip_successful_steps=False,
                            input_overrides=replay_plan.input_overrides or None,
                            step_input_overrides=replay_plan.step_input_overrides
                            or None,
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
                            "Replay divergence detected for execution "
                            f"'{exec_ref}': {exc}",
                            exec_id=str(source_run.id),
                            status="failed",
                            origin=failure_origin,
                        ) from exc
                    raise KitaruBackendError(
                        f"Failed to replay execution '{exec_ref}': {exc}"
                    ) from exc

                replayed_exec_id = str(getattr(replayed_run, "id", ""))
                if not replayed_exec_id:
                    track(
                        AnalyticsEvent.REPLAY_FAILED,
                        {
                            **replay_metadata,
                            "error_type": "KitaruRuntimeError",
                            "failure_origin": FailureOrigin.RUNTIME.value,
                        },
                    )
                    raise KitaruRuntimeError(
                        "Replay did not produce a pipeline run ID."
                    )

                track(
                    AnalyticsEvent.FLOW_REPLAYED,
                    {"replay_path": "pipeline_fallback"},
                )
                replay_handle = FlowHandle(
                    replayed_run,
                    project=self._client_ref._project,
                )
                with _temporary_active_project(self._client_ref._project):
                    safe_persist_replay_submission_metadata(
                        replay_exec_id=replayed_exec_id,
                        original_exec_id=original_id,
                        submission_id=submission_id,
                        tag=tag,
                        steps_to_skip=replay_plan.steps_to_skip,
                        replay_plan=replay_plan.document,
                    )
                row_status: Literal["submitted", "completed", "failed"] = "submitted"
                if wait:
                    replayed_exec_id = self._await_replay_completion(replay_handle)
                    row_status = "completed"
                elif getattr(
                    getattr(replayed_run, "status", None), "is_finished", False
                ):
                    self._persist_replay_terminal_llm_usage_if_terminal(
                        replayed_exec_id
                    )
                compare_ids.extend([original_id, replayed_exec_id])
                results.append(
                    ReplayResultRow(
                        original_exec_ref=exec_ref,
                        original_exec_id=original_id,
                        replay_exec_id=replayed_exec_id,
                        status=row_status,
                        compare_url=safe_compare_url_for_executions(
                            [original_id, replayed_exec_id]
                        ),
                        handle=None if wait else replay_handle,
                    )
                )
            except Exception as exc:
                if on_error == "fail":
                    raise
                failures.append(
                    ReplayFailureRow(
                        original_exec_ref=exec_ref,
                        original_exec_id=original_id,
                        reason=str(exc),
                    )
                )

        return ReplaySubmission.create(
            submission_id=submission_id,
            tag=tag,
            at=at,
            wait=wait,
            plan=plan_document,
            results=results,
            failures=failures,
            skipped=skipped_rows,
            compare_url=safe_compare_url_for_executions(compare_ids),
        )

    def cohort(
        self,
        *,
        flow: str,
        at: str,
        deployment: str | None = None,
        deployment_version: int | None = None,
        order_by: str = "-started_at",
        limit: int = 50,
        originals_only: bool = True,
        status: str | Sequence[str] = "completed",
        since: Any = None,
        until: Any = None,
    ) -> Any:
        """Build a cohort selection query for ``resolve()``."""
        from kitaru.cohort import cohort as build_cohort_query

        return build_cohort_query(
            flow=flow,
            at=at,
            deployment=deployment,
            deployment_version=deployment_version,
            order_by=order_by,
            limit=limit,
            originals_only=originals_only,
            status=status,
            since=since,
            until=until,
            client=self._client_ref,
        )

    def _list_experiment_replays(self, exec_id: str) -> builtins.list[Execution]:
        """Resolve verified tagged replay descendants without hydrating DAGs."""
        experiments = self._client_ref.agents.experiments.list_for_execution(
            exec_id,
            agent=self._client_ref._project,
        )
        results: list[Execution] = []
        seen_ids: set[str] = set()
        page_size = 100
        for experiment in experiments:
            page = 1
            while True:
                run_page = experiment.runs.list(page=page, size=page_size)
                runs = list(getattr(run_page, "items", run_page))
                for run in runs:
                    metadata = _to_plain_dict(getattr(run, "run_metadata", {}))
                    if (
                        metadata.get(EXPERIMENT_TARGET_EXECUTION_ID_METADATA_KEY)
                        != exec_id
                    ):
                        continue
                    run_id = str(getattr(run, "id", "")).strip()
                    if not run_id or run_id in seen_ids:
                        continue
                    seen_ids.add(run_id)
                    results.append(
                        _map_execution(
                            run=run,
                            client=self._client_ref,
                            include_details=False,
                        )
                    )
                if len(runs) < page_size:
                    break
                page += 1
        return results

    def get(self, exec_id: str) -> Execution:
        """Get and map one execution by ID."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        return _map_execution(run=run, client=self._client_ref, include_details=True)

    def _list_replays_for_originals(
        self,
        *,
        original_exec_ids: Sequence[str],
        expected_flow_name: str | None,
        limit: int,
    ) -> tuple[builtins.list[_ReplayLink], bool]:
        """List replay executions linked to any requested original.

        Scans up to ``limit`` lightweight executions for one flow in backend
        order. ``expected_flow_name=None`` selects only executions whose flow
        name is unavailable; it does not disable flow filtering. Returns
        matching native replay links and whether older executions in the scan remain.
        """
        normalized_ids = _validate_non_empty_string_list(
            original_exec_ids,
            name="original_exec_ids",
        )
        if not normalized_ids:
            raise KitaruUsageError("`original_exec_ids` must contain at least one ID.")
        if isinstance(limit, bool) or limit < 1:
            raise KitaruUsageError("`limit` must be >= 1.")
        if (
            expected_flow_name is not None
            and _normalize_flow_name(expected_flow_name) is None
        ):
            return [], False

        server_filters: dict[str, Any] = {}
        if expected_flow_name is not None:
            server_filters["pipeline_name"] = _pipeline_name_filter_value(
                expected_flow_name
            )

        original_ids = set(normalized_ids)
        results: builtins.list[_ReplayLink] = []
        runs = depaginate_stream(
            self._client_ref._client().list_pipeline_runs,
            sort_by="desc:created",
            page=1,
            size=100,
            project=self._client_ref._project,
            hydrate=False,
            **server_filters,
        )
        for scanned_count, run in enumerate(islice(runs, limit + 1), start=1):
            if scanned_count > limit:
                return results, True

            pipeline = getattr(run, "pipeline", None)
            raw_flow_name = (
                _normalize_flow_name(pipeline.name) if pipeline is not None else None
            )
            if raw_flow_name != expected_flow_name:
                continue

            original_run = getattr(run, "original_run", None)
            raw_original_id = str(original_run.id) if original_run is not None else None
            if raw_original_id not in original_ids:
                continue

            results.append(
                _ReplayLink(
                    exec_id=str(run.id),
                    original_exec_id=raw_original_id,
                )
            )

        return results, False

    def evaluate(
        self,
        executions: str | Execution | Sequence[str | Execution],
        scorers: Sequence[Any] | Any,
        *,
        name: str | None = None,
        suite_key: str | None = None,
        idempotency_key: str | None = None,
        comparative: bool | None = None,
        metadata: Mapping[str, Any] | None = None,
        grounded_policy: Any | None = None,
        grounded_capabilities: Mapping[str, Any] | None = None,
    ) -> Any:
        """Evaluate stored executions without running replay or agent code."""
        execution_items: list[str | Execution]
        if isinstance(executions, (str, Execution)):
            execution_items = [executions]
        else:
            execution_items = list(executions)
        scorer_items = (
            list(scorers)
            if isinstance(scorers, Sequence) and not callable(scorers)
            else [scorers]
        )
        service = ScoreEvaluationService(
            project_id=self._project_id(),
            client=self._client_ref._client(),
            run_loader=lambda exec_id: self._client_ref._get_pipeline_run(
                exec_id, hydrate=True
            ),
        )
        return service.evaluate(
            execution_items,
            scorer_items,
            name=name,
            suite_key=suite_key,
            idempotency_key=idempotency_key,
            comparative=comparative,
            metadata=metadata,
            grounded_policy=grounded_policy,
            grounded_capabilities=grounded_capabilities,
        )

    def score_history(
        self,
        exec_id: str,
        *,
        experiment_id: str | None = None,
        scorer_name: str | None = None,
        scorer_revision: str | None = None,
        scorer_configuration_hash: str | None = None,
        valid: bool | None = None,
        include_superseded: bool = True,
    ) -> builtins.list[ScoreObservation]:
        """Return append-only score observations for one execution."""
        from kitaru.scoring import ScoreObservationRepository

        repo = ScoreObservationRepository(
            project_id=self._project_id(),
            client=self._client_ref._client(),
        )
        query = ObservationQuery(
            execution_id=exec_id,
            experiment_id=experiment_id,
            scorer_name=scorer_name,
            scorer_revision=scorer_revision,
            scorer_configuration_hash=scorer_configuration_hash,
            valid=valid,
            include_superseded=include_superseded,
        )
        observations: list[ScoreObservation] = []
        page = 1
        page_size = 1000
        while True:
            chunk = repo.list(query, page=page, size=page_size)
            observations.extend(chunk)
            if len(chunk) < page_size:
                break
            page += 1
        return observations

    def latest_valid_score(
        self,
        exec_id: str,
        *,
        scorer_name: str | None = None,
        scorer_revision: str | None = None,
        scorer_configuration_hash: str | None = None,
    ) -> ScoreObservation | None:
        """Return latest valid score in one explicit scorer revision/config scope."""
        from kitaru.scoring import ScoreObservationRepository

        repo = ScoreObservationRepository(
            project_id=self._project_id(),
            client=self._client_ref._client(),
        )
        latest = repo.latest_valid(
            ObservationQuery(
                execution_id=exec_id,
                scorer_name=scorer_name,
                scorer_revision=scorer_revision,
                scorer_configuration_hash=scorer_configuration_hash,
            )
        )
        if latest is None:
            return None
        if scorer_revision is None or scorer_configuration_hash is None:
            history = repo.list(
                ObservationQuery(
                    execution_id=exec_id,
                    scorer_name=scorer_name,
                    valid=True,
                    status=ScoreObservationStatus.SCORED,
                    include_superseded=False,
                ),
                page=1,
                size=1000,
            )
            scopes = {
                (item.scorer.name, item.scorer.revision, item.scorer.configuration_hash)
                for item in history
            }
            if len(scopes) > 1:
                raise KitaruUsageError(
                    "Latest valid score is ambiguous across scorer revisions/"
                    "configurations. Pass scorer_revision and "
                    "scorer_configuration_hash."
                )
        return latest

    def _project_id(self) -> str:
        project = getattr(self._client_ref._client(), "active_project", None)
        project_id = str(
            getattr(project, "id", "") or self._client_ref._project or ""
        ).strip()
        if not project_id:
            raise KitaruStateError("Scoring requires an active Agent Project.")
        return project_id

    def _score_candidate_ids(self, score: ScoreFilter | None) -> set[str] | None:
        if score is None or score.is_empty:
            return None
        from kitaru.scoring import ScoreObservationRepository

        repo = ScoreObservationRepository(
            project_id=self._project_id(),
            client=self._client_ref._client(),
        )
        status = (
            ScoreObservationStatus.SCORED
            if score.minimum is not None or score.maximum is not None
            else None
        )
        return repo.matching_execution_ids(
            ObservationQuery(
                experiment_id=score.experiment_id,
                scorer_name=score.scorer_name,
                scorer_revision=score.scorer_revision,
                scorer_configuration_hash=score.scorer_configuration_hash,
                status=status,
                valid=score.valid,
                include_superseded=False,
            ),
            minimum=score.minimum,
            maximum=score.maximum,
            cap=score.candidate_cap,
        )

    def list(
        self,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
        limit: int | None = None,
        page: int | None = None,
        size: int | None = None,
        score: ScoreFilter | None = None,
    ) -> builtins.list[Execution]:
        """List executions with optional execution and score filters."""
        status_filter = _coerce_status_filter(status)

        if limit is not None:
            if isinstance(limit, bool) or limit < 1:
                raise KitaruUsageError("`limit` must be >= 1 when provided.")
            if page is not None or size is not None:
                raise KitaruUsageError(
                    "`limit` cannot be combined with `page` or `size`."
                )
        if page is not None and (isinstance(page, bool) or page < 1):
            raise KitaruUsageError("`page` must be >= 1 when provided.")
        if size is not None and (isinstance(size, bool) or size < 1):
            raise KitaruUsageError("`size` must be >= 1 when provided.")
        if page is not None and size is None:
            raise KitaruUsageError("`size` is required when `page` is provided.")
        if size is not None and page is None:
            page = 1

        if flow is not None and _normalize_flow_name(flow) is None:
            return []

        start_index = 0
        stop_index: int | None = None
        if limit is not None:
            stop_index = limit
        elif size is not None:
            assert page is not None
            start_index = (page - 1) * size
            stop_index = start_index + size

        results: list[Execution] = []
        matched_count = 0
        backend_page = 1
        if limit is not None:
            page_size = max(50, limit)
        elif size is not None:
            page_size = max(50, size)
        else:
            page_size = 50

        server_filters: dict[str, Any] = {}
        if flow is not None:
            server_filters["pipeline_name"] = _pipeline_name_filter_value(flow)
        if status_filter is not None:
            server_filters["status"] = _list_status_filter_value(status_filter)
        resolve_wait_status = status_filter in {
            ExecutionStatus.RUNNING,
            ExecutionStatus.WAITING,
        }
        score_candidate_ids = self._score_candidate_ids(score)
        if score_candidate_ids == set():
            return []

        while True:
            run_page = self._client_ref._client().list_pipeline_runs(
                sort_by="desc:created",
                page=backend_page,
                size=page_size,
                project=self._client_ref._project,
                hydrate=True,
                **server_filters,
            )
            runs = list(run_page.items)
            if not runs:
                break

            for run in runs:
                execution = _map_execution(
                    run=run,
                    client=self._client_ref,
                    include_details=False,
                    resolve_wait_status=resolve_wait_status,
                )

                # Server filters only reduce fetched runs; these public checks
                # still decide the final result because flow/status can be derived.
                if flow is not None and execution.flow_name != flow:
                    continue
                if status_filter is not None and execution.status != status_filter:
                    continue
                if (
                    score_candidate_ids is not None
                    and execution.exec_id not in score_candidate_ids
                ):
                    continue

                if matched_count >= start_index:
                    results.append(execution)
                matched_count += 1

                if stop_index is not None and matched_count >= stop_index:
                    return results

            if len(runs) < page_size:
                break
            backend_page += 1

        return results

    def statistics(
        self,
        *,
        group_by: Sequence[ExecutionStatisticsGrouping | str] = (),
        metrics: Sequence[ExecutionStatisticsMetric | Mapping[str, Any] | str] = (),
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
        stack: str | None = None,
        tags: Sequence[str] | None = None,
        max_groups: int = 1000,
    ) -> ExecutionStatistics:
        """Return grouped execution statistics with optional numeric metrics."""
        statistics = get_execution_statistics(
            client=self._client_ref,
            group_by=group_by,
            metrics=metrics,
            flow=flow,
            status=status,
            stack=stack,
            tags=tags,
            max_groups=max_groups,
        )

        normalized_groupings = normalize_execution_statistics_groupings(group_by)
        normalized_metrics = normalize_execution_statistics_metrics(metrics)
        grouping_dimensions = {grouping.dimension for grouping in normalized_groupings}
        metric_sources = {metric.source for metric in normalized_metrics}
        track(
            AnalyticsEvent.EXECUTION_STATISTICS_QUERIED,
            {
                "grouping_count": len(normalized_groupings),
                "metric_count": len(normalized_metrics),
                "has_duration_metric": (
                    ExecutionStatisticsMetricSource.DURATION in metric_sources
                ),
                "has_step_count_metric": (
                    ExecutionStatisticsMetricSource.STEP_COUNT in metric_sources
                ),
                "has_cached_step_count_metric": (
                    ExecutionStatisticsMetricSource.CACHED_STEP_COUNT in metric_sources
                ),
                "has_output_artifact_count_metric": (
                    ExecutionStatisticsMetricSource.OUTPUT_ARTIFACT_COUNT
                    in metric_sources
                ),
                "has_metadata_metric": (
                    ExecutionStatisticsMetricSource.METADATA in metric_sources
                ),
                "has_status_grouping": (
                    ExecutionStatisticsDimension.STATUS in grouping_dimensions
                ),
                "has_flow_grouping": (
                    ExecutionStatisticsDimension.FLOW in grouping_dimensions
                ),
                "has_stack_grouping": (
                    ExecutionStatisticsDimension.STACK in grouping_dimensions
                ),
                "has_tag_grouping": (
                    ExecutionStatisticsDimension.TAG in grouping_dimensions
                ),
                "has_time_grouping": (
                    ExecutionStatisticsDimension.TIME in grouping_dimensions
                ),
                "has_metadata_grouping": (
                    ExecutionStatisticsDimension.METADATA in grouping_dimensions
                ),
                "has_flow_filter": flow is not None,
                "has_status_filter": status is not None,
                "has_stack_filter": stack is not None,
                "tag_filter_count": len(tags or ()),
                "max_groups": max_groups,
                "result_group_count": len(statistics.groups),
                "truncated": statistics.truncated,
            },
        )
        return statistics

    def latest(
        self,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
    ) -> Execution:
        """Return the most recent execution for a filter set."""
        executions = self.list(flow=flow, status=status, limit=1)
        if not executions:
            filters: list[str] = []
            if flow is not None:
                filters.append(f"flow={flow!r}")
            if status is not None:
                filters.append(f"status={str(status)!r}")
            where = " and ".join(filters) if filters else "the current project"
            raise LookupError(f"No executions found for {where}.")
        return executions[0]

    def cancel(self, exec_id: str) -> Execution:
        """Cancel an execution if supported by the backend state."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        raise_if_imported_execution(run, "cancelled")
        stop_run(run=run, graceful=False)
        track(AnalyticsEvent.EXECUTION_CANCELLED, {})
        return self.get(exec_id)


class _ArtifactsAPI:
    """Namespace for artifact browsing operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def list(
        self,
        exec_id: str,
        *,
        name: str | None = None,
        kind: str | None = None,
        producing_call: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[ArtifactRef]:
        """List artifacts for an execution with optional filters."""
        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        execution = self._client_ref.executions.get(exec_id)
        artifacts = execution.artifacts

        if name is not None:
            artifacts = [artifact for artifact in artifacts if artifact.name == name]
        if kind is not None:
            artifacts = [artifact for artifact in artifacts if artifact.kind == kind]
        if producing_call is not None:
            artifacts = [
                artifact
                for artifact in artifacts
                if artifact.producing_call == producing_call
            ]

        if limit is not None:
            return artifacts[:limit]
        return artifacts

    def get(self, artifact_id: str) -> ArtifactRef:
        """Get one artifact by ID."""
        artifact = self._client_ref._get_artifact_version(
            artifact_id,
            hydrate=True,
        )

        producing_call: str | None = None
        if artifact.producer_step_run_id is not None:
            step = self._client_ref._client().get_run_step(
                artifact.producer_step_run_id,
                hydrate=True,
            )
            producing_call = _normalize_checkpoint_name(step.name)

        return _map_artifact_ref(
            artifact=artifact,
            client=self._client_ref,
            producing_call=producing_call,
        )


class _DeploymentsAPI:
    """Namespace for versioned deployment snapshot operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def _list_snapshots(self) -> builtins.list[Any]:
        """List all snapshots visible to the active project."""
        client = self._client_ref._client()
        snapshots: builtins.list[Any] = []
        page = 1
        page_size = 100
        try:
            while True:
                snapshot_page = client.list_snapshots(
                    sort_by="asc:created",
                    page=page,
                    size=page_size,
                    project=self._client_ref._project,
                    named_only=True,
                    hydrate=True,
                )
                items = list(snapshot_page.items)
                snapshots.extend(items)
                if len(items) < page_size:
                    break
                page += 1
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to list deployment snapshots: {exc}"
            ) from exc

        return snapshots

    def _update_snapshot_tags(
        self,
        deployment: DeploymentRecord,
        *,
        add_tags: builtins.list[str] | None = None,
        remove_tags: builtins.list[str] | None = None,
    ) -> Any:
        """Apply native tag updates to a snapshot."""
        try:
            return self._client_ref._client().update_snapshot(
                name_id_or_prefix=deployment.deployment_id,
                project=self._client_ref._project,
                add_tags=add_tags or None,
                remove_tags=remove_tags or None,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update deployment '{deployment.deployment_id}': {exc}"
            ) from exc

    def _delete_snapshot(self, deployment: DeploymentRecord) -> None:
        """Delete a snapshot through the backend."""
        try:
            self._client_ref._client().delete_snapshot(
                name_id_or_prefix=deployment.deployment_id,
                project=self._client_ref._project,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete deployment '{deployment.deployment_id}': {exc}"
            ) from exc

    def _name_source_snapshot(
        self,
        *,
        source_snapshot: Any,
        name: str,
        tags: Mapping[str, bool] | None,
    ) -> Any:
        """Name an existing ZenML snapshot as the requested deployment snapshot."""
        source_snapshot_id = getattr(source_snapshot, "id", source_snapshot)
        return self._client_ref._client().update_snapshot(
            name_id_or_prefix=source_snapshot_id,
            project=self._client_ref._project,
            name=name,
            replace=False,
            add_tags=deployment_native_tags(tags),
        )

    def _list_records(
        self,
        *,
        flow: str | None = None,
    ) -> builtins.list[DeploymentRecord]:
        """List raw Kitaru deployment records, optionally filtered to one flow."""
        normalized_flow = validate_deployment_flow(flow) if flow is not None else None
        deployments: builtins.list[DeploymentRecord] = []
        for snapshot in self._list_snapshots():
            deployment = map_deployment_snapshot(snapshot)
            if deployment is None:
                continue
            if normalized_flow is not None and deployment.flow != normalized_flow:
                continue
            deployments.append(deployment)
        return sorted(
            deployments, key=lambda deployment: (deployment.flow, deployment.version)
        )

    def _wrap(self, deployment: DeploymentRecord) -> Deployment:
        """Return an SDK-facing deployment facade."""
        return Deployment(deployment, self._client_ref)

    def list(self, *, flow: str | None = None) -> builtins.list[Deployment]:
        """List Kitaru deployment versions, optionally filtered to one flow."""
        return [self._wrap(deployment) for deployment in self._list_records(flow=flow)]

    def _resolve_record(
        self,
        *,
        flow: str,
        version: int | None = None,
        tag: str | None = None,
        selector_source: DeploymentSelectorSource | None = None,
    ) -> DeploymentRecord:
        """Resolve a raw deployment record by exact version or tag selector."""
        version, tag = validate_deployment_selector(
            version=version, tag=tag, require_one=True
        )

        deployments = self._list_records(flow=flow)

        if version is not None:
            for deployment in deployments:
                if deployment.version == version:
                    return deployment
            raise LookupError(
                f"No deployment found for flow {flow!r} version {version}."
            )

        assert tag is not None
        matches = [deployment for deployment in deployments if tag in deployment.tags]
        if not matches:
            if selector_source == "implicit_default" and tag == DEFAULT_DEPLOYMENT_TAG:
                if not deployments:
                    raise LookupError(
                        f"No deployments found for flow {flow!r}. Deploy this "
                        "flow first, then invoke it by version or tag."
                    )
                raise KitaruStateError(
                    f"Flow {flow!r} has deployments, but none is currently routed "
                    "as the default deployment. Invoke it with an explicit "
                    "version or tag, or move the reserved 'default' tag to the "
                    "version you want."
                )
            raise LookupError(
                f"No deployment found for flow {flow!r} with tag {tag!r}."
            )
        if len(matches) == 1:
            return matches[0]

        raise KitaruStateError(
            f"Deployment tag {tag!r} is ambiguous for flow {flow!r}; "
            f"matched versions: {', '.join(str(match.version) for match in matches)}."
        )

    @staticmethod
    def _unwrap_deployment_record(
        deployment: DeploymentRecord | Deployment,
    ) -> DeploymentRecord:
        """Return the raw deployment record for a facade or record input."""
        if isinstance(deployment, DeploymentRecord):
            return deployment
        return deployment._record

    def _resolve_deployment_stack(
        self,
        deployment: DeploymentRecord | Deployment,
    ) -> _DeploymentStackModel:
        """Load the stored stack model for a deployment snapshot."""
        deployment_record = self._unwrap_deployment_record(deployment)
        snapshot = self._client_ref._get_snapshot(
            deployment_record.deployment_id,
            hydrate=True,
        )
        stack = getattr(snapshot, "stack", None)
        if stack is None:
            resources = getattr(snapshot, "resources", None)
            stack = getattr(resources, "stack", None) if resources is not None else None
        if stack is None:
            build = getattr(snapshot, "build", None)
            stack = getattr(build, "stack", None) if build is not None else None
        if stack is None and deployment_record.stack is not None:
            try:
                stack = self._client_ref._client().get_stack(
                    name_id_or_prefix=deployment_record.stack,
                    allow_name_prefix_match=False,
                    hydrate=True,
                )
            except Exception as exc:
                raise KitaruStateError(
                    f"Deployment {deployment_record.flow!r} "
                    f"v{deployment_record.version} references stack "
                    f"{deployment_record.stack!r}, but Kitaru could not load "
                    "that stack to verify whether the server can execute it remotely. "
                    "Rebuild the deployment using a stack the Kitaru server can "
                    "execute remotely and try again."
                ) from exc
        if stack is None:
            raise KitaruStateError(
                f"Deployment {deployment_record.flow!r} "
                f"v{deployment_record.version} is missing stack metadata, "
                "so Kitaru cannot verify whether the server can execute it remotely. "
                "Rebuild the deployment using a stack the Kitaru server can execute "
                "remotely and try again."
            )
        return cast(_DeploymentStackModel, stack)

    def _resolve_server_runnable_deployment_stack(
        self,
        deployment: DeploymentRecord | Deployment,
        *,
        operation: Literal["invoke", "curl"],
    ) -> _DeploymentStackModel:
        """Resolve and validate the stack that will execute a deployment."""
        deployment_record = self._unwrap_deployment_record(deployment)
        stack = self._resolve_deployment_stack(deployment_record)
        ensure_stack_is_server_runnable(
            zen_store=self._client_ref._client().zen_store,
            stack=stack,
            operation=operation,
            flow=deployment_record.flow,
            version=deployment_record.version,
        )
        return stack

    def _ensure_deployment_server_runnable(
        self,
        deployment: DeploymentRecord | Deployment,
        *,
        operation: Literal["invoke", "curl"],
    ) -> None:
        """Fail early if a stored deployment cannot run from the server."""
        self._resolve_server_runnable_deployment_stack(
            deployment,
            operation=operation,
        )

    def get(
        self,
        *,
        flow: str,
        version: int | None = None,
        tag: str | None = None,
    ) -> Deployment:
        """Get a deployment by exact version or tag selector."""
        deployment = self._resolve_record(flow=flow, version=version, tag=tag)
        mark_deployment_known(deployment)
        return self._wrap(deployment)

    def invoke(
        self,
        *,
        flow: str,
        version: int | None = None,
        tag: str | None = None,
        selector_source: DeploymentSelectorSource | None = None,
        inputs: Mapping[str, Any] | None = None,
    ) -> Any:
        """Invoke a deployment by version or tag and return a flow handle."""
        from kitaru.flow import FlowHandle

        version, tag = validate_deployment_selector(
            version=version, tag=tag, require_one=True
        )

        deployment = self._resolve_record(
            flow=flow,
            version=version,
            tag=tag,
            selector_source=selector_source,
        )
        known_before = is_deployment_known(flow, deployment.version)
        mark_deployment_known(deployment)
        warn_if_deployment_drifted(
            deployment,
            known_before_resolution=known_before,
        )
        deployment_stack = self._resolve_server_runnable_deployment_stack(
            deployment,
            operation="invoke",
        )
        deployment_metadata = deployment_metadata_for_stack_model(deployment_stack)

        zenml_client = self._client_ref._client()
        trigger_pipeline = getattr(zenml_client, "trigger_pipeline", None)
        if not callable(trigger_pipeline):
            raise KitaruBackendError(
                "This ZenML backend does not expose snapshot invocation via "
                "Client.trigger_pipeline(...). Upgrade ZenML or invoke the "
                "snapshot through a backend-supported route."
            )

        run_inputs = dict(inputs or {})
        run_configuration = (
            PipelineRunConfiguration(parameters=run_inputs) if run_inputs else None
        )
        try:
            run = trigger_pipeline(
                snapshot_name_or_id=deployment.deployment_id,
                run_configuration=run_configuration,
                project=self._client_ref._project,
                synchronous=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to invoke deployment "
                f"{deployment.flow!r} v{deployment.version}: {exc}"
            ) from exc

        if run is None:
            raise KitaruBackendError(
                "Deployment invocation did not produce a pipeline run."
            )
        return FlowHandle(
            run,
            project=self._client_ref._project,
            analytics_metadata=deployment_metadata,
            track_terminal_if_finished=True,
        )

    def _enforce_create_exclusive_tags(
        self,
        *,
        target: DeploymentRecord,
        previous_snapshots: builtins.list[Any],
    ) -> None:
        """Best-effort removal of create-time exclusive tags from older versions."""
        exclusive_tags = [tag for tag, exclusive in target.tags.items() if exclusive]
        if not exclusive_tags:
            return

        previous_deployments = [
            deployment
            for snapshot in previous_snapshots
            if (deployment := map_deployment_snapshot(snapshot)) is not None
            and deployment.flow == target.flow
            and deployment.version != target.version
        ]
        # This mirrors tag exclusivity after the create call, so it is best
        # effort rather than atomic with the snapshot rename. If a concurrent
        # writer races here, the normal ambiguous-tag guard in `get(...)` still
        # prevents silently selecting the wrong deployment.
        for deployment in previous_deployments:
            remove_tags = [
                deployment_public_tag(tag, exclusive=deployment.tags[tag])
                for tag in exclusive_tags
                if tag in deployment.tags
            ]
            if remove_tags:
                self._update_snapshot_tags(deployment, remove_tags=remove_tags)

    def _report_exclusive_tag_cleanup_failure(
        self,
        *,
        target: DeploymentRecord,
        operation: Literal["create", "tag"],
        exclusive_tag_count: int,
        warning_message: str,
        exc: Exception,
    ) -> None:
        """Emit warning/logging/analytics for best-effort cleanup failures."""
        logger.warning(
            "Deployment %s v%d exclusive-tag cleanup failed during %s: %s",
            target.flow,
            target.version,
            operation,
            exc,
            exc_info=True,
        )
        track(
            AnalyticsEvent.DEPLOYMENT_TAG_CLEANUP_FAILED,
            {
                "operation": operation,
                "exclusive_tag_count": exclusive_tag_count,
            },
        )
        warnings.warn(
            warning_message,
            UserWarning,
            stacklevel=3,
        )

    def create(
        self,
        *,
        flow: str,
        source_snapshot: Any,
        tags: Mapping[str, bool] | None = None,
        max_attempts: int = 3,
        publish_default_on_first_deploy: bool = True,
    ) -> Deployment:
        """Create a versioned deployment snapshot from a source snapshot."""
        validate_deployment_flow(flow)
        attempts = max(1, max_attempts)

        last_error: Exception | None = None
        for _ in range(attempts):
            snapshots = self._list_snapshots()
            version = next_deployment_version(snapshots, flow=flow)
            resolved_tags = deployment_tags_for_create(
                is_first_deploy=version == 1,
                tags=tags,
                publish_default_on_first_deploy=publish_default_on_first_deploy,
            )
            snapshot_name = build_deployment_snapshot_name(flow, version)

            try:
                created = self._name_source_snapshot(
                    source_snapshot=source_snapshot,
                    name=snapshot_name,
                    tags=resolved_tags,
                )
            except EntityExistsError as exc:
                last_error = exc
                continue
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to create deployment snapshot {snapshot_name!r}: {exc}"
                ) from exc

            deployment = map_deployment_snapshot(created)
            if deployment is None:
                raise KitaruBackendError(
                    "Backend created a snapshot that does not have a valid "
                    f"Kitaru deployment name: {getattr(created, 'name', None)!r}."
                )
            try:
                self._enforce_create_exclusive_tags(
                    target=deployment,
                    previous_snapshots=snapshots,
                )
            except Exception as exc:
                exclusive_tags = sorted(
                    tag for tag, exclusive in deployment.tags.items() if exclusive
                )
                tag_text = ", ".join(exclusive_tags) if exclusive_tags else "(none)"
                self._report_exclusive_tag_cleanup_failure(
                    target=deployment,
                    operation="create",
                    exclusive_tag_count=len(exclusive_tags),
                    warning_message=(
                        "Created deployment "
                        f"{deployment.flow!r} v{deployment.version}, but failed to "
                        "remove create-time exclusive tag(s) from older versions: "
                        f"{tag_text}. The deployment exists and can be used, but older "
                        "versions might still hold those exclusive tags. Retry by "
                        f"re-applying the tag(s) to v{deployment.version} (for example "
                        "via `deployments.tag(..., exclusive=True)`). "
                        f"Cleanup error: {exc}"
                    ),
                    exc=exc,
                )
            mark_deployment_known(deployment)
            return self._wrap(deployment)

        raise KitaruBackendError(
            "Failed to allocate a deployment version after duplicate-name "
            f"conflicts for flow {flow!r}: {last_error}"
        )

    def delete(self, *, flow: str, version: int) -> None:
        """Delete one deployment version if no exclusive tag protects it."""
        deployment = self._resolve_record(flow=flow, version=version)
        exclusive_tags = sorted(
            tag for tag, exclusive in deployment.tags.items() if exclusive
        )
        if exclusive_tags:
            raise KitaruStateError(
                "Cannot delete deployment "
                f"{flow!r} v{version} while it holds exclusive tag(s): "
                f"{', '.join(exclusive_tags)}."
            )
        self._delete_snapshot(deployment)

    def tag(
        self,
        *,
        flow: str,
        version: int,
        tag: str,
        exclusive: bool = False,
    ) -> Deployment:
        """Attach a public deployment tag to one deployment version."""
        validate_deployment_version(version)
        normalized_tag = validate_deployment_tag(tag)
        effective_exclusive = resolve_deployment_exclusive(normalized_tag, exclusive)

        deployments = self._list_records(flow=flow)
        target = next(
            (deployment for deployment in deployments if deployment.version == version),
            None,
        )
        if target is None:
            raise LookupError(
                f"No deployment found for flow {flow!r} version {version}."
            )

        self._update_snapshot_tags(
            target,
            add_tags=[
                deployment_snapshot_marker_tag(),
                deployment_public_tag(normalized_tag, exclusive=effective_exclusive),
            ],
            remove_tags=[
                deployment_public_tag(
                    normalized_tag, exclusive=not effective_exclusive
                ),
            ],
        )

        if effective_exclusive:
            try:
                for deployment in deployments:
                    if deployment.version == target.version:
                        continue
                    existing_exclusive = deployment.tags.get(normalized_tag)
                    if existing_exclusive is None:
                        continue
                    self._update_snapshot_tags(
                        deployment,
                        remove_tags=[
                            deployment_public_tag(
                                normalized_tag,
                                exclusive=existing_exclusive,
                            )
                        ],
                    )
            except Exception as exc:
                self._report_exclusive_tag_cleanup_failure(
                    target=target,
                    operation="tag",
                    exclusive_tag_count=1,
                    warning_message=(
                        "Applied exclusive deployment tag "
                        f"{normalized_tag!r} to {target.flow!r} v{target.version}, "
                        "but failed to remove that tag from older versions. The tag "
                        "now exists on the target deployment, but older versions "
                        f"might still hold it. Cleanup error: {exc}"
                    ),
                    exc=exc,
                )
        return self.get(flow=flow, version=version)

    def untag(self, *, flow: str, version: int, tag: str) -> Deployment:
        """Remove a public deployment tag from one deployment version."""
        normalized_tag = validate_remove_deployment_tag(tag)

        deployment = self._resolve_record(flow=flow, version=version)
        existing_exclusive = deployment.tags.get(normalized_tag)
        if existing_exclusive is None:
            mark_deployment_known(deployment)
            return self._wrap(deployment)

        self._update_snapshot_tags(
            deployment,
            remove_tags=[
                deployment_public_tag(normalized_tag, exclusive=existing_exclusive)
            ],
        )
        return self.get(flow=flow, version=version)


class _AuthAPI:
    """Namespace for server-level auth-management operations."""

    def __init__(self, client: KitaruClient) -> None:
        self.service_accounts = _ServiceAccountsAPI(client)
        self.api_keys = _APIKeysAPI(client)


class _ServiceAccountsAPI:
    """Service-account management namespace."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def create(
        self,
        name: str,
        *,
        full_name: str | None = None,
        description: str = "",
    ) -> AuthServiceAccount:
        """Create a service account."""
        validated_name = _validate_non_empty_auth_value(name, name="name")
        try:
            service_account = self._client_ref._client().create_service_account(
                name=validated_name,
                full_name=full_name,
                description=description,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to create service account {validated_name!r}: {exc}"
            ) from exc
        return _map_auth_service_account(service_account)

    def get(self, name_or_id: str) -> AuthServiceAccount:
        """Get one service account by exact name or ID."""
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            service_account = self._client_ref._client().get_service_account(
                name_id_or_prefix=validated_name_or_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load service account {validated_name_or_id!r}: {exc}"
            ) from exc
        return _map_auth_service_account(service_account)

    def list(
        self,
        *,
        active: bool | None = None,
        name: str | None = None,
        limit: int | None = None,
        page: int | None = None,
        size: int | None = None,
    ) -> builtins.list[AuthServiceAccount]:
        """List service accounts, optionally filtered and paginated."""
        backend_page, backend_size = _validate_auth_pagination(
            limit=limit,
            page=page,
            size=size,
        )
        try:
            service_accounts = self._client_ref._client().list_service_accounts(
                name=name,
                active=active,
                page=backend_page,
                size=backend_size,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(f"Failed to list service accounts: {exc}") from exc
        return [_map_auth_service_account(item) for item in service_accounts.items]

    def update(
        self,
        name_or_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> AuthServiceAccount:
        """Update mutable service-account metadata."""
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            service_account = self._client_ref._client().update_service_account(
                name_id_or_prefix=validated_name_or_id,
                updated_name=name,
                description=description,
                active=active,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update service account {validated_name_or_id!r}: {exc}"
            ) from exc
        return _map_auth_service_account(service_account)

    def delete(self, name_or_id: str) -> None:
        """Delete a service account."""
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            self._client_ref._client().delete_service_account(
                name_id_or_prefix=validated_name_or_id
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete service account {validated_name_or_id!r}: {exc}"
            ) from exc


class _APIKeysAPI:
    """Service-account API-key management namespace."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def create(
        self,
        service_account: str,
        name: str,
        *,
        description: str = "",
        set_key: bool = False,
    ) -> AuthAPIKeyWithValue:
        """Create an API key and return its one-time raw key value."""
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        validated_name = _validate_non_empty_auth_value(name, name="name")
        try:
            zenml_client = self._client_ref._client()
            api_key = zenml_client.create_api_key(
                service_account_name_id_or_prefix=validated_service_account,
                name=validated_name,
                description=description,
                set_key=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to create API key {validated_name!r} for service "
                f"account {validated_service_account!r}: {exc}"
            ) from exc

        result = _map_auth_api_key_with_value(api_key)
        if set_key:
            return _attempt_local_key_activation(
                result,
                zenml_client=zenml_client,
                operation="create",
            )
        return result

    def get(self, service_account: str, name_or_id: str) -> AuthAPIKey:
        """Get metadata for one API key by exact name or ID."""
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            api_key = self._client_ref._client().get_api_key(
                service_account_name_id_or_prefix=validated_service_account,
                name_id_or_prefix=validated_name_or_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load API key {validated_name_or_id!r} for service "
                f"account {validated_service_account!r}: {exc}"
            ) from exc
        return _map_auth_api_key(api_key)

    def list(
        self,
        service_account: str,
        *,
        active: bool | None = None,
        name: str | None = None,
        limit: int | None = None,
        page: int | None = None,
        size: int | None = None,
    ) -> builtins.list[AuthAPIKey]:
        """List metadata for API keys owned by a service account."""
        backend_page, backend_size = _validate_auth_pagination(
            limit=limit,
            page=page,
            size=size,
        )
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        try:
            api_keys = self._client_ref._client().list_api_keys(
                service_account_name_id_or_prefix=validated_service_account,
                name=name,
                active=active,
                page=backend_page,
                size=backend_size,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to list API keys for service account "
                f"{validated_service_account!r}: {exc}"
            ) from exc
        return [_map_auth_api_key(item) for item in api_keys.items]

    def update(
        self,
        service_account: str,
        name_or_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        active: bool | None = None,
    ) -> AuthAPIKey:
        """Update mutable API-key metadata."""
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            api_key = self._client_ref._client().update_api_key(
                service_account_name_id_or_prefix=validated_service_account,
                name_id_or_prefix=validated_name_or_id,
                name=name,
                description=description,
                active=active,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update API key {validated_name_or_id!r} for service "
                f"account {validated_service_account!r}: {exc}"
            ) from exc
        return _map_auth_api_key(api_key)

    def rotate(
        self,
        service_account: str,
        name_or_id: str,
        *,
        retain_period_minutes: int = 0,
        set_key: bool = False,
    ) -> AuthAPIKeyWithValue:
        """Rotate an API key and return its one-time replacement value."""
        if not _is_strict_int(retain_period_minutes) or retain_period_minutes < 0:
            raise KitaruUsageError("`retain_period_minutes` must be an integer >= 0.")
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            zenml_client = self._client_ref._client()
            api_key = zenml_client.rotate_api_key(
                service_account_name_id_or_prefix=validated_service_account,
                name_id_or_prefix=validated_name_or_id,
                retain_period_minutes=retain_period_minutes,
                set_key=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to rotate API key {validated_name_or_id!r} for service "
                f"account {validated_service_account!r}: {exc}"
            ) from exc

        result = _map_auth_api_key_with_value(api_key)
        if set_key:
            return _attempt_local_key_activation(
                result,
                zenml_client=zenml_client,
                operation="rotate",
            )
        return result

    def delete(self, service_account: str, name_or_id: str) -> None:
        """Delete an API key."""
        validated_service_account = _validate_non_empty_auth_value(
            service_account,
            name="service_account",
        )
        validated_name_or_id = _validate_non_empty_auth_value(
            name_or_id,
            name="name_or_id",
        )
        try:
            self._client_ref._client().delete_api_key(
                service_account_name_id_or_prefix=validated_service_account,
                name_id_or_prefix=validated_name_or_id,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete API key {validated_name_or_id!r} for service "
                f"account {validated_service_account!r}: {exc}"
            ) from exc


class _ProjectScopedAPIUnavailable:
    """Placeholder for project-scoped APIs before a project is selected."""

    def __getattr__(self, name: str) -> NoReturn:
        raise KitaruUsageError(
            "This Kitaru client has no active project. Set KITARU_PROJECT "
            "before using execution, artifact, or deployment APIs."
        )


class _AgentExperimentsAPI:
    """Read-only experiment collection for one hydrated Agent Project."""

    def __init__(self, client_ref: KitaruClient) -> None:
        self._client_ref = client_ref

    def _agent(self, agent: str | None) -> AgentInfo:
        if agent is None:
            return _current_agent(client_factory=self._client_ref._client)
        return _get_agent(agent, client_factory=self._client_ref._client)

    def _view(self, agent: AgentInfo, record: Any) -> Experiment:
        return Experiment(
            record=record,
            runs=ExperimentRunLookup(
                experiment_id=record.spec.experiment_id,
                project_id=agent.agent_id,
                _client_factory=self._client_ref._client,
            ),
        )

    def list(self, *, agent: str | None = None) -> builtins.list[Experiment]:
        """List durable attempts in deterministic newest-first order."""
        agent_info = self._agent(agent)
        return [
            self._view(agent_info, record) for record in agent_info.list_experiments()
        ]

    def get(
        self,
        name_or_id: str,
        *,
        agent: str | None = None,
    ) -> Experiment:
        """Get an attempt by exact ID or unambiguous suite/name."""
        agent_info = self._agent(agent)
        return self._view(agent_info, agent_info.get_experiment(name_or_id))

    def list_for_execution(
        self,
        exec_id: str,
        *,
        agent: str | None = None,
    ) -> builtins.list[Experiment]:
        """List attempts whose verified frozen membership contains an execution."""
        normalized_id = exec_id.strip()
        if not normalized_id:
            raise KitaruUsageError("Execution ID cannot be empty.")
        agent_info = self._agent(agent)
        zenml_client = self._client_ref._client()
        return [
            self._view(agent_info, record)
            for record in agent_info.list_experiments()
            if experiment_targets_execution(
                record,
                normalized_id,
                client=zenml_client,
            )
        ]


class _AgentsAPI:
    """Canonical Agent lifecycle operations for a Kitaru client."""

    def __init__(self, client_ref: KitaruClient) -> None:
        self._client_ref = client_ref
        self.experiments = _AgentExperimentsAPI(client_ref)

    def current(self) -> AgentInfo:
        """Return the active initialized Kitaru Agent."""
        return _current_agent(client_factory=self._client_ref._client)

    def list(self) -> builtins.list[AgentInfo]:
        """List initialized Kitaru Agents visible to the current user."""
        return _list_agents(client_factory=self._client_ref._client)

    def get(self, name_or_id: str) -> AgentInfo:
        """Return an initialized Kitaru Agent by name or ID."""
        return _get_agent(name_or_id, client_factory=self._client_ref._client)

    def create(
        self,
        name: str,
        *,
        description: str = "",
        display_name: str | None = None,
        activate: bool = True,
    ) -> AgentCreateResult:
        """Create a Kitaru Agent on Pro/Cloud and optionally activate it."""
        return _create_agent(
            name,
            description=description,
            display_name=display_name,
            activate=activate,
            client_factory=self._client_ref._client,
        )

    def use(self, name_or_id: str) -> AgentInfo:
        """Set the active Kitaru Agent on Pro/Cloud."""
        return _use_agent(name_or_id, client_factory=self._client_ref._client)

    def delete(self, name_or_id: str) -> AgentDeleteResult:
        """Delete a Kitaru Agent on Pro/Cloud."""
        return _delete_agent(name_or_id, client_factory=self._client_ref._client)


class _ProjectsAPI:
    """Deprecated Project-named compatibility delegate."""

    def __init__(self, client_ref: KitaruClient) -> None:
        self._client_ref = client_ref

    def current(self) -> ProjectInfo:
        """Return the active Kitaru project."""
        return _current_project(client_factory=self._client_ref._client)

    def list(self) -> builtins.list[ProjectInfo]:
        """List Kitaru projects visible to the current user."""
        return _list_projects(client_factory=self._client_ref._client)

    def get(self, name_or_id: str) -> ProjectInfo:
        """Return a Kitaru project by name or ID."""
        return _get_project(name_or_id, client_factory=self._client_ref._client)

    def create(
        self,
        name: str,
        *,
        description: str = "",
        display_name: str | None = None,
        activate: bool = True,
    ) -> ProjectCreateResult:
        """Create a Kitaru project on ZenML Pro/Cloud and optionally activate it."""
        return _create_project(
            name,
            description=description,
            display_name=display_name,
            activate=activate,
            client_factory=self._client_ref._client,
        )

    def use(self, name_or_id: str) -> ProjectInfo:
        """Set the active Kitaru project on ZenML Pro/Cloud."""
        return _use_project(name_or_id, client_factory=self._client_ref._client)

    def delete(self, name_or_id: str) -> ProjectDeleteResult:
        """Delete a Kitaru project on ZenML Pro/Cloud."""
        return _delete_project(name_or_id, client_factory=self._client_ref._client)


class KitaruClient:
    """Client for Agents, executions, artifacts, deployments, imports, and auth."""

    def __init__(
        self,
        *,
        server_url: str | None = None,
        auth_token: str | None = None,
        project: str | None = None,
        _require_project: bool = True,
    ) -> None:
        """Initialize a Kitaru client.

        Args:
            server_url: Optional per-client server override (not yet supported).
            auth_token: Optional per-client auth token override (not yet
                supported).
            project: Optional per-client project override (not yet supported).

        Raises:
            KitaruFeatureNotAvailableError: If per-client connection overrides
                are provided.
        """
        explicit_overrides: dict[str, str] = {}
        if server_url is not None:
            explicit_overrides["server_url"] = server_url
        if auth_token is not None:
            explicit_overrides["auth_token"] = auth_token
        if project is not None:
            explicit_overrides["project"] = project

        if explicit_overrides:
            supplied = ", ".join(sorted(explicit_overrides))
            raise KitaruFeatureNotAvailableError(
                "Per-client connection overrides are not implemented yet "
                f"(received: {supplied}). Use kitaru.connect(...) and active "
                "project settings for now."
            )

        resolved_connection = resolve_connection_config(
            validate_for_use=True,
            require_project=_require_project,
        )
        self._project = resolved_connection.project

        self.auth = _AuthAPI(self)
        self.agents = _AgentsAPI(self)
        self.projects = _ProjectsAPI(self)
        if not _require_project and self._project is None:
            unavailable = _ProjectScopedAPIUnavailable()
            self.executions = unavailable
            self.artifacts = unavailable
            self.deployments = unavailable
            self.imports = unavailable
        else:
            self.executions = _ExecutionsAPI(self)
            self.artifacts = _ArtifactsAPI(self)
            self.deployments = _DeploymentsAPI(self)
            self.imports = ImportsAPI(self)

    @classmethod
    def for_auth_management(cls) -> KitaruClient:
        """Create a client for server-level auth management.

        Normal ``KitaruClient()`` construction remains strict and requires a
        project for env-driven remote connections. Auth management is
        server-level, so this constructor validates server/auth pairing while
        intentionally skipping project validation.
        """
        return cls(_require_project=False)

    @classmethod
    def for_agent_management(cls) -> KitaruClient:
        """Create a client for Agent lifecycle operations.

        Reading Agents can happen before a project-scoped operation runs.
        Agent create/use/delete retain the shared Pro/Cloud lifecycle guard.
        """
        return cls(_require_project=False)

    @classmethod
    def for_project_management(cls) -> KitaruClient:
        """Create a client through the deprecated Project-named compatibility API."""
        return cls.for_agent_management()

    def _client(self) -> Client:
        """Return a ZenML client instance."""
        return Client()

    def _get_pipeline_run(
        self,
        exec_id: str,
        *,
        hydrate: bool,
    ) -> PipelineRunResponse:
        """Fetch a run by execution ID with strict ID matching."""
        try:
            return self._client().get_pipeline_run(
                name_id_or_prefix=exec_id,
                allow_name_prefix_match=False,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load execution '{exec_id}': {exc}"
            ) from exc

    def _get_snapshot(
        self,
        snapshot_id: str,
        *,
        hydrate: bool,
    ) -> Any:
        """Fetch a snapshot by ID with strict ID matching."""
        try:
            return self._client().get_snapshot(
                name_id_or_prefix=snapshot_id,
                allow_prefix_match=False,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load deployment snapshot '{snapshot_id}': {exc}"
            ) from exc

    def _get_artifact_version(
        self,
        artifact_id: str,
        *,
        hydrate: bool,
    ) -> ArtifactVersionResponse:
        """Fetch an artifact version by ID."""
        try:
            return self._client().get_artifact_version(
                name_id_or_prefix=artifact_id,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load artifact '{artifact_id}': {exc}"
            ) from exc


__all__ = [
    "ArtifactRef",
    "AuthAPIKey",
    "AuthAPIKeyWithValue",
    "AuthServiceAccount",
    "CheckpointAttempt",
    "CheckpointCall",
    "Deployment",
    "Execution",
    "ExecutionEvent",
    "ExecutionStatistics",
    "ExecutionStatisticsDimension",
    "ExecutionStatisticsGroup",
    "ExecutionStatisticsGrouping",
    "ExecutionStatisticsMetric",
    "ExecutionStatisticsMetricAggregation",
    "ExecutionStatisticsMetricSource",
    "ExecutionStatisticsTimeGranularity",
    "ExecutionStatus",
    "FailureInfo",
    "KitaruClient",
    "LogEntry",
    "PendingWait",
]
