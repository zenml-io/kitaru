"""Kitaru client for execution, artifact, and memory management.

`KitaruClient` provides a programmatic API for inspecting and managing
executions, artifacts, and memories outside flow bodies.

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
import warnings
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast, runtime_checkable

from pydantic import ValidationError
from zenml.client import Client
from zenml.config.pipeline_run_configuration import PipelineRunConfiguration
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.exceptions import EntityExistsError
from zenml.login.credentials_store import get_credentials_store
from zenml.models import PipelineRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
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
    _WAIT_CONDITION_STATUS_PENDING,
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
    ExecutionStatus,
    FailureInfo,
    LogEntry,
    PendingWait,
)
from kitaru._client._models import (
    Deployment as DeploymentRecord,
)
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
from kitaru.analytics import AnalyticsEvent, track
from kitaru.config import (
    active_stack_log_store,
    resolve_connection_config,
    resolve_log_store,
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
from kitaru.memory import (
    CompactionRecord,
    CompactResult,
    MemoryEntry,
    MemoryReindexResult,
    MemoryScopeInfo,
    PurgeResult,
    _compact_impl,
    _compaction_log_impl,
    _delete_impl,
    _get_entry_impl,
    _history_impl,
    _list_impl,
    _list_scopes_impl,
    _MemoryCompactionSourceMode,
    _MemoryScope,
    _MemoryScopeType,
    _purge_impl,
    _purge_scope_impl,
    _reindex_impl,
    _set_entry_impl,
    _validate_memory_compaction_source_mode,
    _validate_memory_identifier,
    _validate_memory_scope_type,
    _validate_memory_version,
)
from kitaru.replay import build_replay_plan

logger = logging.getLogger(__name__)

_WAIT_CONDITION_RESOLUTION_CONTINUE = "continue"
_WAIT_CONDITION_RESOLUTION_ABORT = "abort"

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


def _validate_auth_pagination(
    *,
    limit: int | None,
    page: int | None,
    size: int | None,
) -> tuple[int, int]:
    """Validate shared auth list pagination and return backend page/size."""
    if limit is not None:
        if isinstance(limit, bool) or limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")
        if page is not None or size is not None:
            raise KitaruUsageError("`limit` cannot be combined with `page` or `size`.")
        return 1, limit
    if page is not None and (isinstance(page, bool) or page < 1):
        raise KitaruUsageError("`page` must be >= 1 when provided.")
    if size is not None and (isinstance(size, bool) or size < 1):
        raise KitaruUsageError("`size` must be >= 1 when provided.")
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


@runtime_checkable
class _ReplayFlowLike(Protocol):
    """Flow wrapper protocol used by client-side replay resolution."""

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Any: ...


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack while running an operation."""
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
    """Import a module by name, falling back to ``sys.modules`` search.

    ZenML records the pipeline source module relative to the archived source
    root (e.g. ``replay_with_overrides``), but in the running process the
    module may be loaded under a different path.  Three fallback strategies:

    1. Direct ``importlib.import_module`` (exact match).
    2. Search ``sys.modules`` for a suffix match (e.g. the module is loaded
       as ``examples.features.replay.replay_with_overrides``).
    3. Return ``__main__`` — when invoked via ``python -m pkg.mod``, the
       module is loaded as ``__main__`` and won't appear under its dotted
       name in ``sys.modules``.
    """
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        pass

    # Search already-loaded modules for a suffix match.
    suffix = f".{module_name}"
    for loaded_name, loaded_module in sys.modules.items():
        if (
            loaded_name == module_name or loaded_name.endswith(suffix)
        ) and loaded_module is not None:
            return loaded_module

    # When run via `python -m`, the module is __main__.
    main_module = sys.modules.get("__main__")
    if main_module is not None:
        return main_module

    raise KitaruRuntimeError(
        f"Failed to import replay source module '{module_name}' for "
        f"execution '{run_id}': no module named '{module_name}' and no "
        "matching module found in sys.modules."
    )


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
        if isinstance(candidate, _ReplayFlowLike):
            return candidate

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


def _restart_run_from_snapshot(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    operation_name: str,
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

    try:
        with _temporary_active_stack(str(snapshot.stack.id)):
            active_stack = client._client().active_stack
            orchestrator = cast(Any, active_stack.orchestrator)
            orchestrator.resume_run(
                snapshot=snapshot,
                run=run,
                stack=active_stack,
            )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to {operation_name} execution '{run.id}': {exc}"
        ) from exc


class _ExecutionsAPI:
    """Namespace for execution lifecycle and inspection operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def _rest_store(self) -> RestZenStore:
        """Return a REST-backed zen store required for runtime log retrieval."""
        zen_store = self._client_ref._client().zen_store
        if isinstance(zen_store, RestZenStore):
            return zen_store

        raise KitaruLogRetrievalError(
            "Runtime log retrieval requires a server-backed connection. "
            "Local database mode does not expose execution log endpoints."
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
        run_status_value = str(getattr(run.status, "value", run.status))
        if run_status_value != ZenMLExecutionStatus.FAILED.value:
            raise KitaruStateError(
                "Only failed executions can be retried. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        _restart_run_from_snapshot(
            run=run,
            client=self._client_ref,
            operation_name="retry",
        )
        track(AnalyticsEvent.EXECUTION_RETRIED, {})
        return self.get(exec_id)

    def resume(self, exec_id: str) -> Execution:
        """Resume a paused execution after all waits are resolved."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        pending_conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        if pending_conditions:
            raise KitaruStateError(
                f"Resolve pending wait input before resuming execution '{exec_id}'."
            )

        run_status_value = str(getattr(run.status, "value", run.status))
        if run_status_value != "paused":
            raise KitaruStateError(
                "Only paused executions can be resumed. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        _restart_run_from_snapshot(
            run=run,
            client=self._client_ref,
            operation_name="resume",
        )
        track(AnalyticsEvent.EXECUTION_RESUMED, {})
        return self.get(exec_id)

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay an execution from a checkpoint boundary."""
        source_run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)

        run_status_value = str(getattr(source_run.status, "value", source_run.status))
        if run_status_value in {
            "initializing",
            "provisioning",
            "running",
            "retrying",
            "stopping",
        }:
            raise KitaruStateError(
                "Replay requires a non-running source execution. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        replay_flow: _ReplayFlowLike | None = None
        try:
            replay_flow = _resolve_flow_for_replay(source_run)
        except KitaruRuntimeError:
            replay_flow = None

        if replay_flow is not None:
            handle = replay_flow.replay(
                exec_id,
                from_=from_,
                overrides=overrides,
                **flow_inputs,
            )
            replay_exec_id = getattr(handle, "exec_id", None)
            if not replay_exec_id:
                raise KitaruRuntimeError(
                    "Resolved flow replay call did not return a valid execution handle."
                )
            return self.get(str(replay_exec_id))

        replay_pipeline = _resolve_pipeline_for_replay(source_run)
        replay_plan = build_replay_plan(
            run=source_run,
            from_=from_,
            overrides=overrides,
            flow_inputs=flow_inputs,
        )

        replay_metadata: dict[str, Any] = {
            "from_checkpoint": from_,
            "replay_path": "pipeline_fallback",
        }
        track(AnalyticsEvent.REPLAY_REQUESTED, replay_metadata)

        try:
            replayed_run = replay_pipeline.replay(
                pipeline_run=source_run.id,
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
                    f"Replay divergence detected for execution '{exec_id}': {exc}",
                    exec_id=str(source_run.id),
                    status="failed",
                    origin=failure_origin,
                ) from exc
            raise KitaruBackendError(
                f"Failed to replay execution '{exec_id}': {exc}"
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
            raise KitaruRuntimeError("Replay did not produce a pipeline run ID.")

        track(AnalyticsEvent.FLOW_REPLAYED, {"replay_path": "pipeline_fallback"})
        return self.get(replayed_exec_id)

    def get(self, exec_id: str) -> Execution:
        """Get and map one execution by ID."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        return _map_execution(run=run, client=self._client_ref, include_details=True)

    def list(
        self,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
        limit: int | None = None,
        page: int | None = None,
        size: int | None = None,
    ) -> builtins.list[Execution]:
        """List executions with optional flow/status filters and pagination."""
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

        while True:
            run_page = self._client_ref._client().list_pipeline_runs(
                sort_by="desc:created",
                page=backend_page,
                size=page_size,
                project=self._client_ref._project,
                hydrate=True,
            )
            runs = list(run_page.items)
            if not runs:
                break

            for run in runs:
                execution = _map_execution(
                    run=run,
                    client=self._client_ref,
                    include_details=False,
                )

                if flow is not None and execution.flow_name != flow:
                    continue
                if status_filter is not None and execution.status != status_filter:
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


class _MemoriesAPI:
    """Namespace for typed memory inspection and control operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def _scope(
        self,
        scope: str,
        *,
        scope_type: _MemoryScopeType,
    ) -> _MemoryScope:
        """Validate and construct a memory scope for client operations."""
        return _MemoryScope(
            scope=_validate_memory_identifier(scope, kind="scope"),
            scope_type=_validate_memory_scope_type(scope_type),
        )

    def get(
        self,
        key: str,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
        version: int | None = None,
    ) -> MemoryEntry | None:
        """Get one typed memory entry by key and scope."""
        return _get_entry_impl(
            self._scope(scope, scope_type=scope_type),
            _validate_memory_identifier(key, kind="key"),
            version=_validate_memory_version(version),
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def list(
        self,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
        prefix: str | None = None,
    ) -> builtins.list[MemoryEntry]:
        """List typed memory entries for a scope, optionally filtered by prefix."""
        normalized_prefix = (
            _validate_memory_identifier(prefix, kind="prefix")
            if prefix is not None
            else None
        )
        return _list_impl(
            self._scope(scope, scope_type=scope_type),
            prefix=normalized_prefix,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def history(
        self,
        key: str,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
    ) -> builtins.list[MemoryEntry]:
        """Return the full version history for one memory key."""
        return _history_impl(
            self._scope(scope, scope_type=scope_type),
            _validate_memory_identifier(key, kind="key"),
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def set(
        self,
        key: str,
        value: Any,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
    ) -> MemoryEntry:
        """Persist a memory value and return the created metadata entry."""
        return _set_entry_impl(
            self._scope(scope, scope_type=scope_type),
            _validate_memory_identifier(key, kind="key"),
            value,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def delete(
        self,
        key: str,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
    ) -> MemoryEntry | None:
        """Soft-delete a memory key and return the tombstone entry."""
        return _delete_impl(
            self._scope(scope, scope_type=scope_type),
            _validate_memory_identifier(key, kind="key"),
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def scopes(self) -> builtins.list[MemoryScopeInfo]:
        """Discover all memory scopes with active entry counts."""
        return _list_scopes_impl(
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def purge(
        self,
        key: str,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
        keep: int | None = None,
    ) -> PurgeResult:
        """Physically delete old versions of a memory key."""
        return _purge_impl(
            self._scope(scope, scope_type=scope_type),
            _validate_memory_identifier(key, kind="key"),
            keep=keep,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def purge_scope(
        self,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
        keep: int | None = None,
        include_deleted: bool = False,
    ) -> PurgeResult:
        """Purge old versions across all keys in a scope."""
        return _purge_scope_impl(
            self._scope(scope, scope_type=scope_type),
            keep=keep,
            include_deleted=include_deleted,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def compact(
        self,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
        key: str | None = None,
        keys: builtins.list[str] | None = None,
        source_mode: _MemoryCompactionSourceMode = "current",
        target_key: str | None = None,
        instruction: str | None = None,
        model: str | None = None,
        max_tokens: int | None = None,
    ) -> CompactResult:
        """Summarize memory values using an LLM and write the result."""
        validated_key = (
            _validate_memory_identifier(key, kind="key") if key is not None else None
        )
        validated_keys = (
            [_validate_memory_identifier(k, kind="key") for k in keys]
            if keys is not None
            else None
        )
        validated_target = (
            _validate_memory_identifier(target_key, kind="key")
            if target_key is not None
            else None
        )
        validated_source_mode = _validate_memory_compaction_source_mode(source_mode)
        return _compact_impl(
            self._scope(scope, scope_type=scope_type),
            key=validated_key,
            keys=validated_keys,
            source_mode=validated_source_mode,
            target_key=validated_target,
            instruction=instruction,
            model=model,
            max_tokens=max_tokens,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def compaction_log(
        self,
        *,
        scope: str,
        scope_type: _MemoryScopeType,
    ) -> builtins.list[CompactionRecord]:
        """Read all compaction audit records for a scope."""
        return _compaction_log_impl(
            self._scope(scope, scope_type=scope_type),
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
        )

    def reindex(
        self,
        *,
        apply: bool = False,
    ) -> MemoryReindexResult:
        """Backfill missing memory indexing tags for historical artifacts."""
        return _reindex_impl(
            dry_run=not apply,
            client_factory=self._client_ref._client,
            project=self._client_ref._project,
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
    ) -> Any:
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
        return stack

    def _ensure_deployment_server_runnable(
        self,
        deployment: DeploymentRecord | Deployment,
        *,
        operation: Literal["invoke", "curl"],
    ) -> None:
        """Fail early if a stored deployment cannot run from the server."""
        deployment_record = self._unwrap_deployment_record(deployment)
        stack = self._resolve_deployment_stack(deployment_record)
        ensure_stack_is_server_runnable(
            zen_store=self._client_ref._client().zen_store,
            stack=stack,
            operation=operation,
            flow=deployment_record.flow,
            version=deployment_record.version,
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
        self._ensure_deployment_server_runnable(deployment, operation="invoke")

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
        return FlowHandle(run)

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
        if isinstance(retain_period_minutes, bool) or retain_period_minutes < 0:
            raise KitaruUsageError("`retain_period_minutes` must be >= 0.")
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


class KitaruClient:
    """Client for Kitaru executions, artifacts, memory, deployments, and auth."""

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
        self.executions = _ExecutionsAPI(self)
        self.artifacts = _ArtifactsAPI(self)
        self.memories = _MemoriesAPI(self)
        self.deployments = _DeploymentsAPI(self)

    @classmethod
    def for_auth_management(cls) -> KitaruClient:
        """Create a client for server-level auth management.

        Normal ``KitaruClient()`` construction remains strict and requires a
        project for env-driven remote connections. Auth management is
        server-level, so this constructor validates server/auth pairing while
        intentionally skipping project validation.
        """
        return cls(_require_project=False)

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
    "ExecutionStatus",
    "FailureInfo",
    "KitaruClient",
    "LogEntry",
    "PendingWait",
]
