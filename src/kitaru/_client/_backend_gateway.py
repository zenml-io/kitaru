"""Internal gateway for backend-facing Kitaru client operations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from pydantic import ValidationError
from zenml.login.credentials_store import get_credentials_store
from zenml.models import PipelineRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.utils.run_utils import stop_run
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru._client._events import (
    QueryParams,
    StreamingStore,
    open_rest_sse_stream,
)
from kitaru._client._logs import (
    _is_empty_log_result_error,
    _is_log_endpoint_version_skew_error,
    _is_otel_log_retrieval_error,
)
from kitaru._config import _stacks as stack_ops
from kitaru._stack_binding import temporary_active_stack
from kitaru.config import active_stack_log_store, resolve_log_store
from kitaru.errors import (
    KitaruBackendError,
    KitaruLogRetrievalError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruWaitValidationError,
)

_ORIGINAL_STACK_OPERATIONS: dict[str, Any] = {
    "current_stack": stack_ops.current_stack,
    "list_stack_entries": stack_ops._list_stack_entries,
    "show_stack_operation": stack_ops._show_stack_operation,
    "create_stack_operation": stack_ops._create_stack_operation,
    "create_local_stack_operation": stack_ops._create_local_stack_operation,
    "create_kubernetes_stack_operation": stack_ops._create_kubernetes_stack_operation,
    "create_vertex_stack_operation": stack_ops._create_vertex_stack_operation,
    "create_sagemaker_stack_operation": stack_ops._create_sagemaker_stack_operation,
    "create_azureml_stack_operation": stack_ops._create_azureml_stack_operation,
    "delete_stack_operation": stack_ops._delete_stack_operation,
    "use_stack": stack_ops.use_stack,
}


@dataclass(frozen=True)
class LocalCredentialSnapshot:
    """Previous locally persisted API-key state for rollback attempts."""

    server_url: str | None
    previous_api_key: str | None = field(default=None, repr=False)
    reason_unavailable: str | None = None

    @property
    def previous_api_key_available(self) -> bool:
        """Return whether rollback has a previous API key to restore."""
        return self.server_url is not None and self.previous_api_key is not None


@dataclass(frozen=True)
class LocalKeyActivationStatus:
    """Result of best-effort local API-key activation."""

    succeeded: bool
    error: str | None = None
    rollback_attempted: bool = False
    rollback_succeeded: bool | None = None
    rollback_error: str | None = None
    rollback_reason: str | None = None


@dataclass(frozen=True)
class APIKeyOperationResult:
    """Backend API-key operation result plus optional local activation status."""

    api_key: Any
    local_key_activation: LocalKeyActivationStatus | None = None


def _api_key_raw_value(api_key: Any) -> str:
    """Return the one-time API-key value or raise the public backend error."""
    raw_key = getattr(api_key, "key", None)
    if not isinstance(raw_key, str) or not raw_key:
        raise KitaruBackendError(
            "The server did not return the one-time API key value for this "
            "create/rotate operation."
        )
    return raw_key


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


def _capture_previous_local_api_key(zenml_client: Any) -> LocalCredentialSnapshot:
    """Return the previous persisted API key if it can be restored safely."""
    server_url = getattr(getattr(zenml_client, "zen_store", None), "url", None)
    if not isinstance(server_url, str) or not server_url:
        return LocalCredentialSnapshot(
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
        return LocalCredentialSnapshot(
            server_url=server_url,
            reason_unavailable=(
                "Kitaru could not read the previous persisted local API key for "
                f"{server_url!r}, so rollback was not possible: {exc}"
            ),
        )

    if not previous_api_key:
        return LocalCredentialSnapshot(
            server_url=server_url,
            reason_unavailable=(
                "No previous persisted local API key was available to restore. "
                "Environment credentials, if any, were not modified."
            ),
        )

    return LocalCredentialSnapshot(
        server_url=server_url,
        previous_api_key=previous_api_key,
    )


class KitaruBackendGateway:
    """Private access point for backend operations used by Kitaru clients."""

    def __init__(
        self,
        *,
        project: str | None,
        client_factory: Callable[[], Any],
        active_stack_log_store_getter: Callable[[], Any] = active_stack_log_store,
        resolve_log_store_getter: Callable[[], Any] = resolve_log_store,
    ) -> None:
        self._project = project
        self._client_factory = client_factory
        self._active_stack_log_store_getter = active_stack_log_store_getter
        self._resolve_log_store_getter = resolve_log_store_getter

    @property
    def project(self) -> str | None:
        """Return the active Kitaru project, if one is required for this client."""
        return self._project

    def zenml_client(self) -> Any:
        """Return a fresh ZenML client instance."""
        return self._client_factory()

    def get_pipeline_run(self, exec_id: str, *, hydrate: bool) -> PipelineRunResponse:
        """Fetch a run by execution ID with strict ID matching."""
        try:
            return self.zenml_client().get_pipeline_run(
                name_id_or_prefix=exec_id,
                allow_name_prefix_match=False,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load execution '{exec_id}': {exc}"
            ) from exc

    def get_snapshot(self, snapshot_id: str, *, hydrate: bool) -> Any:
        """Fetch a snapshot by ID with strict ID matching."""
        try:
            return self.zenml_client().get_snapshot(
                name_id_or_prefix=snapshot_id,
                allow_prefix_match=False,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load deployment snapshot '{snapshot_id}': {exc}"
            ) from exc

    def get_artifact_version(
        self,
        artifact_id: str,
        *,
        hydrate: bool,
    ) -> ArtifactVersionResponse:
        """Fetch an artifact version by ID."""
        try:
            return self.zenml_client().get_artifact_version(
                name_id_or_prefix=artifact_id,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load artifact '{artifact_id}': {exc}"
            ) from exc

    def list_pipeline_runs(self, *, page: int, size: int) -> Any:
        """List pipeline runs for the active project."""
        return self.zenml_client().list_pipeline_runs(
            sort_by="desc:created",
            page=page,
            size=size,
            project=self._project,
            hydrate=True,
        )

    def list_run_steps(self, *, run_id: Any, page: int, size: int) -> Any:
        """List step runs for a pipeline run."""
        try:
            return self.zenml_client().list_run_steps(
                sort_by="asc:created",
                page=page,
                size=size,
                pipeline_run_id=run_id,
                project=self._project,
                exclude_retried=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to fetch checkpoint attempts for execution {run_id}: {exc}"
            ) from exc

    def get_run_step(self, step_id: Any, *, hydrate: bool) -> Any:
        """Fetch one run-step model."""
        return self.zenml_client().get_run_step(step_id, hydrate=hydrate)

    def list_run_wait_conditions(
        self,
        *,
        run_id: Any,
        status: str | None = None,
    ) -> list[Any]:
        """Return wait-condition models for an execution."""
        try:
            wait_conditions_page = self.zenml_client().list_run_wait_conditions(
                pipeline_run=run_id,
                project=self._project,
                status=status,
                hydrate=True,
                sort_by="asc:created",
                size=200,
            )
        except AttributeError:
            return []
        except Exception as exc:
            operation = "pending waits" if status == "pending" else "waits"
            raise KitaruBackendError(
                f"Failed to list {operation} for execution {run_id}: {exc}"
            ) from exc

        return list(wait_conditions_page.items)

    def resolve_run_wait_condition(
        self,
        *,
        run_wait_condition_id: Any,
        resolution: Any,
        result: Any | None,
        wait_name: str,
        exec_id: str,
    ) -> None:
        """Resolve a pending wait condition."""
        try:
            self.zenml_client().resolve_run_wait_condition(
                run_wait_condition_id=run_wait_condition_id,
                resolution=resolution,
                result=result,
            )
        except (ValidationError, TypeError, ValueError) as exc:
            raise KitaruWaitValidationError(
                "Wait input failed validation for "
                f"'{wait_name}' on execution '{exec_id}': {exc}"
            ) from exc
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to resolve wait condition "
                f"'{wait_name}' for execution '{exec_id}': {exc}"
            ) from exc

    def cancel_run(
        self,
        run: PipelineRunResponse,
        *,
        stop_run_fn: Callable[..., Any] = stop_run,
    ) -> None:
        """Cancel an execution through the backend runtime helper."""
        stop_run_fn(run=run, graceful=False)

    def restart_run_from_snapshot(
        self,
        *,
        run: PipelineRunResponse,
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
            with temporary_active_stack(
                str(snapshot.stack.id),
                client_factory=self.zenml_client,
            ) as stack_client:
                assert stack_client is not None
                active_stack = stack_client.active_stack
                active_stack.orchestrator.resume_run(
                    snapshot=snapshot,
                    run=run,
                    stack=active_stack,
                )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to {operation_name} execution '{run.id}': {exc}"
            ) from exc

    def require_rest_store(self, unavailable_error: Exception) -> RestZenStore:
        """Return the active REST store or raise the caller-specific error."""
        zen_store = self.zenml_client().zen_store
        if isinstance(zen_store, RestZenStore):
            return zen_store
        raise unavailable_error

    def resolve_log_endpoint_hint(self) -> str | None:
        """Resolve a best-effort endpoint hint for log-retrieval errors."""
        active_log_store = self._active_stack_log_store_getter()
        if active_log_store is not None and active_log_store.endpoint:
            return active_log_store.endpoint

        try:
            preferred_log_store = self._resolve_log_store_getter()
        except ValueError:
            return None

        return preferred_log_store.endpoint

    def fetch_log_payload(
        self,
        *,
        path: str,
        source: str,
        store: RestZenStore,
    ) -> list[Mapping[str, Any]]:
        """Call a log endpoint and normalize the response payload shape."""
        try:
            payload = store.get(path, params={"source": source})
        except Exception as exc:
            error_message = str(exc)
            if _is_empty_log_result_error(error_message):
                return []

            if _is_otel_log_retrieval_error(error_message):
                endpoint_hint = self.resolve_log_endpoint_hint()
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

        normalized_payload: list[Mapping[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise KitaruLogRetrievalError(
                    "Unexpected log entry payload type returned by the server."
                )
            normalized_payload.append(entry)

        return normalized_payload

    def execution_event_stream_factory(
        self,
        *,
        store: RestZenStore,
        path: str,
        params: QueryParams,
    ) -> Callable[[str | None], Any]:
        """Build the authenticated SSE stream opener used by event watching."""

        def _open_stream(last_event_id: str | None) -> Any:
            return open_rest_sse_stream(
                cast(StreamingStore, store),
                path=path,
                params=params,
                last_event_id=last_event_id,
            )

        return _open_stream

    def list_deployment_snapshots(self) -> list[Any]:
        """List all snapshots visible to the active project."""
        client = self.zenml_client()
        snapshots: list[Any] = []
        page = 1
        page_size = 100
        try:
            while True:
                snapshot_page = client.list_snapshots(
                    sort_by="asc:created",
                    page=page,
                    size=page_size,
                    project=self._project,
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

    def update_snapshot_tags(
        self,
        *,
        deployment_id: str,
        add_tags: list[str] | None = None,
        remove_tags: list[str] | None = None,
    ) -> Any:
        """Apply native tag updates to a snapshot."""
        try:
            return self.zenml_client().update_snapshot(
                name_id_or_prefix=deployment_id,
                project=self._project,
                add_tags=add_tags or None,
                remove_tags=remove_tags or None,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update deployment '{deployment_id}': {exc}"
            ) from exc

    def delete_snapshot(self, *, deployment_id: str) -> None:
        """Delete a snapshot through the backend."""
        try:
            self.zenml_client().delete_snapshot(
                name_id_or_prefix=deployment_id,
                project=self._project,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete deployment '{deployment_id}': {exc}"
            ) from exc

    def name_source_snapshot(
        self,
        *,
        source_snapshot: Any,
        name: str,
        tags: list[str],
    ) -> Any:
        """Name an existing snapshot as a deployment snapshot."""
        source_snapshot_id = getattr(source_snapshot, "id", source_snapshot)
        return self.zenml_client().update_snapshot(
            name_id_or_prefix=source_snapshot_id,
            project=self._project,
            name=name,
            replace=False,
            add_tags=tags,
        )

    def get_deployment_stack(
        self,
        *,
        stack_name_or_id: str,
        flow: str,
        version: int,
    ) -> Any:
        """Load a deployment's referenced stack by name or ID."""
        try:
            return self.zenml_client().get_stack(
                name_id_or_prefix=stack_name_or_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruStateError(
                f"Deployment {flow!r} v{version} references stack "
                f"{stack_name_or_id!r}, but Kitaru could not load that stack "
                "to verify whether the server can execute it remotely. "
                "Rebuild the deployment using a stack the Kitaru server can "
                "execute remotely and try again."
            ) from exc

    def zen_store(self) -> Any:
        """Return the active backend store from a fresh client."""
        return self.zenml_client().zen_store

    def trigger_deployment(
        self,
        *,
        deployment_id: str,
        flow: str,
        version: int,
        run_configuration: Any,
    ) -> Any:
        """Trigger a deployment snapshot and return the backend run model."""
        zenml_client = self.zenml_client()
        trigger_pipeline = getattr(zenml_client, "trigger_pipeline", None)
        if not callable(trigger_pipeline):
            raise KitaruBackendError(
                "This ZenML backend does not expose snapshot invocation via "
                "Client.trigger_pipeline(...). Upgrade ZenML or invoke the "
                "snapshot through a backend-supported route."
            )

        try:
            run = trigger_pipeline(
                snapshot_name_or_id=deployment_id,
                run_configuration=run_configuration,
                project=self._project,
                synchronous=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to invoke deployment {flow!r} v{version}: {exc}"
            ) from exc

        if run is None:
            raise KitaruBackendError(
                "Deployment invocation did not produce a pipeline run."
            )
        return run

    def create_service_account(
        self,
        *,
        name: str,
        full_name: str | None,
        description: str,
    ) -> Any:
        """Create a service account."""
        try:
            return self.zenml_client().create_service_account(
                name=name,
                full_name=full_name,
                description=description,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to create service account {name!r}: {exc}"
            ) from exc

    def get_service_account(self, *, name_or_id: str) -> Any:
        """Get one service account by exact name or ID."""
        try:
            return self.zenml_client().get_service_account(
                name_id_or_prefix=name_or_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load service account {name_or_id!r}: {exc}"
            ) from exc

    def list_service_accounts(
        self,
        *,
        active: bool | None,
        name: str | None,
        page: int,
        size: int,
    ) -> Any:
        """List service accounts."""
        try:
            return self.zenml_client().list_service_accounts(
                name=name,
                active=active,
                page=page,
                size=size,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(f"Failed to list service accounts: {exc}") from exc

    def update_service_account(
        self,
        *,
        name_or_id: str,
        name: str | None,
        description: str | None,
        active: bool | None,
    ) -> Any:
        """Update mutable service-account metadata."""
        try:
            return self.zenml_client().update_service_account(
                name_id_or_prefix=name_or_id,
                updated_name=name,
                description=description,
                active=active,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update service account {name_or_id!r}: {exc}"
            ) from exc

    def delete_service_account(self, *, name_or_id: str) -> None:
        """Delete a service account."""
        try:
            self.zenml_client().delete_service_account(name_id_or_prefix=name_or_id)
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete service account {name_or_id!r}: {exc}"
            ) from exc

    def create_api_key(
        self,
        *,
        service_account: str,
        name: str,
        description: str,
        set_key: bool = False,
    ) -> APIKeyOperationResult:
        """Create an API key and optionally set it as the local credential."""
        zenml_client = self.zenml_client()
        try:
            api_key = zenml_client.create_api_key(
                service_account_name_id_or_prefix=service_account,
                name=name,
                description=description,
                set_key=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to create API key {name!r} for service "
                f"account {service_account!r}: {exc}"
            ) from exc

        activation_status = None
        if set_key:
            activation_status = self._activate_local_api_key(
                api_key=api_key,
                zenml_client=zenml_client,
                operation="create",
            )
        return APIKeyOperationResult(
            api_key=api_key,
            local_key_activation=activation_status,
        )

    def get_api_key(self, *, service_account: str, name_or_id: str) -> Any:
        """Get metadata for one API key by exact name or ID."""
        try:
            return self.zenml_client().get_api_key(
                service_account_name_id_or_prefix=service_account,
                name_id_or_prefix=name_or_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load API key {name_or_id!r} for service "
                f"account {service_account!r}: {exc}"
            ) from exc

    def list_api_keys(
        self,
        *,
        service_account: str,
        active: bool | None,
        name: str | None,
        page: int,
        size: int,
    ) -> Any:
        """List metadata for API keys owned by a service account."""
        try:
            return self.zenml_client().list_api_keys(
                service_account_name_id_or_prefix=service_account,
                name=name,
                active=active,
                page=page,
                size=size,
                hydrate=True,
            )
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to list API keys for service account "
                f"{service_account!r}: {exc}"
            ) from exc

    def update_api_key(
        self,
        *,
        service_account: str,
        name_or_id: str,
        name: str | None,
        description: str | None,
        active: bool | None,
    ) -> Any:
        """Update mutable API-key metadata."""
        try:
            return self.zenml_client().update_api_key(
                service_account_name_id_or_prefix=service_account,
                name_id_or_prefix=name_or_id,
                name=name,
                description=description,
                active=active,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to update API key {name_or_id!r} for service "
                f"account {service_account!r}: {exc}"
            ) from exc

    def rotate_api_key(
        self,
        *,
        service_account: str,
        name_or_id: str,
        retain_period_minutes: int,
        set_key: bool = False,
    ) -> APIKeyOperationResult:
        """Rotate an API key and optionally set it as the local credential."""
        zenml_client = self.zenml_client()
        try:
            api_key = zenml_client.rotate_api_key(
                service_account_name_id_or_prefix=service_account,
                name_id_or_prefix=name_or_id,
                retain_period_minutes=retain_period_minutes,
                set_key=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to rotate API key {name_or_id!r} for service "
                f"account {service_account!r}: {exc}"
            ) from exc

        activation_status = None
        if set_key:
            activation_status = self._activate_local_api_key(
                api_key=api_key,
                zenml_client=zenml_client,
                operation="rotate",
            )
        return APIKeyOperationResult(
            api_key=api_key,
            local_key_activation=activation_status,
        )

    def delete_api_key(self, *, service_account: str, name_or_id: str) -> None:
        """Delete an API key."""
        try:
            self.zenml_client().delete_api_key(
                service_account_name_id_or_prefix=service_account,
                name_id_or_prefix=name_or_id,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to delete API key {name_or_id!r} for service "
                f"account {service_account!r}: {exc}"
            ) from exc

    def _activate_local_api_key(
        self,
        *,
        api_key: Any,
        zenml_client: Any,
        operation: Literal["create", "rotate"],
    ) -> LocalKeyActivationStatus:
        """Best-effort local activation that never discards the one-time key."""
        raw_key = _api_key_raw_value(api_key)
        previous_credential = _capture_previous_local_api_key(zenml_client)
        try:
            zenml_client.set_api_key(key=raw_key)
        except Exception as exc:
            action = "created" if operation == "create" else "rotated"
            sanitized_error = _sanitize_local_key_activation_error(
                exc,
                raw_key=raw_key,
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
                return LocalKeyActivationStatus(
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
                    raw_key=raw_key,
                    previous_key=previous_credential.previous_api_key,
                )
                return LocalKeyActivationStatus(
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

            return LocalKeyActivationStatus(
                succeeded=False,
                error=(
                    f"{base_error} Kitaru restored the previous local credential, "
                    "so this machine should still be using the credential that was "
                    "active before the attempted activation."
                ),
                rollback_attempted=True,
                rollback_succeeded=True,
            )
        return LocalKeyActivationStatus(succeeded=True)

    @staticmethod
    def _stack_operation_is_patched(name: str, operation: Any) -> bool:
        """Return whether a stack terminal function has been monkeypatched."""
        return operation is not _ORIGINAL_STACK_OPERATIONS[name]

    def _call_stack_operation(
        self,
        name: str,
        operation: Any,
        *,
        patched_call: Callable[[], Any],
        default_call: Callable[[], Any],
    ) -> Any:
        """Call a patched stack operation unchanged, otherwise inject gateway deps."""
        if self._stack_operation_is_patched(name, operation):
            return patched_call()
        return default_call()

    def current_stack(self) -> Any:
        """Return the currently active stack."""
        operation = stack_ops.current_stack
        return self._call_stack_operation(
            "current_stack",
            operation,
            patched_call=lambda: operation(),
            default_call=lambda: operation(client_factory=self.zenml_client),
        )

    def list_stack_entries(self) -> list[Any]:
        """List stacks with active + managed metadata for structured output."""
        operation = stack_ops._list_stack_entries
        return self._call_stack_operation(
            "list_stack_entries",
            operation,
            patched_call=lambda: operation(),
            default_call=lambda: operation(client_factory=self.zenml_client),
        )

    def show_stack_operation(self, name_or_id: str) -> Any:
        """Inspect one stack and translate its component metadata."""
        operation = stack_ops._show_stack_operation
        return self._call_stack_operation(
            "show_stack_operation",
            operation,
            patched_call=lambda: operation(name_or_id),
            default_call=lambda: operation(
                name_or_id,
                client_factory=self.zenml_client,
            ),
        )

    def _remote_stack_create_overrides(self) -> dict[Any, Callable[..., Any]]:
        """Build stack-type dispatch functions using this gateway's client factory."""
        return {
            stack_ops.StackType.LOCAL: self.create_local_stack_operation,
            stack_ops.StackType.KUBERNETES: self.create_kubernetes_stack_operation,
            stack_ops.StackType.VERTEX: self.create_vertex_stack_operation,
            stack_ops.StackType.SAGEMAKER: self.create_sagemaker_stack_operation,
            stack_ops.StackType.AZUREML: self.create_azureml_stack_operation,
        }

    def create_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create a stack by dispatching to the requested stack type flow."""
        operation = stack_ops._create_stack_operation
        result = self._call_stack_operation(
            "create_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                operation_overrides=self._remote_stack_create_overrides(),
            ),
        )
        if self._stack_operation_is_patched("create_stack_operation", operation):
            return result

        from kitaru.analytics import AnalyticsEvent, track

        track(
            AnalyticsEvent.STACK_CREATED,
            {
                "stack_type": kwargs.get("stack_type", stack_ops.StackType.LOCAL).value,
                "activate_requested": kwargs.get("activate", True),
            },
        )
        return result

    def create_local_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create a new local stack and return operation details."""
        operation = stack_ops._create_local_stack_operation
        return self._call_stack_operation(
            "create_local_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                client_factory=self.zenml_client,
                current_stack_getter=self.current_stack,
            ),
        )

    def create_kubernetes_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create a Kubernetes-backed stack."""
        operation = stack_ops._create_kubernetes_stack_operation
        return self._call_stack_operation(
            "create_kubernetes_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                client_factory=self.zenml_client,
            ),
        )

    def create_vertex_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create a Vertex AI stack."""
        operation = stack_ops._create_vertex_stack_operation
        return self._call_stack_operation(
            "create_vertex_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                client_factory=self.zenml_client,
            ),
        )

    def create_sagemaker_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create a SageMaker stack."""
        operation = stack_ops._create_sagemaker_stack_operation
        return self._call_stack_operation(
            "create_sagemaker_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                client_factory=self.zenml_client,
            ),
        )

    def create_azureml_stack_operation(self, name: str, **kwargs: Any) -> Any:
        """Create an AzureML stack."""
        operation = stack_ops._create_azureml_stack_operation
        return self._call_stack_operation(
            "create_azureml_stack_operation",
            operation,
            patched_call=lambda: operation(name, **kwargs),
            default_call=lambda: operation(
                name,
                **kwargs,
                client_factory=self.zenml_client,
            ),
        )

    def delete_stack_operation(self, name_or_id: str, **kwargs: Any) -> Any:
        """Delete a stack and return operation details."""
        operation = stack_ops._delete_stack_operation
        return self._call_stack_operation(
            "delete_stack_operation",
            operation,
            patched_call=lambda: operation(name_or_id, **kwargs),
            default_call=lambda: operation(
                name_or_id,
                **kwargs,
                client_factory=self.zenml_client,
                current_stack_getter=self.current_stack,
            ),
        )

    def list_stacks(self) -> list[Any]:
        """List stacks visible to the current user and mark the active one."""
        return [entry.stack for entry in self.list_stack_entries()]

    def use_stack(self, name_or_id: str) -> Any:
        """Set the active stack and return the resulting stack info."""
        operation = stack_ops.use_stack
        result = self._call_stack_operation(
            "use_stack",
            operation,
            patched_call=lambda: operation(name_or_id),
            default_call=lambda: operation(
                name_or_id,
                client_factory=self.zenml_client,
                current_stack_getter=self.current_stack,
            ),
        )

        from kitaru.analytics import AnalyticsEvent, track

        track(AnalyticsEvent.STACK_ACTIVATED, {})
        return result


__all__ = ["APIKeyOperationResult", "KitaruBackendGateway", "LocalKeyActivationStatus"]
