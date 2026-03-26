"""Kitaru client for execution and artifact management.

`KitaruClient` provides a programmatic API for inspecting and managing
executions outside flow bodies.

Example::

    from kitaru import KitaruClient

    client = KitaruClient()
    execution = client.executions.get("exec-123")
    print(execution.status)
"""

from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Protocol, cast, runtime_checkable

from pydantic import ValidationError
from zenml.client import Client
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.models import PipelineRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.utils.run_utils import stop_run
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru._client._logs import (
    _coerce_log_level,
    _coerce_log_lineno,
    _coerce_log_text,
    _is_empty_log_result_error,
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
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
    FailureInfo,
    LogEntry,
    PendingWait,
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
from kitaru.engines.zenml.snapshots import execution_graph_from_run
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
from kitaru.replay import build_replay_plan

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
       as ``examples.replay.replay_with_overrides``).
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
            snapshot=execution_graph_from_run(source_run),
            from_=from_,
            overrides=overrides,
            flow_inputs=flow_inputs,
        )

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
            raise KitaruRuntimeError("Replay did not produce a pipeline run ID.")

        track(AnalyticsEvent.FLOW_REPLAYED, {"execution_id": replayed_exec_id})
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
    ) -> builtins.list[Execution]:
        """List executions with optional flow/status filters."""
        status_filter = _coerce_status_filter(status)

        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        results: list[Execution] = []
        page = 1
        page_size = 50 if limit is None else max(50, limit)

        while True:
            run_page = self._client_ref._client().list_pipeline_runs(
                sort_by="desc:created",
                page=page,
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

                results.append(execution)
                if limit is not None and len(results) >= limit:
                    return results

            if len(runs) < page_size:
                break
            page += 1

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


class KitaruClient:
    """Client for managing Kitaru executions and artifacts."""

    def __init__(
        self,
        *,
        server_url: str | None = None,
        auth_token: str | None = None,
        project: str | None = None,
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

        resolved_connection = resolve_connection_config(validate_for_use=True)
        self._project = resolved_connection.project

        self.executions = _ExecutionsAPI(self)
        self.artifacts = _ArtifactsAPI(self)

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
    "CheckpointAttempt",
    "CheckpointCall",
    "Execution",
    "ExecutionStatus",
    "FailureInfo",
    "KitaruClient",
    "LogEntry",
    "PendingWait",
]
