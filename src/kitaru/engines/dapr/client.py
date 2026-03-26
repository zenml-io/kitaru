"""Dapr client adapter for KitaruClient execution browsing and lifecycle.

Maps Dapr execution ledger records into the public client models
(``Execution``, ``CheckpointCall``, ``ArtifactRef``, etc.) that
``KitaruClient`` returns.  All operations use the ``ExecutionLedgerStore``
backed by a Dapr state store; a best-effort ``WorkflowClient`` lookup
provides live workflow status when available.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from kitaru._client._logs import _normalize_log_source, _sort_log_entries
from kitaru._client._mappers import _coerce_status_filter, _parse_frozen_execution_spec
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
from kitaru.engines._types import (
    CheckpointGraphNode,
    ExecutionGraphSnapshot,
)
from kitaru.engines.dapr.models import (
    CheckpointCallRecord,
    ExecutionLedgerRecord,
    FailureRecord,
)
from kitaru.engines.dapr.store import ExecutionLedgerStore
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.replay import build_replay_plan

if TYPE_CHECKING:
    from kitaru.client import KitaruClient


# ---------------------------------------------------------------------------
# Workflow client duck-typing protocol
# ---------------------------------------------------------------------------


class WorkflowStateLike(Protocol):
    """Minimal contract for Dapr workflow state objects."""

    instance_id: str
    runtime_status: str
    created_at: datetime | None
    last_updated_at: datetime | None


class WorkflowClientLike(Protocol):
    """Minimal contract for Dapr workflow client operations."""

    def get_workflow_state(self, instance_id: str) -> WorkflowStateLike: ...

    def raise_workflow_event(
        self, instance_id: str, event_name: str, data: Any
    ) -> None: ...

    def terminate_workflow(self, instance_id: str) -> None: ...

    def resume_workflow(self, instance_id: str) -> None: ...

    def schedule_new_workflow(
        self,
        workflow_name: str,
        *,
        input: Any,
        instance_id: str | None = None,
    ) -> str: ...


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------

_DAPR_STATUS_MAP: dict[str, ExecutionStatus] = {
    "pending": ExecutionStatus.RUNNING,
    "running": ExecutionStatus.RUNNING,
    "completed": ExecutionStatus.COMPLETED,
    "failed": ExecutionStatus.FAILED,
    "terminated": ExecutionStatus.CANCELLED,
}


def _to_dapr_public_status(
    *,
    ledger_status: str | None,
    workflow_status: str | None,
    has_pending_waits: bool,
) -> ExecutionStatus:
    """Map Dapr workflow + ledger state to public execution status."""
    if has_pending_waits:
        return ExecutionStatus.WAITING

    raw = (workflow_status or ledger_status or "pending").lower()

    if raw == "suspended":
        return ExecutionStatus.RUNNING

    mapped = _DAPR_STATUS_MAP.get(raw)
    if mapped is not None:
        return mapped

    raise KitaruRuntimeError(f"Unsupported Dapr workflow status: {raw!r}.")


# ---------------------------------------------------------------------------
# Checkpoint status mapping
# ---------------------------------------------------------------------------

_CHECKPOINT_STATUS_MAP: dict[str, ExecutionStatus] = {
    "pending": ExecutionStatus.RUNNING,
    "running": ExecutionStatus.RUNNING,
    "completed": ExecutionStatus.COMPLETED,
    "failed": ExecutionStatus.FAILED,
}


def _checkpoint_public_status(raw: str) -> ExecutionStatus:
    mapped = _CHECKPOINT_STATUS_MAP.get(raw.lower())
    if mapped is not None:
        return mapped
    return ExecutionStatus.RUNNING


# ---------------------------------------------------------------------------
# Failure mapping
# ---------------------------------------------------------------------------


def _map_ledger_failure(failure: FailureRecord | None) -> FailureInfo | None:
    """Map a Dapr ledger failure record to the public FailureInfo model."""
    if failure is None:
        return None
    return FailureInfo(
        message=failure.message,
        exception_type=failure.exception_type,
        traceback=failure.traceback,
        origin=failure.origin,
    )


# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------


def _pending_waits_from_ledger(
    record: ExecutionLedgerRecord,
) -> list[PendingWait]:
    """Extract pending wait conditions from the ledger."""
    pending: list[PendingWait] = []
    for w in record.waits:
        if w.status != "pending":
            continue
        pending.append(
            PendingWait(
                wait_id=w.wait_id,
                name=w.name,
                question=w.question,
                schema=w.schema,
                metadata=dict(w.metadata),
                entered_waiting_at=w.entered_at,
            )
        )
    # Sort: earliest entered_at first, then by name, then wait_id
    pending.sort(
        key=lambda pw: (
            pw.entered_waiting_at.isoformat() if pw.entered_waiting_at else "\xff",
            pw.name,
            pw.wait_id,
        )
    )
    return pending


def _select_dapr_wait(
    *,
    exec_id: str,
    wait: str,
    pending: list[PendingWait],
) -> PendingWait:
    """Resolve a wait selector to exactly one pending wait."""
    selector = wait.strip()
    if not selector:
        raise KitaruUsageError("`wait` must be a non-empty string.")

    name_matches = [pw for pw in pending if pw.name == selector]
    if len(name_matches) == 1:
        return name_matches[0]
    if len(name_matches) > 1:
        raise KitaruStateError(
            f"Multiple pending waits match '{selector}' for execution '{exec_id}'."
        )

    id_matches = [pw for pw in pending if pw.wait_id == selector]
    if len(id_matches) == 1:
        return id_matches[0]

    available = ", ".join(sorted({pw.name for pw in pending}))
    raise KitaruStateError(
        f"Execution '{exec_id}' has no pending wait '{selector}'. "
        f"Available waits: {available}."
    )


# ---------------------------------------------------------------------------
# Execution graph mapping (for replay planning)
# ---------------------------------------------------------------------------


def _ledger_to_execution_graph(
    record: ExecutionLedgerRecord,
) -> ExecutionGraphSnapshot:
    """Build a backend-neutral ExecutionGraphSnapshot from ledger data."""
    nodes: list[CheckpointGraphNode] = []
    for cp in record.checkpoints:
        # Use call_id as both call_id and invocation_id for Dapr replay
        nodes.append(
            CheckpointGraphNode(
                call_id=cp.call_id,
                invocation_id=cp.call_id,
                name=cp.name,
                upstream_invocation_ids=cp.upstream_call_ids,
                input_bindings=(),
                output_names=("output",),
                start_time=cp.started_at,
                end_time=cp.ended_at,
            )
        )
    return ExecutionGraphSnapshot(
        exec_id=record.exec_id,
        flow_name=record.flow_name,
        checkpoints=tuple(nodes),
    )


# ---------------------------------------------------------------------------
# DaprClientAdapter
# ---------------------------------------------------------------------------


class DaprClientAdapter:
    """Adapter that maps Dapr ledger + workflow state to KitaruClient models.

    This class is instantiated lazily by ``KitaruClient`` when the active
    engine backend is ``"dapr"``.  All operations read from the
    ``ExecutionLedgerStore``; best-effort ``WorkflowClientLike`` lookups
    provide live status.
    """

    def __init__(
        self,
        *,
        store: ExecutionLedgerStore,
        workflow_client: WorkflowClientLike | None = None,
    ) -> None:
        self._store = store
        self._workflow_client = workflow_client

    # -- Internal helpers ---------------------------------------------------

    def _workflow_status(self, exec_id: str) -> str | None:
        """Best-effort live workflow status lookup."""
        if self._workflow_client is None:
            return None
        try:
            state = self._workflow_client.get_workflow_state(exec_id)
            return state.runtime_status
        except LookupError:
            return None
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to query workflow state for '{exec_id}': {exc}"
            ) from exc

    def _map_checkpoint_call(
        self,
        cp: CheckpointCallRecord,
        client: KitaruClient,
    ) -> CheckpointCall:
        """Map a ledger checkpoint call record to the public model."""
        attempts = [
            CheckpointAttempt(
                attempt_id=a.attempt_id,
                status=_checkpoint_public_status(a.status),
                started_at=a.started_at,
                ended_at=a.ended_at,
                metadata=dict(a.metadata),
                failure=_map_ledger_failure(a.failure),
            )
            for a in cp.attempts
        ]

        artifacts = [
            ArtifactRef(
                artifact_id=a.artifact_id,
                name=a.name,
                kind=a.kind,
                save_type=a.save_type,
                producing_call=cp.name,
                metadata=dict(a.metadata),
                _client=client,
            )
            for a in cp.artifacts
        ]

        failure = _map_ledger_failure(cp.failure)
        if failure is None and attempts:
            failure = attempts[-1].failure

        return CheckpointCall(
            call_id=cp.call_id,
            name=cp.name,
            status=_checkpoint_public_status(cp.status),
            started_at=cp.started_at,
            ended_at=cp.ended_at,
            metadata=dict(cp.metadata),
            original_call_id=cp.original_call_id,
            parent_call_ids=list(cp.upstream_call_ids),
            failure=failure,
            attempts=attempts,
            artifacts=artifacts,
            checkpoint_type=cp.checkpoint_type,
        )

    def _map_execution(
        self,
        record: ExecutionLedgerRecord,
        client: KitaruClient,
        *,
        include_details: bool,
        workflow_status: str | None = None,
    ) -> Execution:
        """Map a ledger record + workflow state into a public Execution."""
        pending = _pending_waits_from_ledger(record)
        has_pending = len(pending) > 0

        status = _to_dapr_public_status(
            ledger_status=record.status,
            workflow_status=workflow_status,
            has_pending_waits=has_pending,
        )

        failure: FailureInfo | None = None
        if status == ExecutionStatus.FAILED:
            failure = _map_ledger_failure(record.failure)
            if failure is None:
                failure = FailureInfo(
                    message=f"Execution {record.exec_id} failed.",
                    exception_type=None,
                    traceback=None,
                    origin=FailureOrigin.UNKNOWN,
                )

        checkpoints: list[CheckpointCall] = []
        artifacts: list[ArtifactRef] = []
        if include_details:
            for cp in record.checkpoints:
                checkpoints.append(self._map_checkpoint_call(cp, client))

            seen_ids: set[str] = set()
            for checkpoint in checkpoints:
                for artifact in checkpoint.artifacts:
                    if artifact.artifact_id not in seen_ids:
                        seen_ids.add(artifact.artifact_id)
                        artifacts.append(artifact)
            # Also include execution-level artifacts not tied to a checkpoint
            for a in record.artifacts:
                if a.artifact_id not in seen_ids:
                    seen_ids.add(a.artifact_id)
                    artifacts.append(
                        ArtifactRef(
                            artifact_id=a.artifact_id,
                            name=a.name,
                            kind=a.kind,
                            save_type=a.save_type,
                            producing_call=a.producing_call_id,
                            metadata=dict(a.metadata),
                            _client=client,
                        )
                    )

        metadata = dict(record.metadata)
        frozen_spec = _parse_frozen_execution_spec(record.frozen_execution_spec)

        return Execution(
            exec_id=record.exec_id,
            flow_name=record.flow_name,
            status=status,
            started_at=record.created_at,
            ended_at=record.ended_at,
            stack_name=None,
            metadata=metadata,
            status_reason=record.status_reason,
            failure=failure,
            pending_wait=pending[0] if pending else None,
            frozen_execution_spec=frozen_spec,
            original_exec_id=record.original_exec_id,
            checkpoints=checkpoints,
            artifacts=artifacts,
            _client=client,
        )

    def _schedule_new_execution(
        self,
        *,
        record: ExecutionLedgerRecord,
        source_exec_id: str,
        payload: dict[str, Any],
        frozen_spec: Any,
        client: KitaruClient,
        operation: str,
    ) -> Execution:
        """Schedule a new workflow and seed its ledger record."""
        assert record.workflow_name is not None
        new_exec_id = str(uuid4())

        if self._workflow_client is not None:
            try:
                new_exec_id = self._workflow_client.schedule_new_workflow(
                    record.workflow_name,
                    input=payload,
                    instance_id=new_exec_id,
                )
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to schedule {operation} for execution "
                    f"'{source_exec_id}': {exc}"
                ) from exc

        new_record = ExecutionLedgerRecord(
            exec_id=new_exec_id,
            project=record.project,
            flow_name=record.flow_name,
            workflow_name=record.workflow_name,
            status="pending",
            created_at=datetime.now(UTC),
            original_exec_id=source_exec_id,
            frozen_execution_spec=frozen_spec,
        )
        with contextlib.suppress(Exception):
            self._store.create_execution(new_record)

        return self._map_execution(
            new_record,
            client,
            include_details=False,
            workflow_status="pending",
        )

    # -- Public API ---------------------------------------------------------

    def get_execution(
        self,
        exec_id: str,
        client: KitaruClient,
        *,
        include_details: bool = True,
    ) -> Execution:
        """Get one execution by ID, merging ledger and workflow state."""
        record = self._store.get_execution(exec_id)
        workflow_status = self._workflow_status(exec_id)
        return self._map_execution(
            record,
            client,
            include_details=include_details,
            workflow_status=workflow_status,
        )

    def list_executions(
        self,
        client: KitaruClient,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
        limit: int | None = None,
    ) -> list[Execution]:
        """List executions with optional filters."""
        status_filter = _coerce_status_filter(status)
        exec_ids = self._store.list_execution_ids()

        results: list[Execution] = []
        for eid in reversed(exec_ids):
            try:
                record = self._store.get_execution(eid)
            except KitaruRuntimeError:
                continue

            # Filter by flow_name before the workflow RPC
            if flow is not None and record.flow_name != flow:
                continue

            workflow_status = self._workflow_status(eid)
            execution = self._map_execution(
                record,
                client,
                include_details=False,
                workflow_status=workflow_status,
            )

            if status_filter is not None and execution.status != status_filter:
                continue

            results.append(execution)
            if limit is not None and len(results) >= limit:
                break

        return results

    def get_pending_waits(self, exec_id: str) -> list[PendingWait]:
        """List pending wait conditions for an execution."""
        record = self._store.get_execution(exec_id)
        return _pending_waits_from_ledger(record)

    def _resolve_or_abort_wait(
        self,
        exec_id: str,
        client: KitaruClient,
        *,
        wait: str,
        event_payload: Any,
        ledger_status: str,
        operation: str,
    ) -> Execution:
        """Shared helper for wait resolution and abort."""
        record = self._store.get_execution(exec_id)
        pending = _pending_waits_from_ledger(record)
        if not pending:
            raise KitaruStateError(
                f"Execution '{exec_id}' has no pending waits to {operation}."
            )
        selected = _select_dapr_wait(exec_id=exec_id, wait=wait, pending=pending)

        if self._workflow_client is not None:
            try:
                self._workflow_client.raise_workflow_event(
                    exec_id, selected.wait_id, event_payload
                )
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to {operation} wait '{selected.name}' "
                    f"on execution '{exec_id}': {exc}"
                ) from exc

        for w in record.waits:
            if w.wait_id == selected.wait_id:
                updated_wait = replace(w, status=ledger_status)
                self._store.upsert_wait(exec_id, updated_wait)
                break

        return self.get_execution(exec_id, client)

    def resolve_wait(
        self,
        exec_id: str,
        client: KitaruClient,
        *,
        wait: str,
        value: Any,
    ) -> Execution:
        """Provide input to resolve a pending wait."""
        return self._resolve_or_abort_wait(
            exec_id,
            client,
            wait=wait,
            event_payload=value,
            ledger_status="resolved",
            operation="resolve",
        )

    def abort_wait(
        self,
        exec_id: str,
        client: KitaruClient,
        *,
        wait: str,
    ) -> Execution:
        """Abort a pending wait condition."""
        return self._resolve_or_abort_wait(
            exec_id,
            client,
            wait=wait,
            event_payload={"__kitaru_resolution": "abort"},
            ledger_status="aborted",
            operation="abort",
        )

    def cancel_execution(
        self,
        exec_id: str,
        client: KitaruClient,
    ) -> Execution:
        """Cancel a running execution."""
        if self._workflow_client is not None:
            try:
                self._workflow_client.terminate_workflow(exec_id)
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to cancel execution '{exec_id}': {exc}"
                ) from exc

        record = self._store.get_execution(exec_id)
        updated = replace(record, status="terminated")
        self._store.replace_execution(exec_id, updated)

        return self.get_execution(exec_id, client)

    def resume_execution(
        self,
        exec_id: str,
        client: KitaruClient,
    ) -> Execution:
        """Resume a suspended execution after waits are resolved."""
        record = self._store.get_execution(exec_id)
        pending = _pending_waits_from_ledger(record)
        if pending:
            raise KitaruStateError(
                f"Resolve pending wait input before resuming execution '{exec_id}'."
            )

        workflow_status = self._workflow_status(exec_id)
        raw_status = (workflow_status or record.status or "").lower()

        if raw_status != "suspended":
            raise KitaruStateError(
                "Only suspended executions can be resumed. "
                f"Execution '{exec_id}' is currently '{raw_status}'."
            )

        if self._workflow_client is not None:
            try:
                self._workflow_client.resume_workflow(exec_id)
            except Exception as exc:
                raise KitaruBackendError(
                    f"Failed to resume execution '{exec_id}': {exc}"
                ) from exc

        return self.get_execution(exec_id, client)

    def retry_execution(
        self,
        exec_id: str,
        client: KitaruClient,
    ) -> Execution:
        """Retry a failed execution by scheduling a new workflow."""
        record = self._store.get_execution(exec_id)
        workflow_status = self._workflow_status(exec_id)

        status = _to_dapr_public_status(
            ledger_status=record.status,
            workflow_status=workflow_status,
            has_pending_waits=False,
        )
        if status != ExecutionStatus.FAILED:
            raise KitaruStateError(
                "Only failed executions can be retried. "
                f"Execution '{exec_id}' is currently '{status.value}'."
            )

        if record.workflow_name is None:
            raise KitaruFeatureNotAvailableError(
                "Cannot retry this execution: it was created before "
                "workflow name persistence was added."
            )

        try:
            original_input = self._store.load_execution_input(exec_id)
        except KitaruRuntimeError as exc:
            raise KitaruFeatureNotAvailableError(
                "Cannot retry this execution: original flow inputs were not persisted."
            ) from exc

        retry_payload = {
            "args": original_input.get("args", ()),
            "kwargs": original_input.get("kwargs", {}),
            "original_exec_id": exec_id,
            "frozen_execution_spec": original_input.get("frozen_execution_spec"),
        }

        return self._schedule_new_execution(
            record=record,
            source_exec_id=exec_id,
            payload=retry_payload,
            frozen_spec=original_input.get("frozen_execution_spec"),
            client=client,
            operation="retry",
        )

    def replay_execution(
        self,
        exec_id: str,
        client: KitaruClient,
        *,
        from_: str,
        overrides: Mapping[str, Any] | None = None,
        flow_inputs: Mapping[str, Any] | None = None,
    ) -> Execution:
        """Replay an execution from a checkpoint boundary."""
        record = self._store.get_execution(exec_id)
        workflow_status = self._workflow_status(exec_id)

        status = _to_dapr_public_status(
            ledger_status=record.status,
            workflow_status=workflow_status,
            has_pending_waits=False,
        )
        if status == ExecutionStatus.RUNNING:
            raise KitaruStateError(
                "Replay requires a non-running source execution. "
                f"Execution '{exec_id}' is currently '{status.value}'."
            )

        if record.workflow_name is None:
            raise KitaruFeatureNotAvailableError(
                "Cannot replay this execution: it was created before "
                "workflow name persistence was added."
            )

        try:
            original_input = self._store.load_execution_input(exec_id)
        except KitaruRuntimeError as exc:
            raise KitaruFeatureNotAvailableError(
                "Cannot replay this execution: original flow inputs were not persisted."
            ) from exc

        # Build execution graph snapshot from ledger
        snapshot = _ledger_to_execution_graph(record)
        replay_plan = build_replay_plan(
            snapshot=snapshot,
            from_=from_,
            overrides=overrides,
            flow_inputs=flow_inputs,
        )

        # Build seeded results from skipped checkpoints using dict for O(1) lookup
        cp_by_id = {cp.call_id: cp for cp in record.checkpoints}
        seeded_results: dict[str, Any] = {}
        for call_id in replay_plan.steps_to_skip:
            cp = cp_by_id.get(call_id)
            if cp is None:
                continue
            for art in cp.artifacts:
                if art.name == "output" or art.producing_call_id == call_id:
                    try:
                        _, value = self._store.load_artifact(art.artifact_id)
                        seeded_results[call_id] = value
                    except KitaruRuntimeError:
                        pass
                    break

        original_kwargs = dict(original_input.get("kwargs", {}))
        if replay_plan.input_overrides:
            original_kwargs.update(replay_plan.input_overrides)

        replay_payload = {
            "args": original_input.get("args", ()),
            "kwargs": original_kwargs,
            "original_exec_id": exec_id,
            "frozen_execution_spec": original_input.get("frozen_execution_spec"),
            "replay_seed": {
                "source_exec_id": exec_id,
                "seeded_results": seeded_results,
            },
            "step_input_overrides": replay_plan.step_input_overrides,
        }

        return self._schedule_new_execution(
            record=record,
            source_exec_id=exec_id,
            payload=replay_payload,
            frozen_spec=original_input.get("frozen_execution_spec"),
            client=client,
            operation="replay",
        )

    def get_logs(
        self,
        exec_id: str,
        *,
        checkpoint: str | None = None,
        source: str = "step",
        limit: int | None = None,
    ) -> list[LogEntry]:
        """Retrieve log entries from the execution ledger."""
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

        record = self._store.get_execution(exec_id)

        entries: list[LogEntry] = []
        for log in record.logs:
            if log.source != normalized_source:
                continue
            if (
                normalized_checkpoint is not None
                and log.checkpoint_name != normalized_checkpoint
            ):
                continue

            timestamp_str: str | None = None
            if log.timestamp is not None:
                timestamp_str = log.timestamp.isoformat()

            entries.append(
                LogEntry(
                    message=log.message,
                    level=log.level,
                    timestamp=timestamp_str,
                    source=log.source,
                    checkpoint_name=log.checkpoint_name,
                    module=log.module,
                    filename=log.filename,
                    lineno=log.lineno,
                )
            )

        sorted_entries = _sort_log_entries(entries)
        if limit is not None:
            return sorted_entries[:limit]
        return sorted_entries

    def get_artifact_ref(
        self,
        artifact_id: str,
        client: KitaruClient,
    ) -> ArtifactRef:
        """Get one artifact by ID from the Dapr store."""
        record, _ = self._store.load_artifact(artifact_id)

        producing_call: str | None = None
        if record.producing_call_id is not None:
            # Try to resolve checkpoint name from owning execution
            owner_exec_id = record.exec_id
            if owner_exec_id is not None:
                try:
                    exec_record = self._store.get_execution(owner_exec_id)
                    for cp in exec_record.checkpoints:
                        if cp.call_id == record.producing_call_id:
                            producing_call = cp.name
                            break
                except KitaruRuntimeError:
                    pass

            if producing_call is None:
                producing_call = record.producing_call_id

        return ArtifactRef(
            artifact_id=record.artifact_id,
            name=record.name,
            kind=record.kind,
            save_type=record.save_type,
            producing_call=producing_call,
            metadata=dict(record.metadata),
            _client=client,
        )

    def load_artifact_value(self, artifact_id: str) -> Any:
        """Load and return an artifact value from the Dapr store."""
        _, value = self._store.load_artifact(artifact_id)
        return value
