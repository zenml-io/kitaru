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
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, cast

from pydantic import ValidationError
from zenml.client import Client
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.models import PipelineRunResponse, StepRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.utils.run_utils import stop_run

from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    FrozenExecutionSpec,
    resolve_connection_config,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    classify_failure_origin,
    traceback_exception_type,
    traceback_last_line,
)

_CHECKPOINT_SOURCE_ALIAS_PREFIX = "__kitaru_checkpoint_source_"
_PIPELINE_SOURCE_ALIAS_PREFIX = "__kitaru_pipeline_source_"


class ExecutionStatus(StrEnum):
    """Simplified public execution status taxonomy."""

    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class PendingWait:
    """Public view of an active wait condition."""

    wait_id: str
    name: str
    question: str | None
    schema: dict[str, Any] | None
    metadata: dict[str, Any]
    entered_waiting_at: datetime | None


@dataclass(frozen=True)
class FailureInfo:
    """Structured failure details for executions/checkpoints."""

    message: str
    exception_type: str | None
    traceback: str | None
    origin: FailureOrigin


@dataclass(frozen=True)
class CheckpointAttempt:
    """One checkpoint attempt in retry/failure journaling history."""

    attempt_id: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    failure: FailureInfo | None


@dataclass(frozen=True)
class ArtifactRef:
    """Reference to an artifact produced by an execution."""

    artifact_id: str
    name: str
    kind: str | None
    save_type: str
    producing_call: str | None
    metadata: dict[str, Any]
    _client: KitaruClient = field(repr=False, compare=False)

    def load(self) -> Any:
        """Load and materialize this artifact value."""
        artifact = self._client._get_artifact_version(
            self.artifact_id,
            hydrate=True,
        )
        return artifact.load()


@dataclass(frozen=True)
class CheckpointCall:
    """Public view of a checkpoint call inside an execution."""

    call_id: str
    name: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    original_call_id: str | None
    parent_call_ids: list[str]
    failure: FailureInfo | None
    attempts: list[CheckpointAttempt]
    artifacts: list[ArtifactRef]


@dataclass(frozen=True)
class Execution:
    """Public view of a Kitaru execution."""

    exec_id: str
    flow_name: str | None
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    stack_name: str | None
    metadata: dict[str, Any]
    status_reason: str | None
    failure: FailureInfo | None
    pending_wait: PendingWait | None
    frozen_execution_spec: FrozenExecutionSpec | None
    original_exec_id: str | None
    checkpoints: list[CheckpointCall]
    artifacts: list[ArtifactRef]
    _client: KitaruClient = field(repr=False, compare=False)

    def refresh(self) -> Execution:
        """Fetch the latest execution state."""
        return self._client.executions.get(self.exec_id)

    def retry(self) -> Execution:
        """Retry this failed execution as a same-execution recovery."""
        return self._client.executions.retry(self.exec_id)

    def cancel(self) -> Execution:
        """Cancel this execution."""
        return self._client.executions.cancel(self.exec_id)

    def replay(
        self,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay this execution from a prior checkpoint boundary."""
        return self._client.executions.replay(
            self.exec_id,
            from_=from_,
            overrides=overrides,
            **flow_inputs,
        )

    def list_checkpoints(self) -> list[CheckpointCall]:
        """Return checkpoint calls for this execution."""
        return list(self.checkpoints)

    def list_artifacts(self) -> list[ArtifactRef]:
        """Return artifact refs for this execution."""
        return list(self.artifacts)


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


def _normalize_checkpoint_name(step_name: str) -> str:
    """Normalize internal checkpoint source alias names."""
    if step_name.startswith(_CHECKPOINT_SOURCE_ALIAS_PREFIX):
        return step_name.removeprefix(_CHECKPOINT_SOURCE_ALIAS_PREFIX)
    return step_name


def _normalize_flow_name(flow_name: str | None) -> str | None:
    """Normalize internal flow source alias names."""
    if flow_name is None:
        return None
    if flow_name.startswith(_PIPELINE_SOURCE_ALIAS_PREFIX):
        return flow_name.removeprefix(_PIPELINE_SOURCE_ALIAS_PREFIX)
    return flow_name


def _to_plain_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    """Convert metadata mappings to plain dictionaries."""
    return {str(key): value for key, value in values.items()}


def _to_public_status(status: ZenMLExecutionStatus) -> ExecutionStatus:
    """Map ZenML execution states to Kitaru public states."""
    status_value = status.value

    if status_value in {
        "initializing",
        "provisioning",
        "running",
        "retrying",
    }:
        return ExecutionStatus.RUNNING
    if status_value == "paused":
        return ExecutionStatus.WAITING
    if status_value in {
        "completed",
        "cached",
        "skipped",
    }:
        return ExecutionStatus.COMPLETED
    if status_value in {
        "failed",
        "retried",
    }:
        return ExecutionStatus.FAILED
    if status_value in {
        "stopped",
        "stopping",
    }:
        return ExecutionStatus.CANCELLED

    raise KitaruRuntimeError(
        f"Unsupported execution status mapping: {status!r} (value={status_value!r})."
    )


def _coerce_status_filter(
    status: ExecutionStatus | str | None,
) -> ExecutionStatus | None:
    """Normalize status filter input."""
    if status is None:
        return None
    if isinstance(status, ExecutionStatus):
        return status

    normalized = status.strip().lower()
    try:
        return ExecutionStatus(normalized)
    except ValueError as exc:
        expected = ", ".join(item.value for item in ExecutionStatus)
        raise KitaruUsageError(
            f"Unsupported status filter {status!r}. Expected one of: {expected}."
        ) from exc


def _parse_frozen_execution_spec(raw_value: Any) -> FrozenExecutionSpec | None:
    """Parse frozen execution metadata when available."""
    if raw_value is None:
        return None
    if not isinstance(raw_value, Mapping):
        return None

    try:
        return FrozenExecutionSpec.model_validate(raw_value)
    except ValidationError:
        return None


def _map_failure_info(
    *,
    status_reason: str | None,
    exception_info: Any,
    default_origin: FailureOrigin,
    fallback_message: str | None = None,
) -> FailureInfo | None:
    """Build structured failure info from status + exception payloads."""
    traceback_text: str | None = None
    if exception_info is not None:
        traceback_text = getattr(exception_info, "traceback", None)

    traceback_tail = traceback_last_line(traceback_text)
    message = status_reason or traceback_tail or fallback_message
    if message is None:
        return None

    origin = classify_failure_origin(
        status_reason=status_reason,
        traceback=traceback_text,
        default=default_origin,
    )
    return FailureInfo(
        message=message,
        exception_type=traceback_exception_type(traceback_text),
        traceback=traceback_text,
        origin=origin,
    )


def _checkpoint_lineage_key(step: StepRunResponse) -> str:
    """Return the stable lineage key for checkpoint retry grouping."""
    if step.original_step_run_id is not None:
        return str(step.original_step_run_id)
    return str(step.id)


def _list_checkpoint_attempts_for_run(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> dict[str, list[StepRunResponse]]:
    """Fetch all step attempts for one execution, including retried runs."""
    grouped_attempts: defaultdict[str, list[StepRunResponse]] = defaultdict(list)
    page = 1
    page_size = 200

    try:
        while True:
            step_page = client._client().list_run_steps(
                sort_by="asc:created",
                page=page,
                size=page_size,
                pipeline_run_id=run.id,
                project=client._project,
                exclude_retried=False,
                hydrate=True,
            )
            step_items = list(step_page.items)
            if not step_items:
                break

            for step in step_items:
                grouped_attempts[_checkpoint_lineage_key(step)].append(step)

            if len(step_items) < page_size:
                break
            page += 1
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to fetch checkpoint attempts for execution {run.id}: {exc}"
        ) from exc

    return dict(grouped_attempts)


def _map_checkpoint_attempt(step: StepRunResponse) -> CheckpointAttempt:
    """Map one step run model into a checkpoint-attempt entry."""
    public_status = _to_public_status(step.status)
    checkpoint_name = _normalize_checkpoint_name(step.name)

    failure = _map_failure_info(
        status_reason=None,
        exception_info=getattr(step, "exception_info", None),
        default_origin=FailureOrigin.USER_CODE,
        fallback_message=(
            f"Checkpoint '{checkpoint_name}' failed."
            if public_status == ExecutionStatus.FAILED
            else None
        ),
    )

    return CheckpointAttempt(
        attempt_id=str(step.id),
        status=public_status,
        started_at=step.start_time,
        ended_at=step.end_time,
        metadata=_to_plain_dict(step.run_metadata),
        failure=failure,
    )


def _map_artifact_ref(
    *,
    artifact: ArtifactVersionResponse,
    client: KitaruClient,
    producing_call: str | None,
) -> ArtifactRef:
    """Map a ZenML artifact response into a Kitaru artifact reference."""
    metadata = _to_plain_dict(artifact.run_metadata)
    raw_kind = metadata.get("kitaru_artifact_type")
    kind = raw_kind if isinstance(raw_kind, str) else None

    return ArtifactRef(
        artifact_id=str(artifact.id),
        name=artifact.name,
        kind=kind,
        save_type=artifact.save_type.value,
        producing_call=producing_call,
        metadata=metadata,
        _client=client,
    )


def _map_checkpoint_call(
    *,
    step: StepRunResponse,
    client: KitaruClient,
    attempts_by_lineage: Mapping[str, list[StepRunResponse]],
) -> CheckpointCall:
    """Map a ZenML step run into a Kitaru checkpoint call."""
    producing_call = _normalize_checkpoint_name(step.name)
    seen_artifact_ids: set[str] = set()
    artifacts: list[ArtifactRef] = []

    for output_artifacts in step.outputs.values():
        for artifact in output_artifacts:
            artifact_id = str(artifact.id)
            if artifact_id in seen_artifact_ids:
                continue
            seen_artifact_ids.add(artifact_id)
            artifacts.append(
                _map_artifact_ref(
                    artifact=artifact,
                    client=client,
                    producing_call=producing_call,
                )
            )

    lineage_key = _checkpoint_lineage_key(step)
    attempt_steps = attempts_by_lineage.get(lineage_key, [step])
    attempts = [_map_checkpoint_attempt(attempt) for attempt in attempt_steps]

    failure = None
    if attempts:
        failure = attempts[-1].failure

    original_call_id: str | None = None
    if step.original_step_run_id is not None:
        original_call_id = str(step.original_step_run_id)

    return CheckpointCall(
        call_id=str(step.id),
        name=producing_call,
        status=_to_public_status(step.status),
        started_at=step.start_time,
        ended_at=step.end_time,
        metadata=_to_plain_dict(step.run_metadata),
        original_call_id=original_call_id,
        parent_call_ids=[str(parent_id) for parent_id in step.parent_step_ids],
        failure=failure,
        attempts=attempts,
        artifacts=artifacts,
    )


def _map_pending_wait(wait_condition: Any) -> PendingWait:
    """Map a wait condition response to the public pending wait model."""
    data_schema = wait_condition.data_schema
    schema: dict[str, Any] | None = None
    if data_schema is not None:
        schema = dict(data_schema)

    return PendingWait(
        wait_id=str(wait_condition.id),
        name=wait_condition.wait_condition_key,
        question=wait_condition.question,
        schema=schema,
        metadata=dict(wait_condition.wait_metadata),
        entered_waiting_at=getattr(wait_condition, "created", None),
    )


def _first_pending_wait(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> PendingWait | None:
    """Resolve the active pending wait condition for a run."""
    resources = cast(Any, run.get_resources())
    active_wait = resources.active_wait_condition
    if active_wait is not None:
        return _map_pending_wait(active_wait)

    try:
        wait_conditions_page = cast(
            Any,
            client._client(),
        ).list_run_wait_conditions(
            run_name_or_id=run.id,
            project=client._project,
            status="pending",
            hydrate=True,
            size=1,
        )
    except AttributeError:
        return None
    if not wait_conditions_page.items:
        return None

    return _map_pending_wait(wait_conditions_page.items[0])


def _map_execution(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    include_details: bool,
) -> Execution:
    """Map a ZenML pipeline run into a Kitaru execution model."""
    status = _to_public_status(run.status)

    status_reason = getattr(run, "status_reason", None)
    run_exception_info = getattr(run, "exception_info", None)

    failure: FailureInfo | None = None
    if status == ExecutionStatus.FAILED:
        failure = _map_failure_info(
            status_reason=status_reason,
            exception_info=run_exception_info,
            default_origin=(
                FailureOrigin.USER_CODE
                if run_exception_info is not None
                else FailureOrigin.UNKNOWN
            ),
            fallback_message=f"Execution {run.id} failed.",
        )

    checkpoints: list[CheckpointCall] = []
    artifacts: list[ArtifactRef] = []
    if include_details:
        attempts_by_lineage: dict[str, list[StepRunResponse]] = {}
        try:
            attempts_by_lineage = _list_checkpoint_attempts_for_run(
                run=run,
                client=client,
            )
        except KitaruBackendError:
            # Degrade gracefully: execution inspection should still work even
            # when deep attempt history cannot be fetched.
            attempts_by_lineage = {}

        latest_steps_by_lineage: dict[str, StepRunResponse] = {}
        for lineage_key, attempts in attempts_by_lineage.items():
            if attempts:
                latest_steps_by_lineage[lineage_key] = attempts[-1]

        for step in run.steps.values():
            lineage_key = _checkpoint_lineage_key(step)
            latest_steps_by_lineage.setdefault(lineage_key, step)
            attempts_by_lineage.setdefault(lineage_key, [step])

        for step in latest_steps_by_lineage.values():
            checkpoints.append(
                _map_checkpoint_call(
                    step=step,
                    client=client,
                    attempts_by_lineage=attempts_by_lineage,
                )
            )

        seen_artifact_ids: set[str] = set()
        for checkpoint in checkpoints:
            for artifact in checkpoint.artifacts:
                if artifact.artifact_id in seen_artifact_ids:
                    continue
                seen_artifact_ids.add(artifact.artifact_id)
                artifacts.append(artifact)

    metadata = _to_plain_dict(run.run_metadata)

    pending_wait: PendingWait | None = None
    if status == ExecutionStatus.WAITING:
        pending_wait = _first_pending_wait(run=run, client=client)

    flow_name: str | None = None
    if run.pipeline is not None:
        flow_name = _normalize_flow_name(run.pipeline.name)

    original_exec_id: str | None = None
    if run.original_run is not None:
        original_exec_id = str(run.original_run.id)

    stack_name: str | None = None
    if run.stack is not None:
        stack_name = run.stack.name

    return Execution(
        exec_id=str(run.id),
        flow_name=flow_name,
        status=status,
        started_at=run.start_time,
        ended_at=run.end_time,
        stack_name=stack_name,
        metadata=metadata,
        status_reason=status_reason,
        failure=failure,
        pending_wait=pending_wait,
        frozen_execution_spec=_parse_frozen_execution_spec(
            metadata.get(FROZEN_EXECUTION_SPEC_METADATA_KEY)
        ),
        original_exec_id=original_exec_id,
        checkpoints=checkpoints,
        artifacts=artifacts,
        _client=client,
    )


class _ExecutionsAPI:
    """Namespace for execution lifecycle and inspection operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def input(self, exec_id: str, *, wait: str, value: Any) -> Execution:
        """Provide input to a waiting execution.

        This API is intentionally deferred until wait/resume branch support is
        available for Kitaru's full resume semantics.
        """
        _ = (exec_id, wait, value)
        raise KitaruFeatureNotAvailableError(
            "client.executions.input() is not implemented yet. "
            "Waiting input/resume depends on pending ZenML wait/resume branch "
            "integration for Kitaru."
        )

    def retry(self, exec_id: str) -> Execution:
        """Retry a failed execution as same-execution recovery."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        if run.status != ZenMLExecutionStatus.FAILED:
            raise KitaruStateError(
                "Only failed executions can be retried. "
                f"Execution '{exec_id}' is currently '{run.status.value}'."
            )

        snapshot = run.snapshot
        if snapshot is None:
            raise KitaruRuntimeError(
                "Unable to retry execution because snapshot metadata is missing."
            )
        if snapshot.stack is None:
            raise KitaruRuntimeError(
                "Unable to retry execution because snapshot stack metadata is missing."
            )

        try:
            with _temporary_active_stack(str(snapshot.stack.id)):
                active_stack = self._client_ref._client().active_stack
                orchestrator = cast(Any, active_stack.orchestrator)
                orchestrator.restart(
                    snapshot=snapshot,
                    run=run,
                    stack=active_stack,
                )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to retry execution '{exec_id}': {exc}"
            ) from exc

        return self.get(exec_id)

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay an execution from a prior checkpoint boundary."""
        _ = (exec_id, from_, overrides, flow_inputs)
        raise KitaruFeatureNotAvailableError(
            "client.executions.replay() is not implemented yet. "
            "Replay support remains branch-dependent and will be added once "
            "Kitaru can safely wrap the upstream replay APIs."
        )

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

        resolved_connection = resolve_connection_config()
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
    "PendingWait",
]
