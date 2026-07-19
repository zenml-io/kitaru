"""Persist imported traces as immutable observed executions."""

import hashlib
import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.config.pipeline_configurations import PipelineConfiguration
from zenml.config.pipeline_spec import PipelineSpec
from zenml.config.step_configurations import Step, StepConfiguration, StepSpec
from zenml.enums import (
    ArtifactSaveType,
    ArtifactType,
    ExecutionStatus,
    MetadataResourceTypes,
    RunWaitConditionLeaseMode,
    RunWaitConditionResolution,
    RunWaitConditionStatus,
    RunWaitConditionType,
    StepType,
)
from zenml.exceptions import EntityExistsError
from zenml.models import (
    ExceptionInfo,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineRunUpdate,
    PipelineSnapshotRequest,
    PipelineSnapshotResponse,
    RunWaitConditionFilter,
    RunWaitConditionLeaseUpdate,
    RunWaitConditionRequest,
    RunWaitConditionResolveRequest,
    RunWaitConditionResponse,
    StepRunRequest,
    StepRunResponse,
)
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.models.v2.misc.run_metadata import RunMetadataResource
from zenml.utils import source_utils

from kitaru._agent_registration import (
    RegisteredAgentVersionBinding,
    verify_hydrated_submitted_run_binding,
    verify_registered_agent_version_binding,
    verify_submitted_run_binding,
)
from kitaru._checkpoint_metadata import adapter_checkpoint_metadata
from kitaru._import_contract import (
    IMPORT_AGENT_NAME_KEY,
    IMPORT_ATTRIBUTION_KEY,
    IMPORT_COHORT_TAG_KEY,
    IMPORT_INTEGRITY_KEY,
    IMPORT_OBSERVATION_COUNT_KEY,
    IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY,
    IMPORT_RAW_EVIDENCE_DIGEST_KEY,
    IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY,
    IMPORT_REPLAY_BUNDLE_DIGEST_KEY,
    IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_PROFILE_VERSION_KEY,
    IMPORT_REPLAY_READINESS_KEY,
    IMPORT_SCHEMA_VERSION_KEY,
    IMPORT_SNAPSHOT_KIND_KEY,
    IMPORT_SOURCE_AGENT_VERSION_ID_KEY,
    IMPORT_SOURCE_AGENT_VERSION_LABEL_KEY,
    IMPORT_SOURCE_CONTENT_DIGEST_KEY,
    IMPORT_SOURCE_FINGERPRINT_KEY,
    IMPORT_SOURCE_PIPELINE_ID_KEY,
    IMPORT_SOURCE_PROJECT_ID_KEY,
    IMPORT_SOURCE_PROVIDER_KEY,
    IMPORT_SOURCE_TRACE_ID_KEY,
    IMPORT_STACK_ID_KEY,
    IMPORT_STATUS_KEY,
    IMPORTED_EXECUTION_ENVIRONMENT_KEY,
    IMPORTED_OBSERVATION_ID_METADATA_KEY,
    IMPORTED_PARENT_OBSERVATION_ID_METADATA_KEY,
)
from kitaru._llm_usage import (
    CalculatedCostMetadata,
    build_usage_record,
    estimate_genai_prices_cost,
    usage_record_metadata,
)
from kitaru.imports._langfuse import LangfuseSourceRecord, strict_json_loads
from kitaru.imports._models import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    ObservationStatus,
    SourceObservationType,
    TraceIntegrity,
)
from kitaru.imports._normalization import normalize_langfuse_records
from kitaru.imports._pydantic_ai_replay import (
    PydanticAIReplayEvidence,
    build_pydantic_ai_replay_evidence,
)
from kitaru.imports._replay_evidence import (
    RawImportedEvidence,
    SourceAttribution,
    SourceAttributionStatus,
    build_raw_imported_evidence,
    canonical_json,
    classify_source_attribution,
    extract_langfuse_provider_stamps,
    sha256_canonical_json,
)

logger = logging.getLogger(__name__)

_IMPORT_SCHEMA_VERSION = 5
_IMPORT_TAG = "kitaru-imported"
_RAW_EVIDENCE_TAG = "kitaru-import-raw-evidence"
_REPLAY_BUNDLE_TAG = "kitaru-import-replay-bundle"
_SNAPSHOT_KIND = "imported_observed"
_ARTIFACT_PROJECT_LOCK = threading.RLock()
_IMPORT_WRITE_OWNER_KEY = "kitaru_import_write_owner_v1"
_IMPORT_WRITE_LEASE_PREFIX = "kitaru-import-write-"
_IMPORT_WRITE_LEASE_DURATION = timedelta(minutes=5)
_IMPORT_WRITE_LEASE_ACQUIRE_GRACE = timedelta(seconds=30)
_IMPORT_WRITE_LEASE_MAX_ATTEMPTS = 3


class ImportedTracePersistenceError(RuntimeError):
    """Raised when a normalized trace cannot be safely persisted."""


class ImportedTraceConflictError(ImportedTracePersistenceError):
    """Raised when a source trace identity already has different content."""

    def __init__(
        self,
        message: str,
        *,
        existing_execution_id: str | None = None,
        resolution: str | None = None,
    ) -> None:
        super().__init__(message)
        self.existing_execution_id = existing_execution_id
        self.resolution = resolution


class ImportedTraceWriteError(ImportedTracePersistenceError):
    """Raised when persistence fails after the synthetic run already exists.

    Carries the execution ID so callers can report which run was left behind
    in a failed state instead of silently orphaning it.
    """

    def __init__(self, message: str, *, execution_id: str) -> None:
        super().__init__(message)
        self.execution_id = execution_id


class ImportedTracePlan(StrEnum):
    """Read-only persistence plan for one imported trace."""

    CREATE = "create"
    RESUME = "resume"
    UNCHANGED = "unchanged"


@dataclass
class _ImportWriteAttempt:
    owner_token: str = field(default_factory=lambda: str(uuid4()))
    lease_id: UUID | None = None
    lease_expires_at: datetime | None = None

    @property
    def claimed(self) -> bool:
        return self.lease_id is not None


@dataclass(frozen=True)
class ImportedExecutionResult:
    """Result of persisting one normalized trace."""

    execution_id: str
    created: bool
    resumed: bool
    observation_count: int
    raw_evidence_artifact_id: str | None = None
    raw_evidence_schema_version: int | None = None
    replay_bundle_artifact_id: str | None = None
    replay_bundle_schema_version: int | None = None


def plan_imported_trace(
    trace: ImportedTrace,
    *,
    binding: RegisteredAgentVersionBinding,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    cohort_tag: str | None = None,
    client: Client | None = None,
    stack_id: UUID | None = None,
) -> ImportedTracePlan:
    """Determine what persistence would do without modifying backend state."""
    _validate_import(trace)
    _validate_evidence(
        trace,
        binding=binding,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
    )
    zenml_client = client or Client()
    verify_registered_agent_version_binding(zenml_client, binding=binding)
    selected_stack_id = stack_id or zenml_client.active_stack_model.id
    identity_digest = _source_identity_digest(trace)
    existing_run = _find_run(
        client=zenml_client,
        project_id=binding.project_id,
        run_name=_run_name(trace, identity_digest=identity_digest),
    )
    if existing_run is None:
        return ImportedTracePlan.CREATE
    _validate_existing_run(
        existing_run,
        trace=trace,
        binding=binding,
        stack_id=selected_stack_id,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        cohort_tag=cohort_tag,
    )
    verify_hydrated_submitted_run_binding(existing_run, binding=binding)
    if not _import_is_complete(
        client=zenml_client,
        run=existing_run,
        project_id=binding.project_id,
        trace=trace,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
    ):
        _reject_active_existing_writer(
            client=zenml_client,
            run=existing_run,
            project_id=binding.project_id,
        )
        return ImportedTracePlan.RESUME
    return ImportedTracePlan.UNCHANGED


def _imported_observation_placeholder() -> None:
    """Identify synthetic dynamic steps; this function must never execute."""
    raise RuntimeError("Synthetic imported observations cannot be executed.")


def persist_imported_trace(
    trace: ImportedTrace,
    *,
    binding: RegisteredAgentVersionBinding,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution,
    cohort_tag: str | None = None,
    client: Client | None = None,
    stack_id: UUID | None = None,
    artifact_store: Any | None = None,
) -> ImportedExecutionResult:
    """Persist one normalized trace as an immutable observed execution.

    No user flow, model, tool, or provider code is executed. The selected ZenML
    stack's artifact store is used to persist artifacts, and the synthetic
    execution snapshot is associated with the same stack. A backend wait-condition
    lease serializes same-source writes across clients and processes.
    """
    return _persist_imported_trace(
        trace,
        binding=binding,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=attribution,
        cohort_tag=cohort_tag,
        client=client,
        stack_id=stack_id,
        artifact_store=artifact_store,
    )


def _persist_imported_trace(
    trace: ImportedTrace,
    *,
    binding: RegisteredAgentVersionBinding,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution,
    cohort_tag: str | None = None,
    client: Client | None = None,
    stack_id: UUID | None = None,
    artifact_store: Any | None = None,
) -> ImportedExecutionResult:
    """Persist one trace after validating evidence and claiming its backend lease."""
    _validate_import(trace)
    _validate_evidence(
        trace,
        binding=binding,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=attribution,
    )

    zenml_client = client or Client()
    verify_registered_agent_version_binding(zenml_client, binding=binding)
    if (stack_id is None) != (artifact_store is None):
        raise ImportedTracePersistenceError(
            "stack_id and artifact_store must be provided together so imported "
            "payloads and the execution snapshot cannot target different stacks."
        )
    selected_stack_id = stack_id or zenml_client.active_stack_model.id
    selected_artifact_store = artifact_store
    if selected_artifact_store is None:
        selected_artifact_store = zenml_client.active_stack.artifact_store
    project_id = binding.project_id
    identity_digest = _source_identity_digest(trace)
    run_name = _run_name(trace, identity_digest=identity_digest)
    import_environment = _import_environment(
        trace,
        binding=binding,
        stack_id=selected_stack_id,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        cohort_tag=cohort_tag,
    )
    existing_run = _find_run(
        client=zenml_client,
        project_id=project_id,
        run_name=run_name,
    )
    if existing_run is not None:
        _validate_existing_run(
            existing_run,
            trace=trace,
            binding=binding,
            stack_id=selected_stack_id,
            raw_evidence=raw_evidence,
            replay_evidence=replay_evidence,
            cohort_tag=cohort_tag,
        )
        verify_hydrated_submitted_run_binding(existing_run, binding=binding)
        if _import_is_complete(
            client=zenml_client,
            run=existing_run,
            project_id=project_id,
            trace=trace,
            raw_evidence=raw_evidence,
            replay_evidence=replay_evidence,
        ):
            return ImportedExecutionResult(
                execution_id=str(existing_run.id),
                created=False,
                resumed=False,
                observation_count=len(trace.observations),
                **_existing_evidence_result_fields(existing_run),
            )

    run_start_time = trace.started_at or min(
        observation.started_at for observation in trace.observations
    )
    run_end_time = trace.ended_at or max(
        observation.ended_at or observation.started_at
        for observation in trace.observations
    )
    step_name_by_observation = {
        observation.id: _step_name(observation, index=index)
        for index, observation in enumerate(trace.observations, start=1)
    }
    step_metadata_by_observation = {
        observation.id: _step_metadata(
            observation,
            step_name=step_name_by_observation[observation.id],
        )
        for observation in trace.observations
    }
    step_config_by_observation = {
        observation.id: _dynamic_step_config(
            step_name=step_name_by_observation[observation.id],
            upstream_names=(
                [step_name_by_observation[observation.parent_id]]
                if observation.parent_id in step_name_by_observation
                else []
            ),
            observation=observation,
            metadata=step_metadata_by_observation[observation.id],
        )
        for observation in trace.observations
    }
    if existing_run is None:
        snapshot = _get_or_create_snapshot(
            client=zenml_client,
            project_id=project_id,
            pipeline_id=binding.pipeline_id,
            pipeline_name=binding.pipeline_name,
            trace=trace,
            step_config_by_observation=step_config_by_observation,
            stack_id=selected_stack_id,
        )
        run_request = PipelineRunRequest(
            project=project_id,
            name=run_name,
            snapshot=snapshot.id,
            orchestrator_run_id=f"kitaru-import-{identity_digest}",
            start_time=run_start_time,
            end_time=run_end_time,
            status=ExecutionStatus.RUNNING,
            orchestrator_environment=import_environment,
            tags=[
                _IMPORT_TAG,
                f"kitaru-import-{_slug(trace.source.provider)}",
                *(
                    [f"kitaru-import-cohort:{cohort_tag}"]
                    if cohort_tag is not None
                    else []
                ),
            ],
        )
        run, created = zenml_client.zen_store.get_or_create_run(run_request)
        if not created:
            run = zenml_client.get_pipeline_run(
                name_id_or_prefix=run.id,
                allow_name_prefix_match=False,
                hydrate=True,
                project=project_id,
            )
            _validate_existing_run(
                run,
                trace=trace,
                binding=binding,
                stack_id=selected_stack_id,
                raw_evidence=raw_evidence,
                replay_evidence=replay_evidence,
                cohort_tag=cohort_tag,
            )
            verify_hydrated_submitted_run_binding(run, binding=binding)
            if _import_is_complete(
                client=zenml_client,
                run=run,
                project_id=project_id,
                trace=trace,
                raw_evidence=raw_evidence,
                replay_evidence=replay_evidence,
            ):
                return ImportedExecutionResult(
                    execution_id=str(run.id),
                    created=False,
                    resumed=False,
                    observation_count=len(trace.observations),
                    **_existing_evidence_result_fields(run),
                )
    else:
        run = existing_run
        created = False

    attempt = _claim_import_write_lease(
        client=zenml_client,
        run=run,
        project_id=project_id,
    )
    try:
        evidence_artifact_ids = _write_imported_run(
            client=zenml_client,
            run=run,
            project_id=project_id,
            trace=trace,
            binding=binding,
            stack_id=selected_stack_id,
            artifact_store=selected_artifact_store,
            raw_evidence=raw_evidence,
            replay_evidence=replay_evidence,
            attribution=attribution,
            cohort_tag=cohort_tag,
            step_name_by_observation=step_name_by_observation,
            step_metadata_by_observation=step_metadata_by_observation,
            step_config_by_observation=step_config_by_observation,
            attempt=attempt,
        )
    except ImportedTraceConflictError as exc:
        if attempt.claimed:
            _mark_import_write_failed(
                client=zenml_client,
                run_id=run.id,
                trace=trace,
                binding=binding,
                stack_id=selected_stack_id,
                raw_evidence=raw_evidence,
                replay_evidence=replay_evidence,
                attribution=attribution,
                cohort_tag=cohort_tag,
                owner_token=attempt.owner_token,
                lease_id=attempt.lease_id,
                allow_missing_owner=created,
                reason="immutable import conflict",
            )
        _release_import_write_lease(client=zenml_client, attempt=attempt)
        if exc.existing_execution_id is None:
            exc.existing_execution_id = str(run.id)
        raise
    except Exception as exc:
        marked_failed = False
        if attempt.claimed:
            marked_failed = _mark_import_write_failed(
                client=zenml_client,
                run_id=run.id,
                trace=trace,
                binding=binding,
                stack_id=selected_stack_id,
                raw_evidence=raw_evidence,
                replay_evidence=replay_evidence,
                attribution=attribution,
                cohort_tag=cohort_tag,
                owner_token=attempt.owner_token,
                lease_id=attempt.lease_id,
                allow_missing_owner=created,
                reason=str(exc),
            )
        _release_import_write_lease(client=zenml_client, attempt=attempt)
        failure_state = (
            "the execution was marked failed and the same export can resume it"
            if marked_failed
            else "the execution was left unchanged by failure cleanup"
        )
        raise ImportedTraceWriteError(
            f"Importing trace {trace.source.trace_id!r} failed for execution "
            f"{run.id}; {failure_state}. Cause: {exc}",
            execution_id=str(run.id),
        ) from exc
    try:
        verify_registered_agent_version_binding(zenml_client, binding=binding)
    finally:
        _release_import_write_lease(client=zenml_client, attempt=attempt)
    verified_run = verify_submitted_run_binding(
        zenml_client,
        run=run,
        binding=binding,
    )
    return ImportedExecutionResult(
        execution_id=str(verified_run.id),
        created=created,
        resumed=not created,
        observation_count=len(trace.observations),
        raw_evidence_artifact_id=evidence_artifact_ids[0],
        raw_evidence_schema_version=raw_evidence.schema_version,
        replay_bundle_artifact_id=evidence_artifact_ids[1],
        replay_bundle_schema_version=replay_evidence.bundle.schema_version,
    )


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _pending_import_write_lease(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
) -> RunWaitConditionResponse | None:
    page = client.zen_store.list_run_wait_conditions(
        RunWaitConditionFilter(
            project=project_id,
            pipeline_run=run.id,
            status=RunWaitConditionStatus.PENDING.value,
            size=2,
        ),
        hydrate=False,
    )
    pending = [
        condition
        for condition in page.items
        if condition.status is RunWaitConditionStatus.PENDING
    ]
    if len(pending) > 1:
        raise ImportedTraceConflictError(
            "The imported execution has multiple pending backend write leases.",
            existing_execution_id=str(run.id),
        )
    if not pending:
        return None
    condition = pending[0]
    if not condition.name.startswith(_IMPORT_WRITE_LEASE_PREFIX):
        raise ImportedTraceConflictError(
            "The imported execution has an unexpected pending wait condition.",
            existing_execution_id=str(run.id),
        )
    return condition


def _lease_is_active(
    condition: RunWaitConditionResponse,
    *,
    now: datetime | None = None,
) -> bool:
    current_time = now or datetime.now(UTC)
    if condition.poller_lease_expires_at is not None:
        return _utc(condition.poller_lease_expires_at) > current_time
    return _utc(condition.created) + _IMPORT_WRITE_LEASE_ACQUIRE_GRACE > current_time


def _active_import_write_lease(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
) -> RunWaitConditionResponse | None:
    condition = _pending_import_write_lease(
        client=client,
        run=run,
        project_id=project_id,
    )
    return condition if condition is not None and _lease_is_active(condition) else None


def _reject_active_existing_writer(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
) -> None:
    if (
        _active_import_write_lease(
            client=client,
            run=run,
            project_id=project_id,
        )
        is not None
    ):
        raise ImportedTraceConflictError(
            "The imported execution is already being written by another attempt.",
            existing_execution_id=str(run.id),
            resolution=(
                "Wait for the active backend lease to expire or finish, then retry "
                "the same export."
            ),
        )


def _resolve_import_write_lease(
    *,
    client: Client,
    condition: RunWaitConditionResponse,
) -> None:
    try:
        client.zen_store.resolve_run_wait_condition(
            condition.id,
            RunWaitConditionResolveRequest(
                user=client.active_user.id,
                resolution=RunWaitConditionResolution.CONTINUE,
                result=None,
            ),
        )
    except Exception:
        current = client.zen_store.get_run_wait_condition(condition.id, hydrate=False)
        if current.status is not RunWaitConditionStatus.RESOLVED:
            raise


def _resolve_stale_import_write_lease(
    *,
    client: Client,
    condition: RunWaitConditionResponse,
    run: PipelineRunResponse,
) -> None:
    current = client.zen_store.get_run_wait_condition(condition.id, hydrate=False)
    if current.status is not RunWaitConditionStatus.PENDING:
        return
    if _lease_is_active(current):
        raise ImportedTraceConflictError(
            "The imported execution write lease was refreshed by its active owner.",
            existing_execution_id=str(run.id),
            resolution="Wait for the active import to finish, then retry.",
        )
    _resolve_import_write_lease(
        client=client,
        condition=current,
    )


def _refresh_import_write_lease(
    *,
    client: Client,
    run: PipelineRunResponse,
    attempt: _ImportWriteAttempt,
) -> None:
    if attempt.lease_id is None:
        raise ImportedTraceConflictError(
            "The import writer has not claimed a backend lease.",
            existing_execution_id=str(run.id),
        )
    current = client.zen_store.get_run_wait_condition(attempt.lease_id, hydrate=False)
    if current.status is not RunWaitConditionStatus.PENDING:
        raise ImportedTraceConflictError(
            "The import writer lost its backend lease before finishing.",
            existing_execution_id=str(run.id),
            resolution="Retry the same export; a new writer can resume missing data.",
        )
    if current.poller_instance_id not in {None, attempt.owner_token}:
        raise ImportedTraceConflictError(
            "The import writer backend lease belongs to another owner.",
            existing_execution_id=str(run.id),
        )
    if (
        current.poller_instance_id == attempt.owner_token
        and current.poller_lease_expires_at is not None
        and not _lease_is_active(current)
    ):
        raise ImportedTraceConflictError(
            "The import writer backend lease expired before it could be refreshed.",
            existing_execution_id=str(run.id),
            resolution="Retry the same export to recover the stale write.",
        )

    expires_at = datetime.now(UTC) + _IMPORT_WRITE_LEASE_DURATION
    client.zen_store.update_run_wait_condition_lease(
        attempt.lease_id,
        RunWaitConditionLeaseUpdate(
            poller_instance_id=attempt.owner_token,
            poller_lease_expires_at=expires_at,
            mode=RunWaitConditionLeaseMode.REFRESH,
        ),
    )
    refreshed = client.zen_store.get_run_wait_condition(attempt.lease_id, hydrate=False)
    if (
        refreshed.status is not RunWaitConditionStatus.PENDING
        or refreshed.poller_instance_id != attempt.owner_token
        or not _lease_is_active(refreshed)
    ):
        raise ImportedTraceConflictError(
            "The import writer could not confirm its backend lease.",
            existing_execution_id=str(run.id),
        )
    attempt.lease_expires_at = refreshed.poller_lease_expires_at


def _claim_import_write_lease(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
) -> _ImportWriteAttempt:
    attempt = _ImportWriteAttempt()
    for _ in range(_IMPORT_WRITE_LEASE_MAX_ATTEMPTS):
        condition = _pending_import_write_lease(
            client=client,
            run=run,
            project_id=project_id,
        )
        if condition is not None:
            if _lease_is_active(condition):
                _reject_active_existing_writer(
                    client=client,
                    run=run,
                    project_id=project_id,
                )
            _resolve_stale_import_write_lease(
                client=client,
                condition=condition,
                run=run,
            )
            continue

        try:
            condition = client.zen_store.create_run_wait_condition(
                RunWaitConditionRequest(
                    project=project_id,
                    run=run.id,
                    name=f"{_IMPORT_WRITE_LEASE_PREFIX}{attempt.owner_token}",
                    type=RunWaitConditionType.EXTERNAL_INPUT,
                    question="Internal Kitaru imported-trace write lease.",
                    metadata={
                        "kitaru_import_write_lease_v1": True,
                        "owner_token": attempt.owner_token,
                    },
                )
            )
        except Exception:
            competing = _pending_import_write_lease(
                client=client,
                run=run,
                project_id=project_id,
            )
            if competing is None:
                raise
            if _lease_is_active(competing):
                _reject_active_existing_writer(
                    client=client,
                    run=run,
                    project_id=project_id,
                )
            continue

        attempt.lease_id = condition.id
        _refresh_import_write_lease(client=client, run=run, attempt=attempt)
        return attempt

    raise ImportedTraceConflictError(
        "The imported execution write lease changed repeatedly during claim.",
        existing_execution_id=str(run.id),
        resolution="Retry after the competing import finishes.",
    )


def _release_import_write_lease(
    *,
    client: Client,
    attempt: _ImportWriteAttempt,
) -> None:
    if attempt.lease_id is None:
        return
    try:
        condition = client.zen_store.get_run_wait_condition(
            attempt.lease_id,
            hydrate=False,
        )
        if (
            condition.status is RunWaitConditionStatus.PENDING
            and condition.poller_instance_id == attempt.owner_token
        ):
            _resolve_import_write_lease(
                client=client,
                condition=condition,
            )
    except Exception:
        logger.debug(
            "Failed to release imported-trace write lease %s.",
            attempt.lease_id,
            exc_info=True,
        )


def _write_imported_run(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
    trace: ImportedTrace,
    binding: RegisteredAgentVersionBinding,
    stack_id: UUID,
    artifact_store: Any,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution,
    cohort_tag: str | None,
    step_name_by_observation: dict[str, str],
    step_metadata_by_observation: dict[str, dict[str, Any]],
    step_config_by_observation: dict[str, Step],
    attempt: _ImportWriteAttempt,
) -> tuple[str, str]:
    """Write all observations while refreshing the backend ownership lease."""
    run_id = run.id

    def refresh_lease() -> None:
        _refresh_import_write_lease(client=client, run=run, attempt=attempt)

    refresh_lease()
    if run.status == ExecutionStatus.FAILED:
        # A previous import attempt died mid-write and left the run failed.
        # FAILED -> RESUMING is the only transition ZenML allows out of a
        # finished run, so reopen it before writing.
        client.zen_store.update_run(
            run_id, PipelineRunUpdate(status=ExecutionStatus.RESUMING)
        )
    refresh_lease()
    _write_run_metadata(
        client=client,
        run_id=run_id,
        trace=trace,
        binding=binding,
        stack_id=stack_id,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=attribution,
        cohort_tag=cohort_tag,
        status="importing",
        write_owner=attempt.owner_token,
    )
    refresh_lease()
    raw_artifact = _ensure_evidence_artifact(
        client=client,
        project_id=project_id,
        run_id=run_id,
        role="raw_evidence",
        payload=raw_evidence.model_dump(mode="json"),
        expected_digest=raw_evidence.raw_content_sha256,
        artifact_store=artifact_store,
    )
    refresh_lease()
    replay_artifact = _ensure_evidence_artifact(
        client=client,
        project_id=project_id,
        run_id=run_id,
        role="replay_bundle",
        payload=replay_evidence.bundle.model_dump(mode="json"),
        expected_digest=replay_evidence.bundle.bundle_digest,
        artifact_store=artifact_store,
    )
    _validate_frozen_artifact_references(
        run,
        raw_artifact_id=str(raw_artifact.id),
        replay_artifact_id=str(replay_artifact.id),
    )
    existing_steps = _steps_by_name(
        client,
        run_id=run_id,
        project_id=project_id,
    )
    _validate_existing_observation_steps(
        client=client,
        project_id=project_id,
        run_id=run_id,
        trace=trace,
        existing_steps=existing_steps,
        step_name_by_observation=step_name_by_observation,
    )
    step_ids_by_observation: dict[str, UUID] = {}
    output_ids_by_observation: dict[str, UUID] = {}
    for observation in trace.observations:
        step_name = step_name_by_observation[observation.id]
        metadata = step_metadata_by_observation[observation.id]
        existing_step = existing_steps.get(step_name)
        if existing_step is not None:
            refresh_lease()
            step_ids_by_observation[observation.id] = existing_step.id
            output_artifacts = existing_step.outputs.get("output", [])
            if output_artifacts:
                output_ids_by_observation[observation.id] = output_artifacts[0].id
            _write_step_metadata(
                client=client,
                step_id=existing_step.id,
                metadata=metadata,
            )
            continue

        inputs: dict[str, list[UUID]] = {}
        if observation.input_present:
            refresh_lease()
            input_artifact = _save_observation_artifact(
                observation.input,
                client=client,
                project_id=project_id,
                run_id=run_id,
                trace=trace,
                observation=observation,
                step_name=step_name,
                role="input",
                artifact_store=artifact_store,
            )
            inputs[_input_name(observation)] = [input_artifact.id]
        outputs: dict[str, list[UUID]] = {}
        if observation.output_present:
            refresh_lease()
            output_artifact = _save_observation_artifact(
                observation.output,
                client=client,
                project_id=project_id,
                run_id=run_id,
                trace=trace,
                observation=observation,
                step_name=step_name,
                role="output",
                artifact_store=artifact_store,
            )
            outputs["output"] = [output_artifact.id]
        parent_ids = []
        if observation.parent_id in step_ids_by_observation:
            parent_id = observation.parent_id
            assert parent_id is not None
            parent_ids.append(step_ids_by_observation[parent_id])
        failed = observation.status is ObservationStatus.ERROR
        refresh_lease()
        step = client.zen_store.create_run_step(
            StepRunRequest(
                project=project_id,
                name=step_name,
                start_time=observation.started_at,
                end_time=observation.ended_at or observation.started_at,
                status=(
                    ExecutionStatus.FAILED if failed else ExecutionStatus.COMPLETED
                ),
                pipeline_run_id=run_id,
                parent_step_ids=parent_ids,
                inputs=inputs,
                outputs=outputs,
                exception_info=(
                    ExceptionInfo(
                        traceback=observation.status_message
                        or "Imported observation failed.",
                        message=observation.status_message
                        or "Imported observation failed.",
                        source="langfuse",
                    )
                    if failed
                    else None
                ),
                # ZenML's dynamic DAG builder iterates persisted run steps in
                # database order and resolves upstream names immediately. That
                # order is not guaranteed to be parent-first, so embedding the
                # observed edges here can make an otherwise valid run fail to
                # render. Parent lineage remains recorded in the step metadata
                # and complete snapshot graph; the client restores public
                # parent_call_ids from that metadata.
                dynamic_config=_dag_safe_dynamic_step_config(
                    step_config_by_observation[observation.id]
                ),
            )
        )
        step_ids_by_observation[observation.id] = step.id
        if observation.output_present:
            output_ids_by_observation[observation.id] = output_artifact.id
        refresh_lease()
        _write_step_metadata(client=client, step_id=step.id, metadata=metadata)

    root = _root_observation(trace)
    run_failed = _trace_failed(trace)
    final_status = ExecutionStatus.FAILED if run_failed else ExecutionStatus.COMPLETED
    final_reason = (
        "Imported trace contains a failed root or agent observation."
        if run_failed
        else None
    )
    run_outputs = {}
    root_output_id = output_ids_by_observation.get(root.id)
    if root_output_id is not None:
        run_outputs["output"] = root_output_id
    refresh_lease()
    client.zen_store.update_run(
        run_id,
        PipelineRunUpdate(
            status=final_status,
            status_reason=final_reason,
            outputs=run_outputs,
            exception_info=(
                ExceptionInfo(
                    traceback=final_reason or "Imported trace failed.",
                    message=final_reason,
                    source="langfuse",
                )
                if run_failed
                else None
            ),
        ),
    )
    refresh_lease()
    _write_run_metadata(
        client=client,
        run_id=run_id,
        trace=trace,
        binding=binding,
        stack_id=stack_id,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=attribution,
        cohort_tag=cohort_tag,
        status="complete",
        raw_evidence_artifact_id=str(raw_artifact.id),
        write_owner=attempt.owner_token,
        replay_bundle_artifact_id=str(replay_artifact.id),
    )
    return str(raw_artifact.id), str(replay_artifact.id)


def _import_is_complete(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: str,
    trace: ImportedTrace,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
) -> bool:
    """Return whether every immutable part of a v5 import is present."""
    step_name_by_observation = {
        observation.id: _step_name(observation, index=index)
        for index, observation in enumerate(trace.observations, start=1)
    }
    expected_step_names = set(step_name_by_observation.values())
    existing_steps = _steps_by_name(
        client,
        run_id=run.id,
        project_id=project_id,
    )
    unexpected_steps = set(existing_steps) - expected_step_names
    if unexpected_steps:
        raise ImportedTraceConflictError(
            "The imported execution contains steps outside the frozen source graph.",
            existing_execution_id=str(run.id),
        )
    if set(existing_steps) != expected_step_names:
        return False
    _validate_existing_observation_steps(
        client=client,
        project_id=project_id,
        run_id=run.id,
        trace=trace,
        existing_steps=existing_steps,
        step_name_by_observation=step_name_by_observation,
    )
    if run.run_metadata.get(IMPORT_STATUS_KEY) != "complete":
        return False
    if not run.status.is_finished:
        return False

    raw_artifact_id = run.run_metadata.get(IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY)
    replay_artifact_id = run.run_metadata.get(IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY)
    if not raw_artifact_id or not replay_artifact_id:
        return False
    _validate_referenced_evidence_artifact(
        client,
        project_id=project_id,
        artifact_id=str(raw_artifact_id),
        expected_name=f"kitaru-import-{run.id}::raw_evidence",
        expected_payload=raw_evidence.model_dump(mode="json"),
        run_id=run.id,
    )
    _validate_referenced_evidence_artifact(
        client,
        project_id=project_id,
        artifact_id=str(replay_artifact_id),
        expected_name=f"kitaru-import-{run.id}::replay_bundle",
        expected_payload=replay_evidence.bundle.model_dump(mode="json"),
        run_id=run.id,
    )
    return True


def _persisted_schema_version(run: PipelineRunResponse, key: str) -> int:
    value = run.run_metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ImportedTraceConflictError(
            "A complete imported execution has an invalid evidence schema version.",
            existing_execution_id=str(run.id),
        )
    return value


def _existing_evidence_result_fields(run: PipelineRunResponse) -> dict[str, Any]:
    """Return persisted evidence references for a complete existing import."""
    return {
        "raw_evidence_artifact_id": str(
            run.run_metadata[IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY]
        ),
        "raw_evidence_schema_version": _persisted_schema_version(
            run,
            IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY,
        ),
        "replay_bundle_artifact_id": str(
            run.run_metadata[IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY]
        ),
        "replay_bundle_schema_version": _persisted_schema_version(
            run,
            IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY,
        ),
    }


def _validate_existing_observation_steps(
    *,
    client: Client,
    project_id: str,
    run_id: UUID,
    trace: ImportedTrace,
    existing_steps: dict[str, StepRunResponse],
    step_name_by_observation: dict[str, str],
) -> None:
    observation_by_step = {
        step_name_by_observation[observation.id]: observation
        for observation in trace.observations
    }
    for step_name, step in existing_steps.items():
        observation = observation_by_step.get(step_name)
        if observation is None:
            continue
        input_ids = [
            artifact.id for values in step.inputs.values() for artifact in values
        ]
        output_ids = [
            artifact.id for values in step.outputs.values() for artifact in values
        ]
        expected_input_count = 1 if observation.input_present else 0
        expected_output_count = 1 if observation.output_present else 0
        if (
            len(input_ids) != expected_input_count
            or len(output_ids) != expected_output_count
        ):
            raise ImportedTraceConflictError(
                "An existing imported step has stale artifact references.",
                existing_execution_id=str(run_id),
            )
        if observation.input_present:
            _validate_referenced_observation_artifact(
                client,
                project_id=project_id,
                artifact_id=str(input_ids[0]),
                expected_name=f"kitaru-import-{run_id}::{step_name}::input",
                expected_payload=observation.input,
                run_id=run_id,
            )
        if observation.output_present:
            _validate_referenced_observation_artifact(
                client,
                project_id=project_id,
                artifact_id=str(output_ids[0]),
                expected_name=f"kitaru-import-{run_id}::{step_name}::output",
                expected_payload=observation.output,
                run_id=run_id,
            )


def _validate_referenced_observation_artifact(
    client: Client,
    *,
    project_id: str,
    artifact_id: str,
    expected_name: str,
    expected_payload: Any,
    run_id: UUID,
) -> None:
    try:
        artifact = client.get_artifact_version(
            name_id_or_prefix=artifact_id,
            project=project_id,
            hydrate=True,
        )
        loaded = artifact.load()
    except Exception as exc:
        raise ImportedTraceConflictError(
            "An imported step references missing artifact content.",
            existing_execution_id=str(run_id),
        ) from exc
    if (
        artifact.name != expected_name
        or str(artifact.project_id) != project_id
        or canonical_json(loaded) != canonical_json(expected_payload)
    ):
        raise ImportedTraceConflictError(
            "An imported step references artifact content from a different identity.",
            existing_execution_id=str(run_id),
        )


def _validate_referenced_evidence_artifact(
    client: Client,
    *,
    project_id: str,
    artifact_id: str,
    expected_name: str,
    expected_payload: dict[str, Any],
    run_id: UUID,
) -> None:
    try:
        artifact = client.get_artifact_version(
            name_id_or_prefix=artifact_id,
            project=project_id,
            hydrate=True,
        )
    except Exception as exc:
        raise ImportedTraceConflictError(
            "An imported execution references missing immutable evidence.",
            existing_execution_id=str(run_id),
        ) from exc
    if artifact.name != expected_name or str(artifact.project_id) != project_id:
        raise ImportedTraceConflictError(
            "An imported execution references evidence from a different identity.",
            existing_execution_id=str(run_id),
        )
    try:
        loaded = artifact.load()
    except Exception as exc:
        raise ImportedTraceConflictError(
            "An imported execution references unreadable immutable evidence.",
            existing_execution_id=str(run_id),
        ) from exc
    if canonical_json(loaded) != canonical_json(expected_payload):
        raise ImportedTraceConflictError(
            "An imported execution references evidence with different content.",
            existing_execution_id=str(run_id),
        )


def _validate_frozen_artifact_references(
    run: PipelineRunResponse,
    *,
    raw_artifact_id: str,
    replay_artifact_id: str,
) -> None:
    for key, expected_id in (
        (IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY, raw_artifact_id),
        (IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY, replay_artifact_id),
    ):
        existing_id = run.run_metadata.get(key)
        if existing_id and str(existing_id) != expected_id:
            raise ImportedTraceConflictError(
                "An imported execution has a stale immutable evidence reference.",
                existing_execution_id=str(run.id),
            )


def _mark_import_write_failed(
    *,
    client: Client,
    run_id: UUID,
    trace: ImportedTrace,
    binding: RegisteredAgentVersionBinding,
    stack_id: UUID,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution,
    cohort_tag: str | None,
    owner_token: str,
    lease_id: UUID | None,
    allow_missing_owner: bool,
    reason: str,
) -> bool:
    """Best-effort: leave an interrupted import FAILED instead of RUNNING.

    Both writes may hit the same backend outage that interrupted the import,
    so failures here are logged and swallowed; the original error is what the
    caller reports.
    """
    if lease_id is None:
        return False
    try:
        lease = client.zen_store.get_run_wait_condition(lease_id, hydrate=False)
        if (
            lease.status is not RunWaitConditionStatus.PENDING
            or lease.poller_instance_id != owner_token
            or not _lease_is_active(lease)
        ):
            return False
        current = client.get_pipeline_run(
            name_id_or_prefix=run_id,
            allow_name_prefix_match=False,
            hydrate=True,
            project=binding.project_id,
        )
    except Exception:
        logger.debug(
            "Failed to verify write ownership for run %s.", run_id, exc_info=True
        )
        return False
    current_owner = current.run_metadata.get(_IMPORT_WRITE_OWNER_KEY)
    owns_run = current_owner == owner_token or (
        allow_missing_owner and current_owner is None
    )
    if not owns_run or current.run_metadata.get(IMPORT_STATUS_KEY) == "complete":
        return False

    attempts = (
        (
            "mark interrupted import run as failed",
            lambda: client.zen_store.update_run(
                run_id,
                PipelineRunUpdate(
                    status=ExecutionStatus.FAILED,
                    status_reason=f"Trace import was interrupted: {reason}",
                ),
            ),
        ),
        (
            "record failed import status metadata",
            lambda: _write_run_metadata(
                client=client,
                run_id=run_id,
                trace=trace,
                binding=binding,
                stack_id=stack_id,
                raw_evidence=raw_evidence,
                replay_evidence=replay_evidence,
                attribution=attribution,
                cohort_tag=cohort_tag,
                status="failed",
                write_owner=owner_token,
            ),
        ),
    )
    marked_failed = False
    for description, attempt in attempts:
        try:
            attempt()
            if description == "mark interrupted import run as failed":
                marked_failed = True
        except Exception:
            logger.debug("Failed to %s for run %s.", description, run_id, exc_info=True)
    return marked_failed


def _validate_import(trace: ImportedTrace) -> None:
    if not trace.observations:
        raise ImportedTracePersistenceError("Cannot persist an empty trace.")
    if trace.integrity is TraceIntegrity.INVALID:
        raise ImportedTracePersistenceError(
            f"Cannot persist trace {trace.source.trace_id!r}: invalid graph."
        )
    if any(
        observation.status is ObservationStatus.UNKNOWN
        for observation in trace.observations
    ):
        raise ImportedTracePersistenceError(
            f"Cannot persist trace {trace.source.trace_id!r}: every observation "
            "must have a terminal status in the source export."
        )


def _validate_evidence(
    trace: ImportedTrace,
    *,
    binding: RegisteredAgentVersionBinding,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution | None = None,
) -> None:
    if raw_evidence.source != trace.source:
        raise ImportedTracePersistenceError(
            "Raw evidence belongs to a different imported source identity."
        )
    for row in raw_evidence.rows:
        try:
            parsed_object = strict_json_loads(row.raw_text)
        except (RecursionError, ValueError) as exc:
            raise ImportedTracePersistenceError(
                "Raw evidence contains invalid source text."
            ) from exc
        if not isinstance(parsed_object, dict) or parsed_object != row.parsed_object:
            raise ImportedTracePersistenceError(
                "Raw evidence source text does not match its parsed object."
            )
    records = tuple(
        LangfuseSourceRecord(
            raw_text=row.raw_text,
            row=dict(row.parsed_object),
            line_number=row.line_number,
            source_order=row.source_order,
        )
        for row in raw_evidence.rows
    )
    expected_raw_evidence = build_raw_imported_evidence(
        source=trace.source,
        records=records,
    )
    if raw_evidence != expected_raw_evidence:
        raise ImportedTracePersistenceError(
            "Raw evidence does not match the supplied source rows."
        )
    normalized = normalize_langfuse_records(
        records,
        project_id=trace.source.project_id,
    )
    if len(normalized) != 1 or normalized[0].trace != trace:
        raise ImportedTracePersistenceError(
            "Raw evidence does not normalize to the supplied imported trace."
        )
    expected_replay_evidence = build_pydantic_ai_replay_evidence(
        trace,
        raw_evidence=expected_raw_evidence,
    )
    if replay_evidence != expected_replay_evidence:
        raise ImportedTracePersistenceError(
            "Replay evidence does not match the supplied imported trace."
        )
    expected_attribution = classify_source_attribution(
        extract_langfuse_provider_stamps([record.row for record in records]),
        git_sha=binding.manifest.git_sha,
        aliases=binding.aliases,
    )
    if expected_attribution.status is SourceAttributionStatus.CONFLICT:
        raise ImportedTracePersistenceError(
            "Provider source-version evidence conflicts with the declared AgentVersion."
        )
    if attribution is not None and attribution != expected_attribution:
        raise ImportedTracePersistenceError(
            "Source attribution does not match the supplied raw evidence and "
            "declared AgentVersion."
        )


def _get_or_create_snapshot(
    *,
    client: Client,
    project_id: str,
    pipeline_id: str,
    pipeline_name: str,
    trace: ImportedTrace,
    step_config_by_observation: dict[str, Step],
    stack_id: UUID,
) -> PipelineSnapshotResponse:
    snapshot_identity = hashlib.sha256(
        "\0".join(
            (
                str(stack_id),
                *trace.source.identity,
                trace.content_digest,
            )
        ).encode("utf-8")
    ).hexdigest()[:24]
    name = f"kitaru-import-v{_IMPORT_SCHEMA_VERSION}-{snapshot_identity}"
    matches = client.list_snapshots(
        name=name,
        pipeline=pipeline_id,
        project=project_id,
        size=2,
        hydrate=True,
    ).items
    if matches:
        snapshot = matches[0]
        configuration = snapshot.pipeline_configuration
        snapshot_pipeline_id = str(getattr(snapshot.pipeline, "id", snapshot.pipeline))
        snapshot_stack_id = str(getattr(snapshot.stack, "id", snapshot.stack))
        if (
            snapshot_pipeline_id != pipeline_id
            or snapshot_stack_id != str(stack_id)
            or configuration.name != pipeline_name
            or (configuration.extra or {}).get(IMPORT_SNAPSHOT_KIND_KEY)
            != _SNAPSHOT_KIND
        ):
            raise ImportedTraceConflictError(
                "An existing imported snapshot conflicts with the frozen binding."
            )
        return snapshot
    request = PipelineSnapshotRequest(
        project=project_id,
        name=name,
        description="Synthetic observed graph for one imported trace.",
        run_name_template="imported-{date}-{time}",
        pipeline_configuration=PipelineConfiguration(
            name=pipeline_name,
            enable_cache=False,
            extra={IMPORT_SNAPSHOT_KIND_KEY: _SNAPSHOT_KIND},
        ),
        step_configurations={
            step.config.name: step for step in step_config_by_observation.values()
        },
        pipeline_spec=PipelineSpec(
            steps=[step.spec for step in step_config_by_observation.values()]
        ),
        is_dynamic=True,
        stack=stack_id,
        pipeline=pipeline_id,
        pipeline_version_hash=name,
        tags=[_IMPORT_TAG, f"{IMPORT_SNAPSHOT_KIND_KEY}:{_SNAPSHOT_KIND}"],
    )
    try:
        return client.zen_store.create_snapshot(request)
    except EntityExistsError:
        return client.get_snapshot(
            name, pipeline_name_or_id=pipeline_id, project=project_id, hydrate=True
        )


def _find_run(
    *, client: Client, project_id: str, run_name: str
) -> PipelineRunResponse | None:
    matches = client.list_pipeline_runs(
        name=run_name,
        project=project_id,
        size=2,
        hydrate=True,
        include_full_metadata=True,
    ).items
    return matches[0] if matches else None


def _validate_existing_run(
    run: PipelineRunResponse,
    *,
    trace: ImportedTrace,
    binding: RegisteredAgentVersionBinding,
    stack_id: UUID,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    cohort_tag: str | None,
) -> None:
    environment = run.orchestrator_environment
    expected = _import_environment(
        trace,
        binding=binding,
        stack_id=stack_id,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        cohort_tag=cohort_tag,
    )
    identity_keys = (
        IMPORT_SCHEMA_VERSION_KEY,
        IMPORT_SNAPSHOT_KIND_KEY,
        IMPORT_SOURCE_PROVIDER_KEY,
        IMPORT_SOURCE_PROJECT_ID_KEY,
        IMPORT_SOURCE_TRACE_ID_KEY,
    )
    if any(environment.get(key) != expected[key] for key in identity_keys):
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} collides with another imported identity.",
            existing_execution_id=str(run.id),
            resolution=(
                "Verify the source project ID and trace ID. If this is a "
                "different source trace, import it with its actual source identity."
            ),
        )
    existing_stack_id = _existing_run_stack_id(run)
    if existing_stack_id != str(stack_id):
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported using stack "
            f"{existing_stack_id!r}, not {str(stack_id)!r}.",
            existing_execution_id=str(run.id),
            resolution=(
                "Kitaru cannot move existing artifact bytes by re-importing. "
                f"Keep execution {str(run.id)!r}, or remove that synthetic "
                "execution and its artifacts before importing the trace into the "
                "new stack."
            ),
        )
    if environment.get(IMPORT_SOURCE_CONTENT_DIGEST_KEY) != trace.content_digest:
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with "
            "different content.",
            existing_execution_id=str(run.id),
            resolution=(
                f"Kitaru will not overwrite execution {str(run.id)!r}. Reuse the "
                "original export for an idempotent import, or delete that execution "
                "before importing the changed trace."
            ),
        )
    binding_keys = (
        IMPORT_SOURCE_AGENT_VERSION_ID_KEY,
        IMPORT_SOURCE_PIPELINE_ID_KEY,
        IMPORT_SOURCE_FINGERPRINT_KEY,
    )
    if any(environment.get(key) != expected[key] for key in binding_keys):
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with a different "
            "source AgentVersion.",
            existing_execution_id=str(run.id),
            resolution=(
                "Retry with the original source AgentVersion. Source attribution "
                "cannot be changed by re-importing an existing execution."
            ),
        )
    evidence_keys = (IMPORT_RAW_EVIDENCE_DIGEST_KEY, IMPORT_REPLAY_BUNDLE_DIGEST_KEY)
    if any(environment.get(key) != expected[key] for key in evidence_keys):
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with different "
            "source evidence.",
            existing_execution_id=str(run.id),
            resolution=(
                "Reuse the exact original source rows and replay evidence. Kitaru "
                "will not replace immutable imported evidence."
            ),
        )
    if environment.get(IMPORT_COHORT_TAG_KEY) != expected[IMPORT_COHORT_TAG_KEY]:
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with a different "
            "cohort tag.",
            existing_execution_id=str(run.id),
            resolution="Retry with the original cohort tag.",
        )


def _steps_by_name(
    client: Client,
    *,
    run_id: UUID,
    project_id: str,
) -> dict[str, StepRunResponse]:
    steps_by_name: dict[str, StepRunResponse] = {}
    page = 1
    page_size = 200
    while True:
        steps = client.list_run_steps(
            pipeline_run_id=run_id,
            project=project_id,
            sort_by="asc:created",
            page=page,
            size=page_size,
            hydrate=True,
            exclude_retried=False,
        ).items
        for step in steps:
            steps_by_name[step.name] = step
        if len(steps) < page_size:
            return steps_by_name
        page += 1


def _dynamic_step_config(
    *,
    step_name: str,
    upstream_names: list[str],
    observation: ImportedObservation,
    metadata: dict[str, Any],
) -> Step:
    source = source_utils.resolve(_imported_observation_placeholder)
    step_type = None
    if observation.kind is ObservationKind.LLM_CALL:
        step_type = StepType.LLM_CALL
    elif observation.kind is ObservationKind.TOOL_CALL:
        step_type = StepType.TOOL_CALL
    configuration = StepConfiguration(
        name=step_name,
        enable_cache=False,
        step_type=step_type,
        extra=metadata,
    )
    return Step(
        spec=StepSpec(
            source=source,
            upstream_steps=upstream_names,
            invocation_id=step_name,
        ),
        config=configuration,
        step_config_overrides=configuration,
    )


def _dag_safe_dynamic_step_config(step: Step) -> Step:
    """Remove dynamic edges ZenML may resolve before creating their nodes."""
    return step.model_copy(
        update={"spec": step.spec.model_copy(update={"upstream_steps": []})}
    )


def _save_observation_artifact(
    value: Any,
    *,
    client: Client,
    project_id: str,
    run_id: UUID,
    trace: ImportedTrace,
    observation: ImportedObservation,
    step_name: str,
    role: str,
    artifact_store: Any,
) -> ArtifactVersionResponse:
    digest = sha256_canonical_json(value)
    return _ensure_manual_artifact(
        client=client,
        project_id=project_id,
        name=f"kitaru-import-{run_id}::{step_name}::{role}",
        version="1",
        payload=value,
        artifact_store=artifact_store,
        tags=[_IMPORT_TAG],
        user_metadata={
            "kitaru_artifact_type": role,
            "kitaru_import_artifact_sha256_v1": digest,
            IMPORT_SOURCE_TRACE_ID_KEY: trace.source.trace_id,
            IMPORTED_OBSERVATION_ID_METADATA_KEY: observation.id,
        },
        include_visualizations=True,
        has_custom_name=False,
    )


def _ensure_evidence_artifact(
    *,
    client: Client,
    project_id: str,
    run_id: UUID,
    role: str,
    payload: dict[str, Any],
    expected_digest: str,
    artifact_store: Any,
) -> ArtifactVersionResponse:
    tag = _RAW_EVIDENCE_TAG if role == "raw_evidence" else _REPLAY_BUNDLE_TAG
    return _ensure_manual_artifact(
        client=client,
        project_id=project_id,
        name=f"kitaru-import-{run_id}::{role}",
        version="1",
        payload=payload,
        artifact_store=artifact_store,
        tags=[_IMPORT_TAG, tag],
        user_metadata={
            "kitaru_import_artifact_role_v1": role,
            "kitaru_import_artifact_sha256_v1": expected_digest,
            "kitaru_import_artifact_schema_version_v1": payload["schema_version"],
        },
        include_visualizations=False,
        has_custom_name=True,
    )


def _ensure_manual_artifact(
    *,
    client: Client,
    project_id: str,
    name: str,
    version: str,
    payload: Any,
    artifact_store: Any,
    tags: list[str],
    user_metadata: dict[str, Any],
    include_visualizations: bool,
    has_custom_name: bool,
) -> ArtifactVersionResponse:
    artifact = _find_manual_artifact(
        client,
        project_id=project_id,
        name=name,
        version=version,
    )
    if artifact is None:
        try:
            artifact = _save_artifact_in_project(
                client=client,
                project_id=project_id,
                data=payload,
                name=name,
                version=version,
                artifact_type=ArtifactType.DATA,
                tags=tags,
                extract_metadata=False,
                include_visualizations=include_visualizations,
                user_metadata=user_metadata,
                save_type=ArtifactSaveType.MANUAL,
                has_custom_name=has_custom_name,
                artifact_store=artifact_store,
            )
        except Exception:
            artifact = _find_manual_artifact(
                client,
                project_id=project_id,
                name=name,
                version=version,
            )
            if artifact is None:
                raise
    _validate_manual_artifact(
        artifact,
        project_id=project_id,
        artifact_store=artifact_store,
        expected_payload=payload,
    )
    return artifact


def _find_manual_artifact(
    client: Client,
    *,
    project_id: str,
    name: str,
    version: str,
) -> ArtifactVersionResponse | None:
    page = client.list_artifact_versions(
        name=f"equals:{name}",
        version=version,
        project=project_id,
        hydrate=True,
        size=2,
    )
    matches = [item for item in page.items if item.name == name]
    if len(matches) > 1:
        raise ImportedTraceConflictError(
            "Multiple immutable artifact versions match one imported artifact role."
        )
    return matches[0] if matches else None


def _validate_manual_artifact(
    artifact: ArtifactVersionResponse,
    *,
    project_id: str,
    artifact_store: Any,
    expected_payload: Any,
) -> None:
    if str(artifact.project_id) != project_id:
        raise ImportedTraceConflictError(
            "An imported artifact belongs to a different Agent Project."
        )
    expected_store_id = str(getattr(artifact_store, "id", ""))
    if expected_store_id and str(artifact.artifact_store_id) != expected_store_id:
        raise ImportedTraceConflictError(
            "An imported artifact belongs to a different artifact store."
        )
    try:
        loaded = artifact.load()
    except Exception as exc:
        raise ImportedTraceConflictError(
            "An immutable imported artifact reference could not be loaded."
        ) from exc
    if canonical_json(loaded) != canonical_json(expected_payload):
        raise ImportedTraceConflictError(
            "An immutable imported artifact has different content."
        )


def _save_artifact_in_project(
    *,
    client: Client,
    project_id: str,
    **kwargs: Any,
) -> ArtifactVersionResponse:
    """Call ZenML's manual saver under the explicit Agent Project."""
    with _ARTIFACT_PROJECT_LOCK:
        active_project_id = str(client.active_project.id)
        if active_project_id != project_id:
            client.set_active_project(project_id)
        try:
            return save_artifact(**kwargs)
        finally:
            if active_project_id != project_id:
                client.set_active_project(active_project_id)


def _step_metadata(
    observation: ImportedObservation, *, step_name: str
) -> dict[str, Any]:
    metadata = {
        **adapter_checkpoint_metadata(
            adapter="langfuse_import",
            kind=observation.kind.value,
            input_slots=(_input_name(observation),),
            output_slots=("output",),
        ),
        IMPORTED_OBSERVATION_ID_METADATA_KEY: observation.id,
        IMPORTED_PARENT_OBSERVATION_ID_METADATA_KEY: observation.parent_id or "",
        "kitaru_import_source_type_v1": observation.source_type.value,
        "kitaru_import_source_status_v1": observation.status.value,
        "kitaru_import_source_name_v1": observation.name,
    }
    if observation.usage is not None:
        metadata["kitaru_import_usage_v1"] = observation.usage.model_dump(mode="json")
    if observation.cost is not None:
        metadata["kitaru_import_cost_v1"] = observation.cost.model_dump(mode="json")
    if observation.model is not None:
        metadata["kitaru_import_model_v1"] = observation.model
    if observation.latency_ms is not None:
        metadata["kitaru_import_latency_ms_v1"] = observation.latency_ms

    token_usage = _token_usage(observation)
    if observation.kind is ObservationKind.LLM_CALL and (
        token_usage is not None
        or observation.cost is not None
        or observation.model is not None
        or observation.latency_ms is not None
    ):
        cost = observation.cost
        actual_cost_usd = (
            cost.total
            if cost is not None
            and (cost.currency is None or cost.currency.upper() == "USD")
            else None
        )
        warnings = ["Imported historical usage; no model call was executed."]
        estimated_cost = CalculatedCostMetadata(None, "none", None)
        if actual_cost_usd is None:
            estimated_cost = estimate_genai_prices_cost(
                provider=None,
                model=observation.model,
                usage=token_usage,
                warnings=warnings,
                adapter_name="Langfuse import",
            )
            if estimated_cost.estimated_cost_usd is not None:
                warnings.append(
                    "Estimated with the current genai-prices catalog; this may "
                    "not match the historical price at execution time."
                )
        record = build_usage_record(
            adapter="langfuse_import",
            surface="model_call",
            call_name=step_name,
            event_id=observation.id,
            checkpoint_name=step_name,
            model=observation.model,
            provider=None,
            usage=token_usage,
            raw_usage=(observation.usage.details if observation.usage else None),
            actual_cost_usd=actual_cost_usd,
            estimated_cost_usd=estimated_cost.estimated_cost_usd,
            cost_source=(
                None if actual_cost_usd is not None else estimated_cost.cost_source
            ),
            cost_source_label=(
                "Langfuse imported provider cost"
                if actual_cost_usd is not None
                else estimated_cost.cost_source_label
            ),
            pricing_version=estimated_cost.pricing_version,
            latency_ms=observation.latency_ms,
            status=(
                "failed"
                if observation.status is ObservationStatus.ERROR
                else "completed"
            ),
            billing_effect="unknown",
            cache_status="unknown",
            warnings=warnings,
            record_id=observation.id,
        )
        metadata.update(usage_record_metadata(record))
    return metadata


def _token_usage(observation: ImportedObservation) -> dict[str, Any] | None:
    usage = observation.usage
    if usage is None:
        return None
    if usage.unit is not None and usage.unit.strip().lower() not in {
        "token",
        "tokens",
    }:
        return None
    values = (usage.input, usage.output, usage.total)
    if any(value is not None and not _is_token_count(value) for value in values):
        return None
    return {
        "input_tokens": usage.input,
        "output_tokens": usage.output,
        "total_tokens": usage.total,
        "details": usage.details,
    }


def _is_token_count(value: int | float) -> bool:
    return not isinstance(value, bool) and value >= 0 and float(value).is_integer()


def _write_run_metadata(
    *,
    client: Client,
    run_id: UUID,
    trace: ImportedTrace,
    binding: RegisteredAgentVersionBinding,
    stack_id: UUID,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    attribution: SourceAttribution,
    cohort_tag: str | None,
    status: str,
    raw_evidence_artifact_id: str | None = None,
    replay_bundle_artifact_id: str | None = None,
    write_owner: str | None = None,
) -> None:
    metadata = {
        **_import_environment(
            trace,
            binding=binding,
            stack_id=stack_id,
            raw_evidence=raw_evidence,
            replay_evidence=replay_evidence,
            cohort_tag=cohort_tag,
        ),
        IMPORT_STATUS_KEY: status,
        IMPORT_SNAPSHOT_KIND_KEY: _SNAPSHOT_KIND,
        IMPORT_INTEGRITY_KEY: trace.integrity.value,
        "kitaru_import_missing_parent_ids_v1": trace.missing_parent_ids,
        "kitaru_import_component_count_v1": trace.component_count,
        IMPORT_OBSERVATION_COUNT_KEY: len(trace.observations),
        IMPORT_ATTRIBUTION_KEY: attribution.model_dump(mode="json"),
        IMPORT_REPLAY_READINESS_KEY: (
            replay_evidence.readiness.model_dump(mode="json")
        ),
        IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY: raw_evidence.schema_version,
        IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY: (
            replay_evidence.bundle.schema_version
        ),
        IMPORT_REPLAY_PROFILE_VERSION_KEY: replay_evidence.bundle.profile_version,
    }
    if write_owner is not None:
        metadata[_IMPORT_WRITE_OWNER_KEY] = write_owner
    if raw_evidence_artifact_id is not None:
        metadata[IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY] = raw_evidence_artifact_id
    if replay_bundle_artifact_id is not None:
        metadata[IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY] = replay_bundle_artifact_id
    client.create_run_metadata(
        metadata=metadata,
        resources=[
            RunMetadataResource(id=run_id, type=MetadataResourceTypes.PIPELINE_RUN)
        ],
    )


def _write_step_metadata(
    *, client: Client, step_id: UUID, metadata: dict[str, Any]
) -> None:
    client.create_run_metadata(
        metadata=metadata,
        resources=[
            RunMetadataResource(id=step_id, type=MetadataResourceTypes.STEP_RUN)
        ],
        publisher_step_id=step_id,
    )


def _import_environment(
    trace: ImportedTrace,
    *,
    binding: RegisteredAgentVersionBinding,
    stack_id: UUID,
    raw_evidence: RawImportedEvidence,
    replay_evidence: PydanticAIReplayEvidence,
    cohort_tag: str | None,
) -> dict[str, Any]:
    return {
        IMPORTED_EXECUTION_ENVIRONMENT_KEY: True,
        IMPORT_SCHEMA_VERSION_KEY: _IMPORT_SCHEMA_VERSION,
        IMPORT_SOURCE_PROVIDER_KEY: trace.source.provider,
        IMPORT_SOURCE_PROJECT_ID_KEY: trace.source.project_id,
        IMPORT_SOURCE_TRACE_ID_KEY: trace.source.trace_id,
        IMPORT_SOURCE_CONTENT_DIGEST_KEY: trace.content_digest,
        IMPORT_AGENT_NAME_KEY: binding.agent_name,
        IMPORT_STACK_ID_KEY: str(stack_id),
        IMPORT_SOURCE_AGENT_VERSION_ID_KEY: binding.manifest.agent_version_id,
        IMPORT_SOURCE_AGENT_VERSION_LABEL_KEY: binding.requested_alias or "",
        IMPORT_SOURCE_PIPELINE_ID_KEY: binding.pipeline_id,
        IMPORT_SOURCE_FINGERPRINT_KEY: binding.fingerprint,
        IMPORT_RAW_EVIDENCE_DIGEST_KEY: raw_evidence.raw_content_sha256,
        IMPORT_REPLAY_BUNDLE_DIGEST_KEY: replay_evidence.bundle.bundle_digest,
        IMPORT_COHORT_TAG_KEY: cohort_tag or "",
        IMPORT_SNAPSHOT_KIND_KEY: _SNAPSHOT_KIND,
    }


def _existing_run_stack_id(run: PipelineRunResponse) -> str | None:
    environment_stack_id = run.orchestrator_environment.get(IMPORT_STACK_ID_KEY)
    if environment_stack_id is not None:
        return str(environment_stack_id)
    snapshot = run.snapshot
    stack = getattr(snapshot, "stack", None)
    return str(getattr(stack, "id", stack)) if stack is not None else None


def _source_identity_digest(trace: ImportedTrace) -> str:
    identity = "\0".join(trace.source.identity)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def imported_flow_name(*, provider: str, agent_name: str) -> str:
    """Return the deterministic flow name used for imported executions."""
    identity = hashlib.sha256(agent_name.encode("utf-8")).hexdigest()[:8]
    return (
        f"imported_{_slug(agent_name, limit=80)}__{_slug(provider, limit=30)}_"
        f"v{_IMPORT_SCHEMA_VERSION}_{identity}"
    )


def _run_name(trace: ImportedTrace, *, identity_digest: str) -> str:
    return (
        f"imported-v{_IMPORT_SCHEMA_VERSION}-"
        f"{_slug(trace.source.trace_id, limit=100)}-{identity_digest}"
    )


def _step_name(observation: ImportedObservation, *, index: int) -> str:
    name = _slug(observation.name, limit=60)
    identity = hashlib.sha256(observation.id.encode("utf-8")).hexdigest()[:10]
    return f"import_{index:04d}_{name}_{identity}"


def _slug(value: str, *, limit: int = 60) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip()).strip("_-").lower()
    return (slug or "unnamed")[:limit]


def _input_name(observation: ImportedObservation) -> str:
    if observation.kind is ObservationKind.TOOL_CALL:
        return "tool_args"
    return "input"


def _root_observation(trace: ImportedTrace) -> ImportedObservation:
    observation_ids = {observation.id for observation in trace.observations}
    return next(
        observation
        for observation in trace.observations
        if observation.parent_id is None or observation.parent_id not in observation_ids
    )


def _trace_failed(trace: ImportedTrace) -> bool:
    observation_ids = {observation.id for observation in trace.observations}
    return any(
        observation.status is ObservationStatus.ERROR
        and (
            observation.source_type is SourceObservationType.AGENT
            or observation.parent_id is None
            or observation.parent_id not in observation_ids
        )
        for observation in trace.observations
    )
