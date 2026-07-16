"""Persist imported traces as synthetic ZenML executions."""

import hashlib
import logging
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

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
    StepType,
)
from zenml.exceptions import EntityExistsError
from zenml.models import (
    ExceptionInfo,
    PipelineRequest,
    PipelineResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineRunUpdate,
    PipelineSnapshotRequest,
    PipelineSnapshotResponse,
    StepRunRequest,
    StepRunResponse,
)
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.models.v2.misc.run_metadata import RunMetadataResource
from zenml.utils import source_utils

from kitaru._checkpoint_metadata import adapter_checkpoint_metadata
from kitaru._import_contract import (
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
from kitaru.imports._models import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    ObservationStatus,
    SourceObservationType,
    TraceIntegrity,
)

logger = logging.getLogger(__name__)

_IMPORT_SCHEMA_VERSION = 4
_IMPORT_TAG = "kitaru-imported"
_SOURCE_DIGEST_KEY = "kitaru_import_content_digest_v1"
_SOURCE_PROVIDER_KEY = "kitaru_import_source_provider_v1"
_SOURCE_PROJECT_KEY = "kitaru_import_source_project_id_v1"
_SOURCE_TRACE_KEY = "kitaru_import_source_trace_id_v1"
_AGENT_NAME_KEY = "kitaru_import_agent_name_v1"
_STACK_ID_KEY = "kitaru_import_stack_id_v1"
_IMPORT_STATUS_KEY = "kitaru_import_status_v1"


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


@dataclass(frozen=True)
class ImportedExecutionResult:
    """Result of persisting one normalized trace."""

    execution_id: str
    created: bool
    resumed: bool
    observation_count: int


def plan_imported_trace(
    trace: ImportedTrace,
    *,
    agent_name: str,
    client: Client | None = None,
    stack_id: UUID | None = None,
) -> ImportedTracePlan:
    """Determine what persistence would do without modifying backend state."""
    normalized_agent_name = _validate_import(trace, agent_name=agent_name)
    zenml_client = client or Client()
    selected_stack_id = stack_id or zenml_client.active_stack_model.id
    identity_digest = _source_identity_digest(trace)
    existing_run = _find_run(
        client=zenml_client,
        project_id=zenml_client.active_project.id,
        run_name=_run_name(trace, identity_digest=identity_digest),
    )
    if existing_run is None:
        return ImportedTracePlan.CREATE
    _validate_existing_run(
        existing_run,
        trace=trace,
        agent_name=normalized_agent_name,
        stack_id=selected_stack_id,
    )
    if _needs_import_write(existing_run):
        return ImportedTracePlan.RESUME
    return ImportedTracePlan.UNCHANGED


def _imported_observation_placeholder() -> None:
    """Identify synthetic dynamic steps; this function must never execute."""
    raise RuntimeError("Synthetic imported observations cannot be executed.")


def persist_imported_trace(
    trace: ImportedTrace,
    *,
    agent_name: str,
    client: Client | None = None,
    stack_id: UUID | None = None,
    artifact_store: Any | None = None,
) -> ImportedExecutionResult:
    """Persist one normalized trace as a synthetic ZenML pipeline run.

    No user flow, model, tool, or provider code is executed. The selected ZenML
    stack's artifact store is used to persist artifacts, and
    the synthetic execution snapshot is associated with the same stack.
    """
    normalized_agent_name = _validate_import(trace, agent_name=agent_name)

    zenml_client = client or Client()
    if (stack_id is None) != (artifact_store is None):
        raise ImportedTracePersistenceError(
            "stack_id and artifact_store must be provided together so imported "
            "payloads and the execution snapshot cannot target different stacks."
        )
    selected_stack_id = stack_id or zenml_client.active_stack_model.id
    selected_artifact_store = artifact_store
    if selected_artifact_store is None:
        selected_artifact_store = zenml_client.active_stack.artifact_store
    project_id = zenml_client.active_project.id
    identity_digest = _source_identity_digest(trace)
    run_name = _run_name(trace, identity_digest=identity_digest)
    import_environment = _import_environment(
        trace,
        agent_name=normalized_agent_name,
        stack_id=selected_stack_id,
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
            agent_name=normalized_agent_name,
            stack_id=selected_stack_id,
        )
        if not _needs_import_write(existing_run):
            if existing_run.run_metadata.get(_IMPORT_STATUS_KEY) != "complete":
                _write_run_metadata(
                    client=zenml_client,
                    run_id=existing_run.id,
                    trace=trace,
                    agent_name=normalized_agent_name,
                    stack_id=selected_stack_id,
                    status="complete",
                )
            return ImportedExecutionResult(
                execution_id=str(existing_run.id),
                created=False,
                resumed=False,
                observation_count=len(trace.observations),
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
        pipeline = _get_or_create_pipeline(
            client=zenml_client,
            project_id=project_id,
            provider=trace.source.provider,
            agent_name=normalized_agent_name,
        )
        snapshot = _get_or_create_snapshot(
            client=zenml_client,
            project_id=project_id,
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
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
            tags=[_IMPORT_TAG, f"kitaru-import-{_slug(trace.source.provider)}"],
        )
        run, created = zenml_client.zen_store.get_or_create_run(run_request)
        if not created:
            _validate_existing_run(
                run,
                trace=trace,
                agent_name=normalized_agent_name,
                stack_id=selected_stack_id,
            )
    else:
        run = existing_run
        created = False

    try:
        _write_imported_run(
            client=zenml_client,
            run=run,
            project_id=project_id,
            trace=trace,
            agent_name=normalized_agent_name,
            stack_id=selected_stack_id,
            artifact_store=selected_artifact_store,
            step_name_by_observation=step_name_by_observation,
            step_metadata_by_observation=step_metadata_by_observation,
            step_config_by_observation=step_config_by_observation,
        )
    except Exception as exc:
        _mark_import_write_failed(
            client=zenml_client,
            run_id=run.id,
            trace=trace,
            agent_name=normalized_agent_name,
            stack_id=selected_stack_id,
            reason=str(exc),
        )
        raise ImportedTraceWriteError(
            f"Importing trace {trace.source.trace_id!r} failed after execution "
            f"{run.id} was created; the execution was marked failed and "
            f"re-importing the same export will resume it. Cause: {exc}",
            execution_id=str(run.id),
        ) from exc
    return ImportedExecutionResult(
        execution_id=str(run.id),
        created=created,
        resumed=not created,
        observation_count=len(trace.observations),
    )


def _write_imported_run(
    *,
    client: Client,
    run: PipelineRunResponse,
    project_id: UUID,
    trace: ImportedTrace,
    agent_name: str,
    stack_id: UUID,
    artifact_store: Any,
    step_name_by_observation: dict[str, str],
    step_metadata_by_observation: dict[str, dict[str, Any]],
    step_config_by_observation: dict[str, Step],
) -> None:
    """Reopen the run if needed, write all observations, finalize status."""
    run_id = run.id
    if run.status == ExecutionStatus.FAILED:
        # A previous import attempt died mid-write and left the run failed.
        # FAILED -> RESUMING is the only transition ZenML allows out of a
        # finished run, so reopen it before writing.
        client.zen_store.update_run(
            run_id, PipelineRunUpdate(status=ExecutionStatus.RESUMING)
        )
    _write_run_metadata(
        client=client,
        run_id=run_id,
        trace=trace,
        agent_name=agent_name,
        stack_id=stack_id,
        status="importing",
    )
    existing_steps = _steps_by_name(client, run_id=run_id)
    step_ids_by_observation: dict[str, UUID] = {}
    output_ids_by_observation: dict[str, UUID] = {}
    for observation in trace.observations:
        step_name = step_name_by_observation[observation.id]
        metadata = step_metadata_by_observation[observation.id]
        existing_step = existing_steps.get(step_name)
        if existing_step is not None:
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

        input_name = _input_name(observation)
        input_artifact = _save_observation_artifact(
            observation.input,
            trace=trace,
            observation=observation,
            step_name=step_name,
            role="input",
            artifact_store=artifact_store,
        )
        output_artifact = _save_observation_artifact(
            observation.output,
            trace=trace,
            observation=observation,
            step_name=step_name,
            role="output",
            artifact_store=artifact_store,
        )
        parent_ids = []
        if observation.parent_id in step_ids_by_observation:
            parent_id = observation.parent_id
            assert parent_id is not None
            parent_ids.append(step_ids_by_observation[parent_id])
        failed = observation.status is ObservationStatus.ERROR
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
                inputs={input_name: [input_artifact.id]},
                outputs={"output": [output_artifact.id]},
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
        output_ids_by_observation[observation.id] = output_artifact.id
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
    _write_run_metadata(
        client=client,
        run_id=run_id,
        trace=trace,
        agent_name=agent_name,
        stack_id=stack_id,
        status="complete",
    )


def _needs_import_write(run: PipelineRunResponse) -> bool:
    """Whether a previously found synthetic run still needs observations.

    A run left FAILED by an interrupted import (its import metadata never
    reached "complete") is resumable. Any other finished run is fully
    written: legitimately failed traces finish with "complete" metadata.
    """
    if not run.status.is_finished:
        return True
    return (
        run.status == ExecutionStatus.FAILED
        and run.run_metadata.get(_IMPORT_STATUS_KEY) != "complete"
    )


def _mark_import_write_failed(
    *,
    client: Client,
    run_id: UUID,
    trace: ImportedTrace,
    agent_name: str,
    stack_id: UUID,
    reason: str,
) -> None:
    """Best-effort: leave an interrupted import FAILED instead of RUNNING.

    Both writes may hit the same backend outage that interrupted the import,
    so failures here are logged and swallowed; the original error is what the
    caller reports.
    """
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
                agent_name=agent_name,
                stack_id=stack_id,
                status="failed",
            ),
        ),
    )
    for description, attempt in attempts:
        try:
            attempt()
        except Exception:
            logger.debug("Failed to %s for run %s.", description, run_id, exc_info=True)


def _validate_import(trace: ImportedTrace, *, agent_name: str) -> str:
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
    normalized_agent_name = agent_name.strip()
    if not normalized_agent_name:
        raise ImportedTracePersistenceError("agent_name cannot be empty.")
    return normalized_agent_name


def _get_or_create_pipeline(
    *, client: Client, project_id: UUID, provider: str, agent_name: str
) -> PipelineResponse:
    name = imported_flow_name(provider=provider, agent_name=agent_name)
    matches = client.list_pipelines(
        name=name, project=project_id, size=2, hydrate=True
    ).items
    if matches:
        return matches[0]
    try:
        return client.zen_store.create_pipeline(
            PipelineRequest(
                project=project_id,
                name=name,
                description=(
                    "Synthetic executions imported from external agent traces. "
                    "This pipeline is not executable."
                ),
                tags=[_IMPORT_TAG],
            )
        )
    except EntityExistsError:
        return client.get_pipeline(name, project=project_id, hydrate=True)


def _get_or_create_snapshot(
    *,
    client: Client,
    project_id: UUID,
    pipeline_id: UUID,
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
        return matches[0]
    request = PipelineSnapshotRequest(
        project=project_id,
        name=name,
        description="Synthetic observed graph for one imported trace.",
        run_name_template="imported-{date}-{time}",
        pipeline_configuration=PipelineConfiguration(
            name=pipeline_name,
            enable_cache=False,
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
        tags=[_IMPORT_TAG],
    )
    try:
        return client.zen_store.create_snapshot(request)
    except EntityExistsError:
        return client.get_snapshot(
            name, pipeline_name_or_id=pipeline_id, project=project_id, hydrate=True
        )


def _find_run(
    *, client: Client, project_id: UUID, run_name: str
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
    agent_name: str,
    stack_id: UUID,
) -> None:
    environment = run.orchestrator_environment
    expected = _import_environment(trace, agent_name=agent_name, stack_id=stack_id)
    identity_keys = (
        _SOURCE_PROVIDER_KEY,
        _SOURCE_PROJECT_KEY,
        _SOURCE_TRACE_KEY,
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
    if environment.get(_SOURCE_DIGEST_KEY) != trace.content_digest:
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
    if environment.get(_AGENT_NAME_KEY) != agent_name:
        existing_agent_name = environment.get(_AGENT_NAME_KEY)
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with agent_name="
            f"{existing_agent_name!r}.",
            existing_execution_id=str(run.id),
            resolution=(
                f"Retry with agent_name={existing_agent_name!r} to reuse it. "
                "To regroup the "
                f"trace as {agent_name!r}, delete execution {str(run.id)!r} first; "
                "imports are not relabeled in place."
            ),
        )


def _steps_by_name(client: Client, *, run_id: UUID) -> dict[str, StepRunResponse]:
    steps_by_name: dict[str, StepRunResponse] = {}
    page = 1
    page_size = 200
    while True:
        steps = client.list_run_steps(
            pipeline_run_id=run_id,
            project=client.active_project.id,
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
    trace: ImportedTrace,
    observation: ImportedObservation,
    step_name: str,
    role: str,
    artifact_store: Any,
) -> ArtifactVersionResponse:
    return save_artifact(
        data=value,
        name=f"{step_name}::{role}",
        artifact_type=ArtifactType.DATA,
        tags=[_IMPORT_TAG],
        include_visualizations=True,
        user_metadata={
            "kitaru_artifact_type": role,
            _SOURCE_TRACE_KEY: trace.source.trace_id,
            IMPORTED_OBSERVATION_ID_METADATA_KEY: observation.id,
        },
        save_type=ArtifactSaveType.MANUAL,
        has_custom_name=False,
        artifact_store=artifact_store,
    )


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
    agent_name: str,
    stack_id: UUID,
    status: str,
) -> None:
    client.create_run_metadata(
        metadata={
            **_import_environment(trace, agent_name=agent_name, stack_id=stack_id),
            _IMPORT_STATUS_KEY: status,
            "kitaru_import_integrity_v1": trace.integrity.value,
            "kitaru_import_missing_parent_ids_v1": trace.missing_parent_ids,
            "kitaru_import_component_count_v1": trace.component_count,
            "kitaru_import_observation_count_v1": len(trace.observations),
        },
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
    trace: ImportedTrace, *, agent_name: str, stack_id: UUID
) -> dict[str, Any]:
    return {
        IMPORTED_EXECUTION_ENVIRONMENT_KEY: True,
        "kitaru_import_schema_version": _IMPORT_SCHEMA_VERSION,
        _SOURCE_PROVIDER_KEY: trace.source.provider,
        _SOURCE_PROJECT_KEY: trace.source.project_id,
        _SOURCE_TRACE_KEY: trace.source.trace_id,
        _SOURCE_DIGEST_KEY: trace.content_digest,
        _AGENT_NAME_KEY: agent_name,
        _STACK_ID_KEY: str(stack_id),
    }


def _existing_run_stack_id(run: PipelineRunResponse) -> str | None:
    environment_stack_id = run.orchestrator_environment.get(_STACK_ID_KEY)
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
