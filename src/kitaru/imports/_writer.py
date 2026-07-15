"""Persist imported traces as synthetic ZenML executions."""

import hashlib
import re
from dataclasses import dataclass
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
from kitaru._llm_usage import build_usage_record, usage_record_metadata
from kitaru.imports._models import (
    ImportedObservation,
    ImportedTrace,
    ObservationKind,
    ObservationStatus,
    SourceObservationType,
    TraceIntegrity,
)

_IMPORT_SCHEMA_VERSION = 1
_IMPORT_TAG = "kitaru-imported"
_SOURCE_DIGEST_KEY = "kitaru_import_content_digest_v1"
_SOURCE_PROVIDER_KEY = "kitaru_import_source_provider_v1"
_SOURCE_PROJECT_KEY = "kitaru_import_source_project_id_v1"
_SOURCE_TRACE_KEY = "kitaru_import_source_trace_id_v1"
_AGENT_NAME_KEY = "kitaru_import_agent_name_v1"
_IMPORT_STATUS_KEY = "kitaru_import_status_v1"
_OBSERVATION_ID_KEY = "kitaru_import_observation_id_v1"


class ImportedTracePersistenceError(RuntimeError):
    """Raised when a normalized trace cannot be safely persisted."""


class ImportedTraceConflictError(ImportedTracePersistenceError):
    """Raised when a source trace identity already has different content."""


@dataclass(frozen=True)
class ImportedExecutionResult:
    """Result of persisting one normalized trace."""

    execution_id: str
    created: bool
    resumed: bool
    observation_count: int


def _imported_observation_placeholder() -> None:
    """Identify synthetic dynamic steps; this function must never execute."""
    raise RuntimeError("Synthetic imported observations cannot be executed.")


def persist_imported_trace(
    trace: ImportedTrace,
    *,
    agent_name: str,
    client: Client | None = None,
) -> ImportedExecutionResult:
    """Persist one normalized trace as a synthetic ZenML pipeline run.

    No user flow, model, tool, or provider code is executed. The active ZenML
    stack is used only to persist artifacts and associate the synthetic run
    with a stack for normal UI rendering.
    """
    if not trace.observations:
        raise ImportedTracePersistenceError("Cannot persist an empty trace.")
    if trace.integrity is TraceIntegrity.INVALID:
        raise ImportedTracePersistenceError(
            f"Cannot persist trace {trace.source.trace_id!r}: invalid graph."
        )
    normalized_agent_name = agent_name.strip()
    if not normalized_agent_name:
        raise ImportedTracePersistenceError("agent_name cannot be empty.")

    zenml_client = client or Client()
    project_id = zenml_client.active_project.id
    identity_digest = _source_identity_digest(trace)
    run_name = _run_name(trace, identity_digest=identity_digest)
    import_environment = _import_environment(trace, agent_name=normalized_agent_name)
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
        )
        if existing_run.status.is_finished:
            if existing_run.run_metadata.get(_IMPORT_STATUS_KEY) != "complete":
                _write_run_metadata(
                    client=zenml_client,
                    run_id=existing_run.id,
                    trace=trace,
                    agent_name=normalized_agent_name,
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
            )
    else:
        run = existing_run
        created = False

    _write_run_metadata(
        client=zenml_client,
        run_id=run.id,
        trace=trace,
        agent_name=normalized_agent_name,
        status="importing",
    )
    existing_steps = _steps_by_name(zenml_client, run_id=run.id)
    step_ids_by_observation: dict[str, UUID] = {}
    output_ids_by_observation: dict[str, UUID] = {}
    step_name_by_observation = {
        observation.id: _step_name(observation, index=index)
        for index, observation in enumerate(trace.observations, start=1)
    }
    for observation in trace.observations:
        step_name = step_name_by_observation[observation.id]
        existing_step = existing_steps.get(step_name)
        if existing_step is not None:
            step_ids_by_observation[observation.id] = existing_step.id
            output_artifacts = existing_step.outputs.get("output", [])
            if output_artifacts:
                output_ids_by_observation[observation.id] = output_artifacts[0].id
            continue

        input_name = _input_name(observation)
        input_artifact = _save_observation_artifact(
            observation.input,
            trace=trace,
            observation=observation,
            step_name=step_name,
            role="input",
        )
        output_artifact = _save_observation_artifact(
            observation.output,
            trace=trace,
            observation=observation,
            step_name=step_name,
            role="output",
        )
        parent_ids = []
        upstream_names = []
        if observation.parent_id in step_ids_by_observation:
            parent_id = observation.parent_id
            assert parent_id is not None
            parent_ids.append(step_ids_by_observation[parent_id])
            upstream_names.append(step_name_by_observation[parent_id])
        metadata = _step_metadata(observation, step_name=step_name)
        dynamic_config = _dynamic_step_config(
            step_name=step_name,
            upstream_names=upstream_names,
            observation=observation,
            metadata=metadata,
        )
        failed = observation.status is ObservationStatus.ERROR
        step = zenml_client.zen_store.create_run_step(
            StepRunRequest(
                project=project_id,
                name=step_name,
                start_time=observation.started_at,
                end_time=observation.ended_at or observation.started_at,
                status=(
                    ExecutionStatus.FAILED if failed else ExecutionStatus.COMPLETED
                ),
                pipeline_run_id=run.id,
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
                dynamic_config=dynamic_config,
            )
        )
        step_ids_by_observation[observation.id] = step.id
        output_ids_by_observation[observation.id] = output_artifact.id
        _write_step_metadata(client=zenml_client, step_id=step.id, metadata=metadata)

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
    zenml_client.zen_store.update_run(
        run.id,
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
        client=zenml_client,
        run_id=run.id,
        trace=trace,
        agent_name=normalized_agent_name,
        status="complete",
    )
    return ImportedExecutionResult(
        execution_id=str(run.id),
        created=created,
        resumed=not created,
        observation_count=len(trace.observations),
    )


def _get_or_create_pipeline(
    *, client: Client, project_id: UUID, provider: str, agent_name: str
) -> PipelineResponse:
    identity = hashlib.sha256(agent_name.encode("utf-8")).hexdigest()[:8]
    name = (
        f"imported_{_slug(agent_name, limit=80)}__{_slug(provider, limit=30)}_"
        f"v{_IMPORT_SCHEMA_VERSION}_{identity}"
    )
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
    *, client: Client, project_id: UUID, pipeline_id: UUID, pipeline_name: str
) -> PipelineSnapshotResponse:
    name = f"kitaru-import-schema-v{_IMPORT_SCHEMA_VERSION}"
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
        description="Synthetic dynamic snapshot for imported traces.",
        run_name_template="imported-{date}-{time}",
        pipeline_configuration=PipelineConfiguration(
            name=pipeline_name,
            enable_cache=False,
        ),
        step_configurations={},
        pipeline_spec=PipelineSpec(steps=[]),
        is_dynamic=True,
        stack=client.active_stack_model.id,
        pipeline=pipeline_id,
        pipeline_version_hash=f"kitaru-import-v{_IMPORT_SCHEMA_VERSION}",
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
    run: PipelineRunResponse, *, trace: ImportedTrace, agent_name: str
) -> None:
    environment = run.orchestrator_environment
    expected = _import_environment(trace, agent_name=agent_name)
    identity_keys = (
        _SOURCE_PROVIDER_KEY,
        _SOURCE_PROJECT_KEY,
        _SOURCE_TRACE_KEY,
    )
    if any(environment.get(key) != expected[key] for key in identity_keys):
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} collides with another imported identity."
        )
    if environment.get(_SOURCE_DIGEST_KEY) != trace.content_digest:
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported with "
            "different content."
        )
    if environment.get(_AGENT_NAME_KEY) != agent_name:
        raise ImportedTraceConflictError(
            f"Trace {trace.source.trace_id!r} was already imported for another agent."
        )


def _steps_by_name(client: Client, *, run_id: UUID) -> dict[str, StepRunResponse]:
    steps = client.list_run_steps(
        pipeline_run_id=run_id,
        project=client.active_project.id,
        size=200,
        hydrate=True,
        exclude_retried=False,
    ).items
    return {step.name: step for step in steps}


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


def _save_observation_artifact(
    value: Any,
    *,
    trace: ImportedTrace,
    observation: ImportedObservation,
    step_name: str,
    role: str,
) -> ArtifactVersionResponse:
    return save_artifact(
        data=value,
        name=f"{step_name}::{role}",
        artifact_type=ArtifactType.DATA,
        tags=[_IMPORT_TAG],
        include_visualizations=False,
        user_metadata={
            "kitaru_artifact_type": role,
            _SOURCE_TRACE_KEY: trace.source.trace_id,
            _OBSERVATION_ID_KEY: observation.id,
        },
        save_type=ArtifactSaveType.MANUAL,
        has_custom_name=False,
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
        _OBSERVATION_ID_KEY: observation.id,
        "kitaru_import_parent_observation_id_v1": observation.parent_id or "",
        "kitaru_import_source_type_v1": observation.source_type.value,
        "kitaru_import_source_status_v1": observation.status.value,
        "kitaru_import_source_name_v1": observation.name,
    }
    if observation.usage is not None or observation.cost is not None:
        usage = observation.usage
        cost = observation.cost
        record = build_usage_record(
            adapter="langfuse_import",
            surface="model_call",
            call_name=step_name,
            event_id=observation.id,
            checkpoint_name=step_name,
            model=observation.model,
            provider="langfuse",
            input_tokens=_integer(usage.input if usage else None),
            output_tokens=_integer(usage.output if usage else None),
            total_tokens=_integer(usage.total if usage else None),
            raw_usage=usage.details if usage else None,
            actual_cost_usd=(
                cost.total
                if cost is not None and cost.currency in {None, "USD", "usd"}
                else None
            ),
            cost_source_label="Langfuse imported provider cost",
            latency_ms=observation.latency_ms,
            status=(
                "failed"
                if observation.status is ObservationStatus.ERROR
                else "completed"
            ),
            billing_effect="unknown",
            cache_status="unknown",
            warnings=("Imported historical usage; no model call was executed.",),
            record_id=observation.id,
        )
        metadata.update(usage_record_metadata(record))
    return metadata


def _write_run_metadata(
    *,
    client: Client,
    run_id: UUID,
    trace: ImportedTrace,
    agent_name: str,
    status: str,
) -> None:
    client.create_run_metadata(
        metadata={
            **_import_environment(trace, agent_name=agent_name),
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


def _import_environment(trace: ImportedTrace, *, agent_name: str) -> dict[str, Any]:
    return {
        "kitaru_synthetic_import": True,
        "kitaru_import_schema_version": _IMPORT_SCHEMA_VERSION,
        _SOURCE_PROVIDER_KEY: trace.source.provider,
        _SOURCE_PROJECT_KEY: trace.source.project_id,
        _SOURCE_TRACE_KEY: trace.source.trace_id,
        _SOURCE_DIGEST_KEY: trace.content_digest,
        _AGENT_NAME_KEY: agent_name,
    }


def _source_identity_digest(trace: ImportedTrace) -> str:
    identity = "\0".join(trace.source.identity)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def _run_name(trace: ImportedTrace, *, identity_digest: str) -> str:
    return f"imported-{_slug(trace.source.trace_id, limit=100)}-{identity_digest}"


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


def _integer(value: int | float | None) -> int | None:
    if value is None:
        return None
    return int(value)
