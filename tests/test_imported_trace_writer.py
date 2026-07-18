"""Integration tests for imported traces persisted as ZenML executions."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from zenml.client import Client
from zenml.enums import RunWaitConditionLeaseMode
from zenml.models import PipelineRequest, RunWaitConditionLeaseUpdate

import kitaru.imports._writer as imported_trace_writer
from kitaru._agent_registration import RegisteredAgentVersionBinding
from kitaru._config._agents import (
    _AgentVersionManifest,
    _reconcile_agent_version_registration,
)
from kitaru.client import KitaruClient
from kitaru.errors import KitaruStateError
from kitaru.imports import (
    LangfuseSourceRecord,
    normalize_langfuse_observations,
    read_langfuse_jsonl_records,
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
    sha256_canonical_json,
)
from kitaru.imports._writer import (
    ImportedTraceConflictError,
    ImportedTracePersistenceError,
    ImportedTraceWriteError,
    persist_imported_trace,
    plan_imported_trace,
)

FIXTURE = Path(__file__).parent / "imports" / "fixtures" / "langfuse_observations.jsonl"


@dataclass(frozen=True)
class _ImportCase:
    trace: Any
    binding: RegisteredAgentVersionBinding
    raw_evidence: RawImportedEvidence
    replay_evidence: PydanticAIReplayEvidence
    attribution: SourceAttribution

    @property
    def writer_kwargs(self) -> dict[str, Any]:
        return {
            "binding": self.binding,
            "raw_evidence": self.raw_evidence,
            "replay_evidence": self.replay_evidence,
            "attribution": self.attribution,
        }


@pytest.fixture
def source_binding(primed_zenml: None) -> RegisteredAgentVersionBinding:
    del primed_zenml
    return _create_binding()


def _create_binding(
    *,
    name: str = "support_agent__av_test",
    label: str = "prod",
) -> RegisteredAgentVersionBinding:
    client = Client()
    pipeline = client.zen_store.create_pipeline(
        PipelineRequest(
            project=client.active_project.id,
            name=name,
            description="Registered source AgentVersion for import tests.",
        )
    )
    pipeline_id = str(pipeline.id)
    manifest = _AgentVersionManifest(
        schema_version=1,
        agent_version_id=pipeline_id,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline.name,
        fingerprint=f"sha256:{pipeline_id}",
        git_sha="0123456789abcdef0123456789abcdef01234567",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash="sha256:configuration",
        worldview_hash="sha256:worldview",
        entrypoint="tests.test_imported_trace_writer:REGISTERED_IMPORT_AGENT",
        registered_at="2026-07-18T10:00:00Z",
        source="registration",
    )
    _reconcile_agent_version_registration(
        project_id=str(client.active_project.id),
        agent_name="support-agent",
        manifest=manifest,
        label=label,
        client_factory=lambda: client,
    )
    return RegisteredAgentVersionBinding(
        project_id=str(client.active_project.id),
        manifest=manifest,
        agent_name="support-agent",
        project_name=str(client.active_project.name),
        aliases=(label,),
        requested_alias=label,
    )


def _case(
    binding: RegisteredAgentVersionBinding,
    trace_id: str = "trace-complete",
) -> _ImportCase:
    normalized = normalize_langfuse_records(
        read_langfuse_jsonl_records(FIXTURE),
        project_id="langfuse-project",
    )
    selected = next(
        item for item in normalized if item.trace.source.trace_id == trace_id
    )
    raw_evidence = build_raw_imported_evidence(
        source=selected.trace.source,
        records=selected.records,
    )
    return _case_from_trace(
        selected.trace,
        binding=binding,
        raw_evidence=raw_evidence,
    )


def _case_from_trace(
    trace: Any,
    *,
    binding: RegisteredAgentVersionBinding,
    raw_evidence: RawImportedEvidence | None = None,
) -> _ImportCase:
    if raw_evidence is None:
        records = []
        for source_order, observation in enumerate(trace.observations):
            row: dict[str, Any] = {
                "id": observation.id,
                "traceId": observation.trace_id,
                "type": observation.source_type.value,
                "name": observation.name,
                "startTime": observation.started_at.isoformat(),
            }
            if observation.parent_id is not None:
                row["parentObservationId"] = observation.parent_id
            if observation.ended_at is not None:
                row["endTime"] = observation.ended_at.isoformat()
            if observation.status.value == "error":
                row["level"] = "ERROR"
            if observation.status_message is not None:
                row["statusMessage"] = observation.status_message
            if observation.input_present:
                row["input"] = observation.input
            if observation.output_present:
                row["output"] = observation.output
            if observation.metadata:
                row["metadata"] = observation.metadata
            if observation.model is not None:
                row["providedModelName"] = observation.model
            raw_text = json.dumps(row, separators=(",", ":")) + "\n"
            records.append(
                LangfuseSourceRecord(
                    line_number=source_order + 1,
                    source_order=source_order,
                    raw_text=raw_text,
                    row=row,
                )
            )
        raw_evidence = build_raw_imported_evidence(
            source=trace.source,
            records=records,
        )
    replay_evidence = build_pydantic_ai_replay_evidence(
        trace,
        raw_evidence=raw_evidence,
    )
    return _ImportCase(
        trace=trace,
        binding=binding,
        raw_evidence=raw_evidence,
        replay_evidence=replay_evidence,
        attribution=SourceAttribution(status=SourceAttributionStatus.CALLER_ATTRIBUTED),
    )


def test_rejects_forged_registered_binding_fields(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    forged_bindings = (
        replace(
            source_binding,
            manifest=source_binding.manifest.model_copy(
                update={"git_sha": "forged-git-sha"}
            ),
        ),
        replace(
            source_binding,
            manifest=source_binding.manifest.model_copy(
                update={"fingerprint": "sha256:forged"}
            ),
        ),
        replace(source_binding, aliases=("forged",)),
        replace(source_binding, agent_name="forged-agent"),
        replace(source_binding, project_name="forged-project"),
        replace(source_binding, requested_alias="forged"),
    )

    for forged_binding in forged_bindings:
        with pytest.raises(KitaruStateError, match="binding differs"):
            persist_imported_trace(
                case.trace,
                binding=forged_binding,
                raw_evidence=case.raw_evidence,
                replay_evidence=case.replay_evidence,
                attribution=case.attribution,
            )


def test_persists_trace_as_visible_execution_without_running_source_code(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    trace = case.trace

    result = persist_imported_trace(trace, **case.writer_kwargs)
    execution = KitaruClient().executions.get(result.execution_id)
    run = Client().get_pipeline_run(result.execution_id, hydrate=True)

    assert result.created is True
    assert result.resumed is False
    assert execution.status.value == "completed"
    # The local ZenML SQL store returns persisted UTC values without tzinfo.
    assert execution.started_at is not None
    assert execution.ended_at is not None
    assert execution.started_at.replace(tzinfo=UTC) == trace.started_at
    assert execution.ended_at.replace(tzinfo=UTC) == trace.ended_at
    assert execution.metadata["kitaru_import_status_v1"] == "complete"
    assert (
        execution.metadata["kitaru_import_source_trace_id_v1"] == trace.source.trace_id
    )
    assert execution.metadata["kitaru_import_content_digest_v1"] == trace.content_digest
    assert execution.metadata["kitaru_import_schema_version"] == 5
    assert execution.metadata["kitaru_snapshot_kind_v1"] == "imported_observed"
    assert (
        execution.metadata["kitaru_import_source_pipeline_id_v1"]
        == source_binding.pipeline_id
    )
    assert str(run.snapshot.pipeline.id) == source_binding.pipeline_id
    raw_artifact = Client().get_artifact_version(
        execution.metadata["kitaru_import_raw_evidence_artifact_id_v1"],
        project=source_binding.project_id,
        hydrate=True,
    )
    replay_artifact = Client().get_artifact_version(
        execution.metadata["kitaru_import_replay_bundle_artifact_id_v1"],
        project=source_binding.project_id,
        hydrate=True,
    )
    assert raw_artifact.load() == case.raw_evidence.model_dump(mode="json")
    assert replay_artifact.load() == case.replay_evidence.bundle.model_dump(mode="json")
    assert result.raw_evidence_artifact_id == str(raw_artifact.id)
    assert result.raw_evidence_schema_version == case.raw_evidence.schema_version
    assert result.replay_bundle_artifact_id == str(replay_artifact.id)
    assert (
        result.replay_bundle_schema_version
        == case.replay_evidence.bundle.schema_version
    )
    assert len(execution.checkpoints) == len(trace.observations)

    checkpoints_by_source_id = {
        checkpoint.metadata["kitaru_import_observation_id_v1"]: checkpoint
        for checkpoint in execution.checkpoints
    }
    agent = checkpoints_by_source_id["agent-1"]
    chain = checkpoints_by_source_id["chain-1"]
    generation = checkpoints_by_source_id["generation-1"]
    tool = checkpoints_by_source_id["tool-1"]
    assert chain.parent_call_ids == [agent.call_id]
    assert generation.parent_call_ids == [chain.call_id]
    assert generation.checkpoint_type == "llm_call"
    assert generation.checkpoint_origin == "adapter"
    assert generation.adapter == "langfuse_import"
    assert generation.llm_usage_records[0]["usage"]["total_tokens"] == 16
    assert generation.llm_usage_records[0]["cost"]["actual_cost_usd"] == pytest.approx(
        0.00016
    )
    assert {artifact.direction for artifact in generation.artifacts} == {
        "input",
        "output",
    }
    generation_input = next(
        artifact for artifact in generation.artifacts if artifact.direction == "input"
    )
    assert generation_input.load() == [{"role": "user", "content": "synthetic"}]
    artifact_version = Client().get_artifact_version(generation_input.artifact_id)
    assert artifact_version.visualizations
    assert tool.status.value == "failed"
    assert tool.failure is not None
    assert tool.failure.message == "Synthetic error"


def test_persistence_distinguishes_absent_fields_from_explicit_null(
    source_binding: RegisteredAgentVersionBinding,
    tmp_path: Path,
) -> None:
    rows = [
        {
            "id": "absent-root",
            "traceId": "trace-absent-fields",
            "type": "AGENT",
            "name": "absent",
            "startTime": "2026-07-15T10:00:00Z",
            "endTime": "2026-07-15T10:00:01Z",
        },
        {
            "id": "null-root",
            "traceId": "trace-explicit-null",
            "type": "AGENT",
            "name": "null",
            "startTime": "2026-07-15T10:01:00Z",
            "endTime": "2026-07-15T10:01:01Z",
            "input": None,
            "output": None,
        },
    ]
    path = tmp_path / "presence.jsonl"
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    normalized = normalize_langfuse_records(
        read_langfuse_jsonl_records(path),
        project_id="langfuse-project",
    )
    client = Client()
    persisted_steps = {}
    for item in normalized:
        raw_evidence = build_raw_imported_evidence(
            source=item.trace.source,
            records=item.records,
        )
        case = _case_from_trace(
            item.trace,
            binding=source_binding,
            raw_evidence=raw_evidence,
        )
        result = persist_imported_trace(item.trace, **case.writer_kwargs)
        persisted_steps[item.trace.source.trace_id] = client.list_run_steps(
            pipeline_run_id=result.execution_id,
            project=source_binding.project_id,
            size=10,
            hydrate=True,
        ).items[0]

    absent = persisted_steps["trace-absent-fields"]
    explicit_null = persisted_steps["trace-explicit-null"]
    assert absent.inputs == {}
    assert absent.outputs == {}
    assert len(explicit_null.inputs) == 1
    assert len(explicit_null.outputs) == 1
    assert next(iter(explicit_null.inputs.values()))[0].load() is None
    assert next(iter(explicit_null.outputs.values()))[0].load() is None


def test_imported_snapshot_contains_every_rendered_step_node(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    result = persist_imported_trace(case.trace, **case.writer_kwargs)
    client = Client()
    run = client.get_pipeline_run(result.execution_id)
    snapshot_id = getattr(run.snapshot, "id", run.snapshot)
    snapshot = client.get_snapshot(
        name_id_or_prefix=snapshot_id,
        project=client.active_project.id,
        hydrate=True,
    )
    run_step_names = {
        step.name
        for step in client.list_run_steps(
            pipeline_run_id=run.id,
            project=client.active_project.id,
            size=100,
            hydrate=True,
        ).items
    }

    assert run_step_names == set(snapshot.step_configurations)
    assert run_step_names == {
        step.invocation_id for step in snapshot.pipeline_spec.steps
    }
    assert str(snapshot.pipeline.id) == source_binding.pipeline_id
    assert snapshot.pipeline_configuration.extra["kitaru_snapshot_kind_v1"] == (
        "imported_observed"
    )


def test_child_first_source_order_renders_without_a_dag_error(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    trace = normalize_langfuse_observations(
        [
            {
                "id": "child",
                "traceId": "child-first",
                "parentObservationId": "parent",
                "type": "GENERATION",
                "name": "child",
                "startTime": "2026-07-15T10:00:01Z",
                "endTime": "2026-07-15T10:00:02Z",
            },
            {
                "id": "parent",
                "traceId": "child-first",
                "type": "AGENT",
                "name": "parent",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:03Z",
            },
        ],
        project_id="project-1",
    )[0]

    case = _case_from_trace(trace, binding=source_binding)
    result = persist_imported_trace(trace, **case.writer_kwargs)
    client = Client()
    dag = client.zen_store.get_pipeline_run_dag(  # type: ignore[attr-defined]
        UUID(result.execution_id)
    )
    step_nodes = [node for node in dag.nodes if node.type == "step"]
    execution = KitaruClient().executions.get(result.execution_id)
    checkpoints = {
        checkpoint.metadata["kitaru_import_observation_id_v1"]: checkpoint
        for checkpoint in execution.checkpoints
    }

    assert {node.name.split("_", maxsplit=3)[2] for node in step_nodes} == {
        "parent",
        "child",
    }
    assert checkpoints["child"].parent_call_ids == [checkpoints["parent"].call_id]


def test_exact_reimport_is_a_noop(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    trace = case.trace

    first = persist_imported_trace(trace, **case.writer_kwargs)
    artifact_count = len(Client().list_artifact_versions(size=100).items)
    second = persist_imported_trace(trace, **case.writer_kwargs)
    execution = KitaruClient().executions.get(first.execution_id)

    assert second.execution_id == first.execution_id
    assert second.created is False
    assert second.resumed is False
    assert len(execution.checkpoints) == len(trace.observations)
    assert len(Client().list_artifact_versions(size=100).items) == artifact_count


def test_writer_rejects_forged_attribution_before_creating_a_run(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    forged = SourceAttribution(status=SourceAttributionStatus.SOURCE_VERIFIED)

    with pytest.raises(
        ImportedTracePersistenceError,
        match="Source attribution does not match",
    ):
        persist_imported_trace(
            case.trace,
            binding=case.binding,
            raw_evidence=case.raw_evidence,
            replay_evidence=case.replay_evidence,
            attribution=forged,
        )


def test_writer_rejects_mismatched_raw_rows_before_creating_a_run(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    first_row = case.raw_evidence.rows[0]
    forged_row = first_row.model_copy(
        update={"parsed_object": {**first_row.parsed_object, "name": "forged"}}
    )
    forged_raw = case.raw_evidence.model_copy(
        update={"rows": (forged_row, *case.raw_evidence.rows[1:])}
    )

    with pytest.raises(
        ImportedTracePersistenceError,
        match="source text does not match",
    ):
        persist_imported_trace(
            case.trace,
            binding=case.binding,
            raw_evidence=forged_raw,
            replay_evidence=case.replay_evidence,
            attribution=case.attribution,
        )


def test_writer_rejects_self_consistent_replay_bundle_with_other_observations(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    values = case.replay_evidence.bundle.model_dump(
        mode="json", exclude={"bundle_digest"}
    )
    values["observations"][0]["input"] = {"forged": True}
    forged_observation = case.replay_evidence.bundle.observations[0].model_copy(
        update={"input": {"forged": True}}
    )
    forged_bundle = case.replay_evidence.bundle.model_copy(
        update={
            "observations": (
                forged_observation,
                *case.replay_evidence.bundle.observations[1:],
            ),
            "bundle_digest": sha256_canonical_json(values),
        }
    )
    forged_replay = case.replay_evidence.model_copy(update={"bundle": forged_bundle})

    with pytest.raises(
        ImportedTracePersistenceError,
        match="Replay evidence does not match",
    ):
        persist_imported_trace(
            case.trace,
            binding=case.binding,
            raw_evidence=case.raw_evidence,
            replay_evidence=forged_replay,
            attribution=case.attribution,
        )


def test_changed_raw_or_replay_evidence_conflicts(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    existing = persist_imported_trace(case.trace, **case.writer_kwargs)
    changed_raw = build_raw_imported_evidence(
        source=case.trace.source,
        records=tuple(
            LangfuseSourceRecord(
                line_number=row.line_number,
                source_order=row.source_order,
                raw_text=json.dumps(row.parsed_object, indent=1) + "\n",
                row=dict(row.parsed_object),
            )
            for row in case.raw_evidence.rows
        ),
    )
    changed = _case_from_trace(
        case.trace,
        binding=source_binding,
        raw_evidence=changed_raw,
    )

    with pytest.raises(
        ImportedTraceConflictError, match="different source evidence"
    ) as exc_info:
        persist_imported_trace(changed.trace, **changed.writer_kwargs)

    assert exc_info.value.existing_execution_id == existing.execution_id


def test_changed_cohort_tag_conflicts(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    persist_imported_trace(case.trace, cohort_tag="customer-a", **case.writer_kwargs)

    with pytest.raises(ImportedTraceConflictError, match="different cohort tag"):
        persist_imported_trace(
            case.trace,
            cohort_tag="customer-b",
            **case.writer_kwargs,
        )


def test_lost_evidence_artifact_response_reuses_created_version(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    save_in_project = imported_trace_writer._save_artifact_in_project
    interrupted = False

    def save_then_lose_response(**kwargs):
        nonlocal interrupted
        artifact = save_in_project(**kwargs)
        if not interrupted and kwargs["name"].endswith("::raw_evidence"):
            interrupted = True
            raise RuntimeError("synthetic lost response")
        return artifact

    monkeypatch.setattr(
        imported_trace_writer,
        "_save_artifact_in_project",
        save_then_lose_response,
    )
    result = persist_imported_trace(case.trace, **case.writer_kwargs)
    raw_versions = (
        Client()
        .list_artifact_versions(
            tags="kitaru-import-raw-evidence",
            project=source_binding.project_id,
            size=10,
        )
        .items
    )

    assert result.created is True
    assert interrupted is True
    assert len(raw_versions) == 1


def test_interrupted_evidence_reference_write_resumes_existing_artifacts(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    write_run_metadata = imported_trace_writer._write_run_metadata
    interrupted = False

    def interrupt_reference_write(**kwargs):
        nonlocal interrupted
        if not interrupted and kwargs.get("raw_evidence_artifact_id") is not None:
            interrupted = True
            raise RuntimeError("synthetic evidence reference interruption")
        return write_run_metadata(**kwargs)

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_run_metadata",
        interrupt_reference_write,
    )
    with pytest.raises(
        ImportedTraceWriteError,
        match="synthetic evidence reference interruption",
    ) as exc_info:
        persist_imported_trace(case.trace, **case.writer_kwargs)

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_run_metadata",
        write_run_metadata,
    )
    resumed = persist_imported_trace(case.trace, **case.writer_kwargs)
    raw_versions = (
        Client()
        .list_artifact_versions(
            tags="kitaru-import-raw-evidence",
            project=source_binding.project_id,
            size=10,
        )
        .items
    )
    replay_versions = (
        Client()
        .list_artifact_versions(
            tags="kitaru-import-replay-bundle",
            project=source_binding.project_id,
            size=10,
        )
        .items
    )

    assert resumed.execution_id == exc_info.value.execution_id
    assert resumed.resumed is True
    assert len(raw_versions) == 1
    assert len(replay_versions) == 1


def test_stale_evidence_reference_conflicts(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    result = persist_imported_trace(case.trace, **case.writer_kwargs)
    client = Client()
    run = client.get_pipeline_run(result.execution_id)
    imported_trace_writer._write_run_metadata(
        client=client,
        run_id=run.id,
        trace=case.trace,
        binding=source_binding,
        stack_id=client.active_stack_model.id,
        raw_evidence=case.raw_evidence,
        replay_evidence=case.replay_evidence,
        attribution=case.attribution,
        cohort_tag=None,
        status="complete",
        raw_evidence_artifact_id=str(UUID(int=999)),
        replay_bundle_artifact_id=(
            run.run_metadata["kitaru_import_replay_bundle_artifact_id_v1"]
        ),
    )

    with pytest.raises(ImportedTraceConflictError, match="missing immutable evidence"):
        persist_imported_trace(case.trace, **case.writer_kwargs)


def test_post_write_pipeline_recreation_fails_closed(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    verify_binding = imported_trace_writer.verify_registered_agent_version_binding
    calls = 0

    def recreate_after_write(client, *, binding):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KitaruStateError(
                "The registered Pipeline name now resolves to a different UUID."
            )
        return verify_binding(client, binding=binding)

    monkeypatch.setattr(
        imported_trace_writer,
        "verify_registered_agent_version_binding",
        recreate_after_write,
    )

    with pytest.raises(KitaruStateError, match="different UUID"):
        persist_imported_trace(case.trace, **case.writer_kwargs)

    assert calls == 2


def test_explicit_stack_requires_its_artifact_store(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)

    with pytest.raises(
        ImportedTracePersistenceError, match="must be provided together"
    ):
        persist_imported_trace(
            case.trace,
            **case.writer_kwargs,
            stack_id=Client().active_stack_model.id,
        )


def test_changed_source_content_conflicts(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    trace = case.trace
    persist_imported_trace(trace, **case.writer_kwargs)
    changed_records = []
    for index, row in enumerate(case.raw_evidence.rows):
        parsed = dict(row.parsed_object)
        if index == 0:
            parsed["input"] = {"changed": True}
        changed_records.append(
            LangfuseSourceRecord(
                line_number=row.line_number,
                source_order=row.source_order,
                raw_text=json.dumps(parsed, separators=(",", ":")) + "\n",
                row=parsed,
            )
        )
    normalized = normalize_langfuse_records(
        changed_records,
        project_id=trace.source.project_id,
    )[0]
    changed_raw = build_raw_imported_evidence(
        source=normalized.trace.source,
        records=normalized.records,
    )
    changed = _case_from_trace(
        normalized.trace,
        binding=source_binding,
        raw_evidence=changed_raw,
    )
    pipeline_count = len(Client().list_pipelines(size=100).items)

    with pytest.raises(
        ImportedTraceConflictError, match="already imported"
    ) as exc_info:
        persist_imported_trace(changed.trace, **changed.writer_kwargs)

    assert exc_info.value.existing_execution_id is not None
    assert "will not overwrite" in (exc_info.value.resolution or "")
    assert len(Client().list_pipelines(size=100).items) == pipeline_count


def test_different_source_version_conflicts(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    case = _case(source_binding)
    existing = persist_imported_trace(case.trace, **case.writer_kwargs)
    different_binding = _create_binding(
        name="support_agent__av_other",
        label="other",
    )
    changed = _ImportCase(
        trace=case.trace,
        binding=different_binding,
        raw_evidence=case.raw_evidence,
        replay_evidence=case.replay_evidence,
        attribution=case.attribution,
    )

    with pytest.raises(
        ImportedTraceConflictError, match="different source AgentVersion"
    ) as exc_info:
        persist_imported_trace(changed.trace, **changed.writer_kwargs)

    assert exc_info.value.existing_execution_id == existing.execution_id
    assert "original source AgentVersion" in (exc_info.value.resolution or "")


def test_invalid_graph_is_not_persisted(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    rows = [
        {
            "id": "a",
            "traceId": "cycle",
            "parentObservationId": "b",
            "type": "SPAN",
            "name": "a",
            "startTime": "2026-07-15T10:00:00Z",
        },
        {
            "id": "b",
            "traceId": "cycle",
            "parentObservationId": "a",
            "type": "SPAN",
            "name": "b",
            "startTime": "2026-07-15T10:00:01Z",
        },
    ]
    trace = normalize_langfuse_observations(rows, project_id="project-1")[0]
    case = _case_from_trace(trace, binding=source_binding)

    with pytest.raises(ImportedTracePersistenceError, match="invalid graph"):
        persist_imported_trace(trace, **case.writer_kwargs)


def test_incomplete_source_trace_is_not_persisted(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    trace = normalize_langfuse_observations(
        [
            {
                "id": "unfinished",
                "traceId": "trace-incomplete",
                "type": "AGENT",
                "name": "unfinished-agent",
                "startTime": "2026-07-15T10:00:00Z",
            }
        ],
        project_id="project-1",
    )[0]
    case = _case_from_trace(trace, binding=source_binding)

    with pytest.raises(ImportedTracePersistenceError, match="terminal status"):
        persist_imported_trace(trace, **case.writer_kwargs)


def test_root_error_fails_execution_but_child_error_does_not(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    root_error = normalize_langfuse_observations(
        [
            {
                "id": "agent-error",
                "traceId": "trace-root-error",
                "type": "AGENT",
                "name": "agent",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "level": "ERROR",
                "statusMessage": "Synthetic root failure",
            }
        ],
        project_id="project-1",
    )[0]
    case = _case_from_trace(root_error, binding=source_binding)

    result = persist_imported_trace(root_error, **case.writer_kwargs)
    execution = KitaruClient().executions.get(result.execution_id)

    assert execution.status.value == "failed"
    assert execution.failure is not None
    assert "failed root or agent observation" in execution.failure.message


def test_get_or_create_collision_verifies_hydrated_binding_before_writes(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    existing = persist_imported_trace(case.trace, **case.writer_kwargs)
    write_imported_run = imported_trace_writer._write_imported_run
    write_called = False

    def unexpected_write(**kwargs):
        nonlocal write_called
        write_called = True
        return write_imported_run(**kwargs)

    monkeypatch.setattr(imported_trace_writer, "_find_run", lambda **kwargs: None)
    monkeypatch.setattr(imported_trace_writer, "_write_imported_run", unexpected_write)

    def reject_binding(*args, **kwargs):
        raise KitaruStateError("different Pipeline UUID")

    monkeypatch.setattr(
        imported_trace_writer,
        "verify_hydrated_submitted_run_binding",
        reject_binding,
    )

    with pytest.raises(KitaruStateError, match="different Pipeline UUID"):
        persist_imported_trace(case.trace, **case.writer_kwargs)

    assert write_called is False
    assert (
        KitaruClient().executions.get(existing.execution_id).status.value == "completed"
    )


def test_independent_clients_share_one_backend_resume_lease(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    original_write = imported_trace_writer._write_imported_run

    def fail_initial_write(**_kwargs):
        raise RuntimeError("initial resume setup failure")

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        fail_initial_write,
    )
    with pytest.raises(ImportedTraceWriteError):
        persist_imported_trace(case.trace, **case.writer_kwargs)

    first_client = Client(root=Path(Client().root))
    second_client = Client(root=Path(Client().root))
    entered = threading.Event()
    release = threading.Event()

    def block_claimed_writer(**kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return original_write(**kwargs)

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        block_claimed_writer,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            persist_imported_trace,
            case.trace,
            **case.writer_kwargs,
            client=first_client,
        )
        assert entered.wait(timeout=10)

        with pytest.raises(ImportedTraceConflictError, match="already being written"):
            plan_imported_trace(
                case.trace,
                binding=case.binding,
                raw_evidence=case.raw_evidence,
                replay_evidence=case.replay_evidence,
                client=second_client,
            )
        with pytest.raises(ImportedTraceConflictError, match="already being written"):
            persist_imported_trace(
                case.trace,
                **case.writer_kwargs,
                client=second_client,
            )

        release.set()
        resumed = future.result(timeout=10)

    assert resumed.resumed is True
    execution = KitaruClient().executions.get(resumed.execution_id)
    assert execution.status.value == "completed"
    assert execution.metadata["kitaru_import_status_v1"] == "complete"


def test_stale_backend_lease_recovers_after_owner_dies(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    original_write = imported_trace_writer._write_imported_run
    first_client = Client(root=Path(Client().root))

    def die_without_cleanup(**_kwargs):
        raise KeyboardInterrupt("synthetic owner death")

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        die_without_cleanup,
    )
    with pytest.raises(KeyboardInterrupt, match="owner death"):
        persist_imported_trace(
            case.trace,
            **case.writer_kwargs,
            client=first_client,
        )

    run = imported_trace_writer._find_run(
        client=first_client,
        project_id=case.binding.project_id,
        run_name=imported_trace_writer._run_name(
            case.trace,
            identity_digest=imported_trace_writer._source_identity_digest(case.trace),
        ),
    )
    assert run is not None
    condition = imported_trace_writer._pending_import_write_lease(
        client=first_client,
        run=run,
        project_id=case.binding.project_id,
    )
    assert condition is not None
    assert condition.poller_instance_id is not None
    first_client.zen_store.update_run_wait_condition_lease(
        condition.id,
        RunWaitConditionLeaseUpdate(
            poller_instance_id=condition.poller_instance_id,
            poller_lease_expires_at=datetime.now(UTC) - timedelta(seconds=1),
            mode=RunWaitConditionLeaseMode.REFRESH,
        ),
    )

    second_client = Client(root=Path(Client().root))
    assert (
        plan_imported_trace(
            case.trace,
            binding=case.binding,
            raw_evidence=case.raw_evidence,
            replay_evidence=case.replay_evidence,
            client=second_client,
        ).value
        == "resume"
    )

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        original_write,
    )
    resumed = persist_imported_trace(
        case.trace,
        **case.writer_kwargs,
        client=second_client,
    )

    assert resumed.resumed is True
    execution = KitaruClient().executions.get(resumed.execution_id)
    assert execution.status.value == "completed"
    assert execution.metadata["kitaru_import_status_v1"] == "complete"


def test_concurrent_same_source_failure_cannot_overwrite_success(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    write_imported_run = imported_trace_writer._write_imported_run
    call_lock = threading.Lock()
    barrier = threading.Barrier(2)
    call_count = 0

    def fail_first_write(**kwargs):
        nonlocal call_count
        with call_lock:
            call_count += 1
            should_fail = call_count == 1
        if should_fail:
            raise RuntimeError("synthetic concurrent failure")
        return write_imported_run(**kwargs)

    def import_once():
        barrier.wait()
        try:
            return persist_imported_trace(case.trace, **case.writer_kwargs)
        except (ImportedTraceWriteError, ImportedTraceConflictError) as exc:
            return exc

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        fail_first_write,
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: import_once(), range(2)))

    failure = next(
        outcome for outcome in outcomes if isinstance(outcome, ImportedTraceWriteError)
    )
    successful = [outcome for outcome in outcomes if not isinstance(outcome, Exception)]
    monkeypatch.setattr(
        imported_trace_writer,
        "_write_imported_run",
        write_imported_run,
    )
    success = (
        successful[0]
        if successful
        else persist_imported_trace(case.trace, **case.writer_kwargs)
    )
    execution = KitaruClient().executions.get(success.execution_id)

    assert failure.execution_id == success.execution_id
    assert success.resumed is True
    assert execution.status.value == "completed"
    assert execution.metadata["kitaru_import_status_v1"] == "complete"


def test_interrupted_import_resumes_missing_steps(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    trace = case.trace
    store = Client().zen_store
    store_type = type(store)
    create_run_step = store_type.create_run_step
    calls = 0

    def interrupt_second_step(self, request):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic interruption")
        return create_run_step(self, request)

    monkeypatch.setattr(store_type, "create_run_step", interrupt_second_step)
    with pytest.raises(ImportedTraceWriteError, match="synthetic interruption") as exc:
        persist_imported_trace(trace, **case.writer_kwargs)

    # The interrupted attempt must not orphan a forever-RUNNING execution:
    # the created run is reported and left in a terminal failed state.
    orphan_id = exc.value.execution_id
    interrupted = KitaruClient().executions.get(orphan_id)
    assert interrupted.status.value == "failed"

    monkeypatch.setattr(store_type, "create_run_step", create_run_step)
    resumed = persist_imported_trace(trace, **case.writer_kwargs)
    execution = KitaruClient().executions.get(resumed.execution_id)

    assert resumed.execution_id == orphan_id
    assert resumed.created is False
    assert resumed.resumed is True
    assert execution.status.value == "completed"
    assert len(execution.checkpoints) == len(trace.observations)


def test_interrupted_metadata_write_is_repaired_on_resume(
    source_binding: RegisteredAgentVersionBinding,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(source_binding)
    trace = case.trace
    write_step_metadata = imported_trace_writer._write_step_metadata
    interrupted = False

    def interrupt_generation_metadata(*, client, step_id, metadata):
        nonlocal interrupted
        if not interrupted and "llm_usage_v1" in metadata:
            interrupted = True
            raise RuntimeError("synthetic metadata interruption")
        return write_step_metadata(client=client, step_id=step_id, metadata=metadata)

    monkeypatch.setattr(
        imported_trace_writer,
        "_write_step_metadata",
        interrupt_generation_metadata,
    )
    with pytest.raises(RuntimeError, match="synthetic metadata interruption"):
        persist_imported_trace(trace, **case.writer_kwargs)

    monkeypatch.setattr(
        imported_trace_writer, "_write_step_metadata", write_step_metadata
    )
    resumed = persist_imported_trace(trace, **case.writer_kwargs)
    execution = KitaruClient().executions.get(resumed.execution_id)
    generation = next(
        checkpoint
        for checkpoint in execution.checkpoints
        if checkpoint.metadata.get("kitaru_import_observation_id_v1") == "generation-1"
    )

    assert resumed.resumed is True
    assert generation.adapter == "langfuse_import"
    assert generation.llm_usage_records[0]["usage"]["total_tokens"] == 16


def test_failed_imported_execution_cannot_be_retried(
    source_binding: RegisteredAgentVersionBinding,
) -> None:
    root_error = normalize_langfuse_observations(
        [
            {
                "id": "agent-error",
                "traceId": "trace-retry-guard",
                "type": "AGENT",
                "name": "agent",
                "startTime": "2026-07-15T10:00:00Z",
                "endTime": "2026-07-15T10:00:01Z",
                "level": "ERROR",
                "statusMessage": "Synthetic root failure",
            }
        ],
        project_id="project-1",
    )[0]
    case = _case_from_trace(root_error, binding=source_binding)
    result = persist_imported_trace(root_error, **case.writer_kwargs)
    client = KitaruClient()

    with pytest.raises(KitaruStateError, match="imported from an external trace"):
        client.executions.retry(result.execution_id)

    # The guard fires before any state transition, so the imported record
    # never leaves its terminal status.
    execution = client.executions.get(result.execution_id)
    assert execution.status.value == "failed"
