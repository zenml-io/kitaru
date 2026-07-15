"""Integration tests for imported traces persisted as ZenML executions."""

from datetime import UTC
from pathlib import Path

import pytest
from zenml.client import Client

import kitaru.imports._writer as imported_trace_writer
from kitaru.client import KitaruClient
from kitaru.imports import normalize_langfuse_observations, read_langfuse_jsonl
from kitaru.imports._writer import (
    ImportedTraceConflictError,
    ImportedTracePersistenceError,
    persist_imported_trace,
)

FIXTURE = Path(__file__).parent / "imports" / "fixtures" / "langfuse_observations.jsonl"


def _trace(trace_id: str = "trace-complete"):
    traces = normalize_langfuse_observations(
        read_langfuse_jsonl(FIXTURE), project_id="langfuse-project"
    )
    return next(trace for trace in traces if trace.source.trace_id == trace_id)


def test_persists_trace_as_visible_execution_without_running_source_code(
    primed_zenml: None,
) -> None:
    del primed_zenml
    trace = _trace()

    result = persist_imported_trace(trace, agent_name="support-agent")
    execution = KitaruClient().executions.get(result.execution_id)

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
    assert next(
        artifact for artifact in generation.artifacts if artifact.direction == "input"
    ).load() == [{"role": "user", "content": "synthetic"}]
    assert tool.status.value == "failed"
    assert tool.failure is not None
    assert tool.failure.message == "Synthetic error"


def test_imported_snapshot_contains_every_rendered_step_node(
    primed_zenml: None,
) -> None:
    del primed_zenml
    trace = _trace()
    result = persist_imported_trace(trace, agent_name="support-agent")
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


def test_child_first_source_order_renders_without_a_dag_error(
    primed_zenml: None,
) -> None:
    del primed_zenml
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

    result = persist_imported_trace(trace, agent_name="support-agent")
    client = Client()
    dag = client.zen_store.get_pipeline_run_dag(result.execution_id)  # type: ignore[attr-defined]
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


def test_exact_reimport_is_a_noop(primed_zenml: None) -> None:
    del primed_zenml
    trace = _trace()

    first = persist_imported_trace(trace, agent_name="support-agent")
    second = persist_imported_trace(trace, agent_name="support-agent")
    execution = KitaruClient().executions.get(first.execution_id)

    assert second.execution_id == first.execution_id
    assert second.created is False
    assert second.resumed is False
    assert len(execution.checkpoints) == len(trace.observations)


def test_explicit_stack_requires_its_artifact_store(primed_zenml: None) -> None:
    del primed_zenml

    with pytest.raises(
        ImportedTracePersistenceError, match="must be provided together"
    ):
        persist_imported_trace(
            _trace(),
            agent_name="support-agent",
            stack_id=Client().active_stack_model.id,
        )


def test_changed_source_content_conflicts(primed_zenml: None) -> None:
    del primed_zenml
    trace = _trace()
    persist_imported_trace(trace, agent_name="support-agent")
    changed = trace.model_copy(update={"content_digest": "f" * 64})
    pipeline_count = len(Client().list_pipelines(size=100).items)

    with pytest.raises(
        ImportedTraceConflictError, match="already imported"
    ) as exc_info:
        persist_imported_trace(changed, agent_name="different-agent")

    assert exc_info.value.existing_execution_id is not None
    assert "will not overwrite" in (exc_info.value.resolution or "")
    assert len(Client().list_pipelines(size=100).items) == pipeline_count


def test_different_agent_conflict_explains_how_to_reuse_or_regroup(
    primed_zenml: None,
) -> None:
    del primed_zenml
    trace = _trace()
    existing = persist_imported_trace(trace, agent_name="support-agent")

    with pytest.raises(
        ImportedTraceConflictError, match="agent_name='support-agent'"
    ) as exc_info:
        persist_imported_trace(trace, agent_name="renamed-agent")

    assert exc_info.value.existing_execution_id == existing.execution_id
    assert "agent_name='support-agent'" in (exc_info.value.resolution or "")
    assert "imports are not relabeled in place" in (exc_info.value.resolution or "")


def test_invalid_graph_is_not_persisted(primed_zenml: None) -> None:
    del primed_zenml
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

    with pytest.raises(ImportedTracePersistenceError, match="invalid graph"):
        persist_imported_trace(trace, agent_name="support-agent")


def test_incomplete_source_trace_is_not_persisted(primed_zenml: None) -> None:
    del primed_zenml
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

    with pytest.raises(ImportedTracePersistenceError, match="terminal status"):
        persist_imported_trace(trace, agent_name="support-agent")


def test_root_error_fails_execution_but_child_error_does_not(
    primed_zenml: None,
) -> None:
    del primed_zenml
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

    result = persist_imported_trace(root_error, agent_name="support-agent")
    execution = KitaruClient().executions.get(result.execution_id)

    assert execution.status.value == "failed"
    assert execution.failure is not None
    assert "failed root or agent observation" in execution.failure.message


def test_interrupted_import_resumes_missing_steps(
    primed_zenml: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    del primed_zenml
    trace = _trace()
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
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        persist_imported_trace(trace, agent_name="support-agent")

    monkeypatch.setattr(store_type, "create_run_step", create_run_step)
    resumed = persist_imported_trace(trace, agent_name="support-agent")
    execution = KitaruClient().executions.get(resumed.execution_id)

    assert resumed.created is False
    assert resumed.resumed is True
    assert execution.status.value == "completed"
    assert len(execution.checkpoints) == len(trace.observations)


def test_interrupted_metadata_write_is_repaired_on_resume(
    primed_zenml: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    del primed_zenml
    trace = _trace()
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
        persist_imported_trace(trace, agent_name="support-agent")

    monkeypatch.setattr(
        imported_trace_writer, "_write_step_metadata", write_step_metadata
    )
    resumed = persist_imported_trace(trace, agent_name="support-agent")
    execution = KitaruClient().executions.get(resumed.execution_id)
    generation = next(
        checkpoint
        for checkpoint in execution.checkpoints
        if checkpoint.metadata.get("kitaru_import_observation_id_v1") == "generation-1"
    )

    assert resumed.resumed is True
    assert generation.adapter == "langfuse_import"
    assert generation.llm_usage_records[0]["usage"]["total_tokens"] == 16
