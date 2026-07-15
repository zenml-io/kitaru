"""Tests for the public Langfuse batch-import service."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from kitaru.errors import KitaruUsageError
from kitaru.imports import (
    ImportedExecutionResult,
    ImportedTraceConflictError,
    ImportOutcomeStatus,
    import_langfuse_jsonl,
)

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def _client(*, remote_server: bool = False) -> MagicMock:
    client = MagicMock()
    client.active_project = SimpleNamespace(id=UUID(int=1), name="default")
    stack = SimpleNamespace(
        id=UUID(int=2),
        name="active-stack",
        components={
            "artifact_store": [
                SimpleNamespace(flavor_name="local", name="local-artifacts")
            ]
        },
    )
    client.active_stack_model = stack
    client.get_stack.return_value = stack
    client.zen_store.is_local_store.return_value = not remote_server
    return client


@pytest.fixture(autouse=True)
def _stub_artifact_store_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: SimpleNamespace(
            config=SimpleNamespace(is_local=component.flavor_name == "local")
        ),
    )


def test_dry_run_plans_without_persisting(monkeypatch: pytest.MonkeyPatch) -> None:
    planned: list[str] = []

    def plan(trace, *, agent_name, client, stack_id):
        del agent_name, client, stack_id
        planned.append(trace.source.trace_id)
        return "create"

    def fail_persist(*args, **kwargs):
        del args, kwargs
        raise AssertionError("dry-run must not persist")

    monkeypatch.setattr("kitaru.imports._service.plan_imported_trace", plan)
    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", fail_persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        dry_run=True,
        client=_client(),
    )

    assert planned == ["trace-complete"]
    assert result.dry_run is True
    assert result.total_trace_count == 3
    assert result.selected_trace_count == 1
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE
    assert result.outcomes[0].execution_id is None
    assert result.flow_name.startswith("imported_support-agent__langfuse_v4_")
    assert "input and output payloads" in result.storage_warning
    assert result.stack_name == "active-stack"
    assert result.stack_was_explicit is False
    assert "No stack was specified" in result.storage_warning


def test_explicit_stack_is_resolved_once_and_forwarded(monkeypatch) -> None:
    client = _client()
    selected = SimpleNamespace(
        id=UUID(int=3),
        name="cloud-stack",
        components={
            "artifact_store": [SimpleNamespace(flavor_name="s3", name="artifacts")]
        },
    )
    client.get_stack.return_value = selected
    artifact_store = SimpleNamespace(config=SimpleNamespace(is_local=False))
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: artifact_store,
    )
    persisted = MagicMock(
        return_value=ImportedExecutionResult(
            execution_id="execution-one",
            created=True,
            resumed=False,
            observation_count=6,
        )
    )
    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persisted)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        stack="cloud-stack",
        dry_run=False,
        confirm_data_storage=True,
        client=client,
    )

    client.get_stack.assert_called_once_with(
        "cloud-stack", allow_name_prefix_match=False, hydrate=True
    )
    client.activate_stack.assert_not_called()
    assert persisted.call_args.kwargs["stack_id"] == selected.id
    assert persisted.call_args.kwargs["artifact_store"] is artifact_store
    assert result.stack_name == "cloud-stack"
    assert result.artifact_store_type == "s3"
    assert result.artifact_store_is_local is False
    assert result.storage_warning.startswith("Writing imported traces")
    assert "No stack was specified" not in result.storage_warning


def test_remote_server_with_local_artifact_store_warns(monkeypatch) -> None:
    client = _client(remote_server=True)
    monkeypatch.setattr(
        "kitaru.imports._service._load_artifact_store",
        lambda component: SimpleNamespace(config=SimpleNamespace(is_local=True)),
    )
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        client=client,
    )

    assert result.artifact_store_is_local is True
    assert result.artifact_store_is_remotely_accessible is False
    assert "execution metadata will be stored on the remote server" in (
        result.storage_warning
    )
    assert "artifact payloads will remain on this machine" in result.storage_warning


def test_write_requires_explicit_data_storage_confirmation() -> None:
    with pytest.raises(KitaruUsageError, match="confirm_data_storage=True"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            dry_run=False,
            client=_client(),
        )


def test_trace_selection_reports_missing_ids() -> None:
    with pytest.raises(KitaruUsageError, match="missing-trace"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            trace_ids=["trace-complete", "missing-trace"],
            client=_client(),
        )


def test_fragmented_trace_is_rejected_by_default() -> None:
    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-fragmented"],
        client=_client(),
    )

    assert result.outcomes[0].status is ImportOutcomeStatus.REJECTED
    assert "fragmented" in (result.outcomes[0].reason or "")


def test_incomplete_trace_is_reported_as_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.read_langfuse_jsonl",
        lambda path: iter(
            [
                {
                    "id": "unfinished",
                    "traceId": "trace-incomplete",
                    "type": "AGENT",
                    "startTime": "2026-07-15T10:00:00Z",
                }
            ]
        ),
    )

    result = import_langfuse_jsonl(
        "unused.jsonl",
        source_project_id="source-project",
        agent_name="support-agent",
        client=_client(),
    )

    assert result.outcomes[0].status is ImportOutcomeStatus.REJECTED
    assert "terminal status" in (result.outcomes[0].reason or "")


def test_limit_applies_after_explicit_trace_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-root-omitted", "trace-complete"],
        limit=1,
        client=_client(),
    )

    assert [outcome.trace_id for outcome in result.outcomes] == ["trace-root-omitted"]


def test_exact_selection_ignores_invalid_unselected_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "id": "good-observation",
            "traceId": "trace-good",
            "type": "SPAN",
            "startTime": "2026-07-15T10:00:00Z",
            "endTime": "2026-07-15T10:00:01Z",
        },
        {
            "id": "bad-observation",
            "traceId": "trace-bad",
            "type": "UNSUPPORTED",
            "startTime": "not-a-timestamp",
        },
    ]
    monkeypatch.setattr(
        "kitaru.imports._service.read_langfuse_jsonl", lambda path: iter(rows)
    )
    monkeypatch.setattr(
        "kitaru.imports._service.plan_imported_trace",
        lambda *args, **kwargs: "create",
    )

    result = import_langfuse_jsonl(
        "unused.jsonl",
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-good"],
        client=_client(),
    )

    assert result.total_trace_count == 2
    assert [outcome.trace_id for outcome in result.outcomes] == ["trace-good"]
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE


def test_actual_import_collects_per_trace_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def persist(trace, *, agent_name, client, stack_id, artifact_store):
        del agent_name, client, stack_id, artifact_store
        if trace.source.trace_id == "trace-root-omitted":
            return ImportedExecutionResult(
                execution_id="execution-existing",
                created=False,
                resumed=False,
                observation_count=len(trace.observations),
            )
        if trace.source.trace_id == "trace-fragmented":
            raise ImportedTraceConflictError("synthetic conflict")
        return ImportedExecutionResult(
            execution_id="execution-created",
            created=True,
            resumed=False,
            observation_count=len(trace.observations),
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", persist)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=True,
        max_workers=2,
        client=_client(),
    )

    assert [outcome.status for outcome in result.outcomes] == [
        ImportOutcomeStatus.CREATED,
        ImportOutcomeStatus.UNCHANGED,
        ImportOutcomeStatus.CONFLICT,
    ]
    assert result.counts == {"conflict": 1, "created": 1, "unchanged": 1}


def test_conflict_outcome_preserves_existing_execution_and_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def conflict(*args, **kwargs):
        del args, kwargs
        raise ImportedTraceConflictError(
            "already imported with another label",
            existing_execution_id="execution-existing",
            resolution="Retry with the original agent name.",
        )

    monkeypatch.setattr("kitaru.imports._service.persist_imported_trace", conflict)

    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
        client=_client(),
    )

    outcome = result.outcomes[0]
    assert outcome.status is ImportOutcomeStatus.CONFLICT
    assert outcome.existing_execution_id == "execution-existing"
    assert outcome.reason == "already imported with another label"
    assert outcome.resolution == "Retry with the original agent name."


def test_rejects_invalid_options() -> None:
    with pytest.raises(KitaruUsageError, match="limit"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            limit=0,
            client=_client(),
        )
    for workers in (0, 9):
        with pytest.raises(KitaruUsageError, match="max_workers"):
            import_langfuse_jsonl(
                FIXTURE,
                source_project_id="source-project",
                agent_name="support-agent",
                max_workers=workers,
                client=_client(),
            )
    with pytest.raises(KitaruUsageError, match="source_project_id"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id=" ",
            agent_name="support-agent",
            client=_client(),
        )
    with pytest.raises(KitaruUsageError, match="agent_name"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name=" ",
            client=_client(),
        )
