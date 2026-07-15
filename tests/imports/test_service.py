"""Tests for the public Langfuse batch-import service."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from kitaru.errors import KitaruUsageError
from kitaru.imports import (
    ImportedExecutionResult,
    ImportedTraceConflictError,
    ImportOutcomeStatus,
    import_langfuse_jsonl,
)

FIXTURE = Path(__file__).parent / "fixtures" / "langfuse_observations.jsonl"


def test_dry_run_plans_without_persisting(monkeypatch: pytest.MonkeyPatch) -> None:
    planned: list[str] = []

    def plan(trace, *, agent_name, client):
        del agent_name, client
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
        client=MagicMock(),
    )

    assert planned == ["trace-complete"]
    assert result.dry_run is True
    assert result.total_trace_count == 3
    assert result.selected_trace_count == 1
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE
    assert result.outcomes[0].execution_id is None
    assert result.flow_name.startswith("imported_support-agent__langfuse_v1_")
    assert "input and output payloads" in result.storage_warning


def test_write_requires_explicit_data_storage_confirmation() -> None:
    with pytest.raises(KitaruUsageError, match="confirm_data_storage=True"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            dry_run=False,
            client=MagicMock(),
        )


def test_trace_selection_reports_missing_ids() -> None:
    with pytest.raises(KitaruUsageError, match="missing-trace"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            trace_ids=["trace-complete", "missing-trace"],
            client=MagicMock(),
        )


def test_fragmented_trace_is_rejected_by_default() -> None:
    result = import_langfuse_jsonl(
        FIXTURE,
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-fragmented"],
        client=MagicMock(),
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
        client=MagicMock(),
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
        client=MagicMock(),
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
        client=MagicMock(),
    )

    assert result.total_trace_count == 2
    assert [outcome.trace_id for outcome in result.outcomes] == ["trace-good"]
    assert result.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE


def test_actual_import_collects_per_trace_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def persist(trace, *, agent_name, client):
        del agent_name, client
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
        client=MagicMock(),
    )

    assert [outcome.status for outcome in result.outcomes] == [
        ImportOutcomeStatus.CREATED,
        ImportOutcomeStatus.UNCHANGED,
        ImportOutcomeStatus.CONFLICT,
    ]
    assert result.counts == {"conflict": 1, "created": 1, "unchanged": 1}


def test_rejects_invalid_options() -> None:
    with pytest.raises(KitaruUsageError, match="limit"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name="support-agent",
            limit=0,
            client=MagicMock(),
        )
    for workers in (0, 9):
        with pytest.raises(KitaruUsageError, match="max_workers"):
            import_langfuse_jsonl(
                FIXTURE,
                source_project_id="source-project",
                agent_name="support-agent",
                max_workers=workers,
                client=MagicMock(),
            )
    with pytest.raises(KitaruUsageError, match="source_project_id"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id=" ",
            agent_name="support-agent",
            client=MagicMock(),
        )
    with pytest.raises(KitaruUsageError, match="agent_name"):
        import_langfuse_jsonl(
            FIXTURE,
            source_project_id="source-project",
            agent_name=" ",
            client=MagicMock(),
        )
