"""End-to-end tests for the public imported-trace SDK namespace."""

from pathlib import Path

from kitaru.client import KitaruClient
from kitaru.imports import ImportOutcomeStatus

FIXTURE = Path(__file__).parent / "imports" / "fixtures" / "langfuse_observations.jsonl"


def test_sdk_previews_then_imports_one_trace(primed_zenml: None) -> None:
    del primed_zenml
    client = KitaruClient()
    preview = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
    )

    assert preview.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE
    assert client.executions.list() == []

    imported = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
    )
    outcome = imported.outcomes[0]

    assert outcome.status is ImportOutcomeStatus.CREATED
    assert outcome.execution_id is not None
    execution = client.executions.get(outcome.execution_id)
    assert len(execution.checkpoints) == 6
    assert execution.metadata["kitaru_import_source_trace_id_v1"] == "trace-complete"

    repeated = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent_name="support-agent",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
    )
    assert repeated.outcomes[0].status is ImportOutcomeStatus.UNCHANGED
    assert repeated.outcomes[0].execution_id == outcome.execution_id
