"""End-to-end tests for the public imported-trace SDK namespace."""

import subprocess
from pathlib import Path

import pytest
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from zenml.client import Client

from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.client import KitaruClient
from kitaru.imports import (
    ImportOutcomeStatus,
    ReplayReadinessStatus,
    SourceAttributionStatus,
)

FIXTURE = Path(__file__).parent / "imports" / "fixtures" / "langfuse_observations.jsonl"
REGISTERED_IMPORT_AGENT = KitaruAgent(
    Agent(TestModel(), name="support-agent", output_type=str)
)


def test_sdk_previews_then_imports_one_trace(
    primed_zenml: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del primed_zenml
    monkeypatch.setattr(
        "kitaru._agent_registration._module_path_within_repository",
        lambda *_args, **_kwargs: True,
    )
    repo_root = Path(Client().root)
    subprocess.run(["git", "init", "-q", str(repo_root)], check=True)
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "config", "user.name", "Test"],
        check=True,
    )
    (repo_root / "registered_agent.txt").write_text("registered\\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repo_root), "add", "registered_agent.txt"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-qm", "register test agent"],
        check=True,
    )
    registration = REGISTERED_IMPORT_AGENT.register(
        label="prod",
        entrypoint=f"{__name__}:REGISTERED_IMPORT_AGENT",
    )
    client = KitaruClient()
    preview = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent=registration.agent.agent_id,
        version="prod",
        trace_ids=["trace-complete"],
    )

    assert preview.outcomes[0].status is ImportOutcomeStatus.WOULD_CREATE
    assert preview.agent_id == registration.agent.agent_id
    assert preview.agent_version_id == registration.agent_version.agent_version_id
    assert preview.pipeline_id == registration.agent_version.pipeline_id
    assert preview.flow_name == registration.agent_version.pipeline_name
    assert preview.outcomes[0].raw_evidence_artifact_id is None
    assert preview.outcomes[0].raw_evidence_schema_version is None
    assert preview.outcomes[0].replay_bundle_artifact_id is None
    assert preview.outcomes[0].replay_bundle_schema_version is None
    assert preview.requested_alias == "prod"
    assert preview.attribution_counts == {"caller_attributed": 1}
    assert preview.outcomes[0].attribution is not None
    assert (
        preview.outcomes[0].attribution.status
        is SourceAttributionStatus.CALLER_ATTRIBUTED
    )
    assert preview.outcomes[0].raw_evidence_digest is not None
    assert preview.outcomes[0].replay_bundle_digest is not None
    assert preview.outcomes[0].replay_readiness is not None
    assert client.executions.list() == []

    imported = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent=registration.agent.agent_id,
        version="prod",
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
    assert execution.import_info is not None
    assert execution.import_info.source_agent_version_id == (
        registration.agent_version.agent_version_id
    )
    assert execution.import_info.source_agent_version_label == "prod"
    assert execution.import_info.attribution.status.value == "caller_attributed"
    assert execution.import_info.raw_evidence is not None
    assert execution.import_info.raw_evidence.sha256 == outcome.raw_evidence_digest
    assert (
        execution.import_info.raw_evidence.artifact_id
        == outcome.raw_evidence_artifact_id
    )
    assert outcome.raw_evidence_schema_version == 1
    assert execution.import_info.replay_bundle is not None
    assert execution.import_info.replay_bundle.sha256 == outcome.replay_bundle_digest
    assert (
        execution.import_info.replay_bundle.artifact_id
        == outcome.replay_bundle_artifact_id
    )
    assert outcome.replay_bundle_schema_version == 1
    assert execution.import_info.replay_readiness is not None
    assert (
        execution.import_info.replay_readiness.root_input_candidate_rerun.status
        is ReplayReadinessStatus.READY
    )

    repeated = client.imports.langfuse(
        FIXTURE,
        source_project_id="langfuse-project",
        agent=registration.agent.agent_id,
        version="prod",
        trace_ids=["trace-complete"],
        dry_run=False,
        confirm_data_storage=True,
    )
    assert repeated.outcomes[0].status is ImportOutcomeStatus.UNCHANGED
    assert repeated.outcomes[0].execution_id == outcome.execution_id
    assert (
        repeated.outcomes[0].raw_evidence_artifact_id
        == outcome.raw_evidence_artifact_id
    )
    assert repeated.outcomes[0].raw_evidence_schema_version == 1
    assert (
        repeated.outcomes[0].replay_bundle_artifact_id
        == outcome.replay_bundle_artifact_id
    )
    assert repeated.outcomes[0].replay_bundle_schema_version == 1
