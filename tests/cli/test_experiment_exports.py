#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""CLI experiment export contracts."""

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from kitaru.cli import experiment_exports
from kitaru.exports.operation import ExportReceipt


async def test_cli_resolves_references_before_shared_operation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    experiment_id = uuid.uuid4()
    cohort_version_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    seen = []

    async def resolve_asset(*_args: Any) -> Any:
        return SimpleNamespace(id=experiment_id)

    async def get_cohort_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=cohort_version_id)

    async def get_agent_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=agent_version_id)

    async def export(_client: Any, request: Any) -> ExportReceipt:
        seen.append(request)
        return ExportReceipt(
            format=request.format,
            dry_run=True,
            experiment_id=str(request.experiment_id),
            cohort_version_id=str(request.cohort_version_id),
            agent_version_id=str(request.agent_version_id),
            session_count=2,
            evaluator_count=1,
            source_digest="0" * 64,
            destination=str(request.destination),
        )

    monkeypatch.setattr(experiment_exports, "resolve_asset", resolve_asset)
    monkeypatch.setattr(experiment_exports, "get_cohort_version", get_cohort_version)
    monkeypatch.setattr(experiment_exports, "get_agent_version", get_agent_version)
    monkeypatch.setattr(experiment_exports, "export_experiment", export)
    result = await experiment_exports.export_experiment_command(
        SimpleNamespace(experiments=object()),
        "experiment",
        cohort_version="cohort@1",
        agent="agent@1",
        format="verifiers-v1",
        source_root=tmp_path,
        destination=tmp_path / "bundle",
        primary_reward="quality:correctness:score",
        required_env=["TOKEN"],
        trace_format=None,
        trace_path=None,
        archive=False,
        dry_run=True,
    )
    assert seen[0].cohort_version_id == cohort_version_id
    assert seen[0].agent_version_id == agent_version_id
    assert result.item["dry_run"] is True
