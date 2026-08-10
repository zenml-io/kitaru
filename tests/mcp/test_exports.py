#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""MCP experiment export workspace confinement."""

import uuid
from pathlib import Path
from typing import Any, cast

import pytest

from kitaru._exports.operation import ExportReceipt
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.exports import ExperimentExportRequest
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools import exports


def _request(source: Path, destination: Path) -> ExperimentExportRequest:
    return ExperimentExportRequest(
        experiment_id=uuid.uuid4(),
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        format="verifiers-v1",
        source_root=str(source),
        destination=str(destination),
        primary_reward="quality:correctness:score",
        dry_run=True,
    )


async def test_export_rejects_source_outside_workspace(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    state = MCPServerState(MCPSettings(workspace_roots=(root,)), cast(Any, object()))
    with pytest.raises(MCPToolError, match="outside"):
        await exports.handle_experiment_export(
            state, _request(outside, root / "bundle")
        )


async def test_export_uses_exact_ids_and_workspace_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    source = root / "source"
    source.mkdir(parents=True)
    seen = []

    async def fake_export(_client: Any, request: Any) -> ExportReceipt:
        seen.append(request)
        return ExportReceipt(
            format=request.format,
            dry_run=True,
            experiment_id=str(request.experiment_id),
            cohort_version_id=str(request.cohort_version_id),
            agent_version_id=str(request.agent_version_id),
            session_count=1,
            evaluator_count=1,
            source_digest="0" * 64,
            destination=str(request.destination),
        )

    monkeypatch.setattr(exports, "export_experiment", fake_export)
    state = MCPServerState(MCPSettings(workspace_roots=(root,)), cast(Any, object()))
    request = _request(source, root / "bundle")
    receipt = await exports.handle_experiment_export(state, request)
    assert seen[0].experiment_id == request.experiment_id
    assert receipt.operation == "experiment_export"
