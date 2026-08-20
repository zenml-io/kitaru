#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""MCP experiment export workspace confinement."""

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru.exports.models import ExportError
from kitaru.exports.operation import ExportOperationStateMachine, ExportReceipt
from kitaru.exports.plugin import ExporterMetadata, LoadedExporter
from kitaru.mcp import registry
from kitaru.mcp.errors import MCPToolError, map_exception
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import ToolResult
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
        content_policy={"omit": ["visible_reasoning"]},
        environment_policy={"mode": "runtime_only"},
        source_policy={"include": ["dist/main.js"], "exclude": ["cache"]},
        dry_run=True,
    )


def _loaded_exporter() -> LoadedExporter:
    metadata = ExporterMetadata(
        contract_version=1,
        distribution_name="kitaru-verifiers-exporter",
        distribution_version="1.2.3",
        format="verifiers-v1",
        target_version="0.3.0",
    )
    return LoadedExporter(implementation=cast(Any, object()), metadata=metadata)


async def test_export_rejects_source_outside_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(exports, "resolve_exporter", lambda _format: _loaded_exporter())
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
    loaded = _loaded_exporter()

    async def fake_export(
        _client: Any, request: Any, *, exporter: LoadedExporter
    ) -> ExportReceipt:
        assert exporter is loaded
        seen.append(request)
        return ExportReceipt(
            format=request.format,
            dry_run=True,
            experiment_id=str(request.experiment_id),
            cohort_version_id=str(request.cohort_version_id),
            agent_version_id=str(request.agent_version_id),
            session_count=1,
            task_count=1,
            evaluator_count=1,
            source_digest="0" * 64,
            destination=str(request.destination),
            exporter=loaded.provenance,
        )

    monkeypatch.setattr(exports, "resolve_exporter", lambda _format: loaded)
    monkeypatch.setattr(exports, "export_experiment", fake_export)
    state = MCPServerState(MCPSettings(workspace_roots=(root,)), cast(Any, object()))
    request = _request(source, root / "bundle")
    receipt = await exports.handle_experiment_export(state, request)
    assert seen[0].experiment_id == request.experiment_id
    assert seen[0].content_policy == request.content_policy
    assert seen[0].environment_policy == request.environment_policy
    assert seen[0].source_policy == request.source_policy
    assert receipt.operation == "experiment_export"


async def test_export_rejects_destination_with_parent_outside_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    source = root / "source"
    source.mkdir(parents=True)
    monkeypatch.setattr(exports, "resolve_exporter", lambda _format: _loaded_exporter())
    state = MCPServerState(MCPSettings(workspace_roots=(root,)), cast(Any, object()))

    with pytest.raises(MCPToolError, match="destination parent") as raised:
        await exports.handle_experiment_export(state, _request(source, root))

    assert raised.value.code == "path_not_allowed"


def test_export_request_validation_is_invalid_arguments() -> None:
    with pytest.raises(ValidationError) as raised:
        ExperimentExportRequest(
            experiment_id=uuid.uuid4(),
            cohort_version_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            format="verifiers-v1",
            source_root="/workspace/source",
            destination="/workspace/bundle",
            primary_reward="quality:correctness:score",
            required_environment_names=["INVALID-NAME"],
        )

    assert map_exception(raised.value).code == "invalid_arguments"


async def test_export_registry_maps_malformed_receipt_to_internal_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    source = root / "source"
    source.mkdir(parents=True)
    loaded = _loaded_exporter()

    async def fake_export(
        _client: Any,
        _request: Any,
        *,
        exporter: LoadedExporter,
        operation: ExportOperationStateMachine,
    ) -> dict[str, str]:
        assert exporter is loaded
        assert operation.state.value == "staging"
        return {"format": "verifiers-v1"}

    monkeypatch.setattr(exports, "resolve_exporter", lambda _format: loaded)
    monkeypatch.setattr(exports, "export_experiment", fake_export)

    class RoutingState:
        settings = MCPSettings(workspace_roots=(root,))
        client = object()

        async def execute_export(self, operation: Any) -> object:
            return await operation(ExportOperationStateMachine())

    state = RoutingState()
    context = SimpleNamespace(request_context=SimpleNamespace(lifespan_context=state))
    result = await registry._invoke(
        cast(Any, context),
        _request(source, root / "bundle"),
        registry.ExperimentExportResult,
        exports.handle_experiment_export,
        export_operation=True,
    )

    assert result.is_error is True
    assert result.structured_content is not None
    assert result.structured_content["error"]["code"] == "internal_error"


async def test_export_maps_missing_plugin_before_local_path_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()

    def reject(_format: str) -> LoadedExporter:
        raise ExportError(
            "exporter_not_installed",
            "Export format verifiers-v1 requires kitaru-verifiers-exporter.",
        )

    monkeypatch.setattr(exports, "resolve_exporter", reject)
    state = MCPServerState(MCPSettings(workspace_roots=(root,)), cast(Any, object()))

    with pytest.raises(MCPToolError) as raised:
        await exports.handle_experiment_export(
            state,
            _request(root / "does-not-exist", root / "bundle"),
        )

    assert raised.value.code == "invalid_configuration"
    assert raised.value.details == {"export_code": "exporter_not_installed"}
    assert raised.value.recovery is not None
    assert "same project environment" in raised.value.recovery
    assert "restart" in raised.value.recovery


async def test_export_registry_path_does_not_use_generic_execution() -> None:
    calls: list[str] = []

    class RoutingState:
        async def execute(self, _operation: Any) -> object:
            raise AssertionError("export used generic MCP execution")

        async def execute_export(self, operation: Any) -> object:
            calls.append("export")
            return await operation(ExportOperationStateMachine())

    async def handler(
        _state: Any,
        _request: object,
        *,
        operation: ExportOperationStateMachine,
    ) -> dict[str, str]:
        assert operation.state.value == "staging"
        return {"result": "ok"}

    context = SimpleNamespace(
        request_context=SimpleNamespace(lifespan_context=RoutingState())
    )
    result = await registry._invoke(
        cast(Any, context),
        object(),
        ToolResult,
        cast(Any, handler),
        export_operation=True,
    )

    assert calls == ["export"]
    assert result.is_error is False
