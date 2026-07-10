"""Tests for Kitaru MCP server tools."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

import kitaru._interface_executions as execution_interface
import kitaru._interface_stacks as stack_interface
from kitaru._flow_loading import _load_flow_target as _load_shared_flow_target
from kitaru.client import ExecutionStatistics, ExecutionStatisticsGroup, ExecutionStatus
from kitaru.config import (
    ActiveEnvironmentVariable,
    AzureMLStackSpec,
    CloudProvider,
    ImageSettings,
    KubernetesStackSpec,
    ModalStackSpec,
    ProjectInfo,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackInfo,
    StackType,
    VertexStackSpec,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.inspection import RuntimeSnapshot
from kitaru.mcp.server import (
    get_execution_logs,
    kitaru_artifacts_get,
    kitaru_artifacts_list,
    kitaru_clean_preview,
    kitaru_deployments_delete,
    kitaru_deployments_deploy,
    kitaru_deployments_get,
    kitaru_deployments_invoke,
    kitaru_deployments_list,
    kitaru_deployments_tag,
    kitaru_deployments_untag,
    kitaru_executions_cancel,
    kitaru_executions_diff_matrix,
    kitaru_executions_get,
    kitaru_executions_input,
    kitaru_executions_latest,
    kitaru_executions_list,
    kitaru_executions_replay,
    kitaru_executions_retry,
    kitaru_executions_run,
    kitaru_executions_statistics,
    kitaru_info,
    kitaru_projects_current,
    kitaru_projects_list,
    kitaru_projects_show,
    kitaru_projects_use,
    kitaru_secrets_create,
    kitaru_secrets_list,
    kitaru_stacks_list,
    kitaru_start_local_server,
    kitaru_status,
    kitaru_stop_local_server,
    manage_stack,
    mcp,
    tracked_mcp_tool,
)
from kitaru.secrets import SecretSummary

_REGISTERED_MCP_TOOL_FUNCTIONS = (
    kitaru_executions_list,
    kitaru_executions_statistics,
    kitaru_executions_get,
    kitaru_executions_latest,
    get_execution_logs,
    kitaru_executions_run,
    kitaru_deployments_deploy,
    kitaru_deployments_invoke,
    kitaru_deployments_list,
    kitaru_deployments_get,
    kitaru_deployments_delete,
    kitaru_deployments_tag,
    kitaru_deployments_untag,
    kitaru_executions_cancel,
    kitaru_executions_input,
    kitaru_executions_retry,
    kitaru_executions_replay,
    kitaru_executions_diff_matrix,
    kitaru_secrets_create,
    kitaru_secrets_list,
    kitaru_artifacts_list,
    kitaru_artifacts_get,
    kitaru_start_local_server,
    kitaru_stop_local_server,
    kitaru_status,
    kitaru_projects_list,
    kitaru_projects_current,
    kitaru_projects_show,
    kitaru_projects_use,
    kitaru_stacks_list,
    manage_stack,
    kitaru_info,
    kitaru_clean_preview,
)


def _write_flow_target_module(path: Path, *, marker: str) -> None:
    """Create a minimal flow target module for direct loader tests."""
    path.write_text(
        "class _FakeFlow:\n"
        f"    marker = {marker!r}\n"
        "    def run(self, *args, **kwargs):\n"
        "        return None\n\n"
        "demo_flow = _FakeFlow()\n",
        encoding="utf-8",
    )


def _load_mcp_flow_target(target: str) -> Any:
    """Load a flow target with the MCP-specific module-name prefix."""
    return _load_shared_flow_target(
        target,
        module_name_prefix="_kitaru_mcp_run_target_",
    )


def _mcp_tool_schemas_by_name() -> dict[str, dict[str, Any]]:
    """Return FastMCP input schemas from the in-process registry."""
    tools = asyncio.run(mcp.list_tools())
    return {tool.name: tool.inputSchema for tool in tools}


def test_fastmcp_registers_public_tools_with_expected_input_schemas() -> None:
    tool_schemas = _mcp_tool_schemas_by_name()
    expected_names = {func.__name__ for func in _REGISTERED_MCP_TOOL_FUNCTIONS}

    assert expected_names <= set(tool_schemas)
    assert "_wrapper" not in tool_schemas

    input_schema = tool_schemas["kitaru_executions_input"]
    assert set(input_schema["properties"]) == {"exec_id", "wait", "value"}
    assert input_schema["required"] == ["exec_id", "wait", "value"]

    invoke_schema = tool_schemas["kitaru_deployments_invoke"]
    assert set(invoke_schema["properties"]) == {
        "flow",
        "version",
        "tag",
        "inputs",
    }
    assert invoke_schema["required"] == ["flow"]
    assert invoke_schema["properties"]["version"]["default"] is None
    assert invoke_schema["properties"]["tag"]["default"] is None
    assert invoke_schema["properties"]["inputs"]["default"] is None

    stack_schema = tool_schemas["manage_stack"]
    assert {"action", "name"}.issubset(stack_schema["properties"])
    assert stack_schema["required"] == ["action", "name"]
    assert stack_schema["properties"]["action"]["enum"] == ["create", "delete"]
    assert stack_schema["properties"]["stack_type"]["default"] == "local"
    assert stack_schema["properties"]["activate"]["default"] is True
    assert stack_schema["properties"]["recursive"]["default"] is False
    assert stack_schema["properties"]["force"]["default"] is False
    assert stack_schema["properties"]["async_mode"]["default"] is False
    assert stack_schema["properties"]["verify"]["default"] is True
    assert "extra" in stack_schema["properties"]

    replay_schema = tool_schemas["kitaru_executions_replay"]
    replay_properties = set(replay_schema["properties"])
    assert replay_properties == {
        "exec_ids",
        "at",
        "flow_overrides",
        "checkpoint_overrides",
        "invocation_overrides",
        "skip",
        "tag",
        "wait",
        "on_error",
    }
    assert replay_schema["required"] == ["exec_ids", "at"]
    assert replay_schema["properties"]["on_error"].get("default") is None
    on_error_variants = replay_schema["properties"]["on_error"]["anyOf"]
    assert {"type": "null"} in on_error_variants
    assert any(
        variant.get("type") == "string"
        and set(variant.get("enum", [])) == {"fail", "collect"}
        for variant in on_error_variants
    )
    assert "kitaru_executions_replay_many" not in tool_schemas
    assert "input" not in replay_properties
    assert "output" not in replay_properties
    assert "tool" not in replay_properties
    assert "llm_model" not in replay_properties
    assert "flow_inputs" not in replay_properties

    projects_list_schema = tool_schemas["kitaru_projects_list"]
    assert projects_list_schema.get("required", []) == []
    assert projects_list_schema.get("properties", {}) == {}

    projects_current_schema = tool_schemas["kitaru_projects_current"]
    assert projects_current_schema.get("required", []) == []
    assert projects_current_schema.get("properties", {}) == {}

    projects_show_schema = tool_schemas["kitaru_projects_show"]
    assert set(projects_show_schema["properties"]) == {"name_or_id"}
    assert projects_show_schema["required"] == ["name_or_id"]

    projects_use_schema = tool_schemas["kitaru_projects_use"]
    assert set(projects_use_schema["properties"]) == {"name_or_id"}
    assert projects_use_schema["required"] == ["name_or_id"]

    assert "kitaru_projects_create" not in tool_schemas
    assert "kitaru_projects_delete" not in tool_schemas
    assert "kitaru_executions_diff_matrix" in tool_schemas
    assert "kitaru_executions_diff_cohort" not in tool_schemas


def test_secrets_list_schema_has_optional_pagination_parameters() -> None:
    schema = _mcp_tool_schemas_by_name()["kitaru_secrets_list"]

    assert set(schema["properties"]) == {"page", "size"}
    assert schema["properties"]["page"]["type"] == "integer"
    assert schema["properties"]["page"]["default"] == 1
    assert schema["properties"]["size"]["type"] == "integer"
    assert schema["properties"]["size"]["default"] == 20
    assert schema.get("required", []) == []


def test_load_flow_target_supports_module_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = f"temp_mcp_flow_module_{tmp_path.name.replace('-', '_')}"
    module_path = tmp_path / f"{module_name}.py"
    _write_flow_target_module(module_path, marker="module")
    monkeypatch.syspath_prepend(str(tmp_path))

    flow_target = _load_mcp_flow_target(f"{module_name}:demo_flow")

    assert flow_target.marker == "module"


def test_load_flow_target_supports_python_file_paths(tmp_path: Path) -> None:
    module_path = tmp_path / "demo_flow.py"
    _write_flow_target_module(module_path, marker="file")

    flow_target = _load_mcp_flow_target(f"{module_path}:demo_flow")

    assert flow_target.marker == "file"


def test_load_flow_target_delegates_to_shared_module_loader() -> None:
    fake_flow = SimpleNamespace(
        marker="patched",
        run=MagicMock(),
    )
    fake_module = SimpleNamespace(demo_flow=fake_flow)

    with patch(
        "kitaru._flow_loading._load_module_from_python_path",
        return_value=fake_module,
    ) as mock_loader:
        flow_target = _load_mcp_flow_target("/tmp/demo_flow.py:demo_flow")

    mock_loader.assert_called_once_with(
        "/tmp/demo_flow.py", module_name_prefix="_kitaru_mcp_run_target_"
    )
    assert flow_target is fake_flow


def test_load_flow_target_reports_missing_module() -> None:
    with pytest.raises(ValueError, match="Unable to import flow module") as exc_info:
        _load_mcp_flow_target("definitely_missing_mcp_flow_module:demo_flow")

    assert "definitely_missing_mcp_flow_module" in str(exc_info.value)


def test_load_flow_target_reports_missing_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = f"temp_mcp_missing_attr_{tmp_path.name.replace('-', '_')}"
    module_path = tmp_path / f"{module_name}.py"
    module_path.write_text("other_name = object()\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ValueError, match="has no attribute `demo_flow`"):
        _load_mcp_flow_target(f"{module_name}:demo_flow")


def test_load_flow_target_rejects_invalid_target_format() -> None:
    with pytest.raises(
        ValueError, match="must use `<module_or_file>:<flow_name>` format"
    ):
        _load_mcp_flow_target("content_pipeline")


def test_executions_list_calls_client_and_serializes(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """List tool should call client list API and return structured summaries."""
    mock_kitaru_client.executions.list.return_value = [sample_execution]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_list(
            status="waiting",
            flow="content_pipeline",
            limit=5,
        )

    mock_kitaru_client.executions.list.assert_called_once_with(
        flow="content_pipeline",
        status="waiting",
        limit=5,
    )
    assert payload[0]["exec_id"] == sample_execution.exec_id
    assert payload[0]["pending_wait"]["name"] == "approve_draft"


def test_executions_list_delegates_to_inspection_serializer(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    mock_kitaru_client.executions.list.return_value = [sample_execution]

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru.inspection.serialize_execution_summary",
            return_value={"exec_id": "delegated", "source": "inspection"},
        ) as mock_serialize,
    ):
        payload = kitaru_executions_list(limit=1)

    mock_serialize.assert_called_once_with(sample_execution)
    assert payload == [{"exec_id": "delegated", "source": "inspection"}]


def test_executions_list_delegates_filtering_to_shared_interface(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.list_executions_filtered",
            return_value=[sample_execution],
        ) as mock_list,
    ):
        payload = kitaru_executions_list(
            status="waiting",
            flow="content_pipeline",
            stack="prod",
            limit=5,
        )

    mock_list.assert_called_once_with(
        mock_kitaru_client,
        flow="content_pipeline",
        status="waiting",
        stack="prod",
        limit=5,
    )
    assert payload[0]["exec_id"] == sample_execution.exec_id


def test_executions_statistics_calls_client_and_serializes(
    mock_kitaru_client: MagicMock,
) -> None:
    """Statistics tool should delegate to the SDK and return serialized counts."""
    statistics = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(keys={"status": "completed"}, execution_count=12),
            ExecutionStatisticsGroup(
                keys={"status": "failed"},
                execution_count=2,
                metrics={"duration_avg": 3.5},
            ),
        ],
        truncated=False,
    )
    mock_kitaru_client.executions.statistics.return_value = statistics

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_statistics(
            group_by=["status"],
            flow="content_pipeline",
            status="completed",
            stack="prod",
            tags=["nightly", "customer-facing"],
            metrics=["duration_avg:duration:avg"],
            max_groups=25,
        )

    mock_kitaru_client.executions.statistics.assert_called_once_with(
        group_by=["status"],
        metrics=["duration_avg:duration:avg"],
        flow="content_pipeline",
        status="completed",
        stack="prod",
        tags=["nightly", "customer-facing"],
        max_groups=25,
    )
    assert payload == {
        "groups": [
            {
                "keys": {"status": "completed"},
                "execution_count": 12,
                "metrics": {},
            },
            {
                "keys": {"status": "failed"},
                "execution_count": 2,
                "metrics": {"duration_avg": 3.5},
            },
        ],
        "truncated": False,
        "group_count": 2,
    }


def test_executions_statistics_forwards_llm_shortcuts_and_serializes(
    mock_kitaru_client: MagicMock,
) -> None:
    """MCP statistics should pass LLM shortcut metric strings to the SDK."""
    statistics = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"flow_id": "flow-123"},
                execution_count=3,
                metrics={"llm_display_cost": 0.42, "llm_total_tokens": 128.0},
            )
        ],
        truncated=False,
    )
    mock_kitaru_client.executions.statistics.return_value = statistics

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_statistics(
            group_by=["flow"],
            metrics=["llm_display_cost", "llm_total_tokens"],
            max_groups=20,
        )

    mock_kitaru_client.executions.statistics.assert_called_once_with(
        group_by=["flow"],
        metrics=["llm_display_cost", "llm_total_tokens"],
        flow=None,
        status=None,
        stack=None,
        tags=None,
        max_groups=20,
    )
    assert payload == {
        "groups": [
            {
                "keys": {"flow_id": "flow-123"},
                "execution_count": 3,
                "metrics": {"llm_display_cost": 0.42, "llm_total_tokens": 128.0},
            }
        ],
        "truncated": False,
        "group_count": 1,
    }


def test_executions_statistics_delegates_to_inspection_serializer(
    mock_kitaru_client: MagicMock,
) -> None:
    """Statistics tool should share the same serializer as CLI/SDK transports."""
    statistics = ExecutionStatistics(
        groups=[ExecutionStatisticsGroup(keys={}, execution_count=18)],
        truncated=False,
    )
    mock_kitaru_client.executions.statistics.return_value = statistics

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru.inspection.serialize_execution_statistics",
            return_value={"source": "inspection"},
        ) as mock_serialize,
    ):
        payload = kitaru_executions_statistics()

    mock_kitaru_client.executions.statistics.assert_called_once_with(
        group_by=[],
        metrics=[],
        flow=None,
        status=None,
        stack=None,
        tags=None,
        max_groups=1000,
    )
    mock_serialize.assert_called_once_with(statistics)
    assert payload == {"source": "inspection"}


@pytest.mark.parametrize("max_groups", [0, 10_001])
def test_executions_statistics_rejects_invalid_max_groups(max_groups: int) -> None:
    """MCP statistics should reject max_groups outside the public range."""
    with pytest.raises(ValueError, match="`max_groups` must be between 1 and 10000"):
        kitaru_executions_statistics(max_groups=max_groups)


def test_executions_statistics_preserves_error_boundary_behavior(
    mock_kitaru_client: MagicMock,
) -> None:
    """Statistics errors should pass through the shared MCP error boundary."""
    mock_kitaru_client.executions.statistics.side_effect = KitaruUsageError(
        "bad grouping"
    )

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        pytest.raises(KitaruUsageError, match="bad grouping"),
    ):
        kitaru_executions_statistics(group_by=["nope"])


def test_executions_list_stack_filter_happens_after_fetch(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Stack filtering should happen client-side without truncating early."""
    other_stack = replace(sample_execution, exec_id="kr-other", stack_name="dev")
    mock_kitaru_client.executions.list.return_value = [other_stack, sample_execution]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_list(stack="prod", limit=1)

    mock_kitaru_client.executions.list.assert_called_once_with(
        flow=None,
        status=None,
        limit=None,
    )
    assert [item["exec_id"] for item in payload] == [sample_execution.exec_id]


def test_executions_get_returns_full_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Get tool should return detailed execution payload."""
    mock_kitaru_client.executions.get.return_value = sample_execution

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_get(sample_execution.exec_id)

    assert payload["exec_id"] == sample_execution.exec_id
    assert payload["checkpoints"][0]["name"] == "write_summary"


def test_executions_latest_with_stack_filter(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Latest tool should support stack filtering even though client API does not."""
    mock_kitaru_client.executions.list.return_value = [sample_execution]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_latest(stack="prod")

    assert payload["exec_id"] == sample_execution.exec_id
    mock_kitaru_client.executions.latest.assert_not_called()


def test_executions_latest_delegates_filtering_to_shared_interface(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.latest_execution_filtered",
            return_value=sample_execution,
        ) as mock_latest,
    ):
        payload = kitaru_executions_latest(
            status="waiting",
            flow="content_pipeline",
            stack="prod",
        )

    mock_latest.assert_called_once_with(
        mock_kitaru_client,
        flow="content_pipeline",
        status="waiting",
        stack="prod",
    )
    assert payload["exec_id"] == sample_execution.exec_id


def test_get_execution_logs_calls_client_with_agent_default_limit(
    mock_kitaru_client: MagicMock,
) -> None:
    """The log tool should call `client.executions.logs` with default limit=200."""
    mock_kitaru_client.executions.logs.return_value = [
        SimpleNamespace(
            message="Starting research",
            level="INFO",
            timestamp="2026-03-09T10:01:12+00:00",
            checkpoint_name="research",
        )
    ]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = get_execution_logs("kr-a8f3c2")

    mock_kitaru_client.executions.logs.assert_called_once_with(
        "kr-a8f3c2",
        checkpoint=None,
        source="step",
        limit=200,
    )
    assert "Starting research" in payload


def test_get_execution_logs_passes_explicit_filters(
    mock_kitaru_client: MagicMock,
) -> None:
    """The log tool should forward explicit source/checkpoint/limit arguments."""
    mock_kitaru_client.executions.logs.return_value = []

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = get_execution_logs(
            "kr-a8f3c2",
            checkpoint="research",
            source="runner",
            limit=50,
        )

    mock_kitaru_client.executions.logs.assert_called_once_with(
        "kr-a8f3c2",
        checkpoint="research",
        source="runner",
        limit=50,
    )
    assert payload == "No log entries found."


def test_get_execution_logs_delegates_rendering_to_shared_interface(
    mock_kitaru_client: MagicMock,
) -> None:
    mock_kitaru_client.executions.logs.return_value = [
        SimpleNamespace(message="Starting research")
    ]

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.format_mcp_execution_logs",
            return_value="delegated log payload",
        ) as mock_format,
    ):
        payload = get_execution_logs("kr-a8f3c2")

    mock_format.assert_called_once_with(mock_kitaru_client.executions.logs.return_value)
    assert payload == "delegated log payload"


def test_executions_run_fetches_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Run tool should run a flow and include execution details when available."""
    invocation_result = execution_interface.FlowInvocationResult(
        handle=SimpleNamespace(exec_id=sample_execution.exec_id),
        exec_id=sample_execution.exec_id,
    )

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.invoke_flow_target",
            return_value=invocation_result,
        ) as mock_invoke,
        patch(
            "kitaru._interface_executions.resolve_started_execution_details",
            return_value=execution_interface.StartedExecutionDetails(
                exec_id=sample_execution.exec_id,
                execution=sample_execution,
                warning=None,
            ),
        ) as mock_resolve,
    ):
        payload = kitaru_executions_run(
            "agent.py:content_pipeline",
            args={"topic": "ai safety"},
        )

    mock_invoke.assert_called_once_with(
        target="agent.py:content_pipeline",
        args={"topic": "ai safety"},
        stack=None,
        module_name_prefix="_kitaru_mcp_run_target_",
    )
    mock_resolve.assert_called_once_with(
        exec_id=sample_execution.exec_id,
        client=mock_kitaru_client,
    )
    assert "invocation" not in payload
    assert payload["execution"]["exec_id"] == sample_execution.exec_id


def test_executions_run_returns_warning_when_details_unavailable(
    mock_kitaru_client: MagicMock,
) -> None:
    """Run tool should still return exec_id if details are not immediately queryable."""
    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.invoke_flow_target",
            return_value=execution_interface.FlowInvocationResult(
                handle=SimpleNamespace(exec_id="kr-new"),
                exec_id="kr-new",
            ),
        ),
        patch(
            "kitaru._interface_executions.resolve_started_execution_details",
            return_value=execution_interface.StartedExecutionDetails(
                exec_id="kr-new",
                execution=None,
                warning=(
                    "Execution started successfully, but details are not "
                    "available yet: store unavailable"
                ),
            ),
        ),
    ):
        payload = kitaru_executions_run(
            "agent.py:content_pipeline",
            args={"topic": "ai safety"},
            stack="prod",
        )

    assert payload["exec_id"] == "kr-new"
    assert payload["execution"] is None
    assert "details are not available yet" in payload["warning"]


def test_deployments_deploy_loads_target_and_serializes(sample_deployment) -> None:
    """kitaru_deployments_deploy should load, deploy, and serialize the record."""
    flow_target = SimpleNamespace(deploy=MagicMock(return_value=sample_deployment))

    with patch(
        "kitaru.mcp.server._load_deployable_flow_target",
        return_value=flow_target,
    ) as mock_loader:
        payload = kitaru_deployments_deploy(
            "agent.py:content_pipeline",
            inputs={"topic": "ai safety"},
            stack="prod",
            image="python:3.12-slim",
            cache=False,
            retries=2,
        )

    mock_loader.assert_called_once_with(
        "agent.py:content_pipeline",
        module_name_prefix="_kitaru_mcp_deploy_target_",
    )
    flow_target.deploy.assert_called_once_with(
        topic="ai safety",
        tags={"default": True},
        stack="prod",
        image=ImageSettings(base_image="python:3.12-slim"),
        cache=False,
        retries=2,
    )
    assert payload["deployment_id"] == sample_deployment.deployment_id
    assert payload["tags"] == sample_deployment.tags


def test_deployments_deploy_forwards_non_default_tag_exclusivity(
    sample_deployment,
) -> None:
    """Non-default deployment tags should honor the explicit exclusive flag."""
    flow_target = SimpleNamespace(deploy=MagicMock(return_value=sample_deployment))

    with patch(
        "kitaru.mcp.server._load_deployable_flow_target",
        return_value=flow_target,
    ):
        kitaru_deployments_deploy(
            "agent.py:content_pipeline",
            inputs={"topic": "ai safety"},
            tag="canary",
            exclusive=True,
        )

    flow_target.deploy.assert_called_once_with(
        topic="ai safety",
        tags={"canary": True},
    )


@pytest.mark.parametrize(
    "reserved_key",
    ["stack", "publish_default_on_first_deploy"],
)
def test_deployments_deploy_rejects_reserved_input_keys(reserved_key: str) -> None:
    """Flow inputs must not shadow deployment-control options."""
    with (
        patch("kitaru.mcp.server._load_deployable_flow_target") as mock_loader,
        pytest.raises(ValueError, match="`inputs` contains reserved"),
    ):
        kitaru_deployments_deploy(
            "agent.py:content_pipeline",
            inputs={reserved_key: "not-a-flow-input"},
        )

    mock_loader.assert_not_called()


def test_deployments_deploy_rejects_non_object_inputs() -> None:
    """MCP inputs should be structured JSON objects, not scalars/lists."""
    with pytest.raises(ValueError, match="`inputs` must be an object"):
        kitaru_deployments_deploy("agent.py:content_pipeline", inputs=["topic"])


def test_deployments_deploy_accepts_structured_image_object(
    sample_deployment,
) -> None:
    """MCP deploy should accept ImageSettings-like dict payloads for image."""
    flow_target = SimpleNamespace(deploy=MagicMock(return_value=sample_deployment))

    with patch(
        "kitaru.mcp.server._load_deployable_flow_target",
        return_value=flow_target,
    ):
        kitaru_deployments_deploy(
            "agent.py:content_pipeline",
            inputs={"topic": "ai safety"},
            image={"requirements": ["kitaru[openai]"]},
        )

    flow_target.deploy.assert_called_once_with(
        topic="ai safety",
        tags={"default": True},
        image=ImageSettings(requirements=["kitaru[openai]"]),
    )


def test_deployments_deploy_rejects_invalid_image_before_loading_target() -> None:
    """Invalid MCP image payloads should fail before the flow target is loaded."""
    with (
        patch("kitaru.mcp.server._load_deployable_flow_target") as mock_loader,
        pytest.raises(
            KitaruUsageError,
            match=(
                "`image` must be either a base image string or an image settings object"
            ),
        ),
    ):
        kitaru_deployments_deploy(
            "agent.py:content_pipeline",
            inputs={"topic": "ai safety"},
            image={"requirements": [" "]},
        )

    mock_loader.assert_not_called()


def test_deployments_deploy_surfaces_shared_stack_guard() -> None:
    """MCP deploy should surface the shared non-runnable-stack error unchanged."""
    flow_target = SimpleNamespace(
        deploy=MagicMock(
            side_effect=KitaruUsageError("the Kitaru server cannot run that stack")
        )
    )

    with (
        patch(
            "kitaru.mcp.server._load_deployable_flow_target",
            return_value=flow_target,
        ),
        pytest.raises(KitaruUsageError, match="cannot run that stack"),
    ):
        kitaru_deployments_deploy("agent.py:content_pipeline", inputs={"topic": "ai"})


def test_deployments_invoke_defaults_to_default_tag(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """kitaru_deployments_invoke should default to the reserved default tag."""
    mock_kitaru_client.deployments.invoke.return_value = SimpleNamespace(
        exec_id=sample_execution.exec_id
    )

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.resolve_started_execution_details",
            return_value=execution_interface.StartedExecutionDetails(
                exec_id=sample_execution.exec_id,
                execution=sample_execution,
                warning=None,
            ),
        ) as mock_resolve,
    ):
        payload = kitaru_deployments_invoke(
            "content_pipeline",
            inputs={"topic": "ai safety"},
        )

    mock_kitaru_client.deployments.invoke.assert_called_once_with(
        flow="content_pipeline",
        version=None,
        tag="default",
        selector_source="implicit_default",
        inputs={"topic": "ai safety"},
    )
    mock_resolve.assert_called_once_with(
        exec_id=sample_execution.exec_id,
        client=mock_kitaru_client,
    )
    assert payload["flow"] == "content_pipeline"
    assert payload["selector"] == {"version": None, "tag": "default"}
    assert payload["execution"]["exec_id"] == sample_execution.exec_id


def test_deployments_invoke_surfaces_non_runnable_deployment_error(
    mock_kitaru_client: MagicMock,
) -> None:
    """MCP invoke should preserve the shared early failure for legacy deployments."""
    mock_kitaru_client.deployments.invoke.side_effect = KitaruStateError(
        "server cannot run this deployment"
    )

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        pytest.raises(KitaruStateError, match="server cannot run this deployment"),
    ):
        kitaru_deployments_invoke("content_pipeline")


def test_deployments_invoke_passes_explicit_tag_source(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Explicit MCP tag selection should stay tag-specific downstream."""
    mock_kitaru_client.deployments.invoke.return_value = SimpleNamespace(
        exec_id=sample_execution.exec_id
    )

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.resolve_started_execution_details",
            return_value=execution_interface.StartedExecutionDetails(
                exec_id=sample_execution.exec_id,
                execution=sample_execution,
                warning=None,
            ),
        ),
    ):
        payload = kitaru_deployments_invoke(
            "content_pipeline",
            tag="stable",
            inputs={"topic": "ai safety"},
        )

    mock_kitaru_client.deployments.invoke.assert_called_once_with(
        flow="content_pipeline",
        version=None,
        tag="stable",
        selector_source="tag",
        inputs={"topic": "ai safety"},
    )
    assert payload["selector"] == {"version": None, "tag": "stable"}


def test_deployments_invoke_rejects_version_and_tag_together(
    mock_kitaru_client: MagicMock,
) -> None:
    """MCP selector validation should match the shared deployment rules."""
    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        pytest.raises(ValueError, match="mutually exclusive"),
    ):
        kitaru_deployments_invoke("content_pipeline", version=2, tag="canary")

    mock_kitaru_client.deployments.invoke.assert_not_called()


def test_deployments_list_forwards_filter_and_serializes(
    mock_kitaru_client: MagicMock,
    sample_deployment,
) -> None:
    """kitaru_deployments_list should delegate to the public deployments API."""
    mock_kitaru_client.deployments.list.return_value = [sample_deployment]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_deployments_list(flow="content_pipeline")

    mock_kitaru_client.deployments.list.assert_called_once_with(flow="content_pipeline")
    assert payload == [
        {
            "deployment_id": "dep-content-v2",
            "flow": "content_pipeline",
            "version": 2,
            "tags": {"default": True, "canary": False},
            "commit_sha": "abc1234",
            "commit_dirty": False,
            "image_digest": "sha256:deadbeef",
            "created_at": "2026-03-08T12:00:00+00:00",
            "schema": {"type": "object"},
            "stack": "prod",
        }
    ]


def test_deployments_get_defaults_to_default_tag(
    mock_kitaru_client: MagicMock,
    sample_deployment,
) -> None:
    """kitaru_deployments_get should use the same default selector as CLI/SDK."""
    mock_kitaru_client.deployments.get.return_value = sample_deployment

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_deployments_get("content_pipeline")

    mock_kitaru_client.deployments.get.assert_called_once_with(
        flow="content_pipeline",
        version=None,
        tag="default",
    )
    assert payload["version"] == 2


def test_deployments_delete_delegates_by_version(
    mock_kitaru_client: MagicMock,
) -> None:
    """kitaru_deployments_delete should leave safety checks to the SDK API."""
    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_deployments_delete("content_pipeline", version=2)

    mock_kitaru_client.deployments.delete.assert_called_once_with(
        flow="content_pipeline",
        version=2,
    )
    assert payload == {"flow": "content_pipeline", "version": 2, "deleted": True}


def test_deployments_tag_delegates_and_serializes(
    mock_kitaru_client: MagicMock,
    sample_deployment,
) -> None:
    """kitaru_deployments_tag should forward version/tag/exclusive to SDK API."""
    mock_kitaru_client.deployments.tag.return_value = sample_deployment

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_deployments_tag(
            "content_pipeline",
            version=2,
            tag="stable",
            exclusive=True,
        )

    mock_kitaru_client.deployments.tag.assert_called_once_with(
        flow="content_pipeline",
        version=2,
        tag="stable",
        exclusive=True,
    )
    assert payload["deployment_id"] == sample_deployment.deployment_id


def test_deployments_untag_delegates_and_serializes(
    mock_kitaru_client: MagicMock,
    sample_deployment,
) -> None:
    """kitaru_deployments_untag should forward tag removal to SDK API."""
    mock_kitaru_client.deployments.untag.return_value = sample_deployment

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_deployments_untag(
            "content_pipeline",
            version=2,
            tag="canary",
        )

    mock_kitaru_client.deployments.untag.assert_called_once_with(
        flow="content_pipeline",
        version=2,
        tag="canary",
    )
    assert payload["flow"] == "content_pipeline"


def test_executions_input_validates_wait_schema(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Input tool should reject payloads that fail known wait schema type checks."""
    mock_kitaru_client.executions.get.return_value = sample_execution

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        pytest.raises(ValueError, match="schema type"),
    ):
        kitaru_executions_input(
            sample_execution.exec_id,
            wait="approve_draft",
            value="yes",
        )

    mock_kitaru_client.executions.input.assert_not_called()


def test_executions_input_resolves_wait_and_returns_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Input tool should call client input API and return updated execution."""
    resumed = replace(
        sample_execution,
        status=ExecutionStatus.RUNNING,
        pending_wait=None,
    )
    mock_kitaru_client.executions.get.return_value = sample_execution
    mock_kitaru_client.executions.input.return_value = resumed

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_input(
            sample_execution.exec_id,
            wait="approve_draft",
            value=True,
        )

    mock_kitaru_client.executions.input.assert_called_once_with(
        sample_execution.exec_id,
        wait="approve_draft",
        value=True,
    )
    assert payload["status"] == "running"


def test_executions_input_delegates_pending_wait_validation(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    resumed = replace(
        sample_execution,
        status=ExecutionStatus.RUNNING,
        pending_wait=None,
    )
    mock_kitaru_client.executions.get.return_value = sample_execution
    mock_kitaru_client.executions.input.return_value = resumed

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru._interface_executions.validate_pending_wait_input"
        ) as mock_validate,
    ):
        payload = kitaru_executions_input(
            sample_execution.exec_id,
            wait="approve_draft",
            value=True,
        )

    mock_validate.assert_called_once_with(
        execution=sample_execution,
        wait="approve_draft",
        value=True,
    )
    assert payload["status"] == "running"


def test_validate_pending_wait_input_accepts_wait_id_alias(sample_execution) -> None:
    execution_interface.validate_pending_wait_input(
        execution=sample_execution,
        wait=sample_execution.pending_wait.wait_id,
        value=True,
    )


def test_validate_pending_wait_input_ignores_non_matching_wait(
    sample_execution,
) -> None:
    execution_interface.validate_pending_wait_input(
        execution=sample_execution,
        wait="different_wait",
        value="yes",
    )


def test_validate_pending_wait_input_ignores_missing_pending_wait(
    sample_execution,
) -> None:
    execution_interface.validate_pending_wait_input(
        execution=replace(sample_execution, pending_wait=None),
        wait="approve_draft",
        value="yes",
    )


def test_executions_replay_forwards_unified_arguments_and_returns_json(
    mock_kitaru_client: MagicMock,
) -> None:
    """Replay tool should call the unified SDK API and return to_json directly."""
    replay_json = {
        "submission_id": "rs-123",
        "tag": "candidate",
        "at": "lookup_policy_tool",
        "wait": True,
        "plan": {},
        "results": [{"original_exec_id": "kr-a", "replay_exec_id": "kr-r"}],
        "failures": [],
        "skipped": [],
        "summary": {"submitted": 1, "completed": 1, "failed": 0, "skipped": 0},
        "compare_url": "https://kitaru.example/compare",
    }
    submission = MagicMock()
    submission.to_json.return_value = replay_json
    mock_kitaru_client.executions.replay.return_value = submission

    flow_overrides = {"prompt": "new prompt"}
    checkpoint_overrides = {"lookup_policy_tool": {"output": {"tier": "gold"}}}
    invocation_overrides = {"model_call_2": {"model": "gpt-4.1-mini"}}
    skip = ["draft_email"]

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_replay(
            ["kr-a", "kr-b"],
            at="lookup_policy_tool",
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
            tag="candidate",
            wait=True,
            on_error="fail",
        )

    mock_kitaru_client.executions.replay.assert_called_once_with(
        ["kr-a", "kr-b"],
        at="lookup_policy_tool",
        flow_overrides=flow_overrides,
        checkpoint_overrides=checkpoint_overrides,
        invocation_overrides=invocation_overrides,
        skip=skip,
        tag="candidate",
        wait=True,
        on_error="fail",
    )
    submission.to_json.assert_called_once_with()
    assert payload == replay_json


def test_executions_replay_forwards_omitted_on_error_as_none(
    mock_kitaru_client: MagicMock,
) -> None:
    """Omitted MCP replay on_error should let the SDK apply its shared default."""
    submission = MagicMock()
    submission.to_json.return_value = {"submission_id": "rs-123"}
    mock_kitaru_client.executions.replay.return_value = submission

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_replay(["kr-a"], at="lookup_policy_tool")

    mock_kitaru_client.executions.replay.assert_called_once_with(
        ["kr-a"],
        at="lookup_policy_tool",
        flow_overrides=None,
        checkpoint_overrides=None,
        invocation_overrides=None,
        skip=None,
        tag=None,
        wait=None,
        on_error=None,
    )
    assert payload == {"submission_id": "rs-123"}


def test_mcp_does_not_expose_replay_many_tool() -> None:
    """MCP exposes multi-ID replay through kitaru_executions_replay only."""
    import kitaru.mcp.server as server

    assert not hasattr(server, "kitaru_executions_replay_many")


def test_executions_diff_matrix_returns_renamed_payload() -> None:
    """Diff matrix tool should call the renamed helper and avoid cohort keys."""
    diff_result = object()

    with (
        patch("kitaru.diff.diff_matrix", return_value=diff_result) as mock_diff_matrix,
        patch(
            "kitaru.diff.serialize_diff_matrix",
            return_value={"rows": [{"original_exec_id": "kr-a"}]},
        ) as mock_serialize,
    ):
        payload = kitaru_executions_diff_matrix(["kr-a", "kr-b"])

    mock_diff_matrix.assert_called_once_with(["kr-a", "kr-b"])
    mock_serialize.assert_called_once_with(diff_result)
    assert payload == {
        "available": True,
        "operation": "diff_matrix",
        "diff_matrix": {"rows": [{"original_exec_id": "kr-a"}]},
    }
    assert "cohort" not in payload


def test_execution_mutation_tools_return_serialized_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Cancel and retry tools should return normalized execution payloads."""
    mock_kitaru_client.executions.cancel.return_value = sample_execution
    mock_kitaru_client.executions.retry.return_value = sample_execution

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        cancel_payload = kitaru_executions_cancel(sample_execution.exec_id)
        retry_payload = kitaru_executions_retry(sample_execution.exec_id)

    assert cancel_payload["exec_id"] == sample_execution.exec_id
    assert retry_payload["exec_id"] == sample_execution.exec_id


def test_artifact_tools_call_client_and_serialize(
    mock_kitaru_client: MagicMock,
    sample_artifact,
) -> None:
    """Artifact list/get tools should expose metadata and loaded value information."""
    artifact_with_value = MagicMock()
    artifact_with_value.artifact_id = sample_artifact.artifact_id
    artifact_with_value.name = sample_artifact.name
    artifact_with_value.kind = sample_artifact.kind
    artifact_with_value.save_type = sample_artifact.save_type
    artifact_with_value.producing_call = sample_artifact.producing_call
    artifact_with_value.metadata = sample_artifact.metadata
    artifact_with_value.load.return_value = object()

    mock_kitaru_client.artifacts.list.return_value = [sample_artifact]
    mock_kitaru_client.artifacts.get.return_value = artifact_with_value

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        listed = kitaru_artifacts_list("kr-a8f3c2", limit=10)
        loaded = kitaru_artifacts_get(sample_artifact.artifact_id)

    assert listed[0]["artifact_id"] == sample_artifact.artifact_id
    assert loaded["artifact_id"] == sample_artifact.artifact_id
    assert loaded["value_format"] == "repr"


def test_artifact_get_delegates_value_serialization_to_inspection(
    mock_kitaru_client: MagicMock,
    sample_artifact,
) -> None:
    artifact_with_value = MagicMock()
    artifact_with_value.artifact_id = sample_artifact.artifact_id
    artifact_with_value.name = sample_artifact.name
    artifact_with_value.kind = sample_artifact.kind
    artifact_with_value.save_type = sample_artifact.save_type
    artifact_with_value.producing_call = sample_artifact.producing_call
    artifact_with_value.metadata = sample_artifact.metadata
    loaded_value = object()
    artifact_with_value.load.return_value = loaded_value

    mock_kitaru_client.artifacts.get.return_value = artifact_with_value

    with (
        patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client),
        patch(
            "kitaru.inspection.serialize_artifact_value",
            return_value={
                "value": "delegated",
                "value_format": "json",
                "value_type": "custom.Type",
            },
        ) as mock_serialize,
    ):
        payload = kitaru_artifacts_get(sample_artifact.artifact_id)

    mock_serialize.assert_called_once_with(loaded_value)
    assert payload["value"] == "delegated"
    assert payload["value_type"] == "custom.Type"


def test_secrets_list_uses_default_pagination() -> None:
    summaries = [
        SecretSummary(
            name=f"secret-{index:02d}",
            id=f"secret-id-{index:02d}",
            private=False,
            keys=["API_KEY"],
        )
        for index in range(25)
    ]

    with patch(
        "kitaru.mcp.server.secrets_api.list_secrets",
        return_value=summaries,
    ) as mock_list:
        payload = kitaru_secrets_list()

    mock_list.assert_called_once_with()
    assert len(payload) == 20
    assert [item["id"] for item in payload] == [
        summary.id for summary in summaries[:20]
    ]


def test_secrets_list_applies_non_default_pagination_without_resorting() -> None:
    summaries = [
        SecretSummary(
            name=name,
            id=f"secret-id-{index}",
            private=False,
            keys=["API_KEY"],
        )
        for index, name in enumerate(["zeta", "alpha", "gamma", "beta", "delta"])
    ]

    with patch(
        "kitaru.mcp.server.secrets_api.list_secrets",
        return_value=summaries,
    ) as mock_list:
        payload = kitaru_secrets_list(page=2, size=2)

    mock_list.assert_called_once_with()
    assert [item["id"] for item in payload] == [
        summaries[2].id,
        summaries[3].id,
    ]


def test_secrets_list_returns_empty_for_out_of_range_page() -> None:
    summaries = [
        SecretSummary(
            name="openai-creds",
            id="secret-id",
            private=False,
            keys=["OPENAI_API_KEY"],
        )
    ]

    with (
        patch(
            "kitaru.mcp.server.secrets_api.list_secrets",
            return_value=summaries,
        ),
        patch(
            "kitaru.mcp.server.inspection.serialize_secret_summary",
        ) as mock_serialize,
    ):
        payload = kitaru_secrets_list(page=2, size=20)

    assert payload == []
    mock_serialize.assert_not_called()


def test_secrets_list_delegates_in_page_items_to_serializer_in_order() -> None:
    summaries = [
        SecretSummary(
            name=f"secret-{index}",
            id=f"secret-id-{index}",
            private=False,
            keys=["API_KEY"],
        )
        for index in range(4)
    ]
    serialized = [{"id": summaries[2].id}, {"id": summaries[3].id}]

    with (
        patch(
            "kitaru.mcp.server.secrets_api.list_secrets",
            return_value=summaries,
        ),
        patch(
            "kitaru.mcp.server.inspection.serialize_secret_summary",
            side_effect=serialized,
        ) as mock_serialize,
    ):
        payload = kitaru_secrets_list(page=2, size=2)

    assert payload == serialized
    assert mock_serialize.call_args_list == [
        call(summaries[2]),
        call(summaries[3]),
    ]


def test_secrets_list_returns_metadata_only_for_public_and_private_secrets() -> None:
    summaries = [
        SecretSummary(
            name="shared-creds",
            id="public-id",
            private=False,
            keys=["OPENAI_API_KEY"],
            has_missing_values=True,
        ),
        SecretSummary(
            name="personal-creds",
            id="private-id",
            private=True,
            keys=["ANTHROPIC_API_KEY"],
        ),
    ]

    with patch(
        "kitaru.mcp.server.secrets_api.list_secrets",
        return_value=summaries,
    ):
        payload = kitaru_secrets_list()

    assert payload == [
        {
            "id": "public-id",
            "name": "shared-creds",
            "visibility": "public",
            "keys": ["OPENAI_API_KEY"],
            "has_missing_values": True,
        },
        {
            "id": "private-id",
            "name": "personal-creds",
            "visibility": "private",
            "keys": ["ANTHROPIC_API_KEY"],
            "has_missing_values": False,
        },
    ]
    for item in payload:
        assert set(item) == {
            "id",
            "name",
            "visibility",
            "keys",
            "has_missing_values",
        }
        assert "values" not in item
        assert "secret_values" not in item


@pytest.mark.parametrize("page", [True, 0, -1])
def test_secrets_list_rejects_invalid_page(page: Any) -> None:
    with (
        patch("kitaru.mcp.server.secrets_api.list_secrets") as mock_list,
        pytest.raises(KitaruUsageError) as exc_info,
    ):
        kitaru_secrets_list(page=page)

    assert str(exc_info.value) == "`page` must be an integer >= 1."
    mock_list.assert_not_called()


@pytest.mark.parametrize("size", [True, 0, -1])
def test_secrets_list_rejects_invalid_size(size: Any) -> None:
    with (
        patch("kitaru.mcp.server.secrets_api.list_secrets") as mock_list,
        pytest.raises(KitaruUsageError) as exc_info,
    ):
        kitaru_secrets_list(size=size)

    assert str(exc_info.value) == "`size` must be an integer >= 1."
    mock_list.assert_not_called()


def test_secrets_list_propagates_sdk_backend_errors() -> None:
    with (
        patch(
            "kitaru.mcp.server.secrets_api.list_secrets",
            side_effect=KitaruBackendError("backend unavailable"),
        ),
        pytest.raises(KitaruBackendError, match="backend unavailable"),
    ):
        kitaru_secrets_list()


def test_secrets_create_returns_metadata_without_values() -> None:
    """MCP secret creation should delegate to SDK creation safely."""
    summary = SecretSummary(
        name="openai-creds",
        id="secret-id",
        private=False,
        keys=["OPENAI_API_KEY"],
    )

    with patch(
        "kitaru.mcp.server.secrets_api.create_secret",
        return_value=summary,
    ) as mock_create:
        payload = kitaru_secrets_create(
            "openai-creds",
            {"OPENAI_API_KEY": "sk-123"},
        )

    mock_create.assert_called_once_with(
        "openai-creds",
        {"OPENAI_API_KEY": "sk-123"},
        private=False,
    )
    assert payload == {
        "id": "secret-id",
        "name": "openai-creds",
        "visibility": "public",
        "keys": ["OPENAI_API_KEY"],
        "has_missing_values": False,
    }
    assert "values" not in payload


def test_secrets_create_forwards_private_flag() -> None:
    """MCP callers can opt into private secret creation."""
    summary = SecretSummary(
        name="openai-creds",
        id="secret-id",
        private=True,
        keys=["OPENAI_API_KEY"],
    )

    with patch(
        "kitaru.mcp.server.secrets_api.create_secret",
        return_value=summary,
    ) as mock_create:
        payload = kitaru_secrets_create(
            "openai-creds",
            {"OPENAI_API_KEY": "sk-123"},
            private=True,
        )

    mock_create.assert_called_once_with(
        "openai-creds",
        {"OPENAI_API_KEY": "sk-123"},
        private=True,
    )
    assert payload["visibility"] == "private"


def test_secrets_create_propagates_sdk_errors() -> None:
    """SDK errors should pass through the MCP boundary for clients to inspect."""
    with (
        patch(
            "kitaru.mcp.server.secrets_api.create_secret",
            side_effect=KitaruRuntimeError("already exists"),
        ),
        pytest.raises(KitaruRuntimeError, match="already exists"),
    ):
        kitaru_secrets_create("openai-creds", {"OPENAI_API_KEY": "sk-123"})


def test_mcp_does_not_expose_secret_delete_tool() -> None:
    """MCP intentionally supports secret creation, not deletion."""
    import kitaru.mcp.server as server

    assert not hasattr(server, "kitaru_secrets_delete")
    assert "kitaru_secrets_delete" not in _mcp_tool_schemas_by_name()


def test_start_local_server_returns_structured_payload() -> None:
    """The MCP local-start tool should reuse the shared helper payload."""
    with patch(
        "kitaru.mcp.server.start_or_connect_local_server",
        return_value=SimpleNamespace(
            url="http://127.0.0.1:8383",
            action="started",
        ),
    ) as mock_start:
        payload = kitaru_start_local_server(port=9090, timeout=45)

    mock_start.assert_called_once_with(port=9090, timeout=45)
    assert payload == {
        "mode": "local",
        "url": "http://127.0.0.1:8383",
        "action": "started",
    }


def test_stop_local_server_returns_structured_payload() -> None:
    """The MCP local-stop tool should expose stop metadata."""
    with patch(
        "kitaru.mcp.server.stop_registered_local_server",
        return_value=SimpleNamespace(
            stopped=True,
            url="http://127.0.0.1:8383",
        ),
    ) as mock_stop:
        payload = kitaru_stop_local_server()

    mock_stop.assert_called_once_with()
    assert payload == {
        "stopped": True,
        "url": "http://127.0.0.1:8383",
    }


def test_start_local_server_propagates_failures() -> None:
    """The MCP local-start tool should propagate lifecycle failures."""
    with (
        patch(
            "kitaru.mcp.server.start_or_connect_local_server",
            side_effect=RuntimeError("missing local deps"),
        ),
        pytest.raises(RuntimeError, match="missing local deps"),
    ):
        kitaru_start_local_server()


def test_status_and_stack_tools_return_structured_payloads() -> None:
    """Status and stack tools should expose query-friendly JSON objects."""
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/kitaru-config",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        repository_root="/work/repo",
        server_version="0.99.0",
        server_database="postgres",
        server_deployment_type="kubernetes",
        local_server_status="not started",
        warning=None,
        log_store_status="datadog (preferred) ⚠ stack uses artifact-store",
        log_store_warning=(
            "Active ZenML stack uses: artifact-store\n"
            "The Kitaru log-store preference is not wired into stack selection yet."
        ),
        environment=[
            ActiveEnvironmentVariable(
                name="KITARU_SERVER_URL",
                value="https://example.com",
            ),
            ActiveEnvironmentVariable(
                name="KITARU_AUTH_TOKEN",
                value="token-12***",
            ),
        ],
    )

    stack_entries = [
        SimpleNamespace(
            stack=StackInfo(id="stack-1", name="prod", is_active=True),
            is_managed=True,
        ),
        SimpleNamespace(
            stack=StackInfo(id="stack-2", name="dev", is_active=False),
            is_managed=False,
        ),
    ]

    with (
        patch("kitaru.inspection.build_runtime_snapshot", return_value=snapshot),
        patch("kitaru._config._stacks._list_stack_entries", return_value=stack_entries),
    ):
        status_payload = kitaru_status()
        stack_payload = kitaru_stacks_list()

    assert status_payload["active_stack"] == "prod"
    assert (
        status_payload["log_store_status"]
        == "datadog (preferred) ⚠ stack uses artifact-store"
    )
    assert status_payload["environment"][0]["name"] == "KITARU_SERVER_URL"
    assert status_payload["environment"][1]["value"] == "token-12***"
    assert [stack["name"] for stack in stack_payload] == ["prod", "dev"]
    assert [stack["is_managed"] for stack in stack_payload] == [True, False]


def test_status_delegates_snapshot_serialization_to_inspection() -> None:
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/kitaru-config",
    )

    with (
        patch("kitaru.inspection.build_runtime_snapshot", return_value=snapshot),
        patch(
            "kitaru.inspection.serialize_runtime_snapshot",
            return_value={"connection": "delegated", "source": "inspection"},
        ) as mock_serialize,
    ):
        payload = kitaru_status()

    mock_serialize.assert_called_once_with(snapshot)
    assert payload == {"connection": "delegated", "source": "inspection"}


def test_projects_list_delegates_to_shared_helpers_and_serializer() -> None:
    projects = [
        ProjectInfo(
            id="project-prod-id",
            name="production",
            display_name="Production",
            description=None,
            is_active=True,
        ),
        ProjectInfo(
            id="project-stage-id",
            name="staging",
            display_name=None,
            description="Test changes safely",
            is_active=False,
        ),
    ]

    with patch(
        "kitaru._config._projects.list_projects", return_value=projects
    ) as mock_list:
        payload = kitaru_projects_list()

    mock_list.assert_called_once_with()
    assert payload == [
        {
            "id": "project-prod-id",
            "name": "production",
            "display_name": "Production",
            "description": None,
            "is_active": True,
        },
        {
            "id": "project-stage-id",
            "name": "staging",
            "display_name": None,
            "description": "Test changes safely",
            "is_active": False,
        },
    ]


def test_projects_current_delegates_to_shared_helpers() -> None:
    project = ProjectInfo(
        id="project-prod-id",
        name="production",
        display_name="Production",
        description=None,
        is_active=True,
    )

    with patch(
        "kitaru._config._projects.current_project",
        return_value=project,
    ) as mock_current:
        payload = kitaru_projects_current()

    mock_current.assert_called_once_with()
    assert payload["name"] == "production"
    assert payload["is_active"] is True


def test_projects_show_delegates_to_shared_helpers() -> None:
    project = ProjectInfo(
        id="project-stage-id",
        name="staging",
        display_name=None,
        description=None,
        is_active=False,
    )

    with patch(
        "kitaru._config._projects.get_project",
        return_value=project,
    ) as mock_get:
        payload = kitaru_projects_show("staging")

    mock_get.assert_called_once_with("staging")
    assert payload["id"] == "project-stage-id"
    assert payload["is_active"] is False


def test_projects_use_delegates_to_shared_helpers() -> None:
    project = ProjectInfo(
        id="project-prod-id",
        name="production",
        display_name=None,
        description=None,
        is_active=True,
    )

    with patch(
        "kitaru._config._projects.use_project",
        return_value=project,
    ) as mock_use:
        payload = kitaru_projects_use("production")

    mock_use.assert_called_once_with("production")
    assert payload["name"] == "production"
    assert payload["is_active"] is True


def test_projects_use_preserves_pro_cloud_feature_error() -> None:
    """MCP project use should preserve the shared Pro/Cloud guard error."""
    with (
        patch(
            "kitaru._config._projects.use_project",
            side_effect=KitaruFeatureNotAvailableError(
                "Kitaru project use requires a ZenML Pro/Cloud server."
            ),
        ),
        pytest.raises(KitaruFeatureNotAvailableError, match="Pro/Cloud"),
    ):
        kitaru_projects_use("staging")


def test_manage_stack_create_returns_structured_result() -> None:
    """MCP manage_stack(create) should reuse the CLI-style serialized payload."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-dev-id", name="dev", is_active=True),
            previous_active_stack="default",
            components_created=(
                "dev (orchestrator)",
                "dev (artifact_store)",
                "dev (sandbox)",
            ),
            stack_type="local",
            service_connectors_created=(),
            resources=None,
        )

        payload = manage_stack("create", "dev", activate=True)

    mock_create_stack.assert_called_once_with(
        "dev",
        activate=True,
        stack_type=StackType.LOCAL,
        remote_spec=None,
        sandbox_flavor="local",
    )
    assert payload == {
        "id": "stack-dev-id",
        "name": "dev",
        "is_active": True,
        "previous_active_stack": "default",
        "components_created": [
            "dev (orchestrator)",
            "dev (artifact_store)",
            "dev (sandbox)",
        ],
        "stack_type": "local",
    }


def test_manage_stack_delegates_request_building_to_shared_interface() -> None:
    request = stack_interface.ManageStackCreateRequest(
        name="dev",
        activate=True,
        stack_type=StackType.LOCAL,
        remote_spec=None,
        sandbox_flavor="local",
    )

    with (
        patch(
            "kitaru._interface_stacks.build_manage_stack_request",
            return_value=request,
        ) as mock_request,
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
    ):
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-dev-id", name="dev", is_active=True),
            previous_active_stack="default",
            components_created=(
                "dev (orchestrator)",
                "dev (artifact_store)",
                "dev (sandbox)",
            ),
            stack_type="local",
            service_connectors_created=(),
            resources=None,
        )

        manage_stack("create", "dev", activate=True)

    mock_request.assert_called_once_with(
        action="create",
        name="dev",
        activate=True,
        recursive=False,
        force=False,
        stack_type="local",
        artifact_store=None,
        sandbox=None,
        container_registry=None,
        cluster=None,
        region=None,
        subscription_id=None,
        resource_group=None,
        workspace=None,
        execution_role=None,
        namespace=None,
        credentials=None,
        extra=None,
        async_mode=False,
        verify=True,
    )
    mock_create_stack.assert_called_once_with(
        "dev",
        activate=True,
        stack_type=StackType.LOCAL,
        remote_spec=None,
        sandbox_flavor="local",
    )


def test_manage_stack_delete_returns_structured_result() -> None:
    """MCP manage_stack(delete) should return delete metadata."""
    with patch("kitaru._config._stacks._delete_stack_operation") as mock_delete_stack:
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="dev",
            components_deleted=("dev (orchestrator)", "dev (artifact_store)"),
            new_active_stack="default",
            recursive=True,
        )

        payload = manage_stack(
            "delete",
            "dev",
            recursive=True,
            force=True,
        )

    mock_delete_stack.assert_called_once_with(
        "dev",
        recursive=True,
        force=True,
    )
    assert payload == {
        "deleted_stack": "dev",
        "components_deleted": ["dev (orchestrator)", "dev (artifact_store)"],
        "new_active_stack": "default",
        "recursive": True,
    }


def test_manage_stack_delete_delegates_request_building_to_shared_interface() -> None:
    request = stack_interface.ManageStackDeleteRequest(
        name="dev",
        recursive=True,
        force=True,
    )

    with (
        patch(
            "kitaru._interface_stacks.build_manage_stack_request",
            return_value=request,
        ) as mock_request,
        patch("kitaru._config._stacks._delete_stack_operation") as mock_delete_stack,
    ):
        mock_delete_stack.return_value = SimpleNamespace(
            deleted_stack="dev",
            components_deleted=("dev (orchestrator)", "dev (artifact_store)"),
            new_active_stack="default",
            recursive=True,
        )

        manage_stack("delete", "dev", recursive=True, force=True)

    mock_request.assert_called_once_with(
        action="delete",
        name="dev",
        activate=True,
        recursive=True,
        force=True,
        stack_type="local",
        artifact_store=None,
        sandbox=None,
        container_registry=None,
        cluster=None,
        region=None,
        subscription_id=None,
        resource_group=None,
        workspace=None,
        execution_role=None,
        namespace=None,
        credentials=None,
        extra=None,
        async_mode=False,
        verify=True,
    )
    mock_delete_stack.assert_called_once_with(
        "dev",
        recursive=True,
        force=True,
    )


@pytest.mark.parametrize(
    ("artifact_store", "container_registry", "region", "expected_provider"),
    [
        (
            "s3://my-bucket/kitaru",
            "123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
            "eu-west-1",
            CloudProvider.AWS,
        ),
        (
            "gs://my-bucket/kitaru",
            "europe-west4-docker.pkg.dev/my-project/my-repo/my-image",
            "europe-west4",
            CloudProvider.GCP,
        ),
    ],
)
def test_manage_stack_create_kubernetes_dispatches_structured_spec(
    artifact_store: str,
    container_registry: str,
    region: str,
    expected_provider: CloudProvider,
) -> None:
    """MCP Kubernetes create should build a shared serialized stack result."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-k8s-id", name="k8s-dev", is_active=False),
            previous_active_stack=None,
            components_created=(
                "k8s-dev (orchestrator)",
                "k8s-dev (artifact_store)",
                "k8s-dev (container_registry)",
            ),
            stack_type="kubernetes",
            service_connectors_created=("k8s-dev-connector",),
            resources={
                "provider": expected_provider.value,
                "cluster": "cluster-1",
                "region": region,
                "namespace": "ml-team",
                "artifact_store": artifact_store,
                "container_registry": container_registry,
            },
        )

        payload = manage_stack(
            "create",
            "k8s-dev",
            stack_type="kubernetes",
            activate=False,
            artifact_store=artifact_store,
            container_registry=container_registry,
            cluster="cluster-1",
            region=region,
            namespace="ml-team",
            verify=False,
        )

    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("k8s-dev",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.KUBERNETES
    assert mock_create_stack.call_args.kwargs["activate"] is False

    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.provider == expected_provider
    assert kubernetes_spec.artifact_store == artifact_store
    assert kubernetes_spec.container_registry == container_registry
    assert kubernetes_spec.cluster == "cluster-1"
    assert kubernetes_spec.region == region
    assert kubernetes_spec.namespace == "ml-team"
    assert kubernetes_spec.credentials is None
    assert kubernetes_spec.verify is False

    assert payload == {
        "id": "stack-k8s-id",
        "name": "k8s-dev",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": [
            "k8s-dev (orchestrator)",
            "k8s-dev (artifact_store)",
            "k8s-dev (container_registry)",
        ],
        "stack_type": "kubernetes",
        "service_connectors_created": ["k8s-dev-connector"],
        "resources": {
            "provider": expected_provider.value,
            "cluster": "cluster-1",
            "region": region,
            "namespace": "ml-team",
            "artifact_store": artifact_store,
            "container_registry": container_registry,
        },
    }


def test_manage_stack_create_modal_dispatches_structured_spec() -> None:
    """MCP Modal create should build a shared serialized stack result."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-modal-id", name="prod-modal", is_active=False),
            previous_active_stack=None,
            components_created=(
                "prod-modal (orchestrator)",
                "prod-modal (artifact_store)",
                "prod-modal (container_registry)",
                "prod-modal-image-builder (image_builder)",
                "prod-modal (sandbox)",
            ),
            stack_type="modal",
            service_connectors_created=(),
            resources={
                "provider": "gcp",
                "artifact_store": "gs://my-bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/my-project/repo",
                "sandbox": "modal",
            },
        )

        payload = manage_stack(
            "create",
            "prod-modal",
            stack_type="modal",
            activate=False,
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/repo",
            sandbox="modal",
            async_mode=True,
            extra={
                "orchestrator": {
                    "token_id": "ak-test",
                    "token_secret": "as-test",
                    "synchronous": False,
                },
                "sandbox": {"timeout": 1800},
            },
        )

    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("prod-modal",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.MODAL
    assert mock_create_stack.call_args.kwargs["activate"] is False
    assert mock_create_stack.call_args.kwargs["sandbox_flavor"] == "modal"
    modal_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(modal_spec, ModalStackSpec)
    assert modal_spec.model_dump(mode="json") == {
        "artifact_store": "gs://my-bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/my-project/repo",
        "region": None,
        "subscription_id": None,
        "credentials": None,
        "verify": True,
    }
    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.model_dump() == {
        "orchestrator": {
            "token_id": "ak-test",
            "token_secret": "as-test",
            "synchronous": False,
        },
        "artifact_store": {},
        "container_registry": {},
        "sandbox": {"timeout": 1800},
    }
    assert payload == {
        "id": "stack-modal-id",
        "name": "prod-modal",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": [
            "prod-modal (orchestrator)",
            "prod-modal (artifact_store)",
            "prod-modal (container_registry)",
            "prod-modal-image-builder (image_builder)",
            "prod-modal (sandbox)",
        ],
        "stack_type": "modal",
        "resources": {
            "provider": "gcp",
            "artifact_store": "gs://my-bucket/kitaru",
            "container_registry": "us-central1-docker.pkg.dev/my-project/repo",
            "sandbox": "modal",
        },
    }


def test_manage_stack_create_modal_requires_storage_and_registry() -> None:
    """Modal MCP create should reject missing required inputs early."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match=r'`stack_type="modal"` requires:'),
    ):
        manage_stack("create", "prod-modal", stack_type="modal")

    mock_create_stack.assert_not_called()


def test_manage_stack_create_modal_accepts_aws_cloud_credentials() -> None:
    """MCP Modal create should pass AWS cloud credentials separately from tokens."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-modal-id", name="prod-modal", is_active=False),
            previous_active_stack=None,
            components_created=(
                "prod-modal (orchestrator)",
                "prod-modal (artifact_store)",
                "prod-modal (container_registry)",
                "prod-modal-image-builder (image_builder)",
            ),
            stack_type="modal",
            service_connectors_created=("prod-modal-aws",),
            resources={
                "provider": "aws",
                "artifact_store": "s3://my-bucket/kitaru",
                "container_registry": (
                    "123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru"
                ),
                "region": "eu-west-1",
            },
        )

        payload = manage_stack(
            "create",
            "prod-modal",
            stack_type="modal",
            artifact_store="s3://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
            region="eu-west-1",
            credentials="aws-profile:ml-team",
            verify=False,
            extra={
                "orchestrator": {
                    "token_id": "ak-test",
                    "token_secret": "as-test",
                }
            },
        )

    modal_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(modal_spec, ModalStackSpec)
    assert modal_spec.model_dump(mode="json") == {
        "artifact_store": "s3://my-bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
        "region": "eu-west-1",
        "subscription_id": None,
        "credentials": "aws-profile:ml-team",
        "verify": False,
    }
    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.orchestrator == {
        "token_id": "ak-test",
        "token_secret": "as-test",
    }
    assert payload["components_created"] == [
        "prod-modal (orchestrator)",
        "prod-modal (artifact_store)",
        "prod-modal (container_registry)",
        "prod-modal-image-builder (image_builder)",
    ]
    assert payload["service_connectors_created"] == ["prod-modal-aws"]


def test_manage_stack_create_modal_rejects_aws_credentials_without_region() -> None:
    """AWS-backed Modal connector requests need region; Modal placement stays extra."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match="AWS-backed Modal cloud credentials"),
    ):
        manage_stack(
            "create",
            "prod-modal",
            stack_type="modal",
            artifact_store="s3://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
            credentials="aws-profile:ml-team",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_modal_rejects_no_verify_without_connector_input() -> None:
    """MCP Modal verify=False should not request a cloud connector by itself."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match="only applies when Kitaru is creating"),
    ):
        manage_stack(
            "create",
            "prod-modal",
            stack_type="modal",
            artifact_store="s3://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
            verify=False,
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_modal_rejects_mismatched_connectorless_resources() -> None:
    """MCP Modal create should reject mixed providers without connector inputs."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match="same cloud provider"),
    ):
        manage_stack(
            "create",
            "prod-modal",
            stack_type="modal",
            artifact_store="gs://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_modal_accepts_gcp_cloud_credentials() -> None:
    """MCP Modal create should pass GCP connector inputs through."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-modal-id", name="gcp-modal", is_active=False),
            previous_active_stack=None,
            components_created=(),
            stack_type="modal",
            service_connectors_created=("gcp-modal-gcp",),
            resources={},
        )

        manage_stack(
            "create",
            "gcp-modal",
            stack_type="modal",
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/repo",
            region="us-central1",
            credentials="gcp-service-account:/tmp/key.json",
        )

    modal_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(modal_spec, ModalStackSpec)
    assert modal_spec.model_dump(mode="json") == {
        "artifact_store": "gs://my-bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/my-project/repo",
        "region": "us-central1",
        "subscription_id": None,
        "credentials": "gcp-service-account:/tmp/key.json",
        "verify": True,
    }


def test_manage_stack_create_modal_accepts_azure_cloud_credentials() -> None:
    """MCP Modal create should pass Azure connector inputs through."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-modal-id", name="azure-modal", is_active=False),
            previous_active_stack=None,
            components_created=(),
            stack_type="modal",
            service_connectors_created=("azure-modal-azure",),
            resources={},
        )

        manage_stack(
            "create",
            "azure-modal",
            stack_type="modal",
            artifact_store="az://container/kitaru",
            container_registry="demo.azurecr.io/kitaru",
            subscription_id="00000000-0000-0000-0000-000000000123",
            credentials="azure-access-token:token-123",
            verify=False,
        )

    modal_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(modal_spec, ModalStackSpec)
    assert modal_spec.model_dump(mode="json") == {
        "artifact_store": "az://container/kitaru",
        "container_registry": "demo.azurecr.io/kitaru",
        "region": None,
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "credentials": "azure-access-token:token-123",
        "verify": False,
    }


def test_manage_stack_create_kubernetes_passes_explicit_sandbox() -> None:
    """MCP callers should be able to attach an explicit sandbox to remote stacks."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-k8s-id", name="k8s-dev", is_active=False),
            previous_active_stack=None,
            components_created=(
                "k8s-dev (orchestrator)",
                "k8s-dev (artifact_store)",
                "k8s-dev (container_registry)",
                "k8s-dev (sandbox)",
            ),
            stack_type="kubernetes",
            service_connectors_created=(),
            resources={"sandbox": "local"},
        )

        payload = manage_stack(
            "create",
            "k8s-dev",
            stack_type="kubernetes",
            artifact_store="s3://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
            cluster="cluster-1",
            region="eu-west-1",
            sandbox="local",
        )

    assert mock_create_stack.call_args.kwargs["sandbox_flavor"] == "local"
    assert payload["resources"] == {"sandbox": "local"}


def test_manage_stack_create_remote_rejects_sandbox_extra_without_sandbox() -> None:
    """MCP sandbox overrides require a sandbox on remote stacks."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError) as exc_info,
    ):
        manage_stack(
            "create",
            "vertex-dev",
            stack_type="vertex",
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/my-repo",
            region="us-central1",
            extra={"sandbox": {"forward_env": False}},
        )

    message = str(exc_info.value)
    assert "sandbox" in message
    assert "extra" in message
    mock_create_stack.assert_not_called()


def test_manage_stack_delete_rejects_sandbox_option() -> None:
    """Sandbox selection is only valid for create actions."""
    with (
        patch("kitaru._config._stacks._delete_stack_operation") as mock_delete_stack,
        pytest.raises(ValueError, match="`sandbox`"),
    ):
        manage_stack("delete", "dev", sandbox="local")

    mock_delete_stack.assert_not_called()


@pytest.mark.parametrize(
    "missing_field",
    ["artifact_store", "container_registry", "cluster", "region"],
)
def test_manage_stack_create_kubernetes_requires_required_fields(
    missing_field: str,
) -> None:
    """Kubernetes MCP create should reject missing required inputs early."""
    create_kwargs: dict[str, str | None] = {
        "stack_type": "kubernetes",
        "artifact_store": "s3://my-bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru",
        "cluster": "cluster-1",
        "region": "eu-west-1",
    }
    create_kwargs[missing_field] = None

    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match="requires:"),
    ):
        manage_stack("create", "k8s-dev", **create_kwargs)

    mock_create_stack.assert_not_called()


_REMOTE_STACK_TYPE_ERROR = (
    'Remote stack options require `stack_type="kubernetes"`, '
    '`stack_type="vertex"`, `stack_type="sagemaker"`, '
    '`stack_type="azureml"`, or `stack_type="modal"`'
)
_CLOUD_CONNECTOR_STACK_TYPE_ERROR = (
    'Remote stack options require `stack_type="kubernetes"`, '
    '`stack_type="vertex"`, `stack_type="sagemaker"`, '
    '`stack_type="azureml"`, or `stack_type="modal"`'
)


@pytest.mark.parametrize(
    ("extra_kwargs", "expected_message"),
    [
        ({"artifact_store": "s3://my-bucket/kitaru"}, _REMOTE_STACK_TYPE_ERROR),
        (
            {
                "container_registry": (
                    "123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru"
                )
            },
            _REMOTE_STACK_TYPE_ERROR,
        ),
        (
            {"cluster": "cluster-1"},
            'Kubernetes-only options require `stack_type="kubernetes"`: `cluster`',
        ),
        ({"region": "eu-west-1"}, _CLOUD_CONNECTOR_STACK_TYPE_ERROR),
        (
            {"namespace": "ml-team"},
            'Kubernetes-only options require `stack_type="kubernetes"`: `namespace`',
        ),
        ({"credentials": "implicit"}, _CLOUD_CONNECTOR_STACK_TYPE_ERROR),
        ({"verify": False}, _CLOUD_CONNECTOR_STACK_TYPE_ERROR),
    ],
)
def test_manage_stack_create_local_rejects_kubernetes_only_options(
    extra_kwargs: dict[str, Any],
    expected_message: str,
) -> None:
    """Local MCP create should reject remote-stack inputs."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match=expected_message),
    ):
        manage_stack("create", "dev", **extra_kwargs)

    mock_create_stack.assert_not_called()


def test_manage_stack_create_kubernetes_normalizes_blank_optional_inputs() -> None:
    """Blank optional Kubernetes inputs should normalize cleanly before dispatch."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-k8s-id", name="k8s-dev", is_active=True),
            previous_active_stack=None,
            components_created=(
                "k8s-dev (orchestrator)",
                "k8s-dev (artifact_store)",
                "k8s-dev (container_registry)",
            ),
            stack_type="kubernetes",
            service_connectors_created=(),
            resources=None,
        )

        manage_stack(
            "create",
            "k8s-dev",
            stack_type="kubernetes",
            artifact_store="  gs://my-bucket/kitaru  ",
            container_registry=(
                "  europe-west4-docker.pkg.dev/my-project/my-repo/my-image  "
            ),
            cluster="  cluster-1  ",
            region="  europe-west4  ",
            namespace="   ",
            credentials="   ",
        )

    kubernetes_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(kubernetes_spec, KubernetesStackSpec)
    assert kubernetes_spec.provider == CloudProvider.GCP
    assert kubernetes_spec.artifact_store == "gs://my-bucket/kitaru"
    assert (
        kubernetes_spec.container_registry
        == "europe-west4-docker.pkg.dev/my-project/my-repo/my-image"
    )
    assert kubernetes_spec.cluster == "cluster-1"
    assert kubernetes_spec.region == "europe-west4"
    assert kubernetes_spec.namespace == "default"
    assert kubernetes_spec.credentials is None
    assert kubernetes_spec.verify is True


def test_manage_stack_create_vertex_passes_extra_and_async_overrides() -> None:
    """MCP manage_stack(create) should pass structured component overrides."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-vertex-id", name="vertex-dev", is_active=False),
            previous_active_stack=None,
            components_created=(
                "vertex-dev (orchestrator)",
                "vertex-dev (artifact_store)",
                "vertex-dev (container_registry)",
            ),
            stack_type="vertex",
            service_connectors_created=(),
            resources=None,
        )

        manage_stack(
            "create",
            "vertex-dev",
            stack_type="vertex",
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/my-repo",
            region="us-central1",
            async_mode=True,
            extra={
                "orchestrator": {"pipeline_root": "gs://bucket/root"},
                "container_registry": {"default_repository": "team-ml"},
            },
        )

    overrides = mock_create_stack.call_args.kwargs["component_overrides"]
    assert isinstance(overrides, StackComponentConfigOverrides)
    assert overrides.model_dump() == {
        "orchestrator": {
            "pipeline_root": "gs://bucket/root",
            "synchronous": False,
        },
        "artifact_store": {},
        "container_registry": {"default_repository": "team-ml"},
        "sandbox": {},
    }


def test_manage_stack_create_async_mode_rejected_for_local() -> None:
    """Local MCP stacks should reject the async convenience flag."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                r"`async_mode` requires `stack_type=\"kubernetes\"`, "
                r"`stack_type=\"vertex\"`, `stack_type=\"sagemaker\"`, "
                r"`stack_type=\"azureml\"`, or `stack_type=\"modal\"`\."
            ),
        ),
    ):
        manage_stack("create", "dev", async_mode=True)

    mock_create_stack.assert_not_called()


def test_manage_stack_create_vertex_dispatches_structured_spec() -> None:
    """MCP Vertex create should build a shared serialized stack result."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-vertex-id", name="vertex-dev", is_active=False),
            previous_active_stack=None,
            components_created=(
                "vertex-dev (orchestrator)",
                "vertex-dev (artifact_store)",
                "vertex-dev (container_registry)",
            ),
            stack_type="vertex",
            service_connectors_created=("vertex-dev-gcp",),
            resources={
                "provider": "gcp",
                "region": "us-central1",
                "artifact_store": "gs://my-bucket/kitaru",
                "container_registry": "us-central1-docker.pkg.dev/my-project/my-repo",
            },
        )

        payload = manage_stack(
            "create",
            "vertex-dev",
            stack_type="vertex",
            activate=False,
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/my-repo",
            region="us-central1",
            verify=False,
        )

    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("vertex-dev",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.VERTEX
    assert mock_create_stack.call_args.kwargs["activate"] is False
    vertex_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(vertex_spec, VertexStackSpec)
    assert vertex_spec.model_dump(mode="json") == {
        "artifact_store": "gs://my-bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/my-project/my-repo",
        "region": "us-central1",
        "credentials": None,
        "verify": False,
    }
    assert payload == {
        "id": "stack-vertex-id",
        "name": "vertex-dev",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": [
            "vertex-dev (orchestrator)",
            "vertex-dev (artifact_store)",
            "vertex-dev (container_registry)",
        ],
        "stack_type": "vertex",
        "service_connectors_created": ["vertex-dev-gcp"],
        "resources": {
            "provider": "gcp",
            "region": "us-central1",
            "artifact_store": "gs://my-bucket/kitaru",
            "container_registry": "us-central1-docker.pkg.dev/my-project/my-repo",
        },
    }


@pytest.mark.parametrize(
    "missing_field",
    ["artifact_store", "container_registry", "region"],
)
def test_manage_stack_create_vertex_requires_required_fields(
    missing_field: str,
) -> None:
    """Vertex MCP create should reject missing required inputs early."""
    create_kwargs: dict[str, str | None] = {
        "stack_type": "vertex",
        "artifact_store": "gs://my-bucket/kitaru",
        "container_registry": "us-central1-docker.pkg.dev/my-project/my-repo",
        "region": "us-central1",
    }
    create_kwargs[missing_field] = None

    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=r'`stack_type="vertex"` requires:',
        ),
    ):
        manage_stack("create", "vertex-dev", **create_kwargs)

    mock_create_stack.assert_not_called()


@pytest.mark.parametrize(
    "missing_field",
    ["artifact_store", "container_registry", "region", "execution_role"],
)
def test_manage_stack_create_sagemaker_requires_required_fields(
    missing_field: str,
) -> None:
    """SageMaker MCP create should reject missing required inputs early."""
    create_kwargs: dict[str, str | None] = {
        "stack_type": "sagemaker",
        "artifact_store": "s3://my-bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "region": "us-east-1",
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
    }
    create_kwargs[missing_field] = None

    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=r'`stack_type="sagemaker"` requires:',
        ),
    ):
        manage_stack("create", "sagemaker-dev", **create_kwargs)

    mock_create_stack.assert_not_called()


def test_manage_stack_create_vertex_rejects_kubernetes_only_options() -> None:
    """Vertex MCP create should reject Kubernetes-only options."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                'Kubernetes-only options require `stack_type="kubernetes"`: `cluster`'
            ),
        ),
    ):
        manage_stack(
            "create",
            "vertex-dev",
            stack_type="vertex",
            artifact_store="gs://my-bucket/kitaru",
            container_registry="us-central1-docker.pkg.dev/my-project/my-repo",
            region="us-central1",
            cluster="cluster-1",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_local_rejects_sagemaker_only_options() -> None:
    """Local MCP create should reject SageMaker-only inputs."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                'SageMaker-only options require `stack_type="sagemaker"`: '
                "`execution_role`"
            ),
        ),
    ):
        manage_stack(
            "create",
            "dev",
            execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_vertex_normalizes_blank_optional_inputs() -> None:
    """Blank optional Vertex inputs should normalize cleanly before dispatch."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-vertex-id", name="vertex-dev", is_active=True),
            previous_active_stack=None,
            components_created=(
                "vertex-dev (orchestrator)",
                "vertex-dev (artifact_store)",
                "vertex-dev (container_registry)",
            ),
            stack_type="vertex",
            service_connectors_created=(),
            resources=None,
        )

        manage_stack(
            "create",
            "vertex-dev",
            stack_type="vertex",
            artifact_store="  gs://my-bucket/kitaru  ",
            container_registry="  us-central1-docker.pkg.dev/my-project/my-repo  ",
            region="  us-central1  ",
            credentials="   ",
        )

    vertex_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(vertex_spec, VertexStackSpec)
    assert vertex_spec.artifact_store == "gs://my-bucket/kitaru"
    assert (
        vertex_spec.container_registry
        == "us-central1-docker.pkg.dev/my-project/my-repo"
    )
    assert vertex_spec.region == "us-central1"
    assert vertex_spec.credentials is None
    assert vertex_spec.verify is True


def test_manage_stack_create_sagemaker_dispatches_structured_spec() -> None:
    """MCP SageMaker create should build a shared serialized stack result."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(
                id="stack-sagemaker-id",
                name="sagemaker-dev",
                is_active=False,
            ),
            previous_active_stack=None,
            components_created=(
                "sagemaker-dev (orchestrator)",
                "sagemaker-dev (artifact_store)",
                "sagemaker-dev (container_registry)",
            ),
            stack_type="sagemaker",
            service_connectors_created=("sagemaker-dev-aws",),
            resources={
                "provider": "aws",
                "region": "us-east-1",
                "artifact_store": "s3://my-bucket/kitaru",
                "container_registry": ("123456789012.dkr.ecr.us-east-1.amazonaws.com"),
                "execution_role": ("arn:aws:iam::123456789012:role/SageMakerRole"),
            },
        )

        payload = manage_stack(
            "create",
            "sagemaker-dev",
            stack_type="sagemaker",
            activate=False,
            artifact_store="s3://my-bucket/kitaru",
            container_registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            region="us-east-1",
            execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
            verify=False,
        )

    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("sagemaker-dev",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.SAGEMAKER
    assert mock_create_stack.call_args.kwargs["activate"] is False
    sagemaker_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(sagemaker_spec, SagemakerStackSpec)
    assert sagemaker_spec.model_dump(mode="json") == {
        "artifact_store": "s3://my-bucket/kitaru",
        "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        "region": "us-east-1",
        "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
        "credentials": None,
        "verify": False,
    }

    assert payload == {
        "id": "stack-sagemaker-id",
        "name": "sagemaker-dev",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": [
            "sagemaker-dev (orchestrator)",
            "sagemaker-dev (artifact_store)",
            "sagemaker-dev (container_registry)",
        ],
        "stack_type": "sagemaker",
        "service_connectors_created": ["sagemaker-dev-aws"],
        "resources": {
            "provider": "aws",
            "region": "us-east-1",
            "artifact_store": "s3://my-bucket/kitaru",
            "container_registry": "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "execution_role": "arn:aws:iam::123456789012:role/SageMakerRole",
        },
    }


def test_manage_stack_create_azureml_dispatches_structured_spec() -> None:
    """MCP AzureML create should build a shared serialized stack result."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(
                id="stack-azure-id",
                name="azure-dev",
                is_active=False,
            ),
            previous_active_stack=None,
            components_created=(
                "azure-dev (orchestrator)",
                "azure-dev (artifact_store)",
                "azure-dev (container_registry)",
            ),
            stack_type="azureml",
            service_connectors_created=("azure-dev-connector",),
            resources={
                "provider": "azure",
                "subscription_id": "00000000-0000-0000-0000-000000000123",
                "resource_group": "rg-demo",
                "workspace": "ws-demo",
                "region": "westeurope",
                "artifact_store": "az://container/kitaru",
                "container_registry": "demo.azurecr.io/team/image",
            },
        )

        payload = manage_stack(
            "create",
            "azure-dev",
            stack_type="azureml",
            activate=False,
            artifact_store="az://container/kitaru",
            container_registry="demo.azurecr.io/team/image",
            subscription_id="00000000-0000-0000-0000-000000000123",
            resource_group="rg-demo",
            workspace="ws-demo",
            region="westeurope",
            verify=False,
        )

    mock_create_stack.assert_called_once()
    assert mock_create_stack.call_args.args == ("azure-dev",)
    assert mock_create_stack.call_args.kwargs["stack_type"] == StackType.AZUREML
    assert mock_create_stack.call_args.kwargs["activate"] is False
    azureml_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(azureml_spec, AzureMLStackSpec)
    assert azureml_spec.model_dump(mode="json") == {
        "artifact_store": "az://container/kitaru",
        "container_registry": "demo.azurecr.io/team/image",
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-demo",
        "workspace": "ws-demo",
        "region": "westeurope",
        "credentials": None,
        "verify": False,
    }

    assert payload == {
        "id": "stack-azure-id",
        "name": "azure-dev",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": [
            "azure-dev (orchestrator)",
            "azure-dev (artifact_store)",
            "azure-dev (container_registry)",
        ],
        "stack_type": "azureml",
        "service_connectors_created": ["azure-dev-connector"],
        "resources": {
            "provider": "azure",
            "subscription_id": "00000000-0000-0000-0000-000000000123",
            "resource_group": "rg-demo",
            "workspace": "ws-demo",
            "region": "westeurope",
            "artifact_store": "az://container/kitaru",
            "container_registry": "demo.azurecr.io/team/image",
        },
    }


@pytest.mark.parametrize(
    "missing_field",
    [
        "artifact_store",
        "container_registry",
        "subscription_id",
        "resource_group",
        "workspace",
    ],
)
def test_manage_stack_create_azureml_requires_required_fields(
    missing_field: str,
) -> None:
    """AzureML MCP create should reject missing required inputs early."""
    create_kwargs: dict[str, str | None] = {
        "stack_type": "azureml",
        "artifact_store": "az://container/kitaru",
        "container_registry": "demo.azurecr.io/team/image",
        "subscription_id": "00000000-0000-0000-0000-000000000123",
        "resource_group": "rg-demo",
        "workspace": "ws-demo",
    }
    create_kwargs[missing_field] = None

    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=r"`stack_type=\"azureml\"` requires:",
        ),
    ):
        manage_stack("create", "azure-dev", **create_kwargs)

    mock_create_stack.assert_not_called()


def test_manage_stack_create_local_rejects_azureml_only_options() -> None:
    """Local MCP create should reject AzureML-only inputs."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                'Stack-specific options require `stack_type="azureml"` or '
                '`stack_type="modal"`: `subscription_id`'
            ),
        ),
    ):
        manage_stack(
            "create",
            "dev",
            subscription_id="00000000-0000-0000-0000-000000000123",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_azureml_rejects_sagemaker_only_options() -> None:
    """AzureML MCP create should reject SageMaker-only inputs."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                'SageMaker-only options require `stack_type="sagemaker"`: '
                "`execution_role`"
            ),
        ),
    ):
        manage_stack(
            "create",
            "azure-dev",
            stack_type="azureml",
            artifact_store="az://container/kitaru",
            container_registry="demo.azurecr.io/team/image",
            subscription_id="00000000-0000-0000-0000-000000000123",
            resource_group="rg-demo",
            workspace="ws-demo",
            execution_role="arn:aws:iam::123456789012:role/SageMakerRole",
        )

    mock_create_stack.assert_not_called()


def test_manage_stack_create_azureml_normalizes_blank_optional_inputs() -> None:
    """Blank optional AzureML inputs should normalize cleanly before dispatch."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-azure-id", name="azure-dev", is_active=True),
            previous_active_stack=None,
            components_created=(
                "azure-dev (orchestrator)",
                "azure-dev (artifact_store)",
                "azure-dev (container_registry)",
            ),
            stack_type="azureml",
            service_connectors_created=(),
            resources=None,
        )

        manage_stack(
            "create",
            "azure-dev",
            stack_type="azureml",
            artifact_store="  az://container/kitaru  ",
            container_registry="  demo.azurecr.io/team/image  ",
            subscription_id=" 00000000-0000-0000-0000-000000000123 ",
            resource_group=" rg-demo ",
            workspace=" ws-demo ",
            region="   ",
            credentials="   ",
        )

    azureml_spec = mock_create_stack.call_args.kwargs["remote_spec"]
    assert isinstance(azureml_spec, AzureMLStackSpec)
    assert azureml_spec.artifact_store == "az://container/kitaru"
    assert azureml_spec.container_registry == "demo.azurecr.io/team/image"
    assert azureml_spec.subscription_id == "00000000-0000-0000-0000-000000000123"
    assert azureml_spec.resource_group == "rg-demo"
    assert azureml_spec.workspace == "ws-demo"
    assert azureml_spec.region is None
    assert azureml_spec.credentials is None
    assert azureml_spec.verify is True


def test_manage_stack_create_kubernetes_rejects_unknown_provider() -> None:
    """MCP create should fail fast when provider inference cannot resolve."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(ValueError, match="Cannot infer cloud provider"),
    ):
        manage_stack(
            "create",
            "k8s-dev",
            stack_type="kubernetes",
            artifact_store="az://my-bucket/kitaru",
            container_registry="registry.example.com/kitaru",
            cluster="cluster-1",
            region="westeurope",
        )

    mock_create_stack.assert_not_called()


@pytest.mark.parametrize(
    "delete_kwargs",
    [
        {"stack_type": "kubernetes"},
        {"artifact_store": "s3://my-bucket/kitaru"},
        {"subscription_id": "00000000-0000-0000-0000-000000000123"},
        {"resource_group": "rg-demo"},
        {"workspace": "ws-demo"},
        {"execution_role": "arn:aws:iam::123456789012:role/SageMakerRole"},
        {"extra": {"orchestrator": {"synchronous": False}}},
        {"async_mode": True},
        {"verify": False},
    ],
)
def test_manage_stack_delete_rejects_kubernetes_create_options(
    delete_kwargs: dict[str, Any],
) -> None:
    """Delete should reject stack-creation inputs."""
    with (
        patch("kitaru._config._stacks._delete_stack_operation") as mock_delete_stack,
        pytest.raises(
            ValueError,
            match='Stack create options are only valid when action="create"',
        ),
    ):
        manage_stack("delete", "dev", **delete_kwargs)

    mock_delete_stack.assert_not_called()


def test_manage_stack_rejects_irrelevant_flags() -> None:
    """MCP manage_stack should reject flag combinations that do not fit the action."""
    with pytest.raises(ValueError, match='only valid when action="delete"'):
        manage_stack("create", "dev", recursive=True)

    with pytest.raises(ValueError, match='only valid when action="create"'):
        manage_stack("delete", "dev", activate=False)


# ── Per-tool analytics tracking ──────────────────────────────────────────────


def test_tracked_mcp_tool_fires_success_event() -> None:
    """tracked_mcp_tool decorator emits a success event after a successful operation."""

    @tracked_mcp_tool
    def _sample_tool() -> dict[str, str]:
        return {"key": "value"}

    with patch("kitaru.mcp.server.track") as mock_track:
        result = _sample_tool()

    assert result == {"key": "value"}
    mock_track.assert_called_once_with(
        "Kitaru MCP tool called",
        {"tool_name": "_sample_tool", "success": True},
    )


def test_tracked_mcp_tool_fires_failure_event_and_reraises() -> None:
    """tracked_mcp_tool decorator emits a failure event and re-raises on error."""

    @tracked_mcp_tool
    def _failing_tool() -> None:
        raise RuntimeError("boom")

    with (
        patch("kitaru.mcp.server.track") as mock_track,
        pytest.raises(RuntimeError, match="boom"),
    ):
        _failing_tool()

    mock_track.assert_called_once_with(
        "Kitaru MCP tool called",
        {
            "tool_name": "_failing_tool",
            "success": False,
            "error_type": "RuntimeError",
        },
    )


def test_tracked_mcp_tool_preserves_function_name() -> None:
    """tracked_mcp_tool should preserve the wrapped function's __name__."""

    @tracked_mcp_tool
    def kitaru_my_custom_tool() -> str:
        return "ok"

    with patch("kitaru.mcp.server.track") as mock_track:
        kitaru_my_custom_tool()

    mock_track.assert_called_once()
    call_args = mock_track.call_args[0]
    assert call_args[1]["tool_name"] == "kitaru_my_custom_tool"


def test_tracked_mcp_tool_captures_concrete_error_type() -> None:
    """The error_type metadata should reflect the actual exception class."""

    @tracked_mcp_tool
    def _value_error_tool() -> None:
        raise ValueError("bad input")

    with (
        patch("kitaru.mcp.server.track") as mock_track,
        pytest.raises(ValueError),
    ):
        _value_error_tool()

    assert mock_track.call_args[0][1]["error_type"] == "ValueError"


def test_start_local_server_fires_analytics_on_success() -> None:
    """kitaru_start_local_server should emit MCP tool analytics after success."""
    with (
        patch(
            "kitaru.mcp.server.start_or_connect_local_server",
            return_value=SimpleNamespace(
                url="http://127.0.0.1:8383",
                action="started",
            ),
        ),
        patch("kitaru.mcp.server.track") as mock_track,
    ):
        kitaru_start_local_server(port=9090, timeout=45)

    mock_track.assert_called_once_with(
        "Kitaru MCP tool called",
        {"tool_name": "kitaru_start_local_server", "success": True},
    )


def test_stop_local_server_fires_analytics_on_success() -> None:
    """kitaru_stop_local_server should emit MCP tool analytics after success."""
    with (
        patch(
            "kitaru.mcp.server.stop_registered_local_server",
            return_value=SimpleNamespace(stopped=True, url="http://127.0.0.1:8383"),
        ),
        patch("kitaru.mcp.server.track") as mock_track,
    ):
        kitaru_stop_local_server()

    mock_track.assert_called_once_with(
        "Kitaru MCP tool called",
        {"tool_name": "kitaru_stop_local_server", "success": True},
    )


def test_start_local_server_fires_analytics_on_failure() -> None:
    """kitaru_start_local_server should emit failure analytics when helper raises."""
    with (
        patch(
            "kitaru.mcp.server.start_or_connect_local_server",
            side_effect=RuntimeError("missing deps"),
        ),
        patch("kitaru.mcp.server.track") as mock_track,
        pytest.raises(RuntimeError, match="missing deps"),
    ):
        kitaru_start_local_server()

    mock_track.assert_called_once_with(
        "Kitaru MCP tool called",
        {
            "tool_name": "kitaru_start_local_server",
            "success": False,
            "error_type": "RuntimeError",
        },
    )


# ---------------------------------------------------------------------------
# kitaru_info
# ---------------------------------------------------------------------------


class TestKitaruInfo:
    """Tests for the kitaru_info MCP tool."""

    def test_delegates_to_snapshot_builder_and_serializer(self) -> None:
        """Default call builds snapshot, serializes it, returns the dict."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.2.0",
            connection="local Kitaru server",
            connection_target="http://127.0.0.1:9000",
            config_directory="/tmp/kitaru",
        )

        with (
            patch(
                "kitaru.inspection.build_runtime_snapshot",
                return_value=snapshot,
            ) as mock_build,
            patch(
                "kitaru.inspection.serialize_runtime_snapshot",
                return_value={"sdk_version": "0.2.0"},
            ) as mock_serialize,
        ):
            payload = kitaru_info()

        mock_build.assert_called_once_with(
            include_packages=False,
            package_names=None,
            include_environment_type=False,
            include_provenance_details=False,
        )
        mock_serialize.assert_called_once_with(
            snapshot,
            include_provenance_details=False,
        )
        assert payload == {"sdk_version": "0.2.0"}

    @pytest.mark.parametrize(
        ("call_kwargs", "expected_build_kwargs"),
        [
            pytest.param(
                {"all": True},
                {
                    "include_packages": True,
                    "package_names": None,
                    "include_environment_type": True,
                    "include_provenance_details": True,
                },
                id="all_includes_packages_and_env",
            ),
            pytest.param(
                {"all_packages": True},
                {
                    "include_packages": True,
                    "package_names": None,
                    "include_environment_type": False,
                    "include_provenance_details": False,
                },
                id="all_packages_without_env",
            ),
            pytest.param(
                {"packages": ["zenml", "kitaru"]},
                {
                    "include_packages": False,
                    "package_names": ["zenml", "kitaru"],
                    "include_environment_type": False,
                    "include_provenance_details": False,
                },
                id="specific_packages",
            ),
            pytest.param(
                {"all": True, "packages": ["zenml"]},
                {
                    "include_packages": True,
                    "package_names": None,
                    "include_environment_type": True,
                    "include_provenance_details": True,
                },
                id="all_overrides_specific_packages",
            ),
        ],
    )
    def test_flag_combinations(
        self,
        call_kwargs: dict[str, Any],
        expected_build_kwargs: dict[str, Any],
    ) -> None:
        """Flag combinations map correctly to build_runtime_snapshot args."""
        snapshot = RuntimeSnapshot(
            sdk_version="0.2.0",
            connection="local",
            connection_target="http://127.0.0.1:9000",
            config_directory="/tmp/kitaru",
        )

        with (
            patch(
                "kitaru.inspection.build_runtime_snapshot",
                return_value=snapshot,
            ) as mock_build,
            patch(
                "kitaru.inspection.serialize_runtime_snapshot",
                return_value={},
            ) as mock_serialize,
        ):
            kitaru_info(**call_kwargs)

        mock_build.assert_called_once_with(**expected_build_kwargs)
        mock_serialize.assert_called_once_with(
            snapshot,
            include_provenance_details=expected_build_kwargs[
                "include_provenance_details"
            ],
        )


# ---------------------------------------------------------------------------
# kitaru_clean_preview
# ---------------------------------------------------------------------------


class TestKitaruCleanPreview:
    """Tests for the kitaru_clean_preview MCP tool."""

    def test_returns_dry_run_result_for_project_scope(self) -> None:
        """Preview of project scope returns serialized dry-run payload."""
        from kitaru._cleanup import (
            CleanScope,
            CleanupPlan,
            CleanupPreviewEntry,
            PreviewEntryType,
        )

        plan = CleanupPlan(
            scope=CleanScope.PROJECT,
            repo_root="/repo",
            project_config_path="/repo/.kitaru",
            preview_entries=(
                CleanupPreviewEntry(
                    path="/repo/.kitaru",
                    entry_type=PreviewEntryType.DIRECTORY,
                    size_bytes=1024,
                ),
            ),
            total_bytes=1024,
        )

        with patch(
            "kitaru.mcp.server.cleanup.build_cleanup_plan",
            return_value=plan,
        ) as mock_plan:
            payload = kitaru_clean_preview(scope="project")

        mock_plan.assert_called_once_with(CleanScope.PROJECT)
        assert payload["dry_run"] is True
        assert payload["scope"] == "project"
        assert payload["total_bytes"] == 1024
        assert len(payload["preview"]) == 1
        assert payload["preview"][0]["path"] == "/repo/.kitaru"

    def test_returns_dry_run_result_for_global_scope(self) -> None:
        """Preview of global scope works and includes warnings."""
        from kitaru._cleanup import CleanScope, CleanupPlan

        plan = CleanupPlan(
            scope=CleanScope.GLOBAL,
            global_config_root="/custom/kitaru",
            total_bytes=5000,
            custom_config_path_warning=(
                "Cleaning custom config path /custom/kitaru (set by KITARU_CONFIG_PATH)"
            ),
        )

        with patch(
            "kitaru.mcp.server.cleanup.build_cleanup_plan",
            return_value=plan,
        ):
            payload = kitaru_clean_preview(scope="global")

        assert payload["dry_run"] is True
        assert payload["scope"] == "global"
        assert len(payload["warnings"]) == 1
        assert "KITARU_CONFIG_PATH" in payload["warnings"][0]

    def test_all_scope_accepted(self) -> None:
        """scope='all' is a valid scope."""
        from kitaru._cleanup import CleanScope, CleanupPlan

        plan = CleanupPlan(scope=CleanScope.ALL, total_bytes=0)

        with patch(
            "kitaru.mcp.server.cleanup.build_cleanup_plan",
            return_value=plan,
        ) as mock_plan:
            payload = kitaru_clean_preview(scope="all")

        mock_plan.assert_called_once_with(CleanScope.ALL)
        assert payload["scope"] == "all"
        assert payload["dry_run"] is True

    def test_never_executes_cleanup(self) -> None:
        """The preview tool must never call execute_cleanup_plan."""
        from kitaru._cleanup import CleanScope, CleanupPlan

        plan = CleanupPlan(scope=CleanScope.GLOBAL, total_bytes=0)

        with (
            patch(
                "kitaru.mcp.server.cleanup.build_cleanup_plan",
                return_value=plan,
            ),
            patch(
                "kitaru.mcp.server.cleanup.execute_cleanup_plan",
            ) as mock_execute,
        ):
            kitaru_clean_preview(scope="global")

        mock_execute.assert_not_called()

    def test_invalid_scope_raises(self) -> None:
        """Invalid scope string raises ValueError."""
        with pytest.raises(ValueError, match="workspace"):
            kitaru_clean_preview(scope="workspace")

    def test_no_project_raises_for_project_scope(self) -> None:
        """Missing project raises KitaruUsageError for project scope."""
        from kitaru.errors import KitaruUsageError

        with (
            patch(
                "kitaru.mcp.server.cleanup.build_cleanup_plan",
                side_effect=KitaruUsageError("No Kitaru project found."),
            ),
            pytest.raises(KitaruUsageError, match="No Kitaru project found"),
        ):
            kitaru_clean_preview(scope="project")
