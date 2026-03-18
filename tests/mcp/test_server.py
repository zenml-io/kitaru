"""Tests for Kitaru MCP server tools."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import kitaru._interface_executions as execution_interface
import kitaru._interface_stacks as stack_interface
from kitaru._flow_loading import _load_flow_target as _load_shared_flow_target
from kitaru.client import ExecutionStatus
from kitaru.config import (
    ActiveEnvironmentVariable,
    AzureMLStackSpec,
    CloudProvider,
    KubernetesStackSpec,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackInfo,
    StackType,
    VertexStackSpec,
)
from kitaru.inspection import RuntimeSnapshot
from kitaru.mcp.server import (
    get_execution_logs,
    kitaru_artifacts_get,
    kitaru_artifacts_list,
    kitaru_executions_cancel,
    kitaru_executions_get,
    kitaru_executions_input,
    kitaru_executions_latest,
    kitaru_executions_list,
    kitaru_executions_replay,
    kitaru_executions_retry,
    kitaru_executions_run,
    kitaru_stacks_list,
    kitaru_status,
    manage_stack,
    tracked_mcp_tool,
)


def _write_flow_target_module(path: Path, *, marker: str) -> None:
    """Create a minimal flow target module for direct loader tests."""
    path.write_text(
        "class _FakeFlow:\n"
        f"    marker = {marker!r}\n"
        "    def run(self, *args, **kwargs):\n"
        "        return None\n"
        "    def deploy(self, *args, **kwargs):\n"
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
        deploy=MagicMock(),
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
        invocation="run",
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
    assert payload["invocation"] == "run"
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
                invocation="deploy",
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


def test_executions_replay_returns_structured_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Replay tool should return replay operation metadata and execution payload."""
    mock_kitaru_client.executions.replay.return_value = sample_execution

    with patch("kitaru.client.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_replay(
            "kr-a8f3c2",
            from_="write_summary",
            flow_inputs={"topic": "new topic"},
        )

    mock_kitaru_client.executions.replay.assert_called_once_with(
        "kr-a8f3c2",
        from_="write_summary",
        overrides=None,
        topic="new topic",
    )
    assert payload["available"] is True
    assert payload["operation"] == "replay"
    assert payload["execution"]["exec_id"] == sample_execution.exec_id


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


def test_manage_stack_create_returns_structured_result() -> None:
    """MCP manage_stack(create) should reuse the CLI-style serialized payload."""
    with patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-dev-id", name="dev", is_active=True),
            previous_active_stack="default",
            components_created=("dev (orchestrator)", "dev (artifact_store)"),
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
    )
    assert payload == {
        "id": "stack-dev-id",
        "name": "dev",
        "is_active": True,
        "previous_active_stack": "default",
        "components_created": ["dev (orchestrator)", "dev (artifact_store)"],
        "stack_type": "local",
    }


def test_manage_stack_delegates_request_building_to_shared_interface() -> None:
    request = stack_interface.ManageStackCreateRequest(
        name="dev",
        activate=True,
        stack_type=StackType.LOCAL,
        remote_spec=None,
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
            components_created=("dev (orchestrator)", "dev (artifact_store)"),
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
    '`stack_type="vertex"`, `stack_type="sagemaker"`, or '
    '`stack_type="azureml"`'
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
        ({"region": "eu-west-1"}, _REMOTE_STACK_TYPE_ERROR),
        (
            {"namespace": "ml-team"},
            'Kubernetes-only options require `stack_type="kubernetes"`: `namespace`',
        ),
        ({"credentials": "implicit"}, _REMOTE_STACK_TYPE_ERROR),
        ({"verify": False}, _REMOTE_STACK_TYPE_ERROR),
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
    }


def test_manage_stack_create_async_mode_rejected_for_local() -> None:
    """Local MCP stacks should reject the async convenience flag."""
    with (
        patch("kitaru._config._stacks._create_stack_operation") as mock_create_stack,
        pytest.raises(
            ValueError,
            match=(
                r"`async_mode` requires `stack_type=\"kubernetes\"`, "
                r"`stack_type=\"vertex\"`, `stack_type=\"sagemaker\"`, or "
                r"`stack_type=\"azureml\"`\."
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
                'AzureML-only options require `stack_type="azureml"`: `subscription_id`'
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
