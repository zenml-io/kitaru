"""Tests for Kitaru MCP server tools."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kitaru.client import ExecutionStatus
from kitaru.config import ActiveEnvironmentVariable, StackInfo
from kitaru.mcp.server import (
    RuntimeSnapshot,
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
)


def test_executions_list_calls_client_and_serializes(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """List tool should call client list API and return structured summaries."""
    mock_kitaru_client.executions.list.return_value = [sample_execution]

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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


def test_executions_list_stack_filter_happens_after_fetch(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Stack filtering should happen client-side without truncating early."""
    other_stack = replace(sample_execution, exec_id="kr-other", stack_name="dev")
    mock_kitaru_client.executions.list.return_value = [other_stack, sample_execution]

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_get(sample_execution.exec_id)

    assert payload["exec_id"] == sample_execution.exec_id
    assert payload["checkpoints"][0]["name"] == "write_summary"


def test_executions_latest_with_stack_filter(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Latest tool should support stack filtering even though client API does not."""
    mock_kitaru_client.executions.list.return_value = [sample_execution]

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_latest(stack="prod")

    assert payload["exec_id"] == sample_execution.exec_id
    mock_kitaru_client.executions.latest.assert_not_called()


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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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


def test_executions_run_fetches_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Run tool should run a flow and include execution details when available."""
    flow_target = MagicMock()
    flow_target.run.return_value = SimpleNamespace(exec_id=sample_execution.exec_id)
    mock_kitaru_client.executions.get.return_value = sample_execution

    with (
        patch("kitaru.mcp.server._load_flow_target", return_value=flow_target),
        patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client),
    ):
        payload = kitaru_executions_run(
            "agent.py:content_pipeline",
            args={"topic": "ai safety"},
        )

    flow_target.run.assert_called_once_with(topic="ai safety")
    assert payload["invocation"] == "run"
    assert payload["execution"]["exec_id"] == sample_execution.exec_id


def test_executions_run_returns_warning_when_details_unavailable(
    mock_kitaru_client: MagicMock,
) -> None:
    """Run tool should still return exec_id if details are not immediately queryable."""
    flow_target = MagicMock()
    flow_target.deploy.return_value = SimpleNamespace(exec_id="kr-new")
    mock_kitaru_client.executions.get.side_effect = RuntimeError("store unavailable")

    with (
        patch("kitaru.mcp.server._load_flow_target", return_value=flow_target),
        patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client),
    ):
        payload = kitaru_executions_run(
            "agent.py:content_pipeline",
            args={"topic": "ai safety"},
            stack="prod",
        )

    flow_target.deploy.assert_called_once_with(stack="prod", topic="ai safety")
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
        patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client),
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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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


def test_executions_replay_returns_structured_execution(
    mock_kitaru_client: MagicMock,
    sample_execution,
) -> None:
    """Replay tool should return replay operation metadata and execution payload."""
    mock_kitaru_client.executions.replay.return_value = sample_execution

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
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

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
        listed = kitaru_artifacts_list("kr-a8f3c2", limit=10)
        loaded = kitaru_artifacts_get(sample_artifact.artifact_id)

    assert listed[0]["artifact_id"] == sample_artifact.artifact_id
    assert loaded["artifact_id"] == sample_artifact.artifact_id
    assert loaded["value_format"] == "repr"


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
        patch("kitaru.mcp.server._build_runtime_snapshot", return_value=snapshot),
        patch("kitaru.mcp.server._list_stack_entries", return_value=stack_entries),
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


def test_manage_stack_create_returns_structured_result() -> None:
    """MCP manage_stack(create) should reuse the CLI-style serialized payload."""
    with patch("kitaru.mcp.server._create_stack_operation") as mock_create_stack:
        mock_create_stack.return_value = SimpleNamespace(
            stack=StackInfo(id="stack-dev-id", name="dev", is_active=True),
            previous_active_stack="default",
            components_created=("dev (orchestrator)", "dev (artifact_store)"),
        )

        payload = manage_stack("create", "dev", activate=True)

    mock_create_stack.assert_called_once_with("dev", activate=True)
    assert payload == {
        "id": "stack-dev-id",
        "name": "dev",
        "is_active": True,
        "previous_active_stack": "default",
        "components_created": ["dev (orchestrator)", "dev (artifact_store)"],
    }


def test_manage_stack_delete_returns_structured_result() -> None:
    """MCP manage_stack(delete) should return delete metadata."""
    with patch("kitaru.mcp.server._delete_stack_operation") as mock_delete_stack:
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


def test_manage_stack_rejects_irrelevant_flags() -> None:
    """MCP manage_stack should reject flag combinations that do not fit the action."""
    with pytest.raises(ValueError, match='only valid when action="delete"'):
        manage_stack("create", "dev", recursive=True)

    with pytest.raises(ValueError, match='only valid when action="create"'):
        manage_stack("delete", "dev", activate=False)
