"""Tests for Kitaru MCP server tools."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kitaru.client import ExecutionStatus
from kitaru.config import StackInfo
from kitaru.errors import KitaruFeatureNotAvailableError
from kitaru.mcp.server import (
    RuntimeSnapshot,
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


def test_executions_replay_returns_structured_not_available(
    mock_kitaru_client: MagicMock,
) -> None:
    """Replay tool should return a stable response when replay is not yet shipped."""
    mock_kitaru_client.executions.replay.side_effect = KitaruFeatureNotAvailableError(
        "Replay not available yet"
    )

    with patch("kitaru.mcp.server.KitaruClient", return_value=mock_kitaru_client):
        payload = kitaru_executions_replay("kr-a8f3c2", from_="write_summary")

    assert payload["available"] is False
    assert "Replay not available" in payload["message"]


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
        config_directory="/tmp/.config/kitaru",
        server_url="https://example.com",
        active_user="alice",
        active_stack="prod",
        repository_root="/work/repo",
        server_version="0.99.0",
        server_database="postgres",
        server_deployment_type="kubernetes",
        local_server_status="not started",
        warning=None,
    )

    stacks = [
        StackInfo(id="stack-1", name="prod", is_active=True),
        StackInfo(id="stack-2", name="dev", is_active=False),
    ]

    with (
        patch("kitaru.mcp.server._build_runtime_snapshot", return_value=snapshot),
        patch("kitaru.mcp.server.get_available_stacks", return_value=stacks),
    ):
        status_payload = kitaru_status()
        stack_payload = kitaru_stacks_list()

    assert status_payload["active_stack"] == "prod"
    assert [stack["name"] for stack in stack_payload] == ["prod", "dev"]
