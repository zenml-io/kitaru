"""Shared fixtures for MCP tool tests."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from kitaru.client import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
    FailureInfo,
    PendingWait,
)
from kitaru.errors import FailureOrigin


@pytest.fixture
def mock_kitaru_client() -> MagicMock:
    """Mocked `KitaruClient` with executions/artifacts namespaces."""
    client = MagicMock()
    client.executions = MagicMock()
    client.artifacts = MagicMock()
    return client


@pytest.fixture
def sample_artifact(mock_kitaru_client: MagicMock) -> ArtifactRef:
    """Sample artifact reference used in execution fixtures."""
    return ArtifactRef(
        artifact_id="art-123",
        name="summary_context",
        kind="json",
        save_type="context",
        producing_call="write_summary",
        metadata={"stage": "draft"},
        _client=mock_kitaru_client,
    )


@pytest.fixture
def sample_execution(
    mock_kitaru_client: MagicMock,
    sample_artifact: ArtifactRef,
) -> Execution:
    """Sample waiting execution with checkpoint and artifact data."""
    started_at = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)

    failure = FailureInfo(
        message="Tool failed once",
        exception_type="RuntimeError",
        traceback="RuntimeError: boom",
        origin=FailureOrigin.RUNTIME,
    )

    checkpoint_attempt = CheckpointAttempt(
        attempt_id="attempt-1",
        status=ExecutionStatus.FAILED,
        started_at=started_at,
        ended_at=started_at,
        metadata={"retry": 1},
        failure=failure,
    )

    checkpoint = CheckpointCall(
        call_id="call-1",
        name="write_summary",
        status=ExecutionStatus.WAITING,
        started_at=started_at,
        ended_at=None,
        metadata={"attempt": 2},
        original_call_id=None,
        parent_call_ids=[],
        failure=None,
        attempts=[checkpoint_attempt],
        artifacts=[sample_artifact],
    )

    pending_wait = PendingWait(
        wait_id="wait-1",
        name="approve_draft",
        question="Approve draft?",
        schema={"type": "boolean"},
        metadata={"priority": "high"},
        entered_waiting_at=started_at,
    )

    return Execution(
        exec_id="kr-a8f3c2",
        flow_name="content_pipeline",
        status=ExecutionStatus.WAITING,
        started_at=started_at,
        ended_at=None,
        stack_name="prod",
        metadata={"team": "platform"},
        status_reason=None,
        failure=None,
        pending_wait=pending_wait,
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=[checkpoint],
        artifacts=[sample_artifact],
        _client=mock_kitaru_client,
    )
