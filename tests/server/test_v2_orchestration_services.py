"""Focused coverage for v2 replay and experiment orchestration."""

import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest

from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import ExperimentUpdate
from kitaru.server.application.models.replay import ReplayCreate
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ConflictError, ValidationError
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import HistoryConfig, ReplayConfig, ToolPolicy
from kitaru.server.domain.session import Session, SessionOrigin, SessionStatus
from kitaru.server.domain.session_node import NodeStatus, NodeType, SessionNode


def _actor() -> AuthContext:
    return AuthContext(account=Account(name="tester"))


def _experiment_service(**overrides: Any) -> ExperimentService:
    dependencies: dict[str, Any] = {
        "repository": AsyncMock(),
        "run_repository": AsyncMock(),
        "cohort_repository": AsyncMock(),
        "session_repository": AsyncMock(),
        "agent_version_repository": AsyncMock(),
        "plugin_repository": AsyncMock(),
        "replay_repository": AsyncMock(),
        "task_repository": AsyncMock(),
        "job_service": AsyncMock(),
    }
    dependencies.update(overrides)
    return ExperimentService(**dependencies)


def _replay_service(**overrides: Any) -> ReplayService:
    dependencies: dict[str, Any] = {
        "repository": AsyncMock(),
        "session_repository": AsyncMock(),
        "session_node_repository": AsyncMock(),
        "agent_repository": AsyncMock(),
        "agent_version_repository": AsyncMock(),
        "plugin_repository": AsyncMock(),
        "run_repository": AsyncMock(),
        "task_repository": AsyncMock(),
        "job_service": AsyncMock(),
    }
    dependencies.update(overrides)
    return ReplayService(**dependencies)


async def test_experiment_config_is_frozen_after_first_run() -> None:
    """Reject config changes once an experiment has a run."""
    actor = _actor()
    config = ReplayConfig(owner_id=actor.account.id, evaluators=[])
    experiment = Experiment(
        owner_id=actor.account.id,
        name="quality",
        replay_config_id=config.id,
    )
    repository = AsyncMock()
    repository.get.return_value = experiment
    repository.get_config.return_value = config
    run_repository = AsyncMock()
    run_repository.query.return_value = (
        [
            ExperimentRun(
                owner_id=actor.account.id,
                experiment_id=experiment.id,
                number=1,
                cohort_id=uuid.uuid4(),
                agent_version_id=uuid.uuid4(),
            )
        ],
        None,
    )
    service = _experiment_service(
        repository=repository,
        run_repository=run_repository,
    )

    with pytest.raises(ConflictError, match="configuration is frozen"):
        await service.update_experiment(
            experiment.id,
            ExperimentUpdate(tool_policy=ToolPolicy()),
            actor,
        )

    repository.update.assert_not_awaited()


async def test_standalone_replay_rejects_in_progress_baseline() -> None:
    """Require a terminal baseline before creating a replay job."""
    actor = _actor()
    baseline = Session(
        owner_id=actor.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
    )
    sessions = AsyncMock()
    sessions.get.return_value = baseline
    service = _replay_service(session_repository=sessions)

    with pytest.raises(ValidationError, match="still in progress"):
        await service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                evaluators=[],
            ),
            actor,
        )


async def test_tool_lookup_uses_baseline_history_scope() -> None:
    """Read the newest recorded tool result in baseline scope."""
    actor = _actor()
    replay_repository = AsyncMock()
    replay = Replay(
        owner_id=actor.account.id,
        job_id=uuid.uuid4(),
        replay_config_id=uuid.uuid4(),
        baseline_session_id=uuid.uuid4(),
    )
    replay_repository.get.return_value = replay
    replay_repository.get_config.return_value = ReplayConfig(
        id=replay.replay_config_id,
        owner_id=actor.account.id,
        tool_policy=ToolPolicy(default=HistoryConfig()),
    )
    nodes = AsyncMock()
    nodes.find_tool_result.return_value = SessionNode(
        session_id=replay.baseline_session_id,
        index=0,
        node_type=NodeType.TOOL_CALL,
        name="weather",
        status=NodeStatus.COMPLETED,
        outputs={"temperature": 18},
    )
    service = _replay_service(
        repository=replay_repository,
        session_node_repository=nodes,
    )

    found, result = await service.tool_lookup(
        replay.id,
        "weather",
        "a" * 64,
        actor,
    )

    assert found is True
    assert result == {"temperature": 18}
    nodes.find_tool_result.assert_awaited_once_with(
        "a" * 64, session_ids=[replay.baseline_session_id]
    )


async def test_agent_history_lookup_delegates_scope_to_repository() -> None:
    """Filter agent history in storage instead of materializing all sessions."""
    actor = _actor()
    baseline = Session(
        owner_id=actor.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
    )
    replay = Replay(
        owner_id=actor.account.id,
        job_id=uuid.uuid4(),
        replay_config_id=uuid.uuid4(),
        baseline_session_id=baseline.id,
    )
    replay_repository = AsyncMock()
    replay_repository.get.return_value = replay
    replay_repository.get_config.return_value = ReplayConfig(
        id=replay.replay_config_id,
        owner_id=actor.account.id,
        tool_policy=ToolPolicy(default=HistoryConfig(scope="agent")),
    )
    sessions = AsyncMock()
    sessions.get.return_value = baseline
    nodes = AsyncMock()
    nodes.find_tool_result.return_value = None
    service = _replay_service(
        repository=replay_repository,
        session_repository=sessions,
        session_node_repository=nodes,
    )

    found, result = await service.tool_lookup(replay.id, "weather", "b" * 64, actor)

    assert found is False
    assert result is None
    sessions.query.assert_not_awaited()
    nodes.find_tool_result.assert_awaited_once_with(
        "b" * 64, agent_id=baseline.agent_id
    )


def test_v2_route_manifest_is_registered() -> None:
    """Expose every command and resource group through the application."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
        )
    )
    paths = set(app.openapi()["paths"])
    expected = {
        "/v1/agents",
        "/v1/agent-versions/{version_id}",
        "/v1/blobs",
        "/v1/cohorts",
        "/v1/evaluations",
        "/v1/evaluators",
        "/v1/experiments",
        "/v1/experiment-runs",
        "/v1/importers",
        "/v1/imports",
        "/v1/jobs",
        "/v1/replays",
        "/v1/session-runs",
        "/v1/sessions",
        "/v1/tags",
        "/v1/tasks",
        "/v1/workers",
    }
    assert expected <= paths
