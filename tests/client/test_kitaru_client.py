#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Round-trip tests for the async user-facing Kitaru client."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_offload_service,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest, AgentResponse
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.replay import ReplayResponse, ReplayStatus
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.client import KitaruClient
from kitaru.client.exceptions import KitaruClientError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_agent_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


def _agent_response(**overrides: Any) -> AgentResponse:
    """Build an agent response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "owner_id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "name": "assistant",
        "description": None,
        "latest_version": 1,
    }
    values.update(overrides)
    return AgentResponse(**values)


def _experiment_response(**overrides: Any) -> ExperimentResponse:
    """Build an experiment response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "owner_id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "name": "regression",
        "description": None,
        "agent_id": uuid.uuid4(),
        "override": None,
        "tool_policy": {"default": {"type": "passthrough"}, "tools": {}},
        "evaluators": [{"evaluator": "accuracy", "version": 1, "params": {}}],
    }
    values.update(overrides)
    return ExperimentResponse(**values)


def _replay_response(**overrides: Any) -> ReplayResponse:
    """Build a replay response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "job_id": uuid.uuid4(),
        "experiment_run_id": None,
        "baseline_session_id": uuid.uuid4(),
        "result_session_id": None,
        "override": None,
        "tool_policy": {"default": {"type": "passthrough"}, "tools": {}},
        "evaluators": [{"evaluator": "accuracy", "version": 1, "params": {}}],
        "evaluate_baselines": False,
        "status": ReplayStatus.PENDING,
        "error": None,
        "created": now,
        "updated": now,
    }
    values.update(overrides)
    return ReplayResponse(**values)


def _experiment_run_response(**overrides: Any) -> ExperimentRunResponse:
    """Build an experiment run response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "owner_id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "experiment_id": uuid.uuid4(),
        "number": 1,
        "status": ExperimentRunStatus.RUNNING,
        "cohort_version_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "evaluate_baselines": False,
        "started_at": None,
        "ended_at": None,
        "error": None,
        "progress": {
            "pending": 1,
            "evaluating": 0,
            "completed": 0,
            "failed": 0,
            "canceled": 0,
            "total": 1,
        },
    }
    values.update(overrides)
    return ExperimentRunResponse(**values)


def _mock_client() -> tuple[KitaruAPIClient, KitaruClient]:
    """Build a KitaruClient over a bare, unrouted API client for mocking."""
    api_client = KitaruAPIClient(base_url="http://test", api_key="k")
    return api_client, KitaruClient(api_client=api_client)


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_repository = FakeAgentRepository()
    session_repository = FakeSessionRepository()
    node_repository = FakeSessionNodeRepository()
    agent_service = AgentService(repository=agent_repository)
    payload_offload = build_payload_offload_service()
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repository),
        replay_repository=FakeReplayRepository(),
        payload_offload=payload_offload,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=FakeTaskRepository(),
        payload_offload=payload_offload,
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


@pytest.fixture
def client(api_client: KitaruAPIClient) -> KitaruClient:
    """Provide a KitaruClient wrapping the fake-backed API client."""
    return KitaruClient(api_client=api_client)


async def test_get_agent_by_id(
    client: KitaruClient, api_client: KitaruAPIClient
) -> None:
    """Get an agent by id."""
    created = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    loaded = await client.get_agent(created.id)
    assert loaded == created


async def test_get_agent_by_name(
    client: KitaruClient, api_client: KitaruAPIClient
) -> None:
    """Get an agent by its exact name."""
    created = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    loaded = await client.get_agent("assistant")
    assert loaded == created


async def test_get_agent_by_name_not_found(client: KitaruClient) -> None:
    """Raise NotFoundError for an unknown agent name."""
    with pytest.raises(NotFoundError):
        await client.get_agent("missing")


async def test_get_agent_duplicate_name_raises() -> None:
    """Raise KitaruClientError when two agents share the exact name."""
    api_client, client = _mock_client()
    duplicates = [_agent_response(name="assistant"), _agent_response(name="assistant")]
    api_client.agents.list = AsyncMock(
        return_value=Page(items=duplicates, next_cursor=None)
    )

    with pytest.raises(KitaruClientError):
        await client.get_agent("assistant")


async def test_list_agents(client: KitaruClient, api_client: KitaruAPIClient) -> None:
    """Iterate over every agent."""
    for name in ["assistant", "reviewer"]:
        await api_client.agents.create(AgentCreateRequest(name=name))

    names = [agent.name async for agent in client.list_agents()]
    assert names == ["reviewer", "assistant"]


async def test_list_sessions_filters_by_resolved_agent_id(
    client: KitaruClient, api_client: KitaruAPIClient
) -> None:
    """Scope sessions to the resolved agent id."""
    assistant = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    reviewer = await api_client.agents.create(AgentCreateRequest(name="reviewer"))
    for agent_id in (assistant.id, assistant.id, reviewer.id):
        await api_client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                origin=SessionOrigin.RECORDED,
                inputs={},
                outputs=None,
            )
        )

    sessions = [session async for session in client.list_sessions(agent="assistant")]
    assert len(sessions) == 2
    assert all(session.agent_id == assistant.id for session in sessions)


async def test_list_session_nodes(
    client: KitaruClient, api_client: KitaruAPIClient
) -> None:
    """Iterate over the nodes of a session in index order."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            inputs={},
            outputs=None,
        )
    )
    await api_client.sessions.ingest_nodes(
        session.id,
        SessionNodeBatchRequest(
            nodes=[
                SessionNodeCreateRequest(
                    index=0,
                    node_type=NodeType.LLM_CALL,
                    name="call-1",
                    status=NodeStatus.COMPLETED,
                    inputs={},
                    outputs={},
                    attributes={},
                ),
                SessionNodeCreateRequest(
                    index=1,
                    node_type=NodeType.LLM_CALL,
                    name="call-2",
                    status=NodeStatus.COMPLETED,
                    inputs={},
                    outputs={},
                    attributes={},
                ),
            ]
        ),
    )

    nodes = [node async for node in client.list_session_nodes(session.id)]
    assert [node.index for node in nodes] == [0, 1]


async def test_replay_wait_true_returns_terminal_replay() -> None:
    """Wait for a replay to reach a terminal status and return it."""
    api_client, client = _mock_client()
    session_id = uuid.uuid4()
    pending = _replay_response(status=ReplayStatus.PENDING)
    completed = _replay_response(id=pending.id, status=ReplayStatus.COMPLETED)
    api_client.replays.create = AsyncMock(return_value=pending)
    api_client.replays.get = AsyncMock(return_value=completed)

    result = await client.replay(
        session_id, evaluators=[EvaluatorConfig(evaluator="accuracy")]
    )

    assert result.status == ReplayStatus.COMPLETED
    api_client.replays.get.assert_awaited_once_with(pending.id)


async def test_replay_wait_false_returns_immediately() -> None:
    """Return immediately without polling when wait is False."""
    api_client, client = _mock_client()
    session_id = uuid.uuid4()
    pending = _replay_response(status=ReplayStatus.PENDING)
    api_client.replays.create = AsyncMock(return_value=pending)
    api_client.replays.get = AsyncMock()

    result = await client.replay(
        session_id, evaluators=[EvaluatorConfig(evaluator="accuracy")], wait=False
    )

    assert result is pending
    api_client.replays.get.assert_not_awaited()


async def test_wait_for_replay_polls_until_terminal() -> None:
    """Poll a replay until it reaches a terminal status."""
    api_client, client = _mock_client()
    replay_id = uuid.uuid4()
    pending = _replay_response(id=replay_id, status=ReplayStatus.PENDING)
    completed = _replay_response(id=replay_id, status=ReplayStatus.COMPLETED)
    api_client.replays.get = AsyncMock(side_effect=[pending, pending, completed])

    result = await client.wait_for_replay(replay_id, poll_interval=0)

    assert result.status == ReplayStatus.COMPLETED
    assert api_client.replays.get.await_count == 3


async def test_wait_for_replay_times_out() -> None:
    """Raise TimeoutError when the replay never reaches a terminal status."""
    api_client, client = _mock_client()
    replay_id = uuid.uuid4()
    pending = _replay_response(id=replay_id, status=ReplayStatus.PENDING)
    api_client.replays.get = AsyncMock(return_value=pending)

    with pytest.raises(TimeoutError):
        await client.wait_for_replay(replay_id, timeout=0.05, poll_interval=0)


async def test_run_experiment_resolves_by_name_and_waits() -> None:
    """Resolve the experiment by name, start a run, and wait for it to finish."""
    api_client, client = _mock_client()
    experiment = _experiment_response(name="regression")
    cohort_version_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    running = _experiment_run_response(
        experiment_id=experiment.id,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        status=ExperimentRunStatus.RUNNING,
    )
    completed = _experiment_run_response(
        id=running.id,
        experiment_id=experiment.id,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        status=ExperimentRunStatus.COMPLETED,
    )
    api_client.experiments.list = AsyncMock(
        return_value=Page(items=[experiment], next_cursor=None)
    )
    api_client.experiments.start_run = AsyncMock(return_value=running)
    api_client.experiment_runs.get = AsyncMock(return_value=completed)

    result = await client.run_experiment(
        "regression",
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        evaluate_baselines=True,
    )

    assert result.status == ExperimentRunStatus.COMPLETED
    api_client.experiments.start_run.assert_awaited_once_with(
        experiment.id,
        ExperimentRunCreateRequest(
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            evaluate_baselines=True,
        ),
    )


def test_api_returns_underlying_client() -> None:
    """Expose the underlying API client."""
    api_client, client = _mock_client()
    assert client.api is api_client
