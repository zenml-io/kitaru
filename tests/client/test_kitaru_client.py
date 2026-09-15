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

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
    build_payload_store,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest, AgentResponse
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionResponse,
    RunSpec,
)
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.client import KitaruClient
from kitaru.client.exceptions import (
    AgentRegistrationError,
    APIError,
    KitaruClientError,
    NotFoundError,
)
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


def _agent_version_response(**overrides: Any) -> AgentVersionResponse:
    """Build an agent version response with sensible defaults."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "owner_id": uuid.uuid4(),
        "created": now,
        "updated": now,
        "agent_id": uuid.uuid4(),
        "version": 1,
        "display_version": None,
        "description": None,
        "run_spec": RunSpec(command="python agent.py"),
        "capabilities": AgentCapabilities(),
    }
    values.update(overrides)
    return AgentVersionResponse(**values)


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
        "baseline_evaluation_mode": BaselineEvaluationMode.NONE,
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
        "baseline_evaluation_mode": BaselineEvaluationMode.NONE,
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
    payload_store = build_payload_store().store
    session_service = SessionService(
        repository=session_repository,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repository),
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=payload_store,
    )
    node_service = SessionNodeService(
        repository=node_repository,
        session_repository=session_repository,
        task_repository=FakeTaskRepository(),
        payload_store=payload_store,
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


async def test_register_agent_creates_parent_and_initial_version() -> None:
    """Build both requests and return both created records."""
    api_client, client = _mock_client()
    run_spec = RunSpec(command="python agent.py")
    capabilities = AgentCapabilities(tools=["search"], skills=["research"])
    agent = _agent_response(name="assistant", description="Parent", latest_version=0)
    version = _agent_version_response(agent_id=agent.id)
    api_client.agents.create = AsyncMock(return_value=agent)
    api_client.agents.create_version = AsyncMock(return_value=version)

    result = await client.register_agent(
        "assistant",
        run_spec,
        description="Parent",
        display_version="2026-09-13",
        version_description="Initial version",
        capabilities=capabilities,
        agent_idempotency_key="agent-key",
        version_idempotency_key="version-key",
    )

    assert result.agent == agent.model_copy(update={"latest_version": version.version})
    assert result.version == version
    api_client.agents.create.assert_awaited_once_with(
        AgentCreateRequest(name="assistant", description="Parent"),
        idempotency_key="agent-key",
    )
    api_client.agents.create_version.assert_awaited_once_with(
        agent.id,
        AgentVersionCreateRequest(
            display_version="2026-09-13",
            description="Initial version",
            run_spec=run_spec,
            capabilities=capabilities,
        ),
        idempotency_key="version-key",
    )


@pytest.mark.parametrize("reference", [uuid.uuid4(), "assistant"])
async def test_register_agent_version_resolves_agent_reference(
    reference: uuid.UUID | str,
) -> None:
    """Resolve an id or exact name before creating the next version."""
    api_client, client = _mock_client()
    run_spec = RunSpec(command="python agent.py")
    agent = _agent_response()
    version = _agent_version_response(agent_id=agent.id, version=2)
    api_client.agents.get = AsyncMock(return_value=agent)
    api_client.agents.list = AsyncMock(
        return_value=Page(items=[agent], next_cursor=None)
    )
    api_client.agents.create_version = AsyncMock(return_value=version)

    result = await client.register_agent_version(
        reference,
        run_spec,
        display_version="v2",
        description="Second version",
        capabilities=AgentCapabilities(mcp_servers=["docs"]),
        idempotency_key="version-key",
    )

    assert result == version
    if isinstance(reference, uuid.UUID):
        api_client.agents.get.assert_not_awaited()
        api_client.agents.list.assert_not_awaited()
    else:
        api_client.agents.list.assert_awaited_once()
        api_client.agents.get.assert_not_awaited()
    api_client.agents.create_version.assert_awaited_once_with(
        reference if isinstance(reference, uuid.UUID) else agent.id,
        AgentVersionCreateRequest(
            display_version="v2",
            description="Second version",
            run_spec=run_spec,
            capabilities=AgentCapabilities(mcp_servers=["docs"]),
        ),
        idempotency_key="version-key",
    )


async def test_register_agent_reports_partial_failure_without_retry() -> None:
    """Preserve the exact recovery request when version outcome is inconclusive."""
    api_client, client = _mock_client()
    agent = _agent_response()
    cause = RuntimeError("version failed")
    run_spec = RunSpec(command="python agent.py")
    api_client.agents.create = AsyncMock(return_value=agent)
    api_client.agents.create_version = AsyncMock(side_effect=cause)
    api_client.agents.delete = AsyncMock()

    with pytest.raises(AgentRegistrationError) as exc_info:
        await client.register_agent("assistant", run_spec)

    assert exc_info.value.agent == agent
    assert exc_info.value.version_request == AgentVersionCreateRequest(
        run_spec=run_spec
    )
    assert uuid.UUID(exc_info.value.version_idempotency_key)
    assert exc_info.value.cause is cause
    assert exc_info.value.__cause__ is cause
    api_client.agents.create.assert_awaited_once()
    api_client.agents.create_version.assert_awaited_once_with(
        agent.id,
        exc_info.value.version_request,
        idempotency_key=exc_info.value.version_idempotency_key,
    )
    api_client.agents.delete.assert_not_awaited()


async def test_register_agent_preserves_recovery_data_on_cancellation() -> None:
    """Keep cancellation semantics and attach the partial registration details."""
    api_client, client = _mock_client()
    agent = _agent_response(latest_version=0)
    api_client.agents.create = AsyncMock(return_value=agent)
    api_client.agents.create_version = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await client.register_agent("assistant", RunSpec(command="python agent.py"))

    version_call = api_client.agents.create_version.await_args
    assert version_call is not None
    version_key = version_call.kwargs["idempotency_key"]
    assert any(
        str(agent.id) in note and version_key in note
        for note in exc_info.value.__notes__
    )


async def test_register_agent_validates_version_request_before_dispatch() -> None:
    """Reject invalid version fields before creating the parent agent."""
    api_client, client = _mock_client()
    api_client.agents.create = AsyncMock()
    api_client.agents.create_version = AsyncMock()

    with pytest.raises(ValueError):
        await client.register_agent(
            "assistant",
            RunSpec(command="python agent.py"),
            display_version=cast(Any, {"invalid": "value"}),
        )

    api_client.agents.create.assert_not_awaited()
    api_client.agents.create_version.assert_not_awaited()


@pytest.mark.parametrize(
    ("agent_key", "version_key"),
    [
        (" ", None),
        (None, "\n"),
    ],
)
async def test_register_agent_validates_keys_before_dispatch(
    agent_key: str | None,
    version_key: str | None,
) -> None:
    """Reject invalid idempotency keys before creating either resource."""
    api_client, client = _mock_client()
    api_client.agents.create = AsyncMock()
    api_client.agents.create_version = AsyncMock()

    with pytest.raises(APIError):
        await client.register_agent(
            "assistant",
            RunSpec(command="python agent.py"),
            agent_idempotency_key=agent_key,
            version_idempotency_key=version_key,
        )

    api_client.agents.create.assert_not_awaited()
    api_client.agents.create_version.assert_not_awaited()


async def test_register_agent_rejects_equal_keys_before_dispatch() -> None:
    """Keep the two account-wide idempotency keys distinct."""
    api_client, client = _mock_client()
    api_client.agents.create = AsyncMock()
    api_client.agents.create_version = AsyncMock()

    with pytest.raises(ValueError, match="must differ"):
        await client.register_agent(
            "assistant",
            RunSpec(command="python agent.py"),
            agent_idempotency_key=" registration-key ",
            version_idempotency_key="registration-key",
        )

    api_client.agents.create.assert_not_awaited()
    api_client.agents.create_version.assert_not_awaited()


async def test_register_agent_propagates_parent_failure_unchanged() -> None:
    """Do not wrap a failure before any agent was created."""
    api_client, client = _mock_client()
    cause = RuntimeError("parent failed")
    api_client.agents.create = AsyncMock(side_effect=cause)
    api_client.agents.create_version = AsyncMock()

    with pytest.raises(RuntimeError) as exc_info:
        await client.register_agent("assistant", RunSpec(command="python agent.py"))

    assert exc_info.value is cause
    api_client.agents.create_version.assert_not_awaited()


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
                    external_id="call-1",
                    node_type=NodeType.LLM_CALL,
                    name="call-1",
                    status=NodeStatus.COMPLETED,
                    inputs={},
                    outputs={},
                    attributes={},
                ),
                SessionNodeCreateRequest(
                    external_id="call-2",
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
    assert [node.external_id for node in nodes] == ["call-1", "call-2"]


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
        baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
    )

    assert result.status == ExperimentRunStatus.COMPLETED
    api_client.experiments.start_run.assert_awaited_once_with(
        experiment.id,
        ExperimentRunCreateRequest(
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
        ),
    )


def test_api_returns_underlying_client() -> None:
    """Expose the underlying API client."""
    api_client, client = _mock_client()
    assert client.api is api_client
