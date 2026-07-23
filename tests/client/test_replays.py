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
"""Round-trip tests for the replays SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from test_experiments import SCORING_POLICY

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.replays import (
    HistoryPolicy,
    HistoryScope,
    ReplayCreateRequest,
    ReplayStatus,
    ReplayUpdateRequest,
    ToolLookupRequest,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.hashing import tool_call_cache_key
from kitaru.server.domain.ids import uuid7


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def create_session(api_client: KitaruAPIClient) -> tuple[uuid.UUID, uuid.UUID]:
    """Store an agent, a runnable version, and a completed session.

    Args:
        api_client: API client routed to the app.

    Returns:
        Ids of the created session and agent version.
    """
    agent = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    version = await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        ),
    )
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    return session.id, version.id


async def test_create_get_list_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a standalone replay through create, get, and list."""
    session_id, version_id = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(
            original_session_id=session_id, scoring_policy=SCORING_POLICY
        )
    )
    assert created.experiment_run_id is None
    assert created.original_session_id == session_id
    assert created.agent_version_id == version_id
    assert created.status is ReplayStatus.PENDING
    assert created.tool_policy == ToolPolicyConfig(default=HistoryPolicy())
    assert created.scoring_policy == SCORING_POLICY

    loaded = await api_client.replays.get(created.id)
    assert loaded == created

    page = await api_client.replays.list(
        original_session_id=session_id,
        status=ReplayStatus.PENDING,
        standalone=True,
    )
    assert page.total == 1
    assert page.items[0].id == created.id

    page = await api_client.replays.list(standalone=False)
    assert page.total == 0


async def test_create_rejects_cohort_scope(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 for a history policy scoped to a cohort."""
    session_id, _ = await create_session(api_client)
    with pytest.raises(APIError) as exc_info:
        await api_client.replays.create(
            ReplayCreateRequest(
                original_session_id=session_id,
                scoring_policy=SCORING_POLICY,
                tool_policy=ToolPolicyConfig(
                    default=HistoryPolicy(scope=HistoryScope.COHORT)
                ),
            )
        )
    assert exc_info.value.status_code == 422


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a NotFoundError."""
    with pytest.raises(NotFoundError):
        await api_client.replays.get(uuid.uuid4())


async def test_runner_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a replay through spec, update, heartbeat, and diff."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(
            original_session_id=session_id, scoring_policy=SCORING_POLICY
        )
    )
    spec = await api_client.replays.get_spec(created.id)
    assert spec.replay_id == created.id
    assert spec.original_session_id == session_id
    assert spec.score_baselines is True
    assert spec.run.command == "python agent.py"
    assert spec.secret_env == {}

    running = await api_client.replays.update(
        created.id, ReplayUpdateRequest(status=ReplayStatus.RUNNING)
    )
    assert running.status is ReplayStatus.RUNNING

    heartbeat = await api_client.replays.heartbeat(created.id)
    assert heartbeat.canceled is False

    session = await api_client.sessions.get(session_id)
    result = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=session.agent_id,
            origin=SessionOrigin.RECORDED,
            replay_id=created.id,
        )
    )
    assert result.origin is SessionOrigin.REPLAY
    await api_client.sessions.update(
        result.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )

    completed = await api_client.replays.update(
        created.id,
        ReplayUpdateRequest(
            status=ReplayStatus.COMPLETED,
            passed=True,
            score=0.8,
            scores={"conciseness": 0.8},
        ),
    )
    assert completed.status is ReplayStatus.COMPLETED
    assert completed.result_session_id == result.id
    assert completed.diff is not None

    diff = await api_client.replays.get_diff(created.id)
    assert diff.replay_id == created.id
    assert diff.original_session_id == session_id
    assert diff.result_session_id == result.id
    assert diff.node_pairs == []


async def test_tool_lookup_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a history tool lookup."""
    agent = await api_client.agents.create(AgentCreateRequest(name="lookup-bot"))
    await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        ),
    )
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    inputs = {"city": "Berlin"}
    await api_client.session_nodes.upsert(
        session.id,
        SessionNodeBatchRequest(
            nodes=[
                SessionNodeCreateRequest(
                    id=uuid7(),
                    sequence=0,
                    node_type=NodeType.TOOL_CALL,
                    name="get_weather",
                    status=NodeStatus.COMPLETED,
                    tool_name="get_weather",
                    inputs=inputs,
                    outputs={"temp": 21},
                )
            ]
        ),
    )
    await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    created = await api_client.replays.create(
        ReplayCreateRequest(
            original_session_id=session.id, scoring_policy=SCORING_POLICY
        )
    )
    found = await api_client.replays.tool_lookup(
        created.id,
        ToolLookupRequest(
            tool_name="get_weather",
            inputs=inputs,
            cache_key=tool_call_cache_key("get_weather", inputs),
        ),
    )
    assert found.found is True
    assert found.result == {"temp": 21}

    miss = await api_client.replays.tool_lookup(
        created.id,
        ToolLookupRequest(
            tool_name="get_weather",
            inputs={"city": "Paris"},
            cache_key=tool_call_cache_key("get_weather", {"city": "Paris"}),
        ),
    )
    assert miss.found is False
    assert miss.result is None


async def test_update_illegal_transition(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for an illegal runner transition."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(
            original_session_id=session_id, scoring_policy=SCORING_POLICY
        )
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.replays.update(
            created.id,
            ReplayUpdateRequest(
                status=ReplayStatus.COMPLETED, passed=True, score=1.0, scores={}
            ),
        )
    assert exc_info.value.status_code == 409
