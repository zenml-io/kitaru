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
    ToolPolicyConfig,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError


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
