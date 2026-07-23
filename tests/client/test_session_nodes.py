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
"""Round-trip tests for the session nodes SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.hashing import tool_call_cache_key
from kitaru.server.adapters.rest.dependencies import (
    authorize,
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
from kitaru.server.domain.ids import uuid7

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    version_repository = FakeAgentVersionRepository(agent_repository)
    session_repository = FakeSessionRepository(agent_repository, version_repository)
    node_repository = FakeSessionNodeRepository(session_repository)
    replay_repository = FakeReplayRepository(
        session_repository, version_repository, FakeReplayConfigRepository()
    )
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        replay_repository=replay_repository,
    )
    node_service = SessionNodeService(
        repository=node_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def create_session(api_client: KitaruAPIClient) -> SessionResponse:
    """Store an agent and a recorded session through the SDK.

    Args:
        api_client: API client routed to the app.

    Returns:
        Created session.
    """
    agent = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    return await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )


async def test_upsert(api_client: KitaruAPIClient) -> None:
    """Upsert a node batch through the SDK."""
    session = await create_session(api_client)
    root = SessionNodeCreateRequest(
        id=uuid7(),
        sequence=0,
        node_type=NodeType.SPAN,
        name="run",
        status=NodeStatus.COMPLETED,
    )
    tool = SessionNodeCreateRequest(
        id=uuid7(),
        sequence=1,
        parent_id=root.id,
        node_type=NodeType.TOOL_CALL,
        name="get_weather",
        status=NodeStatus.COMPLETED,
        tool_name="get_weather",
        inputs={"city": "Berlin"},
        outputs={"temp": 21},
    )
    nodes = await api_client.session_nodes.upsert(
        session.id, SessionNodeBatchRequest(nodes=[root, tool])
    )
    assert [node.key for node in nodes] == [
        "span:run",
        "span:run/tool_call:get_weather",
    ]
    assert nodes[1].cache_key == tool_call_cache_key("get_weather", {"city": "Berlin"})
    assert nodes[1].inputs == {"city": "Berlin"}

    # Retrying the same batch is idempotent.
    retried = await api_client.session_nodes.upsert(
        session.id, SessionNodeBatchRequest(nodes=[root, tool])
    )
    assert [node.id for node in retried] == [node.id for node in nodes]


async def test_upsert_unknown_parent(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 as a typed error."""
    session = await create_session(api_client)
    child = SessionNodeCreateRequest(
        id=uuid7(),
        sequence=0,
        parent_id=uuid7(),
        node_type=NodeType.SPAN,
        name="run",
        status=NodeStatus.COMPLETED,
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.session_nodes.upsert(
            session.id, SessionNodeBatchRequest(nodes=[child])
        )
    assert exc_info.value.status_code == 422


async def test_upsert_unknown_session(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    node = SessionNodeCreateRequest(
        id=uuid7(),
        sequence=0,
        node_type=NodeType.SPAN,
        name="run",
        status=NodeStatus.COMPLETED,
    )
    with pytest.raises(NotFoundError):
        await api_client.session_nodes.upsert(
            uuid.uuid4(), SessionNodeBatchRequest(nodes=[node])
        )


async def test_list(api_client: KitaruAPIClient) -> None:
    """List nodes with and without payloads through the SDK."""
    session = await create_session(api_client)
    node = SessionNodeCreateRequest(
        id=uuid7(),
        sequence=0,
        node_type=NodeType.LLM_CALL,
        name="chat",
        status=NodeStatus.COMPLETED,
        inputs={"messages": ["hi"]},
        outputs={"content": "hello"},
        attributes={"mocked": False},
    )
    await api_client.session_nodes.upsert(
        session.id, SessionNodeBatchRequest(nodes=[node])
    )

    nodes = await api_client.session_nodes.list(session.id)
    assert len(nodes) == 1
    assert nodes[0].inputs is None
    assert nodes[0].outputs is None
    assert nodes[0].attributes is None

    nodes = await api_client.session_nodes.list(session.id, include_payloads=True)
    assert nodes[0].inputs == {"messages": ["hi"]}
    assert nodes[0].outputs == {"content": "hello"}
    assert nodes[0].attributes == {"mocked": False}
