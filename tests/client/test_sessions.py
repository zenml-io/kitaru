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
"""Round-trip tests for the sessions SDK resource."""

import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from conftest import (
    FakeSessionNodeRepository,
    FakeSessionRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeListParams,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    session_repository = FakeSessionRepository()
    node_repository = FakeSessionNodeRepository()
    session_service = SessionService(repository=session_repository)
    node_service = SessionNodeService(
        repository=node_repository, session_repository=session_repository
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a session through the SDK."""
    agent_id = uuid.uuid4()
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs={"prompt": "hi"},
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    assert isinstance(session, SessionResponse)
    assert session.agent_id == agent_id
    assert session.owner_id == ACCOUNT.id
    assert session.status == SessionStatus.IN_PROGRESS


async def test_create_duplicate_external_id(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    request = SessionCreateRequest(
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.IMPORTED,
        inputs=None,
        outputs=None,
        expected=None,
        metadata={},
        provider="langsmith",
        external_id="run-1",
    )
    await api_client.sessions.create(request)
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.create(request)
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a session by id through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    loaded = await api_client.sessions.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate sessions through the SDK."""
    agent_id = uuid.uuid4()
    for _ in range(3):
        await api_client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                origin=SessionOrigin.RECORDED,
                inputs=None,
                outputs=None,
                expected=None,
                metadata={},
            )
        )

    page = await api_client.sessions.list(SessionListParams(agent_id=agent_id, size=2))
    assert len(page.items) == 2
    assert page.next_cursor is not None

    collected = [
        item.id
        async for item in api_client.sessions.iter(
            SessionListParams(agent_id=agent_id, size=2)
        )
    ]
    assert len(collected) == 3


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a session through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    updated = await api_client.sessions.update(
        created.id,
        SessionUpdateRequest(status=SessionStatus.COMPLETED, outputs={"a": 1}),
    )
    assert updated.status == SessionStatus.COMPLETED
    assert updated.outputs == {"a": 1}


async def test_update_status_conflict(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for an illegal status transition."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    await api_client.sessions.update(
        created.id, SessionUpdateRequest(status=SessionStatus.FAILED)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.update(
            created.id, SessionUpdateRequest(status=SessionStatus.IN_PROGRESS)
        )
    assert exc_info.value.status_code == 409


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a session through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    await api_client.sessions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(created.id)


async def test_ingest_nodes_and_list_nodes(api_client: KitaruAPIClient) -> None:
    """Ingest and list session nodes through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    batch = SessionNodeBatchRequest(
        nodes=[
            SessionNodeCreateRequest(
                index=0,
                node_type=NodeType.LLM_CALL,
                name="call",
                status=NodeStatus.COMPLETED,
                cost=Decimal("1.50"),
                inputs={"q": "hi"},
                outputs=None,
                attributes=None,
            ),
            SessionNodeCreateRequest(
                index=1,
                parent_index=0,
                node_type=NodeType.TOOL_CALL,
                name="search",
                status=NodeStatus.COMPLETED,
                tool_name="search",
                inputs={"q": "hi"},
                outputs=None,
                attributes=None,
            ),
        ]
    )
    stored = await api_client.sessions.ingest_nodes(created.id, batch)
    assert len(stored) == 2
    assert stored[1].parent_id == stored[0].id
    assert stored[1].cache_key is not None

    page = await api_client.sessions.list_nodes(created.id)
    assert [item.index for item in page.items] == [0, 1]
    assert page.items[0].inputs is None

    page = await api_client.sessions.list_nodes(
        created.id, SessionNodeListParams(include_payloads=True)
    )
    assert page.items[0].inputs == {"q": "hi"}


async def test_iter_nodes(api_client: KitaruAPIClient) -> None:
    """Iterate every node of a session across pages through the SDK."""
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=uuid.uuid4(),
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
            metadata={},
        )
    )
    batch = SessionNodeBatchRequest(
        nodes=[
            SessionNodeCreateRequest(
                index=index,
                node_type=NodeType.SPAN,
                name="span",
                status=NodeStatus.COMPLETED,
                inputs=None,
                outputs=None,
                attributes=None,
            )
            for index in range(5)
        ]
    )
    await api_client.sessions.ingest_nodes(created.id, batch)

    collected = [
        item.index
        async for item in api_client.sessions.iter_nodes(
            created.id, SessionNodeListParams(size=2)
        )
    ]
    assert collected == [0, 1, 2, 3, 4]
