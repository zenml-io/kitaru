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
from datetime import UTC, datetime

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.agents import AgentCreateRequest, AgentResponse
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionProvider,
    SessionScoresRequest,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import ConflictError, NotFoundError
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

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

ENDED_AT = datetime(2026, 7, 1, 12, 5, tzinfo=UTC)


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
    job_repository = FakeJobRepository(session_repository, version_repository)
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        job_repository=job_repository,
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


async def create_agent(api_client: KitaruAPIClient) -> AgentResponse:
    """Store an agent through the SDK.

    Args:
        api_client: API client routed to the app.

    Returns:
        Created agent.
    """
    return await api_client.agents.create(AgentCreateRequest(name="support-bot"))


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a recorded session through the SDK."""
    agent = await create_agent(api_client)
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            inputs={"prompt": "hi"},
            framework="pydantic_ai",
        )
    )
    assert session.agent_id == agent.id
    assert session.owner_id == ACCOUNT.id
    assert session.origin is SessionOrigin.RECORDED
    assert session.status is SessionStatus.IN_PROGRESS
    assert session.inputs == {"prompt": "hi"}


async def test_create_duplicate_import(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    agent = await create_agent(api_client)
    request = SessionCreateRequest(
        agent_id=agent.id,
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
        provider=SessionProvider.LANGFUSE,
        external_id="lf-1",
    )
    await api_client.sessions.create(request)
    with pytest.raises(ConflictError) as exc_info:
        await api_client.sessions.create(request)
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == (
        "Session external id 'lf-1' is already registered for provider 'langfuse'"
    )


async def test_create_unknown_agent(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.sessions.create(
            SessionCreateRequest(agent_id=uuid.uuid4(), origin=SessionOrigin.RECORDED)
        )


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a session by id through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    loaded = await api_client.sessions.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List sessions with filters and pagination through the SDK."""
    agent = await create_agent(api_client)
    for name in ["one", "two", "three"]:
        await api_client.sessions.create(
            SessionCreateRequest(
                agent_id=agent.id, origin=SessionOrigin.RECORDED, name=name
            )
        )
    await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent.id,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            provider=SessionProvider.LANGFUSE,
            external_id="lf-1",
        )
    )

    page = await api_client.sessions.list()
    assert page.total == 4

    page = await api_client.sessions.list(origin=SessionOrigin.RECORDED)
    assert page.total == 3
    assert [item.name for item in page.items] == ["one", "two", "three"]

    page = await api_client.sessions.list(
        origin=SessionOrigin.RECORDED, page=2, page_size=2
    )
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["three"]

    page = await api_client.sessions.list(
        provider=SessionProvider.LANGFUSE, external_id="lf-1"
    )
    assert page.total == 1
    assert page.items[0].origin is SessionOrigin.IMPORTED


async def test_list_falsy_filters(api_client: KitaruAPIClient) -> None:
    """Send a zero uuid and an empty string filter as query params."""
    agent = await create_agent(api_client)
    await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )

    with pytest.raises(NotFoundError):
        await api_client.sessions.list(agent_id=uuid.UUID(int=0))

    page = await api_client.sessions.list(name="")
    assert page.total == 0


async def test_update_finish(api_client: KitaruAPIClient) -> None:
    """Finish a session through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    finished = await api_client.sessions.update(
        created.id,
        SessionUpdateRequest(
            status=SessionStatus.COMPLETED,
            outputs={"answer": "42"},
            ended_at=ENDED_AT,
        ),
    )
    assert finished.status is SessionStatus.COMPLETED
    assert finished.outputs == {"answer": "42"}
    assert finished.ended_at == ENDED_AT

    with pytest.raises(ConflictError) as exc_info:
        await api_client.sessions.update(
            created.id, SessionUpdateRequest(status=SessionStatus.FAILED)
        )
    assert exc_info.value.status_code == 409


async def test_update_null_clears_fields(api_client: KitaruAPIClient) -> None:
    """Clear name and expected through explicit None on the request."""
    agent = await create_agent(api_client)
    created = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            name="run-1",
            expected={"answer": "42"},
        )
    )
    updated = await api_client.sessions.update(
        created.id, SessionUpdateRequest(name=None, expected=None)
    )
    assert updated.name is None
    assert updated.expected is None
    loaded = await api_client.sessions.get(created.id)
    assert loaded.name is None
    assert loaded.expected is None


async def test_merge_scores(api_client: KitaruAPIClient) -> None:
    """Merge scores through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    updated = await api_client.sessions.merge_scores(
        created.id, SessionScoresRequest(scores={"conciseness": 0.5})
    )
    assert updated.scores == {"conciseness": 0.5}
    updated = await api_client.sessions.merge_scores(
        created.id, SessionScoresRequest(scores={"conciseness": 0.7, "tone": 1.0})
    )
    assert updated.scores == {"conciseness": 0.7, "tone": 1.0}

    page = await api_client.sessions.list(has_score=True)
    assert page.total == 1


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a session through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    await api_client.sessions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.sessions.get(created.id)
