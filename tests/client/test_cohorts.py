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
"""Round-trip tests for the cohorts SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeCohortRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortSessionsListParams,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_cohort_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    session_repository = FakeSessionRepository()
    cohort_repository = FakeCohortRepository(session_repository)
    app.dependency_overrides[get_agent_service] = lambda: AgentService(
        repository=agent_repository
    )
    app.dependency_overrides[get_session_service] = lambda: SessionService(
        repository=session_repository, task_repository=FakeTaskRepository()
    )
    app.dependency_overrides[get_cohort_service] = lambda: CohortService(
        repository=cohort_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _make_session(api_client: KitaruAPIClient, agent_id: uuid.UUID) -> uuid.UUID:
    """Create a session on the given agent through the SDK."""
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            inputs=None,
            outputs=None,
            expected=None,
        )
    )
    return session.id


async def _make_agent_and_sessions(
    api_client: KitaruAPIClient, count: int = 2
) -> tuple[uuid.UUID, list[uuid.UUID]]:
    """Create an agent and a number of sessions attached to it through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name=f"assistant-{uuid.uuid4().hex[:8]}")
    )
    session_ids = [await _make_session(api_client, agent.id) for _ in range(count)]
    return agent.id, session_ids


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a cohort through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(
            name="smoke-test",
            description="A cohort",
            agent_id=agent_id,
            session_ids=session_ids,
        )
    )
    assert isinstance(cohort, CohortResponse)
    assert cohort.name == "smoke-test"
    assert cohort.owner_id == ACCOUNT.id
    assert cohort.agent_id == agent_id
    assert cohort.session_count == 2


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=1)
    await api_client.cohorts.create(
        CohortCreateRequest(
            name="smoke-test", agent_id=agent_id, session_ids=session_ids
        )
    )
    other_session_id = await _make_session(api_client, agent_id)
    with pytest.raises(APIError) as exc_info:
        await api_client.cohorts.create(
            CohortCreateRequest(
                name="smoke-test",
                agent_id=agent_id,
                session_ids=[other_session_id],
            )
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Cohort name 'smoke-test' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a cohort by id through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=1)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id, session_ids=session_ids)
    )
    loaded = await api_client.cohorts.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List cohorts newest-first with filters through the SDK."""
    agent_id, _ = await _make_agent_and_sessions(api_client, count=1)
    for name in ["alpha", "beta"]:
        session_id = await _make_session(api_client, agent_id)
        await api_client.cohorts.create(
            CohortCreateRequest(name=name, agent_id=agent_id, session_ids=[session_id])
        )

    page = await api_client.cohorts.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["beta", "alpha"]

    page = await api_client.cohorts.list(CohortListParams(name="alpha"))
    assert page.items[0].name == "alpha"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every cohort across pages through the SDK."""
    for name in ["alpha", "beta", "gamma"]:
        agent_id, session_ids = await _make_agent_and_sessions(api_client, count=1)
        await api_client.cohorts.create(
            CohortCreateRequest(name=name, agent_id=agent_id, session_ids=session_ids)
        )

    collected = [
        item.name async for item in api_client.cohorts.iter(CohortListParams(size=1))
    ]
    assert collected == ["gamma", "beta", "alpha"]


async def test_list_sessions(api_client: KitaruAPIClient) -> None:
    """List a cohort's sessions in cohort order through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=3)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id, session_ids=session_ids)
    )
    page = await api_client.cohorts.list_sessions(created.id)
    assert page.next_cursor is None
    assert [item.id for item in page.items] == session_ids


async def test_iter_sessions(api_client: KitaruAPIClient) -> None:
    """Iterate every member session of a cohort across pages through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=5)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id, session_ids=session_ids)
    )
    collected = [
        item.id
        async for item in api_client.cohorts.iter_sessions(
            created.id, CohortSessionsListParams(size=2)
        )
    ]
    assert collected == session_ids


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a cohort through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=1)
    created = await api_client.cohorts.create(
        CohortCreateRequest(
            name="cohort", description="old", agent_id=agent_id, session_ids=session_ids
        )
    )
    updated = await api_client.cohorts.update(
        created.id, CohortUpdateRequest(description="new")
    )
    assert updated.description == "new"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a cohort through the SDK."""
    agent_id, session_ids = await _make_agent_and_sessions(api_client, count=1)
    created = await api_client.cohorts.create(
        CohortCreateRequest(name="cohort", agent_id=agent_id, session_ids=session_ids)
    )
    await api_client.cohorts.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(created.id)
