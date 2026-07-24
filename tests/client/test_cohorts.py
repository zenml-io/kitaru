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
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
    FakeTagRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.agents import AgentCreateRequest, AgentResponse
from kitaru.api_models.v1.cohorts import (
    CohortCreateRequest,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    SessionUpdateRequest,
)
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
    version_repository = FakeAgentVersionRepository(agent_repository)
    tag_repository = FakeTagRepository()
    session_repository = FakeSessionRepository(
        agent_repository, version_repository, tag_repository
    )
    node_repository = FakeSessionNodeRepository(session_repository)
    cohort_repository = FakeCohortRepository(
        session_repository, agent_repository, tag_repository
    )
    job_repository = FakeJobRepository(
        session_repository, version_repository, FakeReplayConfigRepository()
    )
    agent_service = AgentService(repository=agent_repository)
    session_service = SessionService(
        repository=session_repository,
        agent_repository=agent_repository,
        agent_version_repository=version_repository,
        node_repository=node_repository,
        job_repository=job_repository,
    )
    cohort_service = CohortService(
        repository=cohort_repository,
        session_repository=session_repository,
        agent_repository=agent_repository,
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
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


async def create_completed_session(
    api_client: KitaruAPIClient, agent_id: uuid.UUID, name: str | None = None
) -> SessionResponse:
    """Store a completed recorded session through the SDK.

    Args:
        api_client: API client routed to the app.
        agent_id: Id of the agent.
        name: Session name.

    Returns:
        Finished session.
    """
    session = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=agent_id, origin=SessionOrigin.RECORDED, name=name
        )
    )
    return await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )


async def test_create_from_session_ids(api_client: KitaruAPIClient) -> None:
    """Create a cohort from explicit session ids through the SDK."""
    agent = await create_agent(api_client)
    first = await create_completed_session(api_client, agent.id)
    second = await create_completed_session(api_client, agent.id)
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[first.id, second.id]
        )
    )
    assert cohort.owner_id == ACCOUNT.id
    assert cohort.name == "baseline"
    assert cohort.agent_id == agent.id
    assert cohort.session_count == 2


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    request = CohortCreateRequest(
        name="baseline", agent_id=agent.id, session_ids=[session.id]
    )
    await api_client.cohorts.create(request)
    with pytest.raises(APIError) as exc_info:
        await api_client.cohorts.create(request)
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Cohort name 'baseline' is already registered"


async def test_create_in_progress_member(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 as a typed error."""
    agent = await create_agent(api_client)
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.cohorts.create(
            CohortCreateRequest(
                name="baseline", agent_id=agent.id, session_ids=[session.id]
            )
        )
    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == f"Session {session.id} is in progress"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a cohort by id through the SDK."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    created = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[session.id]
        )
    )
    loaded = await api_client.cohorts.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List cohorts with filters and pagination through the SDK."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    for name in ["one", "two", "three"]:
        await api_client.cohorts.create(
            CohortCreateRequest(name=name, agent_id=agent.id, session_ids=[session.id])
        )

    page = await api_client.cohorts.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["one", "two", "three"]

    page = await api_client.cohorts.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["three"]

    page = await api_client.cohorts.list(name="two")
    assert page.total == 1


async def test_list_sessions_ordered(api_client: KitaruAPIClient) -> None:
    """List member sessions in position order through the SDK."""
    agent = await create_agent(api_client)
    sessions = [await create_completed_session(api_client, agent.id) for _ in range(3)]
    ordered = [sessions[2], sessions[0], sessions[1]]
    cohort = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline",
            agent_id=agent.id,
            session_ids=[session.id for session in ordered],
        )
    )
    page = await api_client.cohorts.list_sessions(cohort.id)
    assert page.total == 3
    assert [item.id for item in page.items] == [session.id for session in ordered]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a cohort through the SDK."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    created = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[session.id]
        )
    )
    updated = await api_client.cohorts.update(
        created.id, CohortUpdateRequest(name="july", description="July sessions")
    )
    assert updated.name == "july"
    assert updated.description == "July sessions"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a cohort through the SDK."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    created = await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[session.id]
        )
    )
    await api_client.cohorts.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.cohorts.get(created.id)


async def test_delete_session_in_cohort(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for deleting a session that is in a cohort."""
    agent = await create_agent(api_client)
    session = await create_completed_session(api_client, agent.id)
    await api_client.cohorts.create(
        CohortCreateRequest(
            name="baseline", agent_id=agent.id, session_ids=[session.id]
        )
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.sessions.delete(session.id)
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == f"Session {session.id} is referenced by cohorts"
