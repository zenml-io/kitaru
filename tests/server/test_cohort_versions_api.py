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
"""Tests for the cohort version routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_cohort_service,
    get_cohort_version_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def session_repository() -> FakeSessionRepository:
    """Provide the fake session repository backing the app."""
    return FakeSessionRepository()


@pytest.fixture
def cohort_repository() -> FakeCohortRepository:
    """Provide the fake cohort repository backing the app."""
    return FakeCohortRepository()


@pytest.fixture
def cohort_version_repository(
    cohort_repository: FakeCohortRepository, session_repository: FakeSessionRepository
) -> FakeCohortVersionRepository:
    """Provide the fake cohort version repository backing the app."""
    return FakeCohortVersionRepository(cohort_repository, session_repository)


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
    cohort_version_repository: FakeCohortVersionRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed cohort services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    cohort_service = CohortService(
        repository=cohort_repository, agent_repository=agent_repository
    )
    version_service = CohortVersionService(
        repository=cohort_version_repository,
        cohort_repository=cohort_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
    app.dependency_overrides[get_cohort_version_service] = lambda: version_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> str:
    """Provide the id of an agent to attach cohorts and sessions to."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    return str(agent.id)


@pytest.fixture
async def cohort_id(client: httpx.AsyncClient, agent_id: str) -> str:
    """Provide the id of a cohort to version."""
    created = (
        await client.post(
            "/api/v1/cohorts", json={"name": "cohort", "agent_id": agent_id}
        )
    ).json()
    return created["id"]


async def _make_session_id(
    session_repository: FakeSessionRepository, agent_id: str
) -> str:
    """Store a session on the given agent and return its id."""
    session = await create_session(session_repository, ACCOUNT.id, uuid.UUID(agent_id))
    return str(session.id)


async def test_create_cohort_version(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    agent_id: str,
    cohort_id: str,
) -> None:
    """Create the first version of a cohort and observe HTTP 201."""
    session_ids = [
        await _make_session_id(session_repository, agent_id),
        await _make_session_id(session_repository, agent_id),
    ]
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions",
        json={"add_session_ids": session_ids, "display_version": "v1"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["cohort_id"] == cohort_id
    assert body["version"] == 1
    assert body["display_version"] == "v1"
    assert body["session_count"] == 2
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_create_cohort_version_missing_cohort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the cohort does not exist."""
    response = await client.post(f"/api/v1/cohorts/{uuid.uuid4()}/versions", json={})
    assert response.status_code == 404


async def test_create_cohort_version_remove_nonmember(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Observe HTTP 422 when removing a session absent from the base version."""
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions",
        json={"remove_session_ids": [str(uuid.uuid4())]},
    )
    assert response.status_code == 422


async def test_create_cohort_version_add_duplicate_member(
    client: httpx.AsyncClient,
    session_repository: FakeSessionRepository,
    agent_id: str,
    cohort_id: str,
) -> None:
    """Observe HTTP 422 when adding a session already in the base version."""
    session_id = await _make_session_id(session_repository, agent_id)
    await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions", json={"add_session_ids": [session_id]}
    )
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions", json={"add_session_ids": [session_id]}
    )
    assert response.status_code == 422


async def test_create_cohort_version_added_session_missing(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Observe HTTP 422 when an added session does not exist."""
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions",
        json={"add_session_ids": [str(uuid.uuid4())]},
    )
    assert response.status_code == 422


async def test_create_cohort_version_added_session_wrong_agent(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    cohort_id: str,
) -> None:
    """Observe HTTP 422 when an added session belongs to a different agent."""
    other_agent = await create_agent(agent_repository, ACCOUNT.id, name="other")
    foreign_session = await create_session(
        session_repository, ACCOUNT.id, other_agent.id
    )
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions",
        json={"add_session_ids": [str(foreign_session.id)]},
    )
    assert response.status_code == 422


async def test_create_cohort_version_invalid_display_version(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Observe HTTP 422 for a display version starting with a separator."""
    response = await client.post(
        f"/api/v1/cohorts/{cohort_id}/versions", json={"display_version": "/bad"}
    )
    assert response.status_code == 422


async def test_latest_version_reflected_on_cohort(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Bump the cohort's latest_version on every created version."""
    await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})
    await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})
    response = await client.get(f"/api/v1/cohorts/{cohort_id}")
    assert response.json()["latest_version"] == 2


async def test_list_cohort_versions(
    client: httpx.AsyncClient, agent_id: str, cohort_id: str
) -> None:
    """List a cohort's versions, newest-first."""
    other_cohort = (
        await client.post(
            "/api/v1/cohorts", json={"name": "other", "agent_id": agent_id}
        )
    ).json()
    first = (await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})).json()
    second = (
        await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})
    ).json()
    await client.post(f"/api/v1/cohorts/{other_cohort['id']}/versions", json={})

    response = await client.get(f"/api/v1/cohorts/{cohort_id}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["id"] for item in body["items"]] == [second["id"], first["id"]]


async def test_list_cohort_versions_walks_pages(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Walk every page of a cohort's versions via next_cursor."""
    created = [
        (await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})).json()
        for _ in range(3)
    ]
    expected_order = list(reversed([item["id"] for item in created]))

    collected: list[str] = []
    cursor = None
    while True:
        params = {"size": 1}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get(
            f"/api/v1/cohorts/{cohort_id}/versions", params=params
        )
        body = response.json()
        collected.extend(item["id"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        cursor = body["next_cursor"]

    assert collected == expected_order


async def test_get_cohort_version(client: httpx.AsyncClient, cohort_id: str) -> None:
    """Get a cohort version by id."""
    created = (
        await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})
    ).json()
    response = await client.get(f"/api/v1/cohort-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_cohort_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort version."""
    response = await client.get(f"/api/v1/cohort-versions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_update_cohort_version(client: httpx.AsyncClient, cohort_id: str) -> None:
    """Update a cohort version's display version."""
    created = (
        await client.post(
            f"/api/v1/cohorts/{cohort_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/cohort-versions/{created['id']}", json={"display_version": "v1.1"}
    )
    assert response.status_code == 200
    assert response.json()["display_version"] == "v1.1"


async def test_update_cohort_version_clears_display_version(
    client: httpx.AsyncClient, cohort_id: str
) -> None:
    """Clear the display version with an explicit null."""
    created = (
        await client.post(
            f"/api/v1/cohorts/{cohort_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/cohort-versions/{created['id']}", json={"display_version": None}
    )
    assert response.status_code == 200
    assert response.json()["display_version"] is None


async def test_update_cohort_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort version."""
    response = await client.patch(
        f"/api/v1/cohort-versions/{uuid.uuid4()}", json={"display_version": "v2"}
    )
    assert response.status_code == 404


async def test_delete_cohort_version(client: httpx.AsyncClient, cohort_id: str) -> None:
    """Delete a cohort version."""
    created = (
        await client.post(f"/api/v1/cohorts/{cohort_id}/versions", json={})
    ).json()
    response = await client.delete(f"/api/v1/cohort-versions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/cohort-versions/{created['id']}")
    assert response.status_code == 404


async def test_delete_cohort_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort version."""
    response = await client.delete(f"/api/v1/cohort-versions/{uuid.uuid4()}")
    assert response.status_code == 404
