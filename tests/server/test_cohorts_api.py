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
"""Tests for the cohort routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeCohortRepository,
    FakeSessionRepository,
    create_agent,
    create_session,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_cohort_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.cohort_service import CohortService
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
def cohort_repository(
    session_repository: FakeSessionRepository,
) -> FakeCohortRepository:
    """Provide the fake cohort repository backing the app."""
    return FakeCohortRepository(session_repository)


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    cohort_repository: FakeCohortRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed cohort services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    cohort_service = CohortService(
        repository=cohort_repository,
        agent_repository=agent_repository,
        session_repository=session_repository,
    )
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _make_agent_and_sessions(
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
    count: int = 2,
) -> tuple[str, list[str]]:
    """Store an agent and a number of sessions attached to it.

    Returns:
        Agent id and member session ids as strings.
    """
    agent = await create_agent(agent_repository, ACCOUNT.id)
    session_ids = [
        (await create_session(session_repository, ACCOUNT.id, agent.id)).id
        for _ in range(count)
    ]
    return str(agent.id), [str(session_id) for session_id in session_ids]


async def test_create_cohort(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Create a cohort and observe HTTP 201."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository
    )
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "smoke-test",
            "description": "A cohort",
            "agent_id": agent_id,
            "session_ids": session_ids,
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "smoke-test"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["agent_id"] == agent_id
    assert body["session_count"] == 2
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_create_cohort_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "cohort",
            "agent_id": str(uuid.uuid4()),
            "session_ids": [str(uuid.uuid4())],
        },
    )
    assert response.status_code == 404


async def test_create_cohort_empty_members(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an empty member list."""
    response = await client.post(
        "/v1/cohorts",
        json={"name": "cohort", "agent_id": str(uuid.uuid4()), "session_ids": []},
    )
    assert response.status_code == 422


async def test_create_cohort_duplicate_members(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Observe HTTP 422 for a repeated session id."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "cohort",
            "agent_id": agent_id,
            "session_ids": [session_ids[0], session_ids[0]],
        },
    )
    assert response.status_code == 422


async def test_create_cohort_missing_session(
    client: httpx.AsyncClient, agent_repository: FakeAgentRepository
) -> None:
    """Observe HTTP 422 when a member session does not exist."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "cohort",
            "agent_id": str(agent.id),
            "session_ids": [str(uuid.uuid4())],
        },
    )
    assert response.status_code == 422


async def test_create_cohort_session_wrong_agent(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Observe HTTP 422 when a member session belongs to a different agent."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    other_agent = await create_agent(agent_repository, ACCOUNT.id, name="other")
    foreign_session = await create_session(
        session_repository, ACCOUNT.id, other_agent.id
    )
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "cohort",
            "agent_id": str(agent.id),
            "session_ids": [str(foreign_session.id)],
        },
    )
    assert response.status_code == 422


async def test_create_cohort_duplicate_name(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Observe HTTP 409 for a duplicate cohort name."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    body = {
        "name": "cohort",
        "agent_id": agent_id,
        "session_ids": [session_ids[0]],
    }
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 201
    session = await create_session(session_repository, ACCOUNT.id, uuid.UUID(agent_id))
    body = {**body, "session_ids": [str(session.id)]}
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 409
    assert response.json() == {"detail": "Cohort name 'cohort' is already registered"}


async def test_get_cohort(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Get a cohort by id."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "agent_id": agent_id, "session_ids": session_ids},
        )
    ).json()
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.get(f"/v1/cohorts/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_cohorts(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """List cohorts newest-first with a name filter."""
    agent_id, _ = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    for name in ["alpha", "beta"]:
        session = await create_session(
            session_repository, ACCOUNT.id, uuid.UUID(agent_id)
        )
        await client.post(
            "/v1/cohorts",
            json={
                "name": name,
                "agent_id": agent_id,
                "session_ids": [str(session.id)],
            },
        )

    response = await client.get("/v1/cohorts")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["beta", "alpha"]

    response = await client.get("/v1/cohorts", params={"name": "alpha"})
    assert response.json()["items"][0]["name"] == "alpha"


async def test_list_cohort_sessions(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """List a cohort's sessions in cohort order."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=3
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "agent_id": agent_id, "session_ids": session_ids},
        )
    ).json()
    response = await client.get(f"/v1/cohorts/{created['id']}/sessions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["id"] for item in body["items"]] == session_ids


async def test_list_cohort_sessions_walks_pages(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Walk every page of a cohort's sessions in fixed member order."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=5
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "agent_id": agent_id, "session_ids": session_ids},
        )
    ).json()

    collected: list[str] = []
    cursor = None
    while True:
        params = {"size": 2}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get(
            f"/v1/cohorts/{created['id']}/sessions", params=params
        )
        body = response.json()
        collected.extend(item["id"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        cursor = body["next_cursor"]

    assert collected == session_ids


async def test_update_cohort(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Update a cohort's name and description."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={
                "name": "cohort",
                "description": "old",
                "agent_id": agent_id,
                "session_ids": session_ids,
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/cohorts/{created['id']}",
        json={"name": "renamed", "description": "new"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["description"] == "new"


async def test_update_cohort_cannot_clear_name(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Observe HTTP 422 when clearing the cohort name."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "agent_id": agent_id, "session_ids": session_ids},
        )
    ).json()
    response = await client.patch(f"/v1/cohorts/{created['id']}", json={"name": None})
    assert response.status_code == 422


async def test_update_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.patch(
        f"/v1/cohorts/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_cohort(
    client: httpx.AsyncClient,
    agent_repository: FakeAgentRepository,
    session_repository: FakeSessionRepository,
) -> None:
    """Delete a cohort."""
    agent_id, session_ids = await _make_agent_and_sessions(
        agent_repository, session_repository, count=1
    )
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "agent_id": agent_id, "session_ids": session_ids},
        )
    ).json()
    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 404


async def test_delete_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.delete(f"/v1/cohorts/{uuid.uuid4()}")
    assert response.status_code == 404
