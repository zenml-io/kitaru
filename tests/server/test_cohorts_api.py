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

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeAgentRepository, FakeCohortRepository, create_agent
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
def cohort_repository() -> FakeCohortRepository:
    """Provide the fake cohort repository backing the app."""
    return FakeCohortRepository()


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    cohort_repository: FakeCohortRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed cohort services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    cohort_service = CohortService(
        repository=cohort_repository, agent_repository=agent_repository
    )
    app.dependency_overrides[get_cohort_service] = lambda: cohort_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> str:
    """Provide the id of an agent to own cohorts."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    return str(agent.id)


async def test_create_cohort(client: httpx.AsyncClient, agent_id: str) -> None:
    """Create a cohort and observe HTTP 201."""
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "smoke-test",
            "description": "A cohort",
            "agent_id": agent_id,
            "metadata": {"team": "eval"},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "smoke-test"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["agent_id"] == agent_id
    assert body["metadata"] == {"team": "eval"}
    assert body["latest_version"] == 0
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_create_cohort_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/v1/cohorts", json={"name": "cohort", "agent_id": str(uuid.uuid4())}
    )
    assert response.status_code == 404


async def test_create_cohort_duplicate_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 409 for a duplicate cohort name."""
    body = {"name": "cohort", "agent_id": agent_id}
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 409
    assert response.json() == {"detail": "Cohort name 'cohort' is already registered"}


async def test_get_cohort(client: httpx.AsyncClient, agent_id: str) -> None:
    """Get a cohort by id."""
    created = (
        await client.post("/v1/cohorts", json={"name": "cohort", "agent_id": agent_id})
    ).json()
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.get(f"/v1/cohorts/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_cohorts(client: httpx.AsyncClient, agent_id: str) -> None:
    """List cohorts newest-first with a name filter."""
    for name in ["alpha", "beta"]:
        await client.post("/v1/cohorts", json={"name": name, "agent_id": agent_id})

    response = await client.get("/v1/cohorts")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["beta", "alpha"]

    filter_expression = {"field": "name", "op": "eq", "value": "alpha"}
    response = await client.get(
        "/v1/cohorts", params={"filter": json.dumps(filter_expression)}
    )
    assert response.json()["items"][0]["name"] == "alpha"


async def test_update_cohort(client: httpx.AsyncClient, agent_id: str) -> None:
    """Update a cohort's name, description, and metadata."""
    created = (
        await client.post(
            "/v1/cohorts",
            json={"name": "cohort", "description": "old", "agent_id": agent_id},
        )
    ).json()
    response = await client.patch(
        f"/v1/cohorts/{created['id']}",
        json={
            "name": "renamed",
            "description": "new",
            "metadata": {"team": "eval"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["description"] == "new"
    assert body["metadata"] == {"team": "eval"}


async def test_update_cohort_cannot_clear_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when clearing the cohort name."""
    created = (
        await client.post("/v1/cohorts", json={"name": "cohort", "agent_id": agent_id})
    ).json()
    response = await client.patch(f"/v1/cohorts/{created['id']}", json={"name": None})
    assert response.status_code == 422


async def test_update_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.patch(
        f"/v1/cohorts/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_cohort(client: httpx.AsyncClient, agent_id: str) -> None:
    """Delete a cohort."""
    created = (
        await client.post("/v1/cohorts", json={"name": "cohort", "agent_id": agent_id})
    ).json()
    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 404


async def test_delete_cohort_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing cohort."""
    response = await client.delete(f"/v1/cohorts/{uuid.uuid4()}")
    assert response.status_code == 404
