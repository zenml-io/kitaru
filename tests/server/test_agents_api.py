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
"""Tests for the agent routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeSessionRepository,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_agent_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
)

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
async def client(
    repository: FakeAgentRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed agent service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = AgentService(repository=repository)
    app.dependency_overrides[get_agent_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_agent(client: httpx.AsyncClient) -> None:
    """Create an agent and observe HTTP 201."""
    response = await client.post(
        "/v1/agents",
        json={"name": "support-bot", "description": "Answers tickets"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "support-bot"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["description"] == "Answers tickets"
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_agent_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate agent name."""
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 201
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Agent name 'support-bot' is already registered"
    }


async def test_create_agent_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid agent name."""
    response = await client.post("/v1/agents", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_agents(client: httpx.AsyncClient) -> None:
    """List agents with filters and pagination."""
    for name in ["support-bot", "triage-bot", "coder"]:
        response = await client.post("/v1/agents", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["name"] for item in body["items"]] == [
        "support-bot",
        "triage-bot",
        "coder",
    ]

    response = await client.get("/v1/agents", params={"name": "triage-bot"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["name"] == "triage-bot"

    response = await client.get("/v1/agents", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["coder"]


async def test_list_agents_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/v1/agents", params={"page": 0})
    assert response.status_code == 422
    response = await client.get("/v1/agents", params={"page_size": 1001})
    assert response.status_code == 422


async def test_get_agent(client: httpx.AsyncClient) -> None:
    """Get an agent by id."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/agents/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_id} was not found"}


async def test_update_agent(client: httpx.AsyncClient) -> None:
    """Update an agent."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.patch(
        f"/v1/agents/{created['id']}",
        json={"name": "triage-bot", "description": "Sorts tickets"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "triage-bot"
    assert body["description"] == "Sorts tickets"

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json()["name"] == "triage-bot"


async def test_update_agent_absent_fields_unchanged(client: httpx.AsyncClient) -> None:
    """Keep every field on an update with an empty body."""
    created = (
        await client.post(
            "/v1/agents",
            json={"name": "support-bot", "description": "Answers tickets"},
        )
    ).json()
    response = await client.patch(f"/v1/agents/{created['id']}", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "support-bot"
    assert body["description"] == "Answers tickets"


async def test_update_agent_null_clears_description(client: httpx.AsyncClient) -> None:
    """Clear the description on an explicit null."""
    created = (
        await client.post(
            "/v1/agents",
            json={"name": "support-bot", "description": "Answers tickets"},
        )
    ).json()
    response = await client.patch(
        f"/v1/agents/{created['id']}", json={"description": None}
    )
    assert response.status_code == 200
    assert response.json()["description"] is None


async def test_update_agent_null_name_rejected(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an explicit null name."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.patch(f"/v1/agents/{created['id']}", json={"name": None})
    assert response.status_code == 422
    assert response.json() == {"detail": "Agent name cannot be null"}


async def test_update_agent_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when renaming to a registered name."""
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 201
    other = (await client.post("/v1/agents", json={"name": "triage-bot"})).json()
    response = await client.patch(
        f"/v1/agents/{other['id']}", json={"name": "support-bot"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Agent name 'support-bot' is already registered"
    }


async def test_update_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    response = await client.patch(
        f"/v1/agents/{uuid.uuid4()}", json={"name": "triage-bot"}
    )
    assert response.status_code == 404


async def test_delete_agent(client: httpx.AsyncClient) -> None:
    """Delete an agent and observe HTTP 204."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.delete(f"/v1/agents/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 404


async def test_delete_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    response = await client.delete(f"/v1/agents/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_agent_with_versions(
    client: httpx.AsyncClient, repository: FakeAgentRepository
) -> None:
    """Observe HTTP 409 when deleting an agent that still has versions."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    agent_id = uuid.UUID(created["id"])
    version_repository = FakeAgentVersionRepository(repository)
    await version_repository.create(
        AgentVersion(owner_id=ACCOUNT.id, agent_id=agent_id, version="v1")
    )
    response = await client.delete(f"/v1/agents/{agent_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Agent {agent_id} is referenced by agent versions"
    }
    response = await client.get(f"/v1/agents/{agent_id}")
    assert response.status_code == 200


async def test_delete_agent_with_sessions(
    client: httpx.AsyncClient, repository: FakeAgentRepository
) -> None:
    """Observe HTTP 409 when deleting an agent that still has sessions."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    agent_id = uuid.UUID(created["id"])
    session_repository = FakeSessionRepository(repository)
    await session_repository.create(
        Session(
            owner_id=ACCOUNT.id,
            agent_id=agent_id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    response = await client.delete(f"/v1/agents/{agent_id}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent {agent_id} is referenced by sessions"}
    response = await client.get(f"/v1/agents/{agent_id}")
    assert response.status_code == 200


async def test_delete_agent_with_cohorts(
    client: httpx.AsyncClient, repository: FakeAgentRepository
) -> None:
    """Observe HTTP 409 when deleting an agent that is referenced by cohorts."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    agent_id = uuid.UUID(created["id"])
    session_repository = FakeSessionRepository(repository)
    cohort_repository = FakeCohortRepository(session_repository, repository)
    await cohort_repository.create(
        Cohort(
            owner_id=ACCOUNT.id,
            agent_id=agent_id,
            name="baseline",
            session_count=0,
        ),
        [],
    )
    response = await client.delete(f"/v1/agents/{agent_id}")
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent {agent_id} is referenced by cohorts"}
    response = await client.get(f"/v1/agents/{agent_id}")
    assert response.status_code == 200
