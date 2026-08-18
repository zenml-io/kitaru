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

import json
import uuid
from collections.abc import AsyncGenerator
from functools import partial

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
from kitaru.server.api.agent_deletion import get_agent_deleter
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


async def _delete_agent(
    service: AgentService, agent_id: uuid.UUID, actor: AuthContext
) -> None:
    """Drive the deletion through one fake-backed agent service."""
    await service.delete_agent(agent_id, actor=actor)


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed agent services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(
        repository=FakeAgentVersionRepository(agent_repository)
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    app.dependency_overrides[get_agent_deleter] = lambda: partial(
        _delete_agent, agent_service
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_agent(client: httpx.AsyncClient) -> None:
    """Create an agent and observe HTTP 201."""
    response = await client.post(
        "/api/v1/agents", json={"name": "assistant", "description": "Helps"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "assistant"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["description"] == "Helps"
    assert body["latest_version"] == 0
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_agent_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate agent name."""
    response = await client.post("/api/v1/agents", json={"name": "assistant"})
    assert response.status_code == 201
    response = await client.post("/api/v1/agents", json={"name": "assistant"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Agent name 'assistant' is already registered"}


async def test_create_agent_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid agent name."""
    response = await client.post("/api/v1/agents", json={"name": "in valid"})
    assert response.status_code == 422


async def test_get_agent(client: httpx.AsyncClient) -> None:
    """Get an agent by id."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.get(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent."""
    response = await client.get(f"/api/v1/agents/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_agents(client: httpx.AsyncClient) -> None:
    """List agents newest-first with filters."""
    for name in ["assistant", "reviewer", "triager"]:
        await client.post("/api/v1/agents", json={"name": name})

    response = await client.get("/api/v1/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == [
        "triager",
        "reviewer",
        "assistant",
    ]

    filter_expression = {"field": "name", "op": "eq", "value": "reviewer"}
    response = await client.get(
        "/api/v1/agents", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "reviewer"


async def test_update_agent(client: httpx.AsyncClient) -> None:
    """Update an agent's name and description."""
    created = (
        await client.post(
            "/api/v1/agents", json={"name": "assistant", "description": "Helps"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agents/{created['id']}",
        json={"name": "renamed", "description": "Reviews"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["description"] == "Reviews"


async def test_update_agent_clears_description(client: httpx.AsyncClient) -> None:
    """Clear an agent's description with an explicit null."""
    created = (
        await client.post(
            "/api/v1/agents", json={"name": "assistant", "description": "Helps"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agents/{created['id']}", json={"description": None}
    )
    assert response.status_code == 200
    assert response.json()["description"] is None


async def test_update_agent_cannot_clear_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when clearing the agent name."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.patch(
        f"/api/v1/agents/{created['id']}", json={"name": None}
    )
    assert response.status_code == 422


async def test_update_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent."""
    response = await client.patch(
        f"/api/v1/agents/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_agent(client: httpx.AsyncClient) -> None:
    """Delete an agent."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.delete(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 404


async def test_delete_agent_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent."""
    response = await client.delete(f"/api/v1/agents/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_agent_cascades_versions(client: httpx.AsyncClient) -> None:
    """Deleting an agent cascades its versions."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(f"/api/v1/agents/{created['id']}/versions", json={})
    ).json()

    response = await client.delete(f"/api/v1/agents/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/agent-versions/{version['id']}")
    assert response.status_code == 404


async def test_create_agent_version(client: httpx.AsyncClient) -> None:
    """Create a version of an agent and observe HTTP 201."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    response = await client.post(
        f"/api/v1/agents/{agent['id']}/versions",
        json={"display_version": "v1", "description": "First cut"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["agent_id"] == agent["id"]
    assert body["version"] == 1
    assert body["display_version"] == "v1"
    assert body["description"] == "First cut"
    assert body["run_spec"] is None
    assert body["capabilities"] == {"tools": [], "mcp_servers": [], "skills": []}


async def test_create_agent_version_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(f"/api/v1/agents/{uuid.uuid4()}/versions", json={})
    assert response.status_code == 404


async def test_create_agent_version_with_run_spec(client: httpx.AsyncClient) -> None:
    """Create a version carrying a run spec and capabilities."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    secret_id = str(uuid.uuid4())
    response = await client.post(
        f"/api/v1/agents/{agent['id']}/versions",
        json={
            "run_spec": {
                "command": "run.sh",
                "working_dir": "/app",
                "env": {"FOO": "bar"},
                "secret_ids": [secret_id],
                "timeout_seconds": 120,
            },
            "capabilities": {"tools": ["search"]},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["run_spec"]["command"] == "run.sh"
    assert body["run_spec"]["secret_ids"] == [secret_id]
    assert body["run_spec"]["timeout_seconds"] == 120
    assert body["capabilities"] == {
        "tools": ["search"],
        "mcp_servers": [],
        "skills": [],
    }


async def test_list_agent_versions(client: httpx.AsyncClient) -> None:
    """List the versions of one agent, newest-first."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    other_agent = (
        await client.post("/api/v1/agents", json={"name": "reviewer"})
    ).json()
    await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    await client.post(f"/api/v1/agents/{agent['id']}/versions", json={})
    await client.post(f"/api/v1/agents/{other_agent['id']}/versions", json={})

    response = await client.get(f"/api/v1/agents/{agent['id']}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["version"] for item in body["items"]] == [2, 1]
    assert all(item["agent_id"] == agent["id"] for item in body["items"])
