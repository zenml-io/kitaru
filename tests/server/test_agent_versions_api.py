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
"""Tests for the agent version routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeSecretRepository,
    FakeSessionRepository,
    create_secret,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
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
def secret_repository() -> FakeSecretRepository:
    """Provide the fake secret repository backing the app."""
    return FakeSecretRepository()


@pytest.fixture
async def client(
    secret_repository: FakeSecretRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed agent services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    version_repository = FakeAgentVersionRepository(agent_repository, secret_repository)
    job_repository = FakeJobRepository(
        FakeSessionRepository(agent_repository, version_repository),
        version_repository,
    )
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(
        repository=version_repository,
        agent_repository=agent_repository,
        secret_repository=secret_repository,
        job_repository=job_repository,
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_agent(client: httpx.AsyncClient, name: str = "support-bot") -> str:
    """Store an agent through the API.

    Args:
        client: HTTP client for the app.
        name: Agent name.

    Returns:
        Id of the created agent.
    """
    response = await client.post("/v1/agents", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


async def test_create_version(client: httpx.AsyncClient) -> None:
    """Create an agent version and observe HTTP 201."""
    agent_id = await create_agent(client)
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={"version": "v1", "description": "Initial version"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["agent_id"] == agent_id
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["version"] == "v1"
    assert body["description"] == "Initial version"
    assert body["run_spec"] is None
    assert body["capabilities"] == {"tools": [], "mcp_servers": [], "skills": []}
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_version_with_run_spec(
    client: httpx.AsyncClient, secret_repository: FakeSecretRepository
) -> None:
    """Create a runnable agent version referencing a secret."""
    agent_id = await create_agent(client)
    secret = await create_secret(secret_repository, ACCOUNT.id)
    run_spec = {
        "command": "python agent.py",
        "working_dir": "/app",
        "env": {"MODE": "replay"},
        "secret_ids": [str(secret.id)],
        "timeout_seconds": 600,
        "image": "ghcr.io/acme/agent:v1",
        "default_execution_target": "on_demand",
    }
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": run_spec,
            "capabilities": {"tools": ["search"]},
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["run_spec"] == run_spec
    assert body["capabilities"] == {
        "tools": ["search"],
        "mcp_servers": [],
        "skills": [],
    }


async def test_create_version_unknown_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/agents/{missing_id}/versions", json={"version": "v1"}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_id} was not found"}


async def test_create_version_duplicate(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate version label."""
    agent_id = await create_agent(client)
    response = await client.post(
        f"/v1/agents/{agent_id}/versions", json={"version": "v1"}
    )
    assert response.status_code == 201
    response = await client.post(
        f"/v1/agents/{agent_id}/versions", json={"version": "v1"}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Agent version 'v1' is already registered"}


async def test_create_version_missing_secret(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a run spec referencing an unknown secret."""
    agent_id = await create_agent(client)
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {
                "command": "python agent.py",
                "secret_ids": [str(missing_id)],
                "timeout_seconds": 600,
            },
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {missing_id} was not found"}


async def test_create_version_invalid_timeout(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a non-positive run spec timeout."""
    agent_id = await create_agent(client)
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {"command": "python agent.py", "timeout_seconds": 0},
        },
    )
    assert response.status_code == 422


async def test_create_version_duplicate_secret_ids(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for duplicate run spec secret ids."""
    agent_id = await create_agent(client)
    secret_id = str(uuid.uuid4())
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": "v1",
            "run_spec": {
                "command": "python agent.py",
                "secret_ids": [secret_id, secret_id],
                "timeout_seconds": 600,
            },
        },
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Run spec secret ids contain duplicates"}


async def test_list_versions(client: httpx.AsyncClient) -> None:
    """List agent versions with pagination."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    for label in ["v1", "v2", "v3"]:
        response = await client.post(
            f"/v1/agents/{agent_id}/versions", json={"version": label}
        )
        assert response.status_code == 201
    response = await client.post(
        f"/v1/agents/{other_id}/versions", json={"version": "v1"}
    )
    assert response.status_code == 201

    response = await client.get(f"/v1/agents/{agent_id}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["version"] for item in body["items"]] == ["v1", "v2", "v3"]

    response = await client.get(
        f"/v1/agents/{agent_id}/versions", params={"page": 2, "page_size": 2}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["version"] for item in body["items"]] == ["v3"]


async def test_list_versions_unknown_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/agents/{missing_id}/versions")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent {missing_id} was not found"}


async def test_get_version(client: httpx.AsyncClient) -> None:
    """Get an agent version by id."""
    agent_id = await create_agent(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent version id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/agent-versions/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Agent version {missing_id} was not found"}


async def test_update_version(
    client: httpx.AsyncClient, secret_repository: FakeSecretRepository
) -> None:
    """Update an agent version."""
    agent_id = await create_agent(client)
    secret = await create_secret(secret_repository, ACCOUNT.id)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    run_spec = {
        "command": "python agent.py",
        "working_dir": None,
        "env": {},
        "secret_ids": [str(secret.id)],
        "timeout_seconds": 600,
        "image": None,
        "default_execution_target": "pool",
    }
    response = await client.patch(
        f"/v1/agent-versions/{created['id']}",
        json={
            "description": "Tuned prompt",
            "run_spec": run_spec,
            "capabilities": {"skills": ["review"]},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["version"] == "v1"
    assert body["description"] == "Tuned prompt"
    assert body["run_spec"] == run_spec
    assert body["capabilities"] == {
        "tools": [],
        "mcp_servers": [],
        "skills": ["review"],
    }

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json()["run_spec"] == run_spec


async def test_update_version_absent_fields_unchanged(
    client: httpx.AsyncClient,
) -> None:
    """Keep every field on an update with an empty body."""
    agent_id = await create_agent(client)
    created = (
        await client.post(
            f"/v1/agents/{agent_id}/versions",
            json={"version": "v1", "description": "Initial version"},
        )
    ).json()
    response = await client.patch(f"/v1/agent-versions/{created['id']}", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Initial version"
    assert body["run_spec"] == created["run_spec"]
    assert body["capabilities"] == created["capabilities"]


async def test_update_version_null_clears_nullable_fields(
    client: httpx.AsyncClient,
) -> None:
    """Clear the description and run spec on explicit nulls."""
    agent_id = await create_agent(client)
    created = (
        await client.post(
            f"/v1/agents/{agent_id}/versions",
            json={
                "version": "v1",
                "description": "Initial version",
                "run_spec": {"command": "python agent.py", "timeout_seconds": 600},
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/agent-versions/{created['id']}",
        json={"description": None, "run_spec": None},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] is None
    assert body["run_spec"] is None


async def test_update_version_null_capabilities_rejected(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for explicit null capabilities."""
    agent_id = await create_agent(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    response = await client.patch(
        f"/v1/agent-versions/{created['id']}", json={"capabilities": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Agent version capabilities cannot be null"}


async def test_update_version_missing_secret(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a new run spec referencing an unknown secret."""
    agent_id = await create_agent(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    missing_id = uuid.uuid4()
    response = await client.patch(
        f"/v1/agent-versions/{created['id']}",
        json={
            "run_spec": {
                "command": "python agent.py",
                "secret_ids": [str(missing_id)],
                "timeout_seconds": 600,
            }
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {missing_id} was not found"}


async def test_update_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent version id."""
    response = await client.patch(
        f"/v1/agent-versions/{uuid.uuid4()}", json={"description": "Tuned prompt"}
    )
    assert response.status_code == 404


async def test_delete_version(client: httpx.AsyncClient) -> None:
    """Delete an agent version and observe HTTP 204."""
    agent_id = await create_agent(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    response = await client.delete(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 404


async def test_delete_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent version id."""
    response = await client.delete(f"/v1/agent-versions/{uuid.uuid4()}")
    assert response.status_code == 404
