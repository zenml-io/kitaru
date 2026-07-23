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
"""End-to-end agent version tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

VALUES = {"username": "svc", "password": "hunter2"}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created agent versions.
    async with lifespan_client(db_settings()) as client:
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


async def create_secret(client: httpx.AsyncClient, name: str = "db") -> str:
    """Store a secret through the API.

    Args:
        client: HTTP client for the app.
        name: Secret name.

    Returns:
        Id of the created secret.
    """
    response = await client.post("/v1/secrets", json={"name": name, "values": VALUES})
    assert response.status_code == 201
    return response.json()["id"]


async def test_versions_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    agent_id = await create_agent(client)
    secret_id = await create_secret(client)
    run_spec = {
        "command": "python agent.py",
        "working_dir": "/app",
        "env": {"MODE": "replay"},
        "secret_ids": [secret_id],
        "timeout_seconds": 600,
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
    created = response.json()
    assert created["run_spec"] == run_spec

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get(f"/v1/agents/{agent_id}/versions")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_duplicate_version_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
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


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    agent_id = await create_agent(client)
    secret_id = await create_secret(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    run_spec = {
        "command": "python agent.py",
        "working_dir": None,
        "env": {},
        "secret_ids": [secret_id],
        "timeout_seconds": 600,
    }
    response = await client.patch(
        f"/v1/agent-versions/{created['id']}",
        json={"description": "Tuned prompt", "run_spec": run_spec},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Tuned prompt"
    assert body["run_spec"] == run_spec
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    agent_id = await create_agent(client)
    created = (
        await client.post(f"/v1/agents/{agent_id}/versions", json={"version": "v1"})
    ).json()
    response = await client.delete(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 404


async def test_secret_delete_while_referenced(client: httpx.AsyncClient) -> None:
    """Translate the foreign key constraint into HTTP 409."""
    agent_id = await create_agent(client)
    secret_id = await create_secret(client)
    created = (
        await client.post(
            f"/v1/agents/{agent_id}/versions",
            json={
                "version": "v1",
                "run_spec": {
                    "command": "python agent.py",
                    "secret_ids": [secret_id],
                    "timeout_seconds": 600,
                },
            },
        )
    ).json()

    response = await client.delete(f"/v1/secrets/{secret_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Secret {secret_id} is referenced by agent versions"
    }
    response = await client.get(f"/v1/secrets/{secret_id}")
    assert response.status_code == 200

    # Deleting the version removes the reference, so the secret delete
    # succeeds afterwards.
    response = await client.delete(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 204
    response = await client.delete(f"/v1/secrets/{secret_id}")
    assert response.status_code == 204
