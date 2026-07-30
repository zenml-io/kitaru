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
"""End-to-end agent tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created agents.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_agents_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/agents", json={"name": "assistant", "description": "Helps"}
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/agents", json={"name": "assistant"})
    assert response.status_code == 201
    response = await client.post("/v1/agents", json={"name": "assistant"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Agent name 'assistant' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    response = await client.patch(
        f"/v1/agents/{created['id']}", json={"description": "Reviews"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Reviews"
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    response = await client.delete(f"/v1/agents/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 404


async def test_create_version_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Prove a created version is visible from a separate request."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    response = await client.post(
        f"/v1/agents/{agent['id']}/versions",
        json={"display_version": "v1", "description": "First cut"},
    )
    assert response.status_code == 201
    created = response.json()
    assert created["version"] == 1

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get(f"/v1/agents/{agent['id']}")
    assert response.status_code == 200
    assert response.json()["latest_version"] == 1


async def test_version_numbering_sequence(client: httpx.AsyncClient) -> None:
    """Assign consecutive version numbers per agent across requests."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    first = (await client.post(f"/v1/agents/{agent['id']}/versions", json={})).json()
    second = (await client.post(f"/v1/agents/{agent['id']}/versions", json={})).json()
    assert first["version"] == 1
    assert second["version"] == 2


async def test_create_version_with_secrets_round_trips(
    client: httpx.AsyncClient,
) -> None:
    """Round-trip a run spec whose secret ids reference real secrets."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    secret_a = (
        await client.post(
            "/v1/secrets", json={"name": "secret-a", "values": {"k": "v"}}
        )
    ).json()
    secret_b = (
        await client.post(
            "/v1/secrets", json={"name": "secret-b", "values": {"k": "v"}}
        )
    ).json()

    response = await client.post(
        f"/v1/agents/{agent['id']}/versions",
        json={
            "run_spec": {
                "command": "run.sh",
                "secret_ids": [secret_a["id"], secret_b["id"]],
            }
        },
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json()["run_spec"]["secret_ids"] == [
        secret_a["id"],
        secret_b["id"],
    ]


async def test_delete_agent_restricted_by_versions(client: httpx.AsyncClient) -> None:
    """Translate the FK restriction into HTTP 409 when versions exist."""
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    await client.post(f"/v1/agents/{agent['id']}/versions", json={})

    response = await client.delete(f"/v1/agents/{agent['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Agent {agent['id']} has versions and cannot be deleted"
    }
