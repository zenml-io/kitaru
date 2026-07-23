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
        "/v1/agents",
        json={"name": "support-bot", "description": "Answers tickets"},
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/agents")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 201
    response = await client.post("/v1/agents", json={"name": "support-bot"})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Agent name 'support-bot' is already registered"
    }


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.patch(
        f"/v1/agents/{created['id']}",
        json={"name": "triage-bot", "description": "Sorts tickets"},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "triage-bot"
    assert body["description"] == "Sorts tickets"
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    response = await client.delete(f"/v1/agents/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/agents/{created['id']}")
    assert response.status_code == 404


async def test_delete_with_versions_conflict(client: httpx.AsyncClient) -> None:
    """Translate the foreign key constraint into HTTP 409."""
    created = (await client.post("/v1/agents", json={"name": "support-bot"})).json()
    version = (
        await client.post(
            f"/v1/agents/{created['id']}/versions", json={"version": "v1"}
        )
    ).json()

    response = await client.delete(f"/v1/agents/{created['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Agent {created['id']} is referenced by agent versions"
    }

    response = await client.delete(f"/v1/agent-versions/{version['id']}")
    assert response.status_code == 204
    response = await client.delete(f"/v1/agents/{created['id']}")
    assert response.status_code == 204
