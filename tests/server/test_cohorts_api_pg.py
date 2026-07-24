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
"""End-to-end cohort tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created cohorts.
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


async def create_completed_session(
    client: httpx.AsyncClient, agent_id: str, **overrides: object
) -> str:
    """Store a completed recorded session through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        **overrides: Create request body overrides.

    Returns:
        Id of the created session.
    """
    body: dict[str, object] = {"agent_id": agent_id, "origin": "recorded", **overrides}
    response = await client.post("/v1/sessions", json=body)
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    return session_id


async def test_cohort_flow_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Create a cohort, list its ordered members, and delete it."""
    agent_id = await create_agent(client)
    session_ids = [await create_completed_session(client, agent_id) for _ in range(3)]
    ordered = [session_ids[2], session_ids[0], session_ids[1]]
    response = await client.post(
        "/v1/cohorts",
        json={"name": "baseline", "agent_id": agent_id, "session_ids": ordered},
    )
    assert response.status_code == 201
    cohort_id = response.json()["id"]
    assert response.json()["session_count"] == 3

    response = await client.get(f"/v1/cohorts/{cohort_id}/sessions")
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == ordered

    # The membership blocks session deletion until the cohort is gone.
    response = await client.delete(f"/v1/sessions/{ordered[0]}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Session {ordered[0]} is referenced by cohorts"
    }

    response = await client.delete(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 204
    response = await client.get(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 404
    response = await client.delete(f"/v1/sessions/{ordered[0]}")
    assert response.status_code == 204


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    body = {"name": "baseline", "agent_id": agent_id, "session_ids": [session_id]}
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 409
    assert response.json() == {"detail": "Cohort name 'baseline' is already registered"}


async def test_tag_filter(client: httpx.AsyncClient) -> None:
    """Filter cohorts through a tag link."""
    agent_id = await create_agent(client)
    session_id = await create_completed_session(client, agent_id)
    response = await client.post(
        "/v1/cohorts",
        json={"name": "tagged", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 201
    tagged_id = response.json()["id"]
    response = await client.post(
        "/v1/cohorts",
        json={"name": "other", "agent_id": agent_id, "session_ids": [session_id]},
    )
    assert response.status_code == 201
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "cohort", "resource_id": tagged_id},
    )
    assert response.status_code == 201

    response = await client.get("/v1/cohorts", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged_id
