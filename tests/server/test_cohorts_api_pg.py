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
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent to own cohorts."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def test_cohorts_persist_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
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
    created = response.json()

    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/cohorts")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Translate the database constraint into HTTP 409."""
    body = {"name": "smoke-test", "agent_id": agent_id}
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/cohorts", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Cohort name 'smoke-test' is already registered"
    }


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist an update across requests."""
    created = (
        await client.post(
            "/v1/cohorts", json={"name": "smoke-test", "agent_id": agent_id}
        )
    ).json()
    response = await client.patch(
        f"/v1/cohorts/{created['id']}", json={"description": "Reviews"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Reviews"
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a deletion across requests."""
    created = (
        await client.post(
            "/v1/cohorts", json={"name": "smoke-test", "agent_id": agent_id}
        )
    ).json()
    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 404


async def test_delete_cascades_versions(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Cascade a cohort's versions when the cohort is deleted."""
    created = (
        await client.post(
            "/v1/cohorts", json={"name": "smoke-test", "agent_id": agent_id}
        )
    ).json()
    version = (
        await client.post(f"/v1/cohorts/{created['id']}/versions", json={})
    ).json()

    response = await client.delete(f"/v1/cohorts/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/cohort-versions/{version['id']}")
    assert response.status_code == 404


async def test_create_cohort_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/v1/cohorts",
        json={
            "name": "cohort",
            "agent_id": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert response.status_code == 404
