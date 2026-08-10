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
"""End-to-end worker pool tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_worker_pools_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/worker-pools",
        json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/worker-pools")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    body = {"name": "pool-1", "scope": {}}
    response = await client.post("/v1/worker-pools", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/worker-pools", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Worker pool name 'pool-1' is already registered"
    }


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}",
        json={"name": "renamed", "scope": {"kinds": ["importer"]}},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["scope"]["kinds"] == ["importer"]
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.delete(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 404


async def test_stats_reflects_registered_worker_and_pending_task(
    client: httpx.AsyncClient,
) -> None:
    """Compute live worker and queue counts from real tables."""
    pool = (
        await client.post(
            "/v1/worker-pools",
            json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
        )
    ).json()

    await client.post(
        "/v1/workers",
        json={
            "name": "worker-1",
            "pool": pool["name"],
            "scope": {},
            "runtime": {"platform": "bare"},
            "metadata": {},
        },
    )

    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    await client.post(
        "/v1/session-runs",
        json={"agent_version_id": version["id"], "inputs": {"q": "hi"}},
    )

    response = await client.get(f"/v1/worker-pools/{pool['id']}/stats")
    assert response.status_code == 200
    body = response.json()
    assert body["pending_tasks"] == 1
    assert body["in_flight_tasks"] == 0
    assert body["oldest_pending_seconds"] >= 0
    assert body["live_workers"] == 1
    assert body["capacity"] == 1

    by_name = await client.get(f"/v1/worker-pools/{pool['name']}/stats")
    assert by_name.status_code == 200
    assert by_name.json()["pending_tasks"] == 1
    assert by_name.json()["live_workers"] == 1
    assert by_name.json()["capacity"] == 1
