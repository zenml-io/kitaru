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
"""End-to-end worker tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

RUNTIME = {"platform": "bare"}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_workers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/workers",
        json={
            "name": "worker-1",
            "scope": {"kinds": ["agent"]},
            "runtime": RUNTIME,
            "metadata": {"region": "eu"},
        },
    )
    assert response.status_code == 200
    created = response.json()["worker"]

    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/workers")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_upsert_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a re-registration across requests, keeping id and created."""
    first = (
        await client.post(
            "/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"kinds": ["agent"]},
                "runtime": RUNTIME,
                "metadata": {"region": "eu"},
            },
        )
    ).json()["worker"]

    second = (
        await client.post(
            "/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"kinds": ["importer"]},
                "runtime": {"platform": "docker"},
                "metadata": {"region": "us"},
            },
        )
    ).json()["worker"]

    assert second["id"] == first["id"]
    assert second["created"] == first["created"]
    assert second["updated"] > first["updated"]

    response = await client.get(f"/v1/workers/{first['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["scope"]["kinds"] == ["importer"]
    assert body["runtime"]["platform"] == "docker"
    assert body["metadata"] == {"region": "us"}


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (
        await client.post(
            "/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()["worker"]
    response = await client.delete(f"/v1/workers/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 404


async def test_register_with_pool_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Persist a pool-joined registration, resolving the pool's id."""
    pool = (
        await client.post(
            "/v1/worker-pools",
            json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
        )
    ).json()

    response = await client.post(
        "/v1/workers",
        json={
            "name": "worker-1",
            "scope": {},
            "runtime": RUNTIME,
            "metadata": {},
            "pool": "pool-1",
        },
    )
    assert response.status_code == 200
    created = response.json()["worker"]
    assert created["pool_id"] == pool["id"]

    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 200
    assert response.json()["pool_id"] == pool["id"]
