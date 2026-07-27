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

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all registered workers.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_workers_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    version_id = uuid.uuid4()
    response = await client.post(
        "/v1/workers",
        json={
            "name": "runner",
            "scope": {"agent_version_ids": [str(version_id)]},
            "metadata": {"hostname": "pool-1"},
        },
    )
    assert response.status_code == 200
    created = response.json()
    assert created["scope"]["agent_version_ids"] == [str(version_id)]
    assert created["live"] is True

    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/workers")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_register_upserts_across_requests(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into the update path."""
    response = await client.post("/v1/workers", json={"name": "runner"})
    assert response.status_code == 200
    created = response.json()

    version_id = uuid.uuid4()
    response = await client.post(
        "/v1/workers",
        json={
            "name": "runner",
            "scope": {"agent_version_ids": [str(version_id)]},
            "metadata": {"hostname": "pool-2"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == created["id"]
    assert body["scope"]["agent_version_ids"] == [str(version_id)]
    assert body["metadata"] == {"hostname": "pool-2"}
    assert body["last_seen_at"] > created["last_seen_at"]
    assert body["updated"] > created["updated"]

    response = await client.get("/v1/workers")
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/workers", json={"name": "runner"})).json()
    response = await client.delete(f"/v1/workers/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 404
