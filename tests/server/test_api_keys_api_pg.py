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
"""End-to-end API key tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.server.domain.api_key import API_KEY_PREFIX


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created API keys.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_api_keys_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 201
    created = response.json()
    assert created["key"].startswith(API_KEY_PREFIX)

    response = await client.get(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 200
    expected = {field: value for field, value in created.items() if field != "key"}
    assert response.json() == expected

    response = await client.get("/v1/api-keys")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == expected


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 201
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 409
    assert response.json() == {"detail": "API key name 'ci' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.patch(
        f"/v1/api-keys/{created['id']}", json={"active": False}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["active"] is False
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.delete(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 404
