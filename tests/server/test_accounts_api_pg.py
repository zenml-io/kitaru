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
"""End-to-end account tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import lifespan_client, local_settings


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client authenticated as the default account."""
    settings = local_settings(use_db=True, DEFAULT_ACCOUNT_PASSWORD="secret")
    async with lifespan_client(settings) as client:
        response = await client.post(
            "/v1/login", data={"username": "default", "password": "secret"}
        )
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


async def test_accounts_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post(
        "/v1/accounts",
        json={"name": "alice", "email": "alice@example.com", "password": "secret"},
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/accounts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/accounts")
    assert response.status_code == 200
    body = response.json()
    # The lifespan bootstraps the default account before the created one, and
    # the default sort is newest-first.
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["alice", "default"]
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/accounts", json={"name": "alice"})
    assert response.status_code == 201
    response = await client.post("/v1/accounts", json={"name": "alice"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Account name 'alice' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a partial update across requests."""
    created = (await client.post("/v1/accounts", json={"name": "alice"})).json()
    response = await client.post(f"/v1/accounts/{created['id']}/deactivate")
    assert response.status_code == 200

    response = await client.get(f"/v1/accounts/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["active"] is False
    assert body["updated"] > created["updated"]
