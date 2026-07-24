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

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
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
    # The lifespan bootstraps the default account next to the created one.
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["default", "alice"]
    assert body["items"][1] == created


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
    response = await client.patch(
        f"/v1/accounts/{created['id']}", json={"active": False}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/accounts/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["active"] is False
    assert body["updated"] > created["updated"]


async def test_update_null_fields_rejected(client: httpx.AsyncClient) -> None:
    """Translate explicit nulls on required fields into HTTP 422."""
    created = (await client.post("/v1/accounts", json={"name": "alice"})).json()
    response = await client.patch(
        f"/v1/accounts/{created['id']}", json={"active": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Account active state cannot be null"}
