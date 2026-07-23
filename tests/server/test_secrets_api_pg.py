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
"""End-to-end secret tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client

VALUES = {"username": "svc", "password": "hunter2"}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created secrets.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_secrets_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/v1/secrets", json={"name": "db", "values": VALUES})
    assert response.status_code == 201
    created = response.json()
    assert "values" not in created

    response = await client.get(f"/v1/secrets/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get(
        f"/v1/secrets/{created['id']}", params={"include_values": "true"}
    )
    assert response.status_code == 200
    assert response.json()["values"] == VALUES

    response = await client.get("/v1/secrets")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/v1/secrets", json={"name": "db", "values": VALUES})
    assert response.status_code == 201
    response = await client.post("/v1/secrets", json={"name": "db", "values": VALUES})
    assert response.status_code == 409
    assert response.json() == {"detail": "Secret name 'db' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist an update across requests."""
    created = (
        await client.post("/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.patch(
        f"/v1/secrets/{created['id']}",
        json={"type": "database", "values": {"password": "hunter3"}},
    )
    assert response.status_code == 200

    response = await client.get(
        f"/v1/secrets/{created['id']}", params={"include_values": "true"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "database"
    assert body["values"] == {"password": "hunter3"}
    assert body["updated"] > created["updated"]


async def test_delete_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a deletion across requests."""
    created = (
        await client.post("/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.delete(f"/v1/secrets/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/secrets/{created['id']}")
    assert response.status_code == 404
