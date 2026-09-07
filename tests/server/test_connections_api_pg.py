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
"""End-to-end connection tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService

ENV = {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}
SECRETS = {"LANGFUSE_PUBLIC_KEY": "pk", "LANGFUSE_SECRET_KEY": "sk"}


@pytest.fixture
def settings() -> APISettings:
    """Provide settings pointing at a fresh test database."""
    return db_settings()


@pytest.fixture
async def client(settings: APISettings) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created connections.
    async with lifespan_client(settings) as client:
        yield client


async def _create(client: httpx.AsyncClient, **overrides: object) -> dict[str, object]:
    """Create a connection through the API and return the response body."""
    body: dict[str, object] = {
        "name": "langfuse-prod",
        "provider": "langfuse",
        "env": ENV,
        "secrets": SECRETS,
    }
    body.update(overrides)
    response = await client.post("/api/v1/connections", json=body)
    assert response.status_code == 201
    return response.json()


async def test_connections_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    created = await _create(client)
    assert created["env"] == ENV
    assert created["secret_keys"] == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]

    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/api/v1/connections")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    await _create(client)
    response = await client.post(
        "/api/v1/connections", json={"name": "langfuse-prod", "provider": "langfuse"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Connection name 'langfuse-prod' is already registered"
    }


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a merged update across requests."""
    created = await _create(client)
    response = await client.patch(
        f"/api/v1/connections/{created['id']}",
        json={
            "env": {"LANGFUSE_PROJECT": "proj"},
            "secrets": {"LANGFUSE_SECRET_KEY": "rotated"},
        },
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["env"] == {
        "LANGFUSE_BASE_URL": "https://cloud.langfuse.com",
        "LANGFUSE_PROJECT": "proj",
    }
    assert body["secret_keys"] == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert body["updated"] > created["updated"]


async def test_default_clears_the_previous_default(client: httpx.AsyncClient) -> None:
    """Clear the provider's previous default in a separate request."""
    first = await _create(client, name="langfuse-a", default=True)
    second = await _create(client, name="langfuse-b")

    response = await client.patch(
        f"/api/v1/connections/{second['id']}", json={"default": True}
    )
    assert response.status_code == 200
    assert response.json()["default"] is True

    response = await client.get(f"/api/v1/connections/{first['id']}")
    assert response.status_code == 200
    assert response.json()["default"] is False


async def test_delete_removes_the_internal_secret(
    client: httpx.AsyncClient, settings: APISettings
) -> None:
    """Delete the connection's internal secret row alongside the connection."""
    created = await _create(client)
    response = await client.delete(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 404

    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT id FROM secret WHERE id = :id"),
                    {"id": created["secret_id"]},
                )
            ).one_or_none()
            assert row is None
    finally:
        await engine.dispose()


async def test_internal_secret_is_unreachable_through_the_secrets_api(
    client: httpx.AsyncClient,
) -> None:
    """Hide the connection's internal secret from every secret route."""
    created = await _create(client)
    secret_id = created["secret_id"]

    response = await client.get(f"/api/v1/secrets/{secret_id}")
    assert response.status_code == 404
    response = await client.delete(f"/api/v1/secrets/{secret_id}")
    assert response.status_code == 404

    response = await client.get("/api/v1/secrets")
    assert response.status_code == 200
    assert response.json()["items"] == []

    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 200
