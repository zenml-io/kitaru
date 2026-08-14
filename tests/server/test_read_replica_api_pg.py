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
"""End-to-end read-replica auth tests against PostgreSQL."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

import httpx
import pytest
from fastapi import APIRouter, Depends
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import text

from conftest import drop_test_database, local_settings, postgres_available
from kitaru.server.adapters.rest.dependencies import authorize
from kitaru.server.adapters.rest.route import KitaruAPIRoute, read_only
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.database.service import DatabaseService

# Probe routes standing in for a real read-only endpoint. No production route
# is marked read_only yet, so these exercise the auth path the same way a
# real one would, under the reserved "v1" prefix so bundled UI serving never
# shadows them.
_probe_router = APIRouter(route_class=KitaruAPIRoute)


@_probe_router.get("/v1/__test__/read-only")
@read_only
async def _read_only_probe(
    actor: Annotated[AuthContext, Depends(authorize)],
) -> dict[str, str]:
    """Authenticate a request on a read-only route without touching data.

    Args:
        actor: Resolved auth context.

    Returns:
        A fixed status body.
    """
    return {"status": "ok"}


@_probe_router.get("/v1/__test__/normal")
async def _normal_probe(
    actor: Annotated[AuthContext, Depends(authorize)],
) -> dict[str, str]:
    """Authenticate a request on a normal route without touching data.

    Args:
        actor: Resolved auth context.

    Returns:
        A fixed status body.
    """
    return {"status": "ok"}


async def test_read_engine_rejects_writes_at_the_database() -> None:
    """The read engine's read-only execution option rejects writes."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = local_settings(use_db=True)
    settings = settings.model_copy(update={"DB_READ_HOST": settings.DB_HOST})
    await DatabaseService.create_db(settings)
    database = DatabaseService(settings)
    try:
        async with database.read_engine.begin() as connection:
            with pytest.raises(DBAPIError, match="read-only transaction"):
                await connection.execute(text("CREATE TABLE _read_only_probe (id int)"))
    finally:
        await database.cleanup()
        await drop_test_database(settings)


def _read_replica_settings() -> APISettings:
    """Build settings for local auth with a read replica on the same database.

    Returns:
        Settings whose read engine is a distinct engine object connected to
        the same test database as the primary engine.
    """
    settings = local_settings(use_db=True, DEFAULT_ACCOUNT_PASSWORD="secret")
    return settings.model_copy(update={"DB_READ_HOST": settings.DB_HOST})


@asynccontextmanager
async def _client_with_probe_routes(
    settings: APISettings,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Run the app through its lifespan with the read-only probe routes mounted.

    Args:
        settings: API server settings.

    Yields:
        HTTP client routed to the app.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    await DatabaseService.create_db(settings)
    try:
        app = create_app(settings)
        app.include_router(_probe_router)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                yield client
    finally:
        await drop_test_database(settings)


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with the probe routes mounted."""
    async with _client_with_probe_routes(_read_replica_settings()) as client:
        yield client


async def _login(client: httpx.AsyncClient) -> dict[str, str]:
    """Log in as the default account and return its bearer header.

    Args:
        client: HTTP client routed to the app.

    Returns:
        Authorization header for the default account.
    """
    response = await client.post(
        "/v1/login", data={"username": "default", "password": "secret"}
    )
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def test_read_only_route_persists_the_throttled_api_key_last_used(
    client: httpx.AsyncClient,
) -> None:
    """An API key request on a read-only route still records last_used.

    The write goes through the writer auth session, not the read-bound
    request session, so the read-only route's pending-writes guard never
    sees it and the request succeeds.
    """
    user_headers = await _login(client)
    created = (
        await client.post(
            "/v1/api-keys", json={"name": "read-only-probe"}, headers=user_headers
        )
    ).json()
    key_headers = {"Authorization": f"Bearer {created['key']}"}

    response = await client.get("/v1/__test__/read-only", headers=key_headers)
    assert response.status_code == 200

    response = await client.get(f"/v1/api-keys/{created['id']}", headers=user_headers)
    assert response.json()["last_used"] is not None


async def test_normal_route_persists_the_throttled_api_key_last_used(
    client: httpx.AsyncClient,
) -> None:
    """An API key request on a normal route keeps recording last_used as before."""
    user_headers = await _login(client)
    created = (
        await client.post(
            "/v1/api-keys", json={"name": "normal-probe"}, headers=user_headers
        )
    ).json()
    key_headers = {"Authorization": f"Bearer {created['key']}"}

    response = await client.get("/v1/__test__/normal", headers=key_headers)
    assert response.status_code == 200

    response = await client.get(f"/v1/api-keys/{created['id']}", headers=user_headers)
    assert response.json()["last_used"] is not None
