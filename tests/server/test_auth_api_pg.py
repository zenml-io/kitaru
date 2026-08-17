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
"""End-to-end auth tests against PostgreSQL."""

from collections.abc import AsyncGenerator
from typing import Annotated

import httpx
import pytest
from fastapi import APIRouter, Depends, FastAPI

from conftest import (
    drop_test_database,
    lifespan_client,
    local_settings,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.rest.dependencies import authorize
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.database.service import DatabaseService


def pg_settings() -> APISettings:
    """Build API settings for the local auth scheme on the test database.

    Returns:
        Settings for local authentication.
    """
    return local_settings(use_db=True, DEFAULT_ACCOUNT_PASSWORD="secret")


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(pg_settings()) as client:
        yield client


async def test_login_and_api_key_flow(client: httpx.AsyncClient) -> None:
    """Log in as the default account and authenticate with an API key."""
    response = await client.get("/api/v1/api-keys")
    assert response.status_code == 401

    response = await client.post(
        "/api/v1/login", data={"username": "default", "password": "secret"}
    )
    assert response.status_code == 200
    user_headers = {"Authorization": f"Bearer {response.json()['access_token']}"}

    response = await client.post(
        "/api/v1/api-keys", json={"name": "ci"}, headers=user_headers
    )
    assert response.status_code == 201
    created = response.json()
    key = created["key"]

    # The raw API key works directly as a bearer token and records its use.
    key_headers = {"Authorization": f"Bearer {key}"}
    response = await client.get("/api/v1/api-keys", headers=key_headers)
    assert response.status_code == 200
    assert response.json()["items"][0]["last_used"] is not None

    # Revoking the key invalidates it immediately.
    response = await client.patch(
        f"/api/v1/api-keys/{created['id']}",
        json={"active": False},
        headers=user_headers,
    )
    assert response.status_code == 200

    response = await client.get("/api/v1/api-keys", headers=key_headers)
    assert response.status_code == 401


async def test_login_wrong_password(client: httpx.AsyncClient) -> None:
    """Observe HTTP 401 for a wrong default account password."""
    response = await client.post(
        "/api/v1/login", data={"username": "default", "password": "wrong"}
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid username or password."}


async def test_default_account_bootstrap_idempotent() -> None:
    """Bootstrap the same default account across two lifespans."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = pg_settings()
    await DatabaseService.create_db(settings)

    account_ids = []
    try:
        for _ in range(2):
            app = create_app(settings)
            async with app.router.lifespan_context(app):
                database: DatabaseService = app.state.database
                async for session in database.get_async_session():
                    repository = SQLAccountRepository(session)
                    account = await repository.get_by_name(
                        settings.DEFAULT_ACCOUNT_NAME
                    )
                    account_ids.append(account.id)
    finally:
        await drop_test_database(settings)
    assert account_ids[0] == account_ids[1]


async def test_failed_request_still_records_api_key_last_used() -> None:
    """A handler failure after authentication keeps the last_used update."""
    probe_router = APIRouter(route_class=KitaruAPIRoute)

    @probe_router.get("/api/v1/__test__/boom")
    async def boom(actor: Annotated[AuthContext, Depends(authorize)]) -> None:
        raise RuntimeError("handler failure after authentication")

    def mount(app: FastAPI) -> None:
        app.include_router(probe_router)

    async with lifespan_client(pg_settings(), mutate_app=mount) as client:
        response = await client.post(
            "/api/v1/login", data={"username": "default", "password": "secret"}
        )
        assert response.status_code == 200
        user_headers = {"Authorization": f"Bearer {response.json()['access_token']}"}

        created = (
            await client.post(
                "/api/v1/api-keys", json={"name": "boom"}, headers=user_headers
            )
        ).json()
        key_headers = {"Authorization": f"Bearer {created['key']}"}

        with pytest.raises(RuntimeError, match="handler failure"):
            await client.get("/api/v1/__test__/boom", headers=key_headers)

        response = await client.get(
            f"/api/v1/api-keys/{created['id']}", headers=user_headers
        )
        assert response.json()["last_used"] is not None
