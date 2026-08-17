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
"""Tests for the service account routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeAccountRepository, FakePasswordHasher, local_settings
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin", is_admin=True))
NON_ADMIN_ACTOR = AuthContext(
    account=Account(id=uuid.uuid4(), name="alice", is_admin=False)
)


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed account service."""
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: ACTOR
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def non_admin_client_with_target() -> AsyncGenerator[
    tuple[httpx.AsyncClient, uuid.UUID], None
]:
    """Provide a non-admin-authorized client plus a pre-seeded target account id."""
    app = create_app(local_settings())
    repository = FakeAccountRepository()
    target = await repository.create(Account(name="svc", is_service_account=True))
    service = AccountService(
        repository=repository,
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: NON_ADMIN_ACTOR
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, target.id


async def test_create_service_account(client: httpx.AsyncClient) -> None:
    """Create a service account active with no activation token."""
    response = await client.post(
        "/api/v1/service-accounts", json={"name": "svc", "email": "svc@example.com"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "svc"
    assert body["email"] == "svc@example.com"
    assert body["is_service_account"] is True
    assert body["active"] is True
    assert "activation_token" not in body


async def test_create_service_account_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate account name."""
    response = await client.post("/api/v1/service-accounts", json={"name": "svc"})
    assert response.status_code == 201
    response = await client.post("/api/v1/service-accounts", json={"name": "svc"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Account name 'svc' is already registered"}


async def test_update_service_account_metadata(client: httpx.AsyncClient) -> None:
    """Write a service account's metadata as an admin actor."""
    created = (
        await client.post("/api/v1/service-accounts", json={"name": "svc"})
    ).json()
    response = await client.patch(
        f"/api/v1/service-accounts/{created['id']}",
        json={"metadata": {"theme": "dark"}},
    )
    assert response.status_code == 200
    assert response.json()["metadata"] == {"theme": "dark"}


async def test_update_service_account_active(client: httpx.AsyncClient) -> None:
    """Flip a service account's active state and flip it back."""
    created = (
        await client.post("/api/v1/service-accounts", json={"name": "svc"})
    ).json()
    response = await client.patch(
        f"/api/v1/service-accounts/{created['id']}", json={"active": False}
    )
    assert response.status_code == 200
    assert response.json()["active"] is False

    response = await client.patch(
        f"/api/v1/service-accounts/{created['id']}", json={"active": True}
    )
    assert response.status_code == 200
    assert response.json()["active"] is True


async def test_update_service_account_forbidden_for_non_admin(
    non_admin_client_with_target: tuple[httpx.AsyncClient, uuid.UUID],
) -> None:
    """Observe HTTP 403 when a non-admin actor updates a service account."""
    client, target_id = non_admin_client_with_target
    response = await client.patch(
        f"/api/v1/service-accounts/{target_id}", json={"metadata": {"theme": "dark"}}
    )
    assert response.status_code == 403


async def test_update_service_account_user_not_found(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 404 when a service account update targets a user account id."""
    created = (await client.post("/api/v1/users", json={"name": "alice"})).json()
    response = await client.patch(
        f"/api/v1/service-accounts/{created['id']}",
        json={"metadata": {"theme": "dark"}},
    )
    assert response.status_code == 404


async def test_update_service_account_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown account id."""
    response = await client.patch(
        f"/api/v1/service-accounts/{uuid.uuid4()}", json={"metadata": {"theme": "dark"}}
    )
    assert response.status_code == 404
