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
"""Tests for the device authorization flow over the API."""

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakeDeviceRepository,
    FakePasswordHasher,
    local_settings,
)
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_api_key_service,
    get_auth_service,
    get_device_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.account import Account

DEVICE_CODE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
def api_key_repository() -> FakeApiKeyRepository:
    """Provide a fake API key repository."""
    return FakeApiKeyRepository()


@pytest.fixture
def device_repository() -> FakeDeviceRepository:
    """Provide a fake device repository."""
    return FakeDeviceRepository()


def build_app(
    settings: APISettings,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    device_repository: FakeDeviceRepository,
) -> FastAPI:
    """Create the app with fake-backed auth, API key, and device services.

    Args:
        settings: API server settings.
        account_repository: Fake account repository.
        api_key_repository: Fake API key repository.
        device_repository: Fake device repository.

    Returns:
        Application instance.
    """
    app = create_app(settings)
    device_service = DeviceService(repository=device_repository, policy=DevicePolicy())
    app.dependency_overrides[get_device_service] = lambda: device_service
    auth_service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
        device_service=device_service,
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    api_key_service = ApiKeyService(repository=api_key_repository)
    app.dependency_overrides[get_api_key_service] = lambda: api_key_service
    return app


@pytest.fixture
def settings() -> APISettings:
    """Provide local auth scheme settings."""
    return local_settings()


@pytest.fixture
def app(
    settings: APISettings,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    device_repository: FakeDeviceRepository,
) -> FastAPI:
    """Provide the app wired to fake-backed auth, API key, and device services."""
    return build_app(
        settings, account_repository, api_key_repository, device_repository
    )


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app under the local auth scheme."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_account(
    repository: FakeAccountRepository, name: str = "alice", password: str = "secret"
) -> Account:
    """Store an account in the fake repository.

    Args:
        repository: Fake account repository.
        name: Account name.
        password: Login password, stored unhashed via the fake hasher.

    Returns:
        Stored account.
    """
    return await repository.create(
        Account(name=name, password_hash=FakePasswordHasher().hash(password))
    )


async def _authorize_and_activate(
    client: httpx.AsyncClient, account: Account
) -> tuple[dict[str, Any], str, str]:
    """Run the device flow through its first successful token exchange.

    Args:
        client: HTTP client for the app.
        account: Account approving the device.

    Returns:
        Device authorization body, the account's bearer token, and the
        device's bearer token.
    """
    authorization = (await client.post("/v1/device_authorization", data={})).json()
    account_token = (
        await client.post(
            "/v1/login", data={"username": account.name, "password": "secret"}
        )
    ).json()["access_token"]
    await client.post(
        f"/v1/devices/{authorization['device_id']}/verify",
        json={"user_code": authorization["user_code"], "trusted": False},
        headers={"Authorization": f"Bearer {account_token}"},
    )
    device_token = (
        await client.post(
            "/v1/login",
            data={
                "grant_type": DEVICE_CODE_GRANT_TYPE,
                "device_id": authorization["device_id"],
                "device_code": authorization["device_code"],
            },
        )
    ).json()["access_token"]
    return authorization, account_token, device_token


async def test_device_authorization_returns_codes(client: httpx.AsyncClient) -> None:
    """Start a device authorization and receive its codes."""
    response = await client.post("/v1/device_authorization", data={"hostname": "ci"})
    assert response.status_code == 200
    body = response.json()
    assert uuid.UUID(body["device_id"])
    assert body["device_code"]
    assert body["user_code"]
    assert body["verification_uri"].endswith("/devices/verify")
    assert body["user_code"] in body["verification_uri_complete"]
    assert body["expires_in"] > 0
    assert body["interval"] > 0


async def test_poll_before_verification_is_pending(client: httpx.AsyncClient) -> None:
    """Observe HTTP 400 with authorization_pending before an account confirms."""
    authorization = (await client.post("/v1/device_authorization", data={})).json()

    response = await client.post(
        "/v1/login",
        data={
            "grant_type": DEVICE_CODE_GRANT_TYPE,
            "device_id": authorization["device_id"],
            "device_code": authorization["device_code"],
        },
    )
    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "authorization_pending"
    assert body["detail"]


async def test_full_device_flow(
    client: httpx.AsyncClient, account_repository: FakeAccountRepository
) -> None:
    """Verify a device, poll for a token, and use it on a protected route."""
    account = await create_account(account_repository)
    authorization, _, device_token = await _authorize_and_activate(client, account)

    response = await client.get(
        f"/v1/devices/{authorization['device_id']}",
        headers={"Authorization": f"Bearer {device_token}"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "active"


async def test_locking_device_revokes_token(
    client: httpx.AsyncClient, account_repository: FakeAccountRepository
) -> None:
    """Lock the device and observe HTTP 401 for its token."""
    account = await create_account(account_repository)
    authorization, account_token, device_token = await _authorize_and_activate(
        client, account
    )
    device_headers = {"Authorization": f"Bearer {device_token}"}

    response = await client.get(
        f"/v1/devices/{authorization['device_id']}", headers=device_headers
    )
    assert response.status_code == 200

    response = await client.patch(
        f"/v1/devices/{authorization['device_id']}",
        json={"locked": True},
        headers={"Authorization": f"Bearer {account_token}"},
    )
    assert response.status_code == 200

    response = await client.get(
        f"/v1/devices/{authorization['device_id']}", headers=device_headers
    )
    assert response.status_code == 401


async def test_deleting_device_revokes_token(
    client: httpx.AsyncClient, account_repository: FakeAccountRepository
) -> None:
    """Delete the device and observe HTTP 401 for its token."""
    account = await create_account(account_repository)
    authorization, account_token, device_token = await _authorize_and_activate(
        client, account
    )
    device_headers = {"Authorization": f"Bearer {device_token}"}

    response = await client.delete(
        f"/v1/devices/{authorization['device_id']}",
        headers={"Authorization": f"Bearer {account_token}"},
    )
    assert response.status_code == 204

    response = await client.get(
        f"/v1/devices/{authorization['device_id']}", headers=device_headers
    )
    assert response.status_code == 401


async def test_device_grant_rejected_under_none_scheme(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    device_repository: FakeDeviceRepository,
) -> None:
    """Observe HTTP 400 for the device grant type under the none auth scheme."""
    settings = APISettings(
        DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key"
    )
    app = build_app(settings, account_repository, api_key_repository, device_repository)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/login", data={"grant_type": DEVICE_CODE_GRANT_TYPE}
        )
    assert response.status_code == 400
    assert response.json() == {
        "detail": f"Unsupported grant type: {DEVICE_CODE_GRANT_TYPE}"
    }
