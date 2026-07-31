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
"""Tests for the auth routes."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    create_api_key,
    local_settings,
)
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_account_service,
    get_api_key_service,
    get_auth_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.domain.account import Account

COOKIE_NAME = "kitaru_session"


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
def api_key_repository() -> FakeApiKeyRepository:
    """Provide a fake API key repository."""
    return FakeApiKeyRepository()


@pytest.fixture
async def account(account_repository: FakeAccountRepository) -> Account:
    """Provide a stored account with a password."""
    return await account_repository.create(
        Account(name="alice", password_hash=FakePasswordHasher().hash("secret"))
    )


def build_app(
    settings: APISettings,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> FastAPI:
    """Create the app with fake-backed auth and API key services.

    Args:
        settings: API server settings.
        account_repository: Fake account repository.
        api_key_repository: Fake API key repository.

    Returns:
        Application instance.
    """
    app = create_app(settings)
    auth_service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    api_key_service = ApiKeyService(repository=api_key_repository)
    app.dependency_overrides[get_api_key_service] = lambda: api_key_service
    return app


@pytest.fixture
async def client(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    account: Account,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed auth service."""
    _ = account
    app = build_app(local_settings(), account_repository, api_key_repository)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_password_login(client: httpx.AsyncClient) -> None:
    """Log in with a password and use the token on a protected route."""
    response = await client.post(
        "/v1/login", data={"username": "alice", "password": "secret"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["expires_in"] > 0
    assert body["csrf_token"] is None

    response = await client.get(
        "/v1/api-keys",
        headers={"Authorization": f"Bearer {body['access_token']}"},
    )
    assert response.status_code == 200


async def test_direct_api_key_bearer(
    client: httpx.AsyncClient,
    account: Account,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Use a raw API key as a bearer token on a protected route."""
    api_key, key = await create_api_key(api_key_repository, account.id)
    response = await client.get(
        "/v1/api-keys", headers={"Authorization": f"Bearer {key}"}
    )
    assert response.status_code == 200
    stored = await api_key_repository.get(api_key.id)
    assert stored.last_used is not None


async def test_api_key_login(
    client: httpx.AsyncClient,
    account: Account,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Exchange an API key for a token and use the token on a protected route."""
    _, key = await create_api_key(api_key_repository, account.id)
    response = await client.post(
        "/v1/login",
        data={"grant_type": "api-key"},
        headers={"Authorization": f"Bearer {key}"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["expires_in"] > 0

    response = await client.get(
        "/v1/api-keys",
        headers={"Authorization": f"Bearer {body['access_token']}"},
    )
    assert response.status_code == 200


async def test_api_key_login_rejects_a_deactivated_key(
    client: httpx.AsyncClient,
    account: Account,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Observe HTTP 401 when the key was deactivated before the exchange."""
    _, key = await create_api_key(api_key_repository, account.id, active=False)
    response = await client.post(
        "/v1/login",
        data={"grant_type": "api-key"},
        headers={"Authorization": f"Bearer {key}"},
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid API key."}


async def test_api_key_login_without_a_credential(client: httpx.AsyncClient) -> None:
    """Observe HTTP 401 when the api-key grant carries no authorization header."""
    response = await client.post("/v1/login", data={"grant_type": "api-key"})
    assert response.status_code == 401
    assert response.json() == {"detail": "Missing API key."}


async def test_login_wrong_password(client: httpx.AsyncClient) -> None:
    """Observe HTTP 401 for a wrong password."""
    response = await client.post(
        "/v1/login", data={"username": "alice", "password": "wrong"}
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid username or password."}


async def test_login_unknown_user(client: httpx.AsyncClient) -> None:
    """Observe HTTP 401 with the same detail for an unknown user."""
    response = await client.post(
        "/v1/login", data={"username": "unknown", "password": "secret"}
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid username or password."}


async def test_login_missing_fields(client: httpx.AsyncClient) -> None:
    """Observe HTTP 400 when the login form omits the username."""
    response = await client.post("/v1/login", data={})
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid request: username is required."}

    response = await client.post("/v1/login", data={"username": "alice"})
    assert response.status_code == 401


async def test_login_unknown_grant_type(client: httpx.AsyncClient) -> None:
    """Observe HTTP 400 for a grant type this server does not know."""
    response = await client.post("/v1/login", data={"grant_type": "client_credentials"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Unsupported grant type: client_credentials"}


async def test_login_grant_type_rejected_by_scheme(client: httpx.AsyncClient) -> None:
    """Observe HTTP 400 for the control plane grant type under the local scheme."""
    response = await client.post("/v1/login", data={"grant_type": "control-plane"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Unsupported grant type: control-plane"}


async def test_login_unavailable_under_none_scheme(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    account: Account,
) -> None:
    """Observe HTTP 400 for password login under the none auth scheme."""
    _ = account
    app = build_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key"),
        account_repository,
        api_key_repository,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/login", data={"username": "alice", "password": "secret"}
        )
        assert response.status_code == 400
        assert response.json() == {"detail": "Unsupported grant type: password"}


async def test_none_scheme_requires_bootstrap(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Fail under none scheme when the default account was not initialized."""
    app = build_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key"),
        account_repository,
        api_key_repository,
    )
    account_service = AccountService(
        repository=account_repository, password_hasher=FakePasswordHasher()
    )
    app.dependency_overrides[get_account_service] = lambda: account_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(RuntimeError, match="Default account is not initialized"):
            await client.get("/v1/accounts")


async def test_none_scheme_resolves_default_account(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Resolve the stored default account under the none auth scheme."""
    account = await account_repository.create(Account(name="default"))
    await create_api_key(api_key_repository, account.id)
    app = build_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key"),
        account_repository,
        api_key_repository,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/v1/api-keys")
        assert response.status_code == 200
        assert len(response.json()["items"]) == 1


async def test_missing_bearer_credential(client: httpx.AsyncClient) -> None:
    """Observe HTTP 401 for a protected route without a credential."""
    response = await client.get("/v1/api-keys")
    assert response.status_code == 401
    assert response.json() == {"detail": "Missing bearer credential."}


async def test_api_key_login_never_sets_the_auth_cookie(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    account: Account,
) -> None:
    """Leave the auth cookie unset for the api-key grant on a cookie server."""
    _, key = await create_api_key(api_key_repository, account.id)
    app = build_app(
        local_settings(AUTH_COOKIE_NAME=COOKIE_NAME),
        account_repository,
        api_key_repository,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/login",
            data={"grant_type": "api-key"},
            headers={"Authorization": f"Bearer {key}"},
        )
        assert response.status_code == 200
        assert response.json()["csrf_token"] is None
        assert COOKIE_NAME not in response.cookies


async def test_cookie_login_and_logout(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    account: Account,
) -> None:
    """Set the auth cookie on login and clear it on logout."""
    _ = account
    app = build_app(
        local_settings(AUTH_COOKIE_NAME=COOKIE_NAME),
        account_repository,
        api_key_repository,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/login", data={"username": "alice", "password": "secret"}
        )
        assert response.status_code == 200
        csrf_token = response.json()["csrf_token"]
        assert csrf_token is not None
        assert response.cookies[COOKIE_NAME] == response.json()["access_token"]
        assert "httponly" in response.headers["set-cookie"].lower()

        # The cookie session requires the CSRF token header.
        response = await client.get("/v1/api-keys")
        assert response.status_code == 401
        response = await client.get(
            "/v1/api-keys", headers={"X-CSRF-Token": csrf_token}
        )
        assert response.status_code == 200

        response = await client.post("/v1/logout")
        assert response.status_code == 204
        assert COOKIE_NAME not in client.cookies
