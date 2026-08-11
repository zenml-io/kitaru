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
"""Tests for the control plane auth routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakeControlPlaneClient,
    FakePasswordHasher,
    control_plane_settings,
    create_api_key,
    lifespan_client,
)
from kitaru.api_models.v1.auth import CONTROL_PLANE_API_KEY_PREFIX
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneError,
    ControlPlaneUser,
)
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.rest.dependencies import (
    get_account_service,
    get_api_key_service,
    get_auth_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account


@pytest.fixture
def settings() -> APISettings:
    """Provide control plane auth scheme settings."""
    return control_plane_settings()


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
def api_key_repository() -> FakeApiKeyRepository:
    """Provide a fake API key repository."""
    return FakeApiKeyRepository()


@pytest.fixture
def control_plane_client() -> FakeControlPlaneClient:
    """Provide a fake control plane API client."""
    return FakeControlPlaneClient()


def build_app(
    settings: APISettings,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    control_plane_client: FakeControlPlaneClient,
) -> FastAPI:
    """Create the app with fake-backed auth, API key, and control plane services.

    Args:
        settings: API server settings.
        account_repository: Fake account repository.
        api_key_repository: Fake API key repository.
        control_plane_client: Fake control plane API client.

    Returns:
        Application instance.
    """
    app = create_app(settings)
    app.state.control_plane_client = control_plane_client
    control_plane = ControlPlaneAuthenticator(
        client=control_plane_client,
        account_repository=account_repository,
        server_id=settings.SERVER_ID or uuid.uuid4(),
    )
    auth_service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
        control_plane=control_plane,
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    api_key_service = ApiKeyService(repository=api_key_repository)
    app.dependency_overrides[get_api_key_service] = lambda: api_key_service
    account_service = AccountService(
        repository=account_repository,
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: account_service
    return app


def authenticate(
    control_plane_client: FakeControlPlaneClient, settings: APISettings
) -> None:
    """Script the fake control plane to authorize a user.

    Args:
        control_plane_client: Fake control plane API client.
        settings: API server settings.
    """
    control_plane_client.user = ControlPlaneUser(id=uuid.uuid4(), username="alice")


@pytest.fixture
def app(
    settings: APISettings,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    control_plane_client: FakeControlPlaneClient,
) -> FastAPI:
    """Provide the app wired to fake-backed auth and control plane services."""
    return build_app(
        settings, account_repository, api_key_repository, control_plane_client
    )


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app under the control plane auth scheme."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_control_plane_login_authenticates_token(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Log in with a control plane credential and use the token on a protected route."""
    control_plane_client.user = ControlPlaneUser(
        id=uuid.uuid4(), username="alice", email="alice@example.com"
    )

    response = await client.post(
        "/v1/login", headers={"Authorization": "Bearer cp-credential"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"

    response = await client.get(
        "/v1/api-keys",
        headers={"Authorization": f"Bearer {body['access_token']}"},
    )
    assert response.status_code == 200


async def test_control_plane_login_missing_credential(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 401 for control plane login without a bearer credential."""
    response = await client.post("/v1/login", data={})
    assert response.status_code == 401
    assert response.json() == {"detail": "Missing control plane credential."}


async def test_control_plane_login_rejected_credential(
    client: httpx.AsyncClient, control_plane_client: FakeControlPlaneClient
) -> None:
    """Observe HTTP 401 when the control plane rejects the credential."""
    control_plane_client.error = ControlPlaneError("Invalid credential.")

    response = await client.post(
        "/v1/login", headers={"Authorization": "Bearer cp-credential"}
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid control plane credential."}


async def test_control_plane_login_explicit_grant_type(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Log in with the control plane grant type stated explicitly."""
    authenticate(control_plane_client, settings)

    response = await client.post(
        "/v1/login",
        data={"grant_type": "control-plane"},
        headers={"Authorization": "Bearer cp-credential"},
    )
    assert response.status_code == 200


async def test_password_grant_type_rejected(client: httpx.AsyncClient) -> None:
    """Observe HTTP 400 for password login under the control plane scheme."""
    response = await client.post(
        "/v1/login", data={"username": "alice", "password": "secret"}
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Unsupported grant type: password"}


async def test_local_api_key_rejected(
    client: httpx.AsyncClient,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Reject a local Kitaru API key under the control plane auth scheme."""
    account = await account_repository.create(
        Account(name="alice", external_id=uuid.uuid4())
    )
    _, key = await create_api_key(api_key_repository, account.id)

    response = await client.get(
        "/v1/api-keys", headers={"Authorization": f"Bearer {key}"}
    )
    assert response.status_code == 401
    assert response.json() == {
        "detail": "Local API keys are rejected under control plane authentication."
    }


async def test_session_token_without_external_id_rejected(
    client: httpx.AsyncClient,
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    settings: APISettings,
) -> None:
    """Reject a session token issued for an account with no external id."""
    account = await account_repository.create(Account(name="alice"))
    auth_service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    token = auth_service.issue_token(AuthContext(account=account)).token

    response = await client.get(
        "/v1/api-keys", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 401
    assert response.json() == {
        "detail": "Local accounts are rejected under control plane authentication."
    }


async def test_control_plane_api_key_authenticates_directly(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Authenticate a request with a control plane API key without logging in."""
    control_plane_client.user = ControlPlaneUser(
        id=uuid.uuid4(), username="ci-bot", is_service_account=True
    )

    response = await client.get(
        "/v1/api-keys",
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 200


async def test_create_account_forbidden(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Observe HTTP 403 when creating a user under the control plane scheme."""
    authenticate(control_plane_client, settings)

    response = await client.post(
        "/v1/users",
        json={"name": "alice"},
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 403
    assert response.json() == {
        "detail": "This server does not manage its own accounts."
    }


async def test_create_service_account_forbidden(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Observe HTTP 403 when creating a service account under control plane."""
    authenticate(control_plane_client, settings)

    response = await client.post(
        "/v1/service-accounts",
        json={"name": "svc"},
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 403
    assert response.json() == {
        "detail": "This server does not manage its own accounts."
    }


async def test_update_service_account_forbidden(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Observe HTTP 403 when updating a service account under control plane."""
    authenticate(control_plane_client, settings)

    response = await client.patch(
        f"/v1/service-accounts/{uuid.uuid4()}",
        json={"metadata": {"theme": "dark"}},
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 403
    assert response.json() == {
        "detail": "This server does not manage its own accounts."
    }


async def test_update_account_forbidden(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Observe HTTP 403 when updating a user under the control plane scheme."""
    authenticate(control_plane_client, settings)

    response = await client.patch(
        f"/v1/users/{uuid.uuid4()}",
        json={"password": "new", "old_password": "old"},
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 403


async def test_update_account_is_admin_forbidden(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Observe HTTP 403 when setting the admin flag under the control plane scheme."""
    authenticate(control_plane_client, settings)

    response = await client.patch(
        f"/v1/users/{uuid.uuid4()}",
        json={"is_admin": True},
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 403


async def test_list_accounts_allowed(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Keep reading accounts available under the control plane scheme."""
    authenticate(control_plane_client, settings)

    response = await client.get(
        "/v1/accounts",
        headers={"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"},
    )
    assert response.status_code == 200


async def test_control_plane_scheme_skips_default_account_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip the default-account bootstrap at startup under the control plane scheme."""
    called = False

    async def _record_call(
        self: AccountService, name: str, password: str | None
    ) -> None:
        _ = self, name, password
        nonlocal called
        called = True

    monkeypatch.setattr(AccountService, "ensure_account", _record_call)
    async with lifespan_client(control_plane_settings(use_db=True)):
        pass
    assert called is False


async def test_update_own_account_metadata_allowed(
    client: httpx.AsyncClient,
    control_plane_client: FakeControlPlaneClient,
    settings: APISettings,
) -> None:
    """Write own metadata under the control plane scheme."""
    authenticate(control_plane_client, settings)
    headers = {"Authorization": f"Bearer {CONTROL_PLANE_API_KEY_PREFIX}abc123"}

    listed = await client.get("/v1/accounts", headers=headers)
    account_id = listed.json()["items"][0]["id"]

    response = await client.patch(
        f"/v1/users/{account_id}",
        json={"metadata": {"theme": "dark"}},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["metadata"] == {"theme": "dark"}
