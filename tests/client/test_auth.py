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
"""Round-trip tests for the auth SDK resource."""

from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    asgi_api_client,
    create_api_key,
    local_settings,
)
from kitaru.api_models.v1.auth import (
    API_KEY_PREFIX,
    CONTROL_PLANE_API_KEY_PREFIX,
    TokenResponse,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import AuthenticationError
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import get_auth_service
from kitaru.server.api.app import create_app
from kitaru.server.domain.account import Account


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


@pytest.fixture
async def api_client(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    account: Account,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    _ = account
    settings = local_settings()
    app = create_app(settings)
    service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
    )
    app.dependency_overrides[get_auth_service] = lambda: service
    async with asgi_api_client(app) as client:
        yield client


async def test_login(api_client: KitaruAPIClient) -> None:
    """Log in with a password through the SDK."""
    token = await api_client.auth.login(username="alice", password="secret")
    assert isinstance(token, TokenResponse)
    assert token.token_type == "bearer"
    assert token.expires_in > 0
    assert token.csrf_token is None


async def test_login_invalid_credentials(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 401 as a typed error."""
    with pytest.raises(AuthenticationError) as exc_info:
        await api_client.auth.login(username="alice", password="wrong")
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid username or password."


async def test_exchange_api_key(
    api_client: KitaruAPIClient,
    account: Account,
    api_key_repository: FakeApiKeyRepository,
) -> None:
    """Exchange an API key for a session token through the SDK."""
    _, key = await create_api_key(api_key_repository, account.id)
    token = await api_client.auth.exchange_api_key(key)
    assert isinstance(token, TokenResponse)
    assert token.token_type == "bearer"
    assert token.expires_in > 0


async def test_exchange_invalid_api_key(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 401 for a key the server does not accept."""
    with pytest.raises(AuthenticationError) as exc_info:
        await api_client.auth.exchange_api_key(f"{API_KEY_PREFIX}bogus")
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid API key."


async def test_control_plane_api_key_is_not_pinned_as_a_header() -> None:
    """Exchange a control plane API key rather than sending it on every request."""
    client = KitaruAPIClient(
        base_url="http://test", api_key=f"{CONTROL_PLANE_API_KEY_PREFIX}secret"
    )
    try:
        assert "Authorization" not in client._http.headers
        assert client._auth is not None
    finally:
        await client.close()


async def test_server_api_key_is_pinned_as_a_header() -> None:
    """Send a server API key as a bearer token without a token provider."""
    key = f"{API_KEY_PREFIX}secret"
    client = KitaruAPIClient(base_url="http://test", api_key=key)
    try:
        assert client._http.headers["Authorization"] == f"Bearer {key}"
        assert client._auth is None
    finally:
        await client.close()
