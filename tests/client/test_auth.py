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
    local_settings,
)
from kitaru.api_models.v1.auth import TokenResponse
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


async def test_logout(api_client: KitaruAPIClient) -> None:
    """Log out through the SDK."""
    assert await api_client.auth.logout() is None


async def test_login_invalid_credentials(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 401 as a typed error."""
    with pytest.raises(AuthenticationError) as exc_info:
        await api_client.auth.login(username="alice", password="wrong")
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid username or password."
