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
"""Round-trip tests for the accounts SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAccountRepository,
    FakePasswordHasher,
    asgi_api_client,
    local_settings,
)
from kitaru.api_models.v1.accounts import (
    AccountCreateRequest,
    AccountResponse,
    AccountUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.domain.account import Account

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin"))


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: ACTOR
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an account through the SDK."""
    account = await api_client.accounts.create(
        AccountCreateRequest(name="alice", email="alice@example.com", password="secret")
    )
    assert isinstance(account, AccountResponse)
    assert account.name == "alice"
    assert account.email == "alice@example.com"
    assert account.is_service_account is False
    assert account.active is True


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.accounts.create(AccountCreateRequest(name="alice"))
    with pytest.raises(APIError) as exc_info:
        await api_client.accounts.create(AccountCreateRequest(name="alice"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Account name 'alice' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an account by id through the SDK."""
    created = await api_client.accounts.create(AccountCreateRequest(name="alice"))
    loaded = await api_client.accounts.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.accounts.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List accounts with filters and pagination through the SDK."""
    for name in ["alice", "bob", "carol"]:
        await api_client.accounts.create(AccountCreateRequest(name=name))

    page = await api_client.accounts.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["alice", "bob", "carol"]

    page = await api_client.accounts.list(name="bob")
    assert page.total == 1
    assert page.items[0].name == "bob"

    page = await api_client.accounts.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["carol"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an account through the SDK."""
    created = await api_client.accounts.create(AccountCreateRequest(name="alice"))
    updated = await api_client.accounts.update(
        created.id, AccountUpdateRequest(active=False)
    )
    assert updated.active is False
    updated = await api_client.accounts.update(
        created.id, AccountUpdateRequest(password="new")
    )
    assert updated.active is False
