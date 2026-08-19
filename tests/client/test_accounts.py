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
from datetime import UTC, datetime

import pytest

from conftest import (
    FakeAccountRepository,
    FakePasswordHasher,
    asgi_api_client,
    local_settings,
    override_idempotency,
)
from kitaru.api_models.v1.account import AccountListParams, UserCreateRequest
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import Account

ACTOR = AuthContext(
    account=Account(
        id=uuid.uuid4(),
        name="admin",
        is_admin=True,
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
    )
)


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: ACTOR
    override_idempotency(app, ACTOR.account)
    async with asgi_api_client(app) as client:
        yield client


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an account by id through the SDK."""
    created = await api_client.users.create(
        UserCreateRequest(name="alice", password="secret")
    )
    loaded = await api_client.accounts.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.accounts.get(uuid.uuid4())


async def test_get_current(api_client: KitaruAPIClient) -> None:
    """Get the calling account through the SDK."""
    account = await api_client.accounts.get_current()
    assert account.id == ACTOR.account.id
    assert account.name == "admin"
    assert account.is_admin is True


async def test_list(api_client: KitaruAPIClient) -> None:
    """List accounts newest-first with filters through the SDK."""
    for name in ["alice", "bob", "carol"]:
        await api_client.users.create(UserCreateRequest(name=name))

    page = await api_client.accounts.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["carol", "bob", "alice"]

    page = await api_client.accounts.list(
        AccountListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="bob")
        )
    )
    assert page.next_cursor is None
    assert page.items[0].name == "bob"


async def test_list_walks_pages_with_cursor(api_client: KitaruAPIClient) -> None:
    """Walk every page of accounts via next_cursor through the SDK."""
    for name in ["alice", "bob", "carol"]:
        await api_client.users.create(UserCreateRequest(name=name))

    collected: list[str] = []
    params = AccountListParams(size=2)
    while True:
        page = await api_client.accounts.list(params)
        collected.extend(item.name for item in page.items)
        if page.next_cursor is None:
            break
        params = AccountListParams(cursor=page.next_cursor, size=2)

    assert collected == ["carol", "bob", "alice"]


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every account across pages through the SDK."""
    for name in ["alice", "bob", "carol"]:
        await api_client.users.create(UserCreateRequest(name=name))

    collected = [
        item.name async for item in api_client.accounts.iter(AccountListParams(size=2))
    ]

    assert collected == ["carol", "bob", "alice"]
