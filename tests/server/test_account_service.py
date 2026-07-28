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
"""Tests for account use cases."""

import uuid

import pytest

from conftest import FakeAccountRepository, FakePasswordHasher
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin"))


@pytest.fixture
def service() -> AccountService:
    """Provide an account service backed by fakes."""
    return AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
    )


async def test_create_account(service: AccountService) -> None:
    """Create an account with all fields."""
    account = await service.create_account(
        name="alice", email="alice@example.com", password=None, actor=ACTOR
    )
    assert account.name == "alice"
    assert account.email == "alice@example.com"
    assert account.password_hash is None
    assert account.active is True
    assert account.is_service_account is False
    assert account.created is not None
    assert account.updated is not None


async def test_create_account_hashes_password(service: AccountService) -> None:
    """Store the hash of a given password, never the plaintext."""
    account = await service.create_account(
        name="alice", email=None, password="secret", actor=ACTOR
    )
    assert account.password_hash == "hashed:secret"


async def test_create_account_duplicate_name(service: AccountService) -> None:
    """Reject a second account with the same name."""
    await service.create_account(name="alice", email=None, password=None, actor=ACTOR)
    with pytest.raises(
        DuplicateAccountName, match="Account name 'alice' is already registered"
    ):
        await service.create_account(
            name="alice", email=None, password=None, actor=ACTOR
        )


async def test_get_account(service: AccountService) -> None:
    """Load a stored account by id."""
    created = await service.create_account(
        name="alice", email=None, password=None, actor=ACTOR
    )
    loaded = await service.get_account(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_account_not_found(service: AccountService) -> None:
    """Raise for an unknown account id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AccountNotFound, match=f"Account {missing_id} was not found"):
        await service.get_account(missing_id, actor=ACTOR)


async def test_list_accounts(service: AccountService) -> None:
    """List accounts newest-first with filters."""
    for name in ["alice", "bob", "carol"]:
        await service.create_account(name=name, email=None, password=None, actor=ACTOR)

    accounts, next_cursor = await service.list_accounts(AccountFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [account.name for account in accounts] == ["carol", "bob", "alice"]

    accounts, next_cursor = await service.list_accounts(
        AccountFilter(name="bob"), actor=ACTOR
    )
    assert next_cursor is None
    assert accounts[0].name == "bob"


async def test_list_accounts_walks_pages(service: AccountService) -> None:
    """Walk every page of accounts via next_cursor."""
    for name in ["alice", "bob", "carol"]:
        await service.create_account(name=name, email=None, password=None, actor=ACTOR)

    collected: list[str] = []
    cursor = None
    while True:
        accounts, next_cursor = await service.list_accounts(
            AccountFilter(cursor=cursor, size=2), actor=ACTOR
        )
        collected.extend(account.name for account in accounts)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["carol", "bob", "alice"]


async def test_update_account_active(service: AccountService) -> None:
    """Deactivate and reactivate an account."""
    created = await service.create_account(
        name="alice", email=None, password=None, actor=ACTOR
    )
    updated = await service.update_account(
        created.id, active=False, password=None, actor=ACTOR
    )
    assert updated.active is False
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    updated = await service.update_account(
        created.id, active=True, password=None, actor=ACTOR
    )
    assert updated.active is True


async def test_update_account_password(service: AccountService) -> None:
    """Replace the stored password hash."""
    created = await service.create_account(
        name="alice", email=None, password="old", actor=ACTOR
    )
    updated = await service.update_account(
        created.id, active=None, password="new", actor=ACTOR
    )
    assert updated.password_hash == "hashed:new"
    assert updated.active is True


async def test_update_account_not_found(service: AccountService) -> None:
    """Raise for an unknown account id."""
    with pytest.raises(AccountNotFound):
        await service.update_account(
            uuid.uuid4(), active=False, password=None, actor=ACTOR
        )
