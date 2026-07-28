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
"""Contract tests for account repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeAccountRepository, pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)


@pytest.fixture(params=["fake", "postgres"])
async def repository(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[AccountRepository, None]:
    """Provide each account repository implementation."""
    if request.param == "fake":
        yield FakeAccountRepository()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        yield SQLAccountRepository(session)


async def test_create_sets_timestamps(repository: AccountRepository) -> None:
    """Store a new account with both timestamps set."""
    account = await repository.create(Account(name="alice"))
    assert account.name == "alice"
    assert account.created is not None
    assert account.updated is not None


async def test_create_duplicate_name(repository: AccountRepository) -> None:
    """Reject a second account with the same name."""
    await repository.create(Account(name="alice"))
    with pytest.raises(
        DuplicateAccountName, match="Account name 'alice' is already registered"
    ):
        await repository.create(Account(name="alice"))


async def test_create_after_duplicate_failure(
    repository: AccountRepository,
) -> None:
    """Keep the repository usable after a duplicate name failure."""
    await repository.create(Account(name="alice"))
    with pytest.raises(DuplicateAccountName):
        await repository.create(Account(name="alice"))
    account = await repository.create(Account(name="bob"))
    assert account.name == "bob"


async def test_same_name_across_account_kinds(
    repository: AccountRepository,
) -> None:
    """Allow one user account and one service account with the same name."""
    await repository.create(Account(name="alice"))
    account = await repository.create(Account(name="alice", is_service_account=True))
    assert account.is_service_account is True


async def test_get(repository: AccountRepository) -> None:
    """Load a stored account by id."""
    created = await repository.create(
        Account(name="alice", email="alice@example.com", password_hash="hash")
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(repository: AccountRepository) -> None:
    """Raise for an unknown account id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AccountNotFound, match=f"Account {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_by_name(repository: AccountRepository) -> None:
    """Load a stored account by name and account kind."""
    created = await repository.create(Account(name="alice"))
    service_account = await repository.create(
        Account(name="alice", is_service_account=True)
    )
    loaded = await repository.get_by_name("alice")
    assert loaded == created
    loaded = await repository.get_by_name("alice", is_service_account=True)
    assert loaded == service_account


async def test_get_by_name_not_found(repository: AccountRepository) -> None:
    """Raise for an unknown account name."""
    await repository.create(Account(name="alice", is_service_account=True))
    with pytest.raises(AccountNotFound, match="Account alice was not found"):
        await repository.get_by_name("alice")


async def test_query(repository: AccountRepository) -> None:
    """Query accounts with filters and pagination."""
    alice = await repository.create(Account(name="alice"))
    await repository.create(Account(name="bob"))
    carol = await repository.create(Account(name="carol", active=False))

    accounts, total = await repository.query(AccountFilter())
    assert total == 3
    assert [account.name for account in accounts] == ["alice", "bob", "carol"]

    accounts, total = await repository.query(AccountFilter(name="alice"))
    assert total == 1
    assert accounts[0] == alice

    accounts, total = await repository.query(AccountFilter(active=False))
    assert total == 1
    assert accounts[0] == carol

    accounts, total = await repository.query(AccountFilter(page=2, page_size=2))
    assert total == 3
    assert [account.name for account in accounts] == ["carol"]

    accounts, total = await repository.query(AccountFilter(name="missing"))
    assert total == 0
    assert accounts == []


async def test_update(repository: AccountRepository) -> None:
    """Persist field changes and renew the updated timestamp."""
    created = await repository.create(Account(name="alice"))
    created.update_active(False)
    created.update_password_hash("hash")
    updated = await repository.update(created)
    assert updated.active is False
    assert updated.password_hash == "hash"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(repository: AccountRepository) -> None:
    """Raise for an unknown account id."""
    account = Account(name="alice")
    with pytest.raises(AccountNotFound, match=f"Account {account.id} was not found"):
        await repository.update(account)


async def test_update_duplicate_name(repository: AccountRepository) -> None:
    """Reject renaming an account to a registered name."""
    await repository.create(Account(name="alice"))
    bob = await repository.create(Account(name="bob"))
    bob.name = "alice"
    with pytest.raises(
        DuplicateAccountName, match="Account name 'alice' is already registered"
    ):
        await repository.update(bob)
