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
"""Contract tests for API key repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest

from conftest import FakeApiKeyRepository, pg_session, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.api_key_repository import (
    SQLApiKeyRepository,
)
from kitaru.server.application.interfaces.api_key_repository import (
    ApiKeyRepository,
)
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.filtering import FilterCondition

Setup = tuple[ApiKeyRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each API key repository implementation plus two owner ids."""
    if request.param == "fake":
        yield FakeApiKeyRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        yield SQLApiKeyRepository(session), owner.id, other_owner.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new API key with both timestamps set."""
    repository, owner_id, _ = setup
    api_key = await repository.create(
        ApiKey(owner_id=owner_id, name="ci", key_hash="hash")
    )
    assert api_key.name == "ci"
    assert api_key.owner_id == owner_id
    assert api_key.last_used is None
    assert api_key.created is not None
    assert api_key.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second API key with the same name."""
    repository, owner_id, other_owner_id = setup
    await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    with pytest.raises(
        DuplicateApiKeyName, match="API key name 'ci' is already registered"
    ):
        await repository.create(
            ApiKey(owner_id=other_owner_id, name="ci", key_hash="hash")
        )


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, _ = setup
    await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    with pytest.raises(DuplicateApiKeyName):
        await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    api_key = await repository.create(
        ApiKey(owner_id=owner_id, name="deploy", key_hash="hash")
    )
    assert api_key.name == "deploy"


async def test_get(setup: Setup) -> None:
    """Load a stored API key by id."""
    repository, owner_id, _ = setup
    created = await repository.create(
        ApiKey(owner_id=owner_id, name="ci", key_hash="hash")
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown API key id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(ApiKeyNotFound, match=f"API key {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query API keys newest-first with filters."""
    repository, owner_id, other_owner_id = setup
    ci = await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    await repository.create(ApiKey(owner_id=owner_id, name="deploy", key_hash="hash"))
    local = await repository.create(
        ApiKey(owner_id=other_owner_id, name="local", key_hash="hash")
    )

    api_keys, next_cursor = await repository.query(ApiKeyFilter())
    assert next_cursor is None
    assert [api_key.name for api_key in api_keys] == ["local", "deploy", "ci"]

    api_keys, next_cursor = await repository.query(
        ApiKeyFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="ci")
        )
    )
    assert next_cursor is None
    assert api_keys[0] == ci

    api_keys, next_cursor = await repository.query(
        ApiKeyFilter(owner_id=other_owner_id)
    )
    assert next_cursor is None
    assert api_keys[0] == local

    api_keys, next_cursor = await repository.query(
        ApiKeyFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        )
    )
    assert next_cursor is None
    assert api_keys == []


async def test_query_sort_created_asc(setup: Setup) -> None:
    """Sort API keys oldest-first with sort=created:asc."""
    repository, owner_id, _ = setup
    ci = await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    deploy = await repository.create(
        ApiKey(owner_id=owner_id, name="deploy", key_hash="hash")
    )
    local = await repository.create(
        ApiKey(owner_id=owner_id, name="local", key_hash="hash")
    )

    api_keys, next_cursor = await repository.query(ApiKeyFilter(sort="created:asc"))
    assert next_cursor is None
    assert api_keys == [ci, deploy, local]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, _ = setup
    created = [
        await repository.create(
            ApiKey(owner_id=owner_id, name=f"key-{i}", key_hash="hash")
        )
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[ApiKey] = []
    cursor = None
    while True:
        api_keys, next_cursor = await repository.query(
            ApiKeyFilter(cursor=cursor, size=2)
        )
        collected.extend(api_keys)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({api_key.id for api_key in collected}) == 5


async def test_query_filter_persists_across_cursor(setup: Setup) -> None:
    """Keep a filter applied across every page of a cursor walk."""
    repository, owner_id, other_owner_id = setup
    for i in range(3):
        await repository.create(
            ApiKey(owner_id=owner_id, name=f"mine-{i}", key_hash="hash")
        )
    for i in range(2):
        await repository.create(
            ApiKey(owner_id=other_owner_id, name=f"theirs-{i}", key_hash="hash")
        )

    collected: list[ApiKey] = []
    cursor = None
    while True:
        api_keys, next_cursor = await repository.query(
            ApiKeyFilter(owner_id=owner_id, cursor=cursor, size=1)
        )
        collected.extend(api_keys)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert len(collected) == 3
    assert all(api_key.owner_id == owner_id for api_key in collected)


async def test_query_invalid_cursor(setup: Setup) -> None:
    """Raise for a cursor string that fails to decode."""
    repository, _, _ = setup
    with pytest.raises(ValidationError):
        await repository.query(ApiKeyFilter(cursor="not-a-valid-cursor"))


async def test_query_cursor_sort_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed with a different sort."""
    repository, owner_id, _ = setup
    await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    await repository.create(ApiKey(owner_id=owner_id, name="deploy", key_hash="hash"))
    _, next_cursor = await repository.query(ApiKeyFilter(size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            ApiKeyFilter(cursor=next_cursor, size=1, sort="created:asc")
        )


async def test_query_cursor_filter_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed after the filter changes."""
    repository, owner_id, other_owner_id = setup
    await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    await repository.create(ApiKey(owner_id=owner_id, name="deploy", key_hash="hash"))
    _, next_cursor = await repository.query(ApiKeyFilter(owner_id=owner_id, size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            ApiKeyFilter(cursor=next_cursor, size=1, owner_id=other_owner_id)
        )


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, _ = setup
    created = await repository.create(
        ApiKey(owner_id=owner_id, name="ci", key_hash="hash")
    )
    last_used = datetime.now(UTC)
    created.update_active(False)
    created.mark_used(last_used)
    updated = await repository.update(created)
    assert updated.active is False
    assert updated.last_used == last_used
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown API key id."""
    repository, owner_id, _ = setup
    api_key = ApiKey(owner_id=owner_id, name="ci", key_hash="hash")
    with pytest.raises(ApiKeyNotFound, match=f"API key {api_key.id} was not found"):
        await repository.update(api_key)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming an API key to a registered name."""
    repository, owner_id, _ = setup
    await repository.create(ApiKey(owner_id=owner_id, name="ci", key_hash="hash"))
    deploy = await repository.create(
        ApiKey(owner_id=owner_id, name="deploy", key_hash="hash")
    )
    deploy.name = "ci"
    with pytest.raises(
        DuplicateApiKeyName, match="API key name 'ci' is already registered"
    ):
        await repository.update(deploy)


async def test_delete(setup: Setup) -> None:
    """Delete a stored API key."""
    repository, owner_id, _ = setup
    created = await repository.create(
        ApiKey(owner_id=owner_id, name="ci", key_hash="hash")
    )
    await repository.delete(created.id)
    with pytest.raises(ApiKeyNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown API key id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(ApiKeyNotFound, match=f"API key {missing_id} was not found"):
        await repository.delete(missing_id)
