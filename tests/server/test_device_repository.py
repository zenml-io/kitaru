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
"""Contract tests for device repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest

from conftest import (
    FakeDeviceRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.device_repository import (
    SQLDeviceRepository,
)
from kitaru.server.application.interfaces.device_repository import DeviceRepository
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.device import Device, DeviceNotFound, DeviceStatus
from kitaru.server.domain.keys import hash_secret

Setup = tuple[DeviceRepository, uuid.UUID, uuid.UUID, Callable[[], Awaitable[None]]]


async def _commit_noop() -> None:
    """Stand in for a session commit for the in-memory fake."""


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each device repository implementation, two account ids, and a commit."""
    if request.param == "fake":
        yield FakeDeviceRepository(), uuid.uuid4(), uuid.uuid4(), _commit_noop
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        # The account_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        yield (
            SQLDeviceRepository(session, engine),
            owner.id,
            other_owner.id,
            session.commit,
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new device with both timestamps set."""
    repository, owner_id, _, _ = setup
    device = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    assert device.account_id == owner_id
    assert device.status == DeviceStatus.PENDING
    assert device.created is not None
    assert device.updated is not None


async def test_create_unclaimed(setup: Setup) -> None:
    """Store a device authorization no account has approved yet."""
    repository, _, _, _ = setup
    device = await repository.create(
        Device(
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    assert device.account_id is None


async def test_get(setup: Setup) -> None:
    """Load a stored device by id."""
    repository, owner_id, _, _ = setup
    created = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown device id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(DeviceNotFound, match=f"Device {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query devices newest-first with filters."""
    repository, owner_id, other_owner_id, _ = setup
    ci = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("ci-user"),
            device_code_hash=hash_secret("ci-device"),
        )
    )
    deploy = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("deploy-user"),
            device_code_hash=hash_secret("deploy-device"),
            status=DeviceStatus.ACTIVE,
        )
    )
    local = await repository.create(
        Device(
            account_id=other_owner_id,
            user_code_hash=hash_secret("local-user"),
            device_code_hash=hash_secret("local-device"),
        )
    )

    devices, next_cursor = await repository.query(DeviceFilter())
    assert next_cursor is None
    assert devices == [local, deploy, ci]

    devices, next_cursor = await repository.query(DeviceFilter(account_id=owner_id))
    assert next_cursor is None
    assert devices == [deploy, ci]

    devices, next_cursor = await repository.query(
        DeviceFilter(status=DeviceStatus.ACTIVE)
    )
    assert next_cursor is None
    assert devices == [deploy]


async def test_query_sort_created_asc(setup: Setup) -> None:
    """Sort devices oldest-first with sort=created:asc."""
    repository, owner_id, _, _ = setup
    ci = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("a"),
            device_code_hash=hash_secret("a"),
        )
    )
    deploy = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("b"),
            device_code_hash=hash_secret("b"),
        )
    )
    local = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("c"),
            device_code_hash=hash_secret("c"),
        )
    )

    devices, next_cursor = await repository.query(DeviceFilter(sort="created:asc"))
    assert next_cursor is None
    assert devices == [ci, deploy, local]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, _, _ = setup
    created = [
        await repository.create(
            Device(
                account_id=owner_id,
                user_code_hash=hash_secret(f"user-{i}"),
                device_code_hash=hash_secret(f"device-{i}"),
            )
        )
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Device] = []
    cursor = None
    while True:
        devices, next_cursor = await repository.query(
            DeviceFilter(cursor=cursor, size=2)
        )
        collected.extend(devices)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({device.id for device in collected}) == 5


async def test_query_filter_persists_across_cursor(setup: Setup) -> None:
    """Keep a filter applied across every page of a cursor walk."""
    repository, owner_id, other_owner_id, _ = setup
    for i in range(3):
        await repository.create(
            Device(
                account_id=owner_id,
                user_code_hash=hash_secret(f"mine-user-{i}"),
                device_code_hash=hash_secret(f"mine-device-{i}"),
            )
        )
    for i in range(2):
        await repository.create(
            Device(
                account_id=other_owner_id,
                user_code_hash=hash_secret(f"theirs-user-{i}"),
                device_code_hash=hash_secret(f"theirs-device-{i}"),
            )
        )

    collected: list[Device] = []
    cursor = None
    while True:
        devices, next_cursor = await repository.query(
            DeviceFilter(account_id=owner_id, cursor=cursor, size=1)
        )
        collected.extend(devices)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert len(collected) == 3
    assert all(device.account_id == owner_id for device in collected)


async def test_query_invalid_cursor(setup: Setup) -> None:
    """Raise for a cursor string that fails to decode."""
    repository, _, _, _ = setup
    with pytest.raises(ValidationError):
        await repository.query(DeviceFilter(cursor="not-a-valid-cursor"))


async def test_query_cursor_sort_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed with a different sort."""
    repository, owner_id, _, _ = setup
    await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("a"),
            device_code_hash=hash_secret("a"),
        )
    )
    await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("b"),
            device_code_hash=hash_secret("b"),
        )
    )
    _, next_cursor = await repository.query(DeviceFilter(size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            DeviceFilter(cursor=next_cursor, size=1, sort="created:asc")
        )


async def test_query_cursor_filter_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed after the filter changes."""
    repository, owner_id, other_owner_id, _ = setup
    await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("a"),
            device_code_hash=hash_secret("a"),
        )
    )
    await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("b"),
            device_code_hash=hash_secret("b"),
        )
    )
    _, next_cursor = await repository.query(DeviceFilter(account_id=owner_id, size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            DeviceFilter(cursor=next_cursor, size=1, account_id=other_owner_id)
        )


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, _, _ = setup
    created = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    now = datetime.now(UTC)
    created.mark_verified(owner_id, trusted=True)
    created.mark_active(now + timedelta(minutes=5), now)
    updated = await repository.update(created)
    assert updated.status == DeviceStatus.ACTIVE
    assert updated.trusted is True
    assert updated.last_login == now
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown device id."""
    repository, owner_id, _, _ = setup
    device = Device(
        account_id=owner_id,
        user_code_hash=hash_secret("user-code"),
        device_code_hash=hash_secret("device-code"),
    )
    with pytest.raises(DeviceNotFound, match=f"Device {device.id} was not found"):
        await repository.update(device)


async def test_delete(setup: Setup) -> None:
    """Delete a stored device."""
    repository, owner_id, _, _ = setup
    created = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    await repository.delete(created.id)
    with pytest.raises(DeviceNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown device id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(DeviceNotFound, match=f"Device {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_record_failed_attempt(setup: Setup) -> None:
    """Persist an incremented attempt counter and renew the updated timestamp."""
    repository, owner_id, _, commit = setup
    created = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    # record_failed_attempt writes through its own connection, so the row it
    # targets has to be committed first for that connection to see it.
    await commit()
    created.register_failed_attempt(max_attempts=3)
    await repository.record_failed_attempt(created)

    loaded = await repository.get(created.id)
    assert loaded.failed_auth_attempts == 1
    assert loaded.locked is False
    assert loaded.updated is not None
    assert created.updated is not None
    assert loaded.updated > created.updated


async def test_record_failed_attempt_locks_at_limit(setup: Setup) -> None:
    """Lock the device once the failed attempt count reaches the limit."""
    repository, owner_id, _, commit = setup
    created = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("user-code"),
            device_code_hash=hash_secret("device-code"),
        )
    )
    await commit()
    for _ in range(3):
        created.register_failed_attempt(max_attempts=3)
        await repository.record_failed_attempt(created)

    loaded = await repository.get(created.id)
    assert loaded.failed_auth_attempts == 3
    assert loaded.locked is True


async def test_delete_expired(setup: Setup) -> None:
    """Delete every expired device, whether or not an account approved it."""
    repository, owner_id, _, _ = setup
    now = datetime.now(UTC)
    expired_unclaimed = await repository.create(
        Device(
            user_code_hash=hash_secret("a"),
            device_code_hash=hash_secret("a"),
            expires=now - timedelta(minutes=1),
        )
    )
    expired_claimed = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("b"),
            device_code_hash=hash_secret("b"),
            expires=now - timedelta(minutes=1),
        )
    )
    unexpired = await repository.create(
        Device(
            user_code_hash=hash_secret("c"),
            device_code_hash=hash_secret("c"),
            expires=now + timedelta(minutes=1),
        )
    )
    never_expires = await repository.create(
        Device(
            account_id=owner_id,
            user_code_hash=hash_secret("d"),
            device_code_hash=hash_secret("d"),
            expires=None,
        )
    )

    deleted = await repository.delete_expired(now)

    assert deleted == 2
    with pytest.raises(DeviceNotFound):
        await repository.get(expired_unclaimed.id)
    with pytest.raises(DeviceNotFound):
        await repository.get(expired_claimed.id)
    assert await repository.get(unexpired.id) == unexpired
    assert await repository.get(never_expires.id) == never_expires
