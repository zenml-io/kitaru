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
"""Tests for device authorization use cases."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from conftest import FakeDeviceRepository, create_device
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DeviceFingerprint, DevicePolicy
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.account import Account
from kitaru.server.domain.device import (
    DeviceAuthorizationPending,
    DeviceExpired,
    DeviceLocked,
    DeviceNotFound,
    DeviceNotVerified,
    DeviceStatus,
    InvalidDeviceCode,
)
from kitaru.server.domain.keys import hash_secret

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))


@pytest.fixture
def repository() -> FakeDeviceRepository:
    """Provide a fake device repository."""
    return FakeDeviceRepository()


@pytest.fixture
def policy() -> DevicePolicy:
    """Provide a device authorization policy with distinct trust lifetimes."""
    return DevicePolicy(
        auth_timeout_seconds=300,
        polling_interval_seconds=5,
        max_failed_attempts=3,
        expiration_minutes=60,
        trusted_expiration_minutes=600,
    )


@pytest.fixture
def service(repository: FakeDeviceRepository, policy: DevicePolicy) -> DeviceService:
    """Provide a device service backed by the fake repository."""
    return DeviceService(repository=repository, policy=policy)


async def test_request_authorization_issues_distinct_codes(
    service: DeviceService,
) -> None:
    """Issue distinct codes per authorization and store only their hashes."""
    first, first_user_code, first_device_code = await service.request_authorization(
        DeviceFingerprint()
    )
    _, second_user_code, second_device_code = await service.request_authorization(
        DeviceFingerprint()
    )

    assert first_user_code != second_user_code
    assert first_device_code != second_device_code
    assert first.user_code_hash == hash_secret(first_user_code)
    assert first.device_code_hash == hash_secret(first_device_code)
    assert first.status == DeviceStatus.PENDING
    assert first.account_id is None
    assert first.created is not None
    assert first.updated is not None
    dumped = first.model_dump()
    assert first_user_code not in dumped.values()
    assert first_device_code not in dumped.values()


async def test_verify_device_happy_path(service: DeviceService) -> None:
    """Approve a pending device authorization."""
    device, user_code, _ = await service.request_authorization(DeviceFingerprint())
    verified = await service.verify_device(
        device.id, user_code, trusted=True, actor=ACTOR
    )
    assert verified.status == DeviceStatus.VERIFIED
    assert verified.account_id == ACTOR.account.id
    assert verified.trusted is True
    assert verified.failed_auth_attempts == 0


async def test_verify_wrong_user_code_locks_after_three_attempts(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Lock the device after the third wrong user code."""
    device, _, _ = await service.request_authorization(DeviceFingerprint())

    for _ in range(3):
        with pytest.raises(InvalidDeviceCode):
            await service.verify_device(
                device.id, "WRONG-CODE", trusted=False, actor=ACTOR
            )

    stored = await repository.get(device.id)
    assert stored.locked is True
    assert stored.failed_auth_attempts == 3

    with pytest.raises(DeviceLocked):
        await service.verify_device(device.id, "WRONG-CODE", trusted=False, actor=ACTOR)


async def test_verify_rejects_foreign_account(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise not found for a device another account already approved."""
    device, user_code, _ = await create_device(
        repository, account_id=FOREIGN_ACTOR.account.id, status=DeviceStatus.VERIFIED
    )
    with pytest.raises(DeviceNotFound, match=f"Device {device.id} was not found"):
        await service.verify_device(device.id, user_code, trusted=False, actor=ACTOR)


async def test_verify_rejects_expired_authorization(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise for a device authorization past its expiry."""
    now = datetime.now(UTC)
    device, user_code, _ = await create_device(
        repository, expires=now - timedelta(seconds=1)
    )
    with pytest.raises(DeviceExpired):
        await service.verify_device(device.id, user_code, trusted=False, actor=ACTOR)


async def test_authenticate_device_pending_before_verification(
    service: DeviceService,
) -> None:
    """Raise while no account has approved the device yet."""
    device, _, device_code = await service.request_authorization(DeviceFingerprint())
    with pytest.raises(DeviceAuthorizationPending):
        await service.authenticate_device(device.id, device_code)


async def test_authenticate_device_activates_with_untrusted_expiry(
    service: DeviceService, policy: DevicePolicy
) -> None:
    """Flip a verified device to active with the untrusted lifetime."""
    device, user_code, device_code = await service.request_authorization(
        DeviceFingerprint()
    )
    await service.verify_device(device.id, user_code, trusted=False, actor=ACTOR)

    before = datetime.now(UTC)
    active = await service.authenticate_device(device.id, device_code)

    assert active.status == DeviceStatus.ACTIVE
    assert active.last_login is not None
    assert active.failed_auth_attempts == 0
    expiration_minutes = policy.expiration_minutes
    assert expiration_minutes is not None
    expected = before + timedelta(minutes=expiration_minutes)
    assert active.expires is not None
    assert abs((active.expires - expected).total_seconds()) < 5


async def test_authenticate_device_activates_with_trusted_expiry(
    service: DeviceService, policy: DevicePolicy
) -> None:
    """Flip a verified device to active with the trusted lifetime."""
    device, user_code, device_code = await service.request_authorization(
        DeviceFingerprint()
    )
    await service.verify_device(device.id, user_code, trusted=True, actor=ACTOR)

    before = datetime.now(UTC)
    active = await service.authenticate_device(device.id, device_code)

    assert active.status == DeviceStatus.ACTIVE
    trusted_expiration_minutes = policy.trusted_expiration_minutes
    assert trusted_expiration_minutes is not None
    expected = before + timedelta(minutes=trusted_expiration_minutes)
    assert active.expires is not None
    assert abs((active.expires - expected).total_seconds()) < 5


async def test_authorize_session_rejects_locked(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise for a locked device."""
    device, _, _ = await create_device(
        repository,
        account_id=ACTOR.account.id,
        status=DeviceStatus.ACTIVE,
        locked=True,
    )
    with pytest.raises(DeviceLocked):
        await service.authorize_session(device.id, ACTOR.account.id)


async def test_authorize_session_rejects_expired(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise for an expired device."""
    now = datetime.now(UTC)
    device, _, _ = await create_device(
        repository,
        account_id=ACTOR.account.id,
        status=DeviceStatus.ACTIVE,
        expires=now - timedelta(seconds=1),
    )
    with pytest.raises(DeviceExpired):
        await service.authorize_session(device.id, ACTOR.account.id)


async def test_authorize_session_rejects_foreign_account(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise not found for a device bound to another account."""
    device, _, _ = await create_device(
        repository, account_id=FOREIGN_ACTOR.account.id, status=DeviceStatus.ACTIVE
    )
    with pytest.raises(DeviceNotFound, match=f"Device {device.id} was not found"):
        await service.authorize_session(device.id, ACTOR.account.id)


async def test_authorize_session_rejects_non_active(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise for a device that has not gone active yet."""
    device, _, _ = await create_device(
        repository, account_id=ACTOR.account.id, status=DeviceStatus.VERIFIED
    )
    with pytest.raises(DeviceNotVerified):
        await service.authorize_session(device.id, ACTOR.account.id)


async def test_authorize_session_accepts_active_device(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Return the device and record its use."""
    device, _, _ = await create_device(
        repository, account_id=ACTOR.account.id, status=DeviceStatus.ACTIVE
    )
    session_device = await service.authorize_session(device.id, ACTOR.account.id)
    assert session_device.id == device.id
    assert session_device.last_login is not None


async def test_get_device_returns_unclaimed_pending_device(
    service: DeviceService,
) -> None:
    """Return a pending device no account approved yet."""
    device, _, _ = await service.request_authorization(DeviceFingerprint())
    stored = await service.get_device(device.id, actor=ACTOR)
    assert stored.id == device.id
    assert stored.account_id is None


async def test_get_device_rejects_foreign_account(
    service: DeviceService, repository: FakeDeviceRepository
) -> None:
    """Raise not found for a device another account already approved."""
    device, _, _ = await create_device(
        repository, account_id=FOREIGN_ACTOR.account.id, status=DeviceStatus.VERIFIED
    )
    with pytest.raises(DeviceNotFound):
        await service.get_device(device.id, actor=ACTOR)


async def test_update_and_delete_reject_unclaimed_device(
    service: DeviceService,
) -> None:
    """Raise not found when updating or deleting a device no account approved."""
    device, _, _ = await service.request_authorization(DeviceFingerprint())
    with pytest.raises(DeviceNotFound):
        await service.update_device(device.id, actor=ACTOR, locked=True)
    with pytest.raises(DeviceNotFound):
        await service.delete_device(device.id, actor=ACTOR)


async def test_delete_expired_removes_claimed_and_unclaimed(
    repository: FakeDeviceRepository,
) -> None:
    """Delete every expired device, whether or not an account approved it."""
    now = datetime.now(UTC)
    expired_unclaimed, _, _ = await create_device(
        repository, expires=now - timedelta(minutes=1)
    )
    expired_claimed, _, _ = await create_device(
        repository, account_id=ACTOR.account.id, expires=now - timedelta(minutes=1)
    )
    unexpired, _, _ = await create_device(
        repository, expires=now + timedelta(minutes=1)
    )
    never_expires, _, _ = await create_device(
        repository, account_id=ACTOR.account.id, expires=None
    )

    deleted = await repository.delete_expired(now)

    assert deleted == 2
    with pytest.raises(DeviceNotFound):
        await repository.get(expired_unclaimed.id)
    with pytest.raises(DeviceNotFound):
        await repository.get(expired_claimed.id)
    assert await repository.get(unexpired.id) == unexpired
    assert await repository.get(never_expires.id) == never_expires
