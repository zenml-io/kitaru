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
"""Round-trip tests for the devices SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from device_fakes import FakeDeviceRepository

from conftest import asgi_api_client
from kitaru.api_models.v1.device import (
    DeviceListParams,
    DeviceResponse,
    DeviceStatus,
    DeviceUpdateRequest,
    DeviceVerifyRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_device_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.account import Account
from kitaru.server.domain.device import Device
from kitaru.server.domain.device import DeviceStatus as DomainDeviceStatus
from kitaru.server.domain.keys import generate_secret, hash_secret

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


async def create_device(
    repository: FakeDeviceRepository,
    account_id: uuid.UUID | None = ACCOUNT.id,
    status: DomainDeviceStatus = DomainDeviceStatus.ACTIVE,
    hostname: str = "laptop",
    user_code: str = "TEST-CODE",
) -> Device:
    """Store a device in the fake repository.

    Args:
        repository: Fake device repository.
        account_id: Owning account id, or None for an unclaimed device.
        status: Device status.
        hostname: Device hostname.
        user_code: Plaintext user code.

    Returns:
        Stored device.
    """
    device = Device(
        account_id=account_id,
        user_code_hash=hash_secret(user_code),
        device_code_hash=hash_secret(generate_secret()),
        status=status,
        hostname=hostname,
    )
    return await repository.create(device)


@pytest.fixture
def device_repository() -> FakeDeviceRepository:
    """Provide a fake device repository."""
    return FakeDeviceRepository()


@pytest.fixture
async def api_client(
    device_repository: FakeDeviceRepository,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = DeviceService(repository=device_repository, policy=DevicePolicy())
    app.dependency_overrides[get_device_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_get(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Get a device by id through the SDK."""
    device = await create_device(device_repository)
    loaded = await api_client.devices.get(device.id)
    assert isinstance(loaded, DeviceResponse)
    assert loaded.id == device.id
    assert loaded.status == DeviceStatus.ACTIVE
    assert loaded.hostname == "laptop"


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.devices.get(uuid.uuid4())


async def test_get_owned_by_another_account_not_found(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Surface HTTP 404 for a device owned by another account."""
    device = await create_device(device_repository, account_id=uuid.uuid4())
    with pytest.raises(NotFoundError):
        await api_client.devices.get(device.id)


async def test_list(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """List devices of the caller through the SDK."""
    for hostname in ["ci", "laptop", "desktop"]:
        await create_device(device_repository, hostname=hostname)

    page = await api_client.devices.list()
    assert page.next_cursor is None
    assert [item.hostname for item in page.items] == ["desktop", "laptop", "ci"]

    status_filter = FilterCondition(
        field="status", op=FilterOp.EQ, value=DeviceStatus.ACTIVE
    )
    page = await api_client.devices.list(DeviceListParams(filter=status_filter))
    assert len(page.items) == 3


async def test_iter(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Iterate every device across pages through the SDK."""
    for hostname in ["ci", "laptop", "desktop"]:
        await create_device(device_repository, hostname=hostname)

    collected = [
        item.hostname
        async for item in api_client.devices.iter(DeviceListParams(size=2))
    ]

    assert collected == ["desktop", "laptop", "ci"]


async def test_verify(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Approve a pending device authorization through the SDK."""
    device = await create_device(
        device_repository,
        account_id=None,
        status=DomainDeviceStatus.PENDING,
        user_code="USER-CODE",
    )

    verified = await api_client.devices.verify(
        device.id, DeviceVerifyRequest(user_code="USER-CODE", trusted=True)
    )

    assert verified.status == DeviceStatus.VERIFIED
    assert verified.trusted is True


async def test_verify_wrong_code(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Surface HTTP 422 for a user code that does not match."""
    device = await create_device(
        device_repository,
        account_id=None,
        status=DomainDeviceStatus.PENDING,
        user_code="USER-CODE",
    )

    with pytest.raises(APIError) as exc_info:
        await api_client.devices.verify(
            device.id, DeviceVerifyRequest(user_code="WRONG-CODE")
        )
    assert exc_info.value.status_code == 422


async def test_update(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Update a device through the SDK."""
    device = await create_device(device_repository)
    updated = await api_client.devices.update(
        device.id, DeviceUpdateRequest(locked=True)
    )
    assert updated.locked is True
    updated = await api_client.devices.update(
        device.id, DeviceUpdateRequest(locked=False, trusted=True)
    )
    assert updated.locked is False
    assert updated.trusted is True


async def test_delete(
    api_client: KitaruAPIClient, device_repository: FakeDeviceRepository
) -> None:
    """Delete a device through the SDK."""
    device = await create_device(device_repository)
    await api_client.devices.delete(device.id)
    with pytest.raises(NotFoundError):
        await api_client.devices.get(device.id)
