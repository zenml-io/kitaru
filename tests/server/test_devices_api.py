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
"""Tests for the device routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeDeviceRepository, create_device
from kitaru.server.adapters.rest.dependencies import authorize, get_device_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.device import DevicePolicy
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.account import Account
from kitaru.server.domain.device import DeviceStatus

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
FOREIGN_ACCOUNT = Account(id=uuid.uuid4(), name="bob")


@pytest.fixture
def repository() -> FakeDeviceRepository:
    """Provide a fake device repository."""
    return FakeDeviceRepository()


@pytest.fixture
def service(repository: FakeDeviceRepository) -> DeviceService:
    """Provide a device service backed by the fake repository."""
    return DeviceService(repository=repository, policy=DevicePolicy())


@pytest.fixture
async def client(
    service: DeviceService,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed device service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_device_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_list_devices(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """List devices of the caller newest-first."""
    for _ in range(3):
        await create_device(repository, account_id=ACCOUNT.id)
    await create_device(repository, account_id=FOREIGN_ACCOUNT.id)

    response = await client.get("/api/v1/devices")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert len(body["items"]) == 3


async def test_list_devices_scoped_to_caller(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Force the caller's account id into the filter."""
    mine, _, _ = await create_device(repository, account_id=ACCOUNT.id)
    await create_device(repository, account_id=FOREIGN_ACCOUNT.id)

    response = await client.get("/api/v1/devices")
    assert response.status_code == 200
    body = response.json()
    assert [item["id"] for item in body["items"]] == [str(mine.id)]


async def test_get_device(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Get a device by id."""
    device, _, _ = await create_device(
        repository, account_id=ACCOUNT.id, status=DeviceStatus.ACTIVE, trusted=True
    )
    response = await client.get(f"/api/v1/devices/{device.id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(device.id)
    assert body["status"] == "active"
    assert body["trusted"] is True
    assert body["locked"] is False


async def test_get_device_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown device id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/devices/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Device {missing_id} was not found"}


async def test_get_device_foreign_owner(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Observe HTTP 404 for a device owned by another account."""
    device, _, _ = await create_device(repository, account_id=FOREIGN_ACCOUNT.id)
    response = await client.get(f"/api/v1/devices/{device.id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Device {device.id} was not found"}


async def test_get_unclaimed_device_requires_user_code(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Observe an unapproved device only with its user code."""
    device, user_code, _ = await create_device(repository)

    without_code = await client.get(f"/api/v1/devices/{device.id}")
    assert without_code.status_code == 404

    wrong_code = await client.get(
        f"/api/v1/devices/{device.id}", params={"user_code": "WRONG-CODE"}
    )
    assert wrong_code.status_code == 404

    with_code = await client.get(
        f"/api/v1/devices/{device.id}", params={"user_code": user_code}
    )
    assert with_code.status_code == 200
    assert with_code.json()["id"] == str(device.id)


async def test_verify_device(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Approve a pending device authorization on behalf of the caller."""
    device, user_code, _ = await create_device(repository)
    response = await client.post(
        f"/api/v1/devices/{device.id}/verify",
        json={"user_code": user_code, "trusted": True},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "verified"
    assert body["trusted"] is True


async def test_verify_device_wrong_code(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Observe HTTP 422 for a wrong user code."""
    device, _, _ = await create_device(repository)
    response = await client.post(
        f"/api/v1/devices/{device.id}/verify",
        json={"user_code": "WRONG-CODE", "trusted": False},
    )
    assert response.status_code == 422


async def test_verify_device_locks_after_three_wrong_attempts(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Lock the device after the third wrong user code."""
    device, _, _ = await create_device(repository)
    for _ in range(3):
        response = await client.post(
            f"/api/v1/devices/{device.id}/verify",
            json={"user_code": "WRONG-CODE", "trusted": False},
        )
        assert response.status_code == 422

    stored = await repository.get(device.id)
    assert stored.locked is True


async def test_verify_device_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown device id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/api/v1/devices/{missing_id}/verify",
        json={"user_code": "WHATEVER", "trusted": False},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Device {missing_id} was not found"}


async def test_update_device(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Lock a device of the caller."""
    device, _, _ = await create_device(
        repository, account_id=ACCOUNT.id, status=DeviceStatus.ACTIVE
    )
    response = await client.patch(f"/api/v1/devices/{device.id}", json={"locked": True})
    assert response.status_code == 200
    body = response.json()
    assert body["locked"] is True


async def test_update_device_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown device id."""
    response = await client.patch(
        f"/api/v1/devices/{uuid.uuid4()}", json={"locked": True}
    )
    assert response.status_code == 404


async def test_update_device_foreign_owner(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Observe HTTP 404 for a device owned by another account."""
    device, _, _ = await create_device(
        repository, account_id=FOREIGN_ACCOUNT.id, status=DeviceStatus.ACTIVE
    )
    response = await client.patch(f"/api/v1/devices/{device.id}", json={"locked": True})
    assert response.status_code == 404
    loaded = await repository.get(device.id)
    assert loaded.locked is False


async def test_delete_device(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Delete a device of the caller and observe HTTP 204."""
    device, _, _ = await create_device(repository, account_id=ACCOUNT.id)
    response = await client.delete(f"/api/v1/devices/{device.id}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/devices/{device.id}")
    assert response.status_code == 404


async def test_delete_device_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown device id."""
    response = await client.delete(f"/api/v1/devices/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_device_foreign_owner(
    client: httpx.AsyncClient, repository: FakeDeviceRepository
) -> None:
    """Observe HTTP 404 for a device owned by another account."""
    device, _, _ = await create_device(repository, account_id=FOREIGN_ACCOUNT.id)
    response = await client.delete(f"/api/v1/devices/{device.id}")
    assert response.status_code == 404
    assert await repository.get(device.id) == device
