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
"""Shared device fakes and app wiring for client SDK tests.

Not named conftest.py: a second conftest.py under tests/client would collide
with tests/conftest.py in sys.modules, since neither directory is a package.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI

from conftest import FakeAccountRepository, FakeApiKeyRepository, FakePasswordHasher
from conftest import local_settings as base_local_settings
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_auth_service,
    get_device_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.device import DeviceFilter, DevicePolicy
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.device import Device, DeviceNotFound


def _renewed_timestamp(previous: datetime | None) -> datetime:
    """Return an update time strictly after the stored updated timestamp.

    Args:
        previous: Stored updated timestamp.

    Returns:
        Update time.
    """
    now = datetime.now(UTC)
    if previous is not None and now <= previous:
        now = previous + timedelta(microseconds=1)
    return now


class FakeDeviceRepository:
    """In-memory device repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._devices: dict[uuid.UUID, Device] = {}

    async def create(self, device: Device) -> Device:
        """Persist a new device.

        Args:
            device: Device to store.

        Returns:
            Stored device with timestamps set.
        """
        now = datetime.now(UTC)
        stored = device.model_copy(update={"created": now, "updated": now})
        self._devices[stored.id] = stored
        return stored.model_copy()

    async def get(self, device_id: uuid.UUID) -> Device:
        """Load a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device.
        """
        device = self._devices.get(device_id)
        if device is None:
            raise DeviceNotFound(device_id)
        return device.model_copy()

    async def query(
        self, device_filter: DeviceFilter
    ) -> tuple[list[Device], str | None]:
        """Query devices matching a filter.

        Args:
            device_filter: Filter and pagination parameters.

        Returns:
            Page of matching devices and the next cursor.
        """
        devices = list(self._devices.values())
        if device_filter.account_id is not None:
            devices = [
                device
                for device in devices
                if device.account_id == device_filter.account_id
            ]
        if device_filter.status is not None:
            devices = [
                device for device in devices if device.status == device_filter.status
            ]
        _, _, direction = device_filter.sort.partition(":")
        descending = direction == "desc"
        filter_hash = device_filter.compute_filter_hash()
        cursor = None
        if device_filter.cursor is not None:
            cursor = decode_cursor(
                device_filter.cursor, device_filter.sort, filter_hash
            )

        ordered = sorted(devices, key=lambda device: device.id, reverse=descending)
        if cursor is not None:
            last_id = uuid.UUID(cursor.id)
            ordered = [
                device
                for device in ordered
                if (device.id < last_id if descending else device.id > last_id)
            ]

        page = ordered[: device_filter.size + 1]
        next_cursor = None
        if len(page) > device_filter.size:
            page = page[: device_filter.size]
            next_cursor = encode_cursor(
                device_filter.sort, str(page[-1].id), filter_hash
            )
        return [device.model_copy() for device in page], next_cursor

    async def update(self, device: Device) -> Device:
        """Persist changes to an existing device.

        Args:
            device: Device with modified fields.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device with the updated timestamp renewed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            raise DeviceNotFound(device.id)
        now = _renewed_timestamp(stored.updated)
        updated = device.model_copy(update={"created": stored.created, "updated": now})
        self._devices[device.id] = updated
        return updated.model_copy()

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.
        """
        if device_id not in self._devices:
            raise DeviceNotFound(device_id)
        del self._devices[device_id]

    async def record_failed_attempt(self, device: Device) -> None:
        """Persist a failed code check independently of the caller's update.

        Args:
            device: Device whose attempt counter and locked state changed.
        """
        stored = self._devices.get(device.id)
        if stored is None:
            return
        now = _renewed_timestamp(stored.updated)
        self._devices[device.id] = device.model_copy(
            update={"created": stored.created, "updated": now}
        )

    async def delete_expired(self, now: datetime) -> int:
        """Delete every device past its expiry.

        Args:
            now: Current time.

        Returns:
            Number of deleted devices.
        """
        expired_ids = [
            device_id
            for device_id, device in self._devices.items()
            if device.is_expired(now)
        ]
        for device_id in expired_ids:
            del self._devices[device_id]
        return len(expired_ids)


def build_device_auth_app(
    account_repository: FakeAccountRepository,
    api_key_repository: FakeApiKeyRepository,
    device_repository: FakeDeviceRepository,
    **overrides: Any,
) -> FastAPI:
    """Build the local-scheme app with auth and device services on fakes.

    Args:
        account_repository: Fake account repository.
        api_key_repository: Fake API key repository.
        device_repository: Fake device repository.
        **overrides: Additional API settings overrides.

    Returns:
        Application with get_auth_service and get_device_service overridden.
    """
    settings: APISettings = base_local_settings(**overrides)
    app = create_app(settings)
    device_service = DeviceService(
        repository=device_repository,
        policy=DevicePolicy(
            auth_timeout_seconds=settings.DEVICE_AUTH_TIMEOUT_SECONDS,
            polling_interval_seconds=settings.DEVICE_AUTH_POLLING_INTERVAL_SECONDS,
            max_failed_attempts=settings.MAX_FAILED_DEVICE_AUTH_ATTEMPTS,
            expiration_minutes=settings.DEVICE_EXPIRATION_MINUTES,
            trusted_expiration_minutes=settings.TRUSTED_DEVICE_EXPIRATION_MINUTES,
        ),
    )
    auth_service = AuthService(
        settings=settings,
        account_repository=account_repository,
        api_key_repository=api_key_repository,
        password_hasher=FakePasswordHasher(),
        device_service=device_service,
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_device_service] = lambda: device_service
    return app
