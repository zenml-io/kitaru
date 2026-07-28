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
"""Device repository interface."""

import uuid
from datetime import datetime
from typing import Protocol

from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.domain.device import Device


class DeviceRepository(Protocol):
    """Device persistence operations."""

    async def create(self, device: Device) -> Device:
        """Persist a new device.

        Args:
            device: Device to store.

        Returns:
            Stored device with timestamps set.
        """
        ...

    async def get(self, device_id: uuid.UUID) -> Device:
        """Load a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device.
        """
        ...

    async def query(
        self, device_filter: DeviceFilter
    ) -> tuple[list[Device], str | None]:
        """Query devices matching a filter.

        Args:
            device_filter: Filter and pagination parameters.

        Returns:
            Page of matching devices and the next cursor.
        """
        ...

    async def update(self, device: Device) -> Device:
        """Persist changes to an existing device.

        Args:
            device: Device with modified fields.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device with the updated timestamp renewed.
        """
        ...

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.
        """
        ...

    async def record_failed_attempt(self, device: Device) -> None:
        """Persist a failed code check in its own transaction.

        The routes that check a code answer with an error, which rolls the
        request transaction back, so the attempt counter has to be written
        outside it or a caller could guess codes forever.

        Args:
            device: Device whose attempt counter and locked state changed.
        """
        ...

    async def delete_expired(self, now: datetime) -> int:
        """Delete every device past its expiry.

        An expired device cannot authenticate or be approved, so it is dead
        weight whether or not an account ever approved it. Devices without an
        expiry are never deleted.

        Args:
            now: Current time.

        Returns:
            Number of deleted devices.
        """
        ...
