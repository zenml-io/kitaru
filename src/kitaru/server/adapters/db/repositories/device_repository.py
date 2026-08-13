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
"""SQL device repository."""

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime

from sqlalchemy import CursorResult, delete, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.device import DeviceORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.device import Device, DeviceNotFound

DEVICE_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "status": DeviceORM.status,
}


class SQLDeviceRepository(BaseSQLRepository[DeviceORM]):
    """Device repository backed by the application database."""

    orm_class = DeviceORM

    def __init__(self, session: AsyncSession, engine: AsyncEngine) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
            engine: Engine used for the writes that must outlive a rolled back
                request transaction.
        """
        super().__init__(session)
        self._engine = engine

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return DeviceNotFound(entity_id)

    async def create(self, device: Device) -> Device:
        """Persist a new device.

        Args:
            device: Device to store.

        Returns:
            Stored device with timestamps set.
        """
        row = DeviceORM.from_domain(device)
        await self._add(row)
        return row.to_domain()

    async def get(self, device_id: uuid.UUID) -> Device:
        """Load a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device.
        """
        row = await self._get_row(device_id)
        return row.to_domain()

    async def query(
        self, device_filter: DeviceFilter
    ) -> tuple[list[Device], str | None]:
        """Query devices matching a filter.

        Args:
            device_filter: Filter and pagination parameters.

        Returns:
            Page of matching devices and the next cursor.
        """
        statement = select(DeviceORM)
        if device_filter.account_id is not None:
            statement = statement.where(
                DeviceORM.account_id == device_filter.account_id
            )
        if device_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    device_filter.expression, DEVICE_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session,
            statement,
            device_filter,
            id_column=DeviceORM.id,
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, device: Device) -> Device:
        """Persist changes to an existing device.

        Args:
            device: Device with modified fields.

        Raises:
            DeviceNotFound: No device has this id.

        Returns:
            Stored device with the updated timestamp renewed.
        """
        row = await self._get_row(device.id)
        row.account_id = device.account_id
        row.user_code_hash = device.user_code_hash
        row.device_code_hash = device.device_code_hash
        row.status = device.status.value
        row.locked = device.locked
        row.trusted = device.trusted
        row.failed_auth_attempts = device.failed_auth_attempts
        row.expires = device.expires
        row.last_login = device.last_login
        row.hostname = device.hostname
        row.os = device.os
        row.ip_address = device.ip_address
        row.python_version = device.python_version
        row.client_version = device.client_version
        await self._flush()
        return row.to_domain()

    async def delete(self, device_id: uuid.UUID) -> None:
        """Delete a device by id.

        Args:
            device_id: Id of the device.

        Raises:
            DeviceNotFound: No device has this id.
        """
        await self._delete_row(device_id)

    async def record_failed_attempt(self, device: Device) -> None:
        """Persist a failed code check in its own transaction.

        The routes that check a code answer with an error, which rolls the
        request transaction back, so the attempt counter has to be written
        outside it or a caller could guess codes forever.

        Args:
            device: Device whose attempt counter and locked state changed.
        """
        statement = (
            update(DeviceORM)
            .where(DeviceORM.id == device.id)
            .values(
                failed_auth_attempts=device.failed_auth_attempts,
                locked=device.locked,
                updated=datetime.now(UTC),
            )
        )
        async with self._engine.begin() as connection:
            await connection.execute(statement)

    async def delete_expired(self, now: datetime) -> int:
        """Delete every device past its expiry.

        An expired device cannot authenticate or be approved, so it is dead
        weight whether or not an account ever approved it. Devices without an
        expiry are never deleted, since a NULL never satisfies the comparison.

        Args:
            now: Current time.

        Returns:
            Number of deleted devices.
        """
        statement = delete(DeviceORM).where(DeviceORM.expires < now)
        result = await self._session.execute(statement)
        return result.rowcount if isinstance(result, CursorResult) else 0
