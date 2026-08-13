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
"""Authorized device ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.device import (
    MAX_DEVICE_FIELD_LENGTH,
    Device,
    DeviceStatus,
)

DEVICE_ACCOUNT_ID_FOREIGN_KEY = foreign_key_name("device", ["account_id"])
DEVICE_ACCOUNT_ID_INDEX = index_name("device", ["account_id"])
DEVICE_EXPIRES_INDEX = index_name("device", ["expires"])


class DeviceORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Authorized device table."""

    __tablename__ = "device"
    __table_args__ = (
        ForeignKeyConstraint(
            ["account_id"], ["account.id"], name=DEVICE_ACCOUNT_ID_FOREIGN_KEY
        ),
        Index(DEVICE_ACCOUNT_ID_INDEX, "account_id"),
        Index(DEVICE_EXPIRES_INDEX, "expires"),
    )

    account_id: Mapped[uuid.UUID | None]
    user_code_hash: Mapped[str] = mapped_column(String(128))
    device_code_hash: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    locked: Mapped[bool]
    trusted: Mapped[bool]
    failed_auth_attempts: Mapped[int]
    expires: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    hostname: Mapped[str | None] = mapped_column(String(MAX_DEVICE_FIELD_LENGTH))
    os: Mapped[str | None] = mapped_column(String(MAX_DEVICE_FIELD_LENGTH))
    ip_address: Mapped[str | None] = mapped_column(String(MAX_DEVICE_FIELD_LENGTH))
    python_version: Mapped[str | None] = mapped_column(String(MAX_DEVICE_FIELD_LENGTH))
    client_version: Mapped[str | None] = mapped_column(String(MAX_DEVICE_FIELD_LENGTH))

    @classmethod
    def from_domain(cls, device: Device) -> "DeviceORM":
        """Build a row from a domain device.

        Args:
            device: Device to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=device.id,
            account_id=device.account_id,
            user_code_hash=device.user_code_hash,
            device_code_hash=device.device_code_hash,
            status=device.status.value,
            locked=device.locked,
            trusted=device.trusted,
            failed_auth_attempts=device.failed_auth_attempts,
            expires=device.expires,
            last_login=device.last_login,
            hostname=device.hostname,
            os=device.os,
            ip_address=device.ip_address,
            python_version=device.python_version,
            client_version=device.client_version,
        )

    def to_domain(self) -> Device:
        """Build a domain device from this row.

        Returns:
            Device with timestamps set.
        """
        return Device(
            id=self.id,
            account_id=self.account_id,
            user_code_hash=self.user_code_hash,
            device_code_hash=self.device_code_hash,
            status=DeviceStatus(self.status),
            locked=self.locked,
            trusted=self.trusted,
            failed_auth_attempts=self.failed_auth_attempts,
            expires=self.expires,
            last_login=self.last_login,
            hostname=self.hostname,
            os=self.os,
            ip_address=self.ip_address,
            python_version=self.python_version,
            client_version=self.client_version,
            created=self.created,
            updated=self.updated,
        )
