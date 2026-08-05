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
"""API key ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.api_key import ApiKey
from kitaru.server.domain.names import MAX_NAME_LENGTH

API_KEY_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("api_key", ["name"])
API_KEY_OWNER_ID_FOREIGN_KEY = foreign_key_name("api_key", ["owner_id"])
API_KEY_OWNER_ID_INDEX = index_name("api_key", ["owner_id"])


class ApiKeyORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """API key table."""

    __tablename__ = "api_key"
    __table_args__ = (
        UniqueConstraint("name", name=API_KEY_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=API_KEY_OWNER_ID_FOREIGN_KEY
        ),
        Index(API_KEY_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    key_hash: Mapped[str] = mapped_column(String(128))
    previous_key_hash: Mapped[str | None] = mapped_column(String(128))
    retain_period_minutes: Mapped[int]
    active: Mapped[bool]
    last_used: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_rotated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @classmethod
    def from_domain(cls, api_key: ApiKey) -> "ApiKeyORM":
        """Build a row from a domain API key.

        Args:
            api_key: API key to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=api_key.id,
            owner_id=api_key.owner_id,
            name=api_key.name,
            key_hash=api_key.key_hash,
            previous_key_hash=api_key.previous_key_hash,
            retain_period_minutes=api_key.retain_period_minutes,
            active=api_key.active,
            last_used=api_key.last_used,
            last_rotated=api_key.last_rotated,
        )

    def to_domain(self) -> ApiKey:
        """Build a domain API key from this row.

        Returns:
            API key with timestamps set.
        """
        return ApiKey(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            key_hash=self.key_hash,
            previous_key_hash=self.previous_key_hash,
            retain_period_minutes=self.retain_period_minutes,
            active=self.active,
            last_used=self.last_used,
            last_rotated=self.last_rotated,
            created=self.created,
            updated=self.updated,
        )
