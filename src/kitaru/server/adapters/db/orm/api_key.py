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

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, UniqueConstraint
from sqlmodel import Field

from kitaru.server.adapters.db.orm.base import (
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


class ApiKeyORM(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """API key table."""

    __tablename__ = "api_key"
    __table_args__ = (
        UniqueConstraint("name", name=API_KEY_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=API_KEY_OWNER_ID_FOREIGN_KEY
        ),
        Index(API_KEY_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    key_hash: str = Field(max_length=128, nullable=False)
    active: bool = Field(nullable=False)
    last_used: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )

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
            active=api_key.active,
            last_used=api_key.last_used,
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
            active=self.active,
            last_used=self.last_used,
            created=self.created,
            updated=self.updated,
        )
