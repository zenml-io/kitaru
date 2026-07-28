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
"""Secret ORM table."""

import uuid

from pydantic import SecretStr
from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    String,
    Text,
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
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.secret import MAX_SECRET_TYPE_LENGTH, Secret

SECRET_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("secret", ["name"])
SECRET_OWNER_ID_FOREIGN_KEY = foreign_key_name("secret", ["owner_id"])
SECRET_OWNER_ID_INDEX = index_name("secret", ["owner_id"])


class SecretORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Secret table."""

    __tablename__ = "secret"
    __table_args__ = (
        UniqueConstraint("name", name=SECRET_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=SECRET_OWNER_ID_FOREIGN_KEY
        ),
        Index(SECRET_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    internal: Mapped[bool]
    type: Mapped[str | None] = mapped_column(String(MAX_SECRET_TYPE_LENGTH))
    values_encrypted: Mapped[str] = mapped_column(Text)

    @classmethod
    def from_domain(cls, secret: Secret, values_encrypted: str) -> "SecretORM":
        """Build a row from a domain secret.

        Args:
            secret: Secret to store.
            values_encrypted: Encrypted secret values.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=secret.id,
            owner_id=secret.owner_id,
            name=secret.name,
            internal=secret.internal,
            type=secret.type,
            values_encrypted=values_encrypted,
        )

    def to_domain(self, values: dict[str, SecretStr]) -> Secret:
        """Build a domain secret from this row.

        Args:
            values: Decrypted secret values.

        Returns:
            Secret with timestamps set.
        """
        return Secret(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            internal=self.internal,
            type=self.type,
            values=values,
            created=self.created,
            updated=self.updated,
        )
