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
"""Connection ORM table."""

import uuid

from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
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
from kitaru.server.domain.connection import MAX_PROVIDER_LENGTH, Connection
from kitaru.server.domain.names import MAX_NAME_LENGTH

CONNECTION_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("connection", ["name"])
# A partial unique index, not a plain unique constraint, since Postgres only
# supports a WHERE predicate on an index. A provider has at most one default.
CONNECTION_PROVIDER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "connection", ["provider"]
)
CONNECTION_OWNER_ID_FOREIGN_KEY = foreign_key_name("connection", ["owner_id"])
CONNECTION_SECRET_ID_FOREIGN_KEY = foreign_key_name("connection", ["secret_id"])
CONNECTION_OWNER_ID_INDEX = index_name("connection", ["owner_id"])

# "default" is a reserved word, so the predicate quotes the column.
DEFAULT_PREDICATE = '"default"'


class ConnectionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Connection table."""

    __tablename__ = "connection"
    __table_args__ = (
        UniqueConstraint("name", name=CONNECTION_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=CONNECTION_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["secret_id"], ["secret.id"], name=CONNECTION_SECRET_ID_FOREIGN_KEY
        ),
        Index(CONNECTION_OWNER_ID_INDEX, "owner_id"),
        Index(
            CONNECTION_PROVIDER_UNIQUE_CONSTRAINT,
            "provider",
            unique=True,
            postgresql_where=text(DEFAULT_PREDICATE),
        ),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    provider: Mapped[str] = mapped_column(String(MAX_PROVIDER_LENGTH))
    env: Mapped[dict[str, str]] = mapped_column(JSONB)
    secret_id: Mapped[uuid.UUID]
    default: Mapped[bool]

    @classmethod
    def from_domain(cls, connection: Connection) -> "ConnectionORM":
        """Build a row from a domain connection.

        Args:
            connection: Connection to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=connection.id,
            owner_id=connection.owner_id,
            name=connection.name,
            provider=connection.provider,
            env=connection.env,
            secret_id=connection.secret_id,
            default=connection.default,
        )

    def to_domain(self) -> Connection:
        """Build a domain connection from this row.

        Returns:
            Connection with timestamps set.
        """
        return Connection(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            provider=self.provider,
            env=self.env,
            secret_id=self.secret_id,
            default=self.default,
            created=self.created,
            updated=self.updated,
        )
