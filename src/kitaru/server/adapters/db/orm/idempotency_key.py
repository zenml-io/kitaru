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
"""Idempotency key ORM table."""

import uuid

from sqlalchemy import (
    ForeignKeyConstraint,
    Index,
    LargeBinary,
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
from kitaru.server.domain.idempotency_key import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    IdempotencyKey,
)

IDEMPOTENCY_KEY_ACCOUNT_ID_FOREIGN_KEY = foreign_key_name(
    "idempotency_key", ["account_id"]
)
IDEMPOTENCY_KEY_ACCOUNT_ID_KEY_UNIQUE_CONSTRAINT = unique_constraint_name(
    "idempotency_key", ["account_id", "key"]
)
IDEMPOTENCY_KEY_CREATED_INDEX = index_name("idempotency_key", ["created"])


class IdempotencyKeyORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Idempotency key table."""

    __tablename__ = "idempotency_key"
    __table_args__ = (
        ForeignKeyConstraint(
            ["account_id"],
            ["account.id"],
            name=IDEMPOTENCY_KEY_ACCOUNT_ID_FOREIGN_KEY,
        ),
        UniqueConstraint(
            "account_id", "key", name=IDEMPOTENCY_KEY_ACCOUNT_ID_KEY_UNIQUE_CONSTRAINT
        ),
        Index(IDEMPOTENCY_KEY_CREATED_INDEX, "created"),
    )

    account_id: Mapped[uuid.UUID]
    key: Mapped[str] = mapped_column(String(MAX_IDEMPOTENCY_KEY_LENGTH))
    fingerprint: Mapped[str] = mapped_column(String(64))
    method: Mapped[str] = mapped_column(String(16))
    path: Mapped[str] = mapped_column(String(2048))
    response_status: Mapped[int | None]
    response_body: Mapped[bytes | None] = mapped_column(LargeBinary)
    response_content_type: Mapped[str | None] = mapped_column(String(255))

    @classmethod
    def from_domain(cls, idempotency_key: IdempotencyKey) -> "IdempotencyKeyORM":
        """Build a row from a domain idempotency key.

        Args:
            idempotency_key: Idempotency key to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=idempotency_key.id,
            account_id=idempotency_key.account_id,
            key=idempotency_key.key,
            fingerprint=idempotency_key.fingerprint,
            method=idempotency_key.method,
            path=idempotency_key.path,
            response_status=idempotency_key.response_status,
            response_body=idempotency_key.response_body,
            response_content_type=idempotency_key.response_content_type,
        )

    def to_domain(self) -> IdempotencyKey:
        """Build a domain idempotency key from this row.

        Returns:
            Idempotency key with the created timestamp set.
        """
        return IdempotencyKey(
            id=self.id,
            account_id=self.account_id,
            key=self.key,
            fingerprint=self.fingerprint,
            method=self.method,
            path=self.path,
            response_status=self.response_status,
            response_body=self.response_body,
            response_content_type=self.response_content_type,
            created=self.created,
        )
