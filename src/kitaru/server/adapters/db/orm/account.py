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
"""Account ORM table."""

import uuid
from typing import Any

from sqlalchemy import Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.names import MAX_NAME_LENGTH

# Name leads so name-only filters can use the backing index.
ACCOUNT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name(
    "account", ["name", "is_service_account"]
)
ACCOUNT_EXTERNAL_ID_INDEX = index_name("account", ["external_id"])


class AccountORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Account table."""

    __tablename__ = "account"
    __table_args__ = (
        UniqueConstraint(
            "name", "is_service_account", name=ACCOUNT_NAME_UNIQUE_CONSTRAINT
        ),
        Index(ACCOUNT_EXTERNAL_ID_INDEX, "external_id"),
    )

    is_service_account: Mapped[bool]
    external_id: Mapped[uuid.UUID | None]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    email: Mapped[str | None] = mapped_column(String(255))
    password_hash: Mapped[str | None] = mapped_column(String(128))
    activation_token_hash: Mapped[str | None] = mapped_column(String(64))
    active: Mapped[bool]
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, account: Account) -> "AccountORM":
        """Build a row from a domain account.

        Args:
            account: Account to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=account.id,
            is_service_account=account.is_service_account,
            external_id=account.external_id,
            name=account.name,
            email=account.email,
            password_hash=account.password_hash,
            activation_token_hash=account.activation_token_hash,
            active=account.active,
            metadata_=account.metadata,
        )

    def to_domain(self) -> Account:
        """Build a domain account from this row.

        Returns:
            Account with timestamps set.
        """
        return Account(
            id=self.id,
            is_service_account=self.is_service_account,
            external_id=self.external_id,
            name=self.name,
            email=self.email,
            password_hash=self.password_hash,
            activation_token_hash=self.activation_token_hash,
            active=self.active,
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
