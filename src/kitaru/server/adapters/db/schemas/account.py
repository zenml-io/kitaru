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

from sqlalchemy import UniqueConstraint
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import (
    unique_constraint_name,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.names import MAX_NAME_LENGTH

# Name leads so name-only filters can use the backing index.
ACCOUNT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name(
    "account", ["name", "is_service_account"]
)


class AccountSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Account table."""

    __tablename__ = "account"
    __table_args__ = (
        UniqueConstraint(
            "name", "is_service_account", name=ACCOUNT_NAME_UNIQUE_CONSTRAINT
        ),
    )

    is_service_account: bool = Field(nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    email: str | None = Field(default=None, max_length=255)
    password_hash: str | None = Field(default=None, max_length=128)
    active: bool = Field(nullable=False)

    @classmethod
    def from_domain(cls, account: Account) -> "AccountSchema":
        """Build a row from a domain account.

        Args:
            account: Account to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=account.id,
            is_service_account=account.is_service_account,
            name=account.name,
            email=account.email,
            password_hash=account.password_hash,
            active=account.active,
        )

    def to_domain(self) -> Account:
        """Build a domain account from this row.

        Returns:
            Account with timestamps set.
        """
        return Account(
            id=self.id,
            is_service_account=self.is_service_account,
            name=self.name,
            email=self.email,
            password_hash=self.password_hash,
            active=self.active,
            created=self.created,
            updated=self.updated,
        )
