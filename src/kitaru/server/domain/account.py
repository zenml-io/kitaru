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
"""Account entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class AccountNotFound(NotFoundError):
    """Raised when an account lookup does not resolve."""

    def __init__(self, account: uuid.UUID | str) -> None:
        """Initialize the error.

        Args:
            account: Id or name of the missing account.
        """
        super().__init__(f"Account {account} was not found")


class DuplicateAccountName(ConflictError):
    """Raised when an account name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Account name '{name}' is already registered")


class Account(DomainModel):
    """Account."""

    id: uuid.UUID = Field(default_factory=uuid7)
    is_service_account: bool = False
    name: Name
    email: str | None = None
    password_hash: str | None = None
    active: bool = True
    created: datetime | None = None
    updated: datetime | None = None

    def update_active(self, active: bool) -> None:
        """Set whether the account may authenticate.

        Args:
            active: New active state.
        """
        self.active = active

    def update_password_hash(self, password_hash: str) -> None:
        """Set a new password hash.

        Args:
            password_hash: Hash of the new password.
        """
        self.password_hash = password_hash
