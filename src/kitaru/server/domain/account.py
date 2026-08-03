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
from typing import Any

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import AccountName


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
    external_id: uuid.UUID | None = None
    name: AccountName
    email: str | None = None
    password_hash: str | None = None
    activation_token_hash: str | None = None
    active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None

    def update_active(self, active: bool) -> None:
        """Set whether the account may authenticate.

        Args:
            active: New active state.
        """
        self.active = active

    def update_identity(self, name: str, email: str | None) -> None:
        """Set the account name and contact email.

        Args:
            name: New account name.
            email: New contact email.
        """
        self.name = name
        self.email = email

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new account metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata

    def update_activation_token_hash(self, activation_token_hash: str | None) -> None:
        """Set the hash of the activation token.

        Args:
            activation_token_hash: Hash of the new token, ``None`` clears it.
        """
        self.activation_token_hash = activation_token_hash

    def update_password_hash(self, password_hash: str) -> None:
        """Set a new password hash.

        Args:
            password_hash: Hash of the new password.
        """
        self.password_hash = password_hash
