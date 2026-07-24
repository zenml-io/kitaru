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
"""Secret entity and errors."""

import uuid
from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, Field, SecretStr

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name

MAX_SECRET_TYPE_LENGTH = 64

MAX_SECRET_VALUES_BYTES = 64 * 1024


class SecretNotFound(NotFoundError):
    """Raised when a secret lookup does not resolve."""

    def __init__(self, secret_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            secret_id: Id of the missing secret.
        """
        super().__init__(f"Secret {secret_id} was not found")


class DuplicateSecretName(ConflictError):
    """Raised when a secret name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Secret name '{name}' is already registered")


class SecretInUse(ConflictError):
    """Raised when a secret deletion is blocked by existing references."""

    def __init__(self, secret_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            secret_id: Id of the referenced secret.
        """
        super().__init__(f"Secret {secret_id} is referenced by agent versions")


class InvalidSecret(ValidationError):
    """Raised when a secret violates its shape rules."""


class InvalidSecretType(ValidationError):
    """Raised when a secret type exceeds the length limit."""


def validate_type(value: str) -> str:
    """Validate a secret type against the length limit.

    Args:
        value: Type to validate.

    Raises:
        InvalidSecretType: ``value`` exceeds the length limit.

    Returns:
        Validated type.
    """
    if len(value) > MAX_SECRET_TYPE_LENGTH:
        raise InvalidSecretType(f"Type exceeds {MAX_SECRET_TYPE_LENGTH} characters")
    return value


SecretType = Annotated[str, AfterValidator(validate_type)]


class SecretValuesTooLarge(ValidationError):
    """Raised when secret values exceed the size limit."""


def validate_values(value: dict[str, SecretStr]) -> dict[str, SecretStr]:
    """Validate secret values against the size limit.

    Args:
        value: Values to validate.

    Raises:
        SecretValuesTooLarge: ``value`` exceeds the size limit.

    Returns:
        Validated values.
    """
    size = sum(
        len(key.encode("utf-8")) + len(entry.get_secret_value().encode("utf-8"))
        for key, entry in value.items()
    )
    if size > MAX_SECRET_VALUES_BYTES:
        raise SecretValuesTooLarge(f"Values exceed {MAX_SECRET_VALUES_BYTES} bytes")
    return value


SecretValues = Annotated[dict[str, SecretStr], AfterValidator(validate_values)]


class Secret(DomainModel):
    """Secret."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    internal: bool = False
    type: SecretType | None = None
    values: SecretValues
    created: datetime | None = None
    updated: datetime | None = None

    def update_type(self, type: str | None) -> None:
        """Set a new secret type.

        Args:
            type: New type, ``None`` clears it.
        """
        self.type = type

    def update_values(self, values: dict[str, SecretStr]) -> None:
        """Set new secret values.

        Args:
            values: New values.
        """
        self.values = values
