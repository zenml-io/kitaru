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
"""Connection entity and errors."""

import uuid
from collections.abc import Iterable
from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import MAX_NAME_LENGTH, Name

MAX_PROVIDER_LENGTH = MAX_NAME_LENGTH

# The worker owns every KITARU_ variable in the task process environment, so a
# connection may not set one.
RESERVED_ENV_PREFIX = "KITARU_"


class ConnectionNotFound(NotFoundError):
    """Raised when a connection lookup does not resolve."""

    def __init__(self, connection_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            connection_id: Id of the missing connection.
        """
        super().__init__(f"Connection {connection_id} was not found")


class DuplicateConnectionName(ConflictError):
    """Raised when a connection name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Connection name '{name}' is already registered")


class InvalidConnectionValues(ValidationError):
    """Raised when connection keys are reserved or collide."""


class InvalidConnectionProvider(ValidationError):
    """Raised when a connection provider exceeds the length limit."""


def validate_provider(value: str) -> str:
    """Validate a connection provider against the length limit.

    Args:
        value: Provider to validate.

    Raises:
        InvalidConnectionProvider: ``value`` is empty or exceeds the length
            limit.

    Returns:
        Validated provider.
    """
    if not value:
        raise InvalidConnectionProvider("Provider must not be empty")
    if len(value) > MAX_PROVIDER_LENGTH:
        raise InvalidConnectionProvider(
            f"Provider exceeds {MAX_PROVIDER_LENGTH} characters"
        )
    return value


Provider = Annotated[str, AfterValidator(validate_provider)]


def validate_env(value: dict[str, str]) -> dict[str, str]:
    """Validate connection env keys against the reserved prefix.

    Args:
        value: Env to validate.

    Raises:
        InvalidConnectionValues: A key uses the reserved prefix.

    Returns:
        Validated env.
    """
    for key in value:
        if key.startswith(RESERVED_ENV_PREFIX):
            raise InvalidConnectionValues(
                f"Key '{key}' uses the reserved prefix '{RESERVED_ENV_PREFIX}'"
            )
    return value


ConnectionEnv = Annotated[dict[str, str], AfterValidator(validate_env)]


class Connection(DomainModel):
    """Connection."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    provider: Provider
    env: ConnectionEnv = Field(default_factory=dict)
    secret_id: uuid.UUID
    default: bool = False
    created: datetime | None = None
    updated: datetime | None = None

    def check_secret_keys(self, keys: Iterable[str]) -> None:
        """Check secret keys against the reserved prefix and the env keys.

        Args:
            keys: Secret keys to check.

        Raises:
            InvalidConnectionValues: A key uses the reserved prefix or is
                also an env key.
        """
        for key in keys:
            if key.startswith(RESERVED_ENV_PREFIX):
                raise InvalidConnectionValues(
                    f"Key '{key}' uses the reserved prefix '{RESERVED_ENV_PREFIX}'"
                )
            if key in self.env:
                raise InvalidConnectionValues(
                    f"Key '{key}' is set as both an env value and a secret"
                )

    def update_env(self, env: dict[str, str]) -> None:
        """Set new connection env.

        Args:
            env: New env.
        """
        self.env = env

    def update_default(self, default: bool) -> None:
        """Set whether the connection is the default of its provider.

        Args:
            default: New default state.
        """
        self.default = default
