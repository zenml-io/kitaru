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
"""Plugin entities and errors."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import AfterValidator, Field, model_validator

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name

MAX_PLUGIN_PROVIDER_LENGTH = 64

MAX_PLUGIN_ENTRYPOINT_LENGTH = 255


class PluginKind(StrEnum):
    """Plugin kind."""

    SCORER = "scorer"
    IMPORTER = "importer"


class PluginFormat(StrEnum):
    """Plugin code format."""

    INLINE = "inline"


class PluginNotFound(NotFoundError):
    """Raised when a plugin lookup does not resolve."""

    def __init__(self, plugin_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            plugin_id: Id of the missing plugin.
        """
        super().__init__(f"Plugin {plugin_id} was not found")


class PluginNameNotFound(NotFoundError):
    """Raised when a plugin name lookup does not resolve."""

    def __init__(self, kind: PluginKind, name: str) -> None:
        """Initialize the error.

        Args:
            kind: Kind of the missing plugin.
            name: Name of the missing plugin.
        """
        super().__init__(f"Plugin '{name}' of kind '{kind}' was not found")


class PluginVersionIdNotFound(NotFoundError):
    """Raised when a plugin version id lookup does not resolve."""

    def __init__(self, version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            version_id: Id of the missing plugin version.
        """
        super().__init__(f"Plugin version {version_id} was not found")


class PluginVersionNotFound(NotFoundError):
    """Raised when a plugin version lookup does not resolve."""

    def __init__(self, plugin_id: uuid.UUID, version: int) -> None:
        """Initialize the error.

        Args:
            plugin_id: Id of the plugin.
            version: Missing version number.
        """
        super().__init__(f"Plugin {plugin_id} has no version {version}")


class DuplicatePluginName(ConflictError):
    """Raised when a plugin name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Plugin name '{name}' is already registered")


class DuplicatePluginVersion(ConflictError):
    """Raised when a plugin version is already registered for its plugin."""

    def __init__(self, version: int) -> None:
        """Initialize the error.

        Args:
            version: Version that is already registered.
        """
        super().__init__(f"Plugin version {version} is already registered")


class InvalidPlugin(ValidationError):
    """Raised when a plugin violates its shape rules."""


class InvalidPluginVersion(ValidationError):
    """Raised when a plugin version violates its shape rules."""


def validate_provider(value: str) -> str:
    """Validate a plugin provider against the length limit.

    Args:
        value: Provider to validate.

    Raises:
        InvalidPlugin: ``value`` is empty or exceeds the length limit.

    Returns:
        Validated provider.
    """
    if not value:
        raise InvalidPlugin("Provider must not be empty")
    if len(value) > MAX_PLUGIN_PROVIDER_LENGTH:
        raise InvalidPlugin(f"Provider exceeds {MAX_PLUGIN_PROVIDER_LENGTH} characters")
    return value


PluginProvider = Annotated[str, AfterValidator(validate_provider)]


def validate_entrypoint(value: str) -> str:
    """Validate a plugin entrypoint against the length limit.

    Args:
        value: Entrypoint to validate.

    Raises:
        InvalidPluginVersion: ``value`` is empty or exceeds the length
            limit.

    Returns:
        Validated entrypoint.
    """
    if not value:
        raise InvalidPluginVersion("Entrypoint must not be empty")
    if len(value) > MAX_PLUGIN_ENTRYPOINT_LENGTH:
        raise InvalidPluginVersion(
            f"Entrypoint exceeds {MAX_PLUGIN_ENTRYPOINT_LENGTH} characters"
        )
    return value


PluginEntrypoint = Annotated[str, AfterValidator(validate_entrypoint)]


class Plugin(DomainModel):
    """Plugin."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    kind: PluginKind
    name: Name
    provider: PluginProvider | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    latest_version: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def validate_kind_arm(self) -> Self:
        """Validate the fields the plugin kind governs.

        Raises:
            InvalidPlugin: The plugin carries fields its kind does not
                support.

        Returns:
            Validated plugin.
        """
        if self.kind is PluginKind.SCORER:
            if self.provider is not None:
                raise InvalidPlugin("Scorers do not carry a provider")
            if self.metadata:
                raise InvalidPlugin("Scorers do not carry metadata")
        return self


class PluginVersion(DomainModel):
    """Plugin version."""

    id: uuid.UUID = Field(default_factory=uuid7)
    plugin_id: uuid.UUID
    version: int = 0
    format: PluginFormat
    blob_id: uuid.UUID
    entrypoint: PluginEntrypoint
    created: datetime | None = None
