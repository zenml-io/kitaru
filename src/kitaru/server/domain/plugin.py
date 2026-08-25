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
"""Plugin registry entities, code sources, and errors."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import AfterValidator, Field, model_validator

from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import (
    RESERVED_NAMESPACE,
    NamespacedName,
    VersionName,
)
from kitaru.source_refs import parse_source_ref

MAX_REQUIREMENT_LENGTH = 255


class PluginKind(StrEnum):
    """Plugin kind."""

    EVALUATOR = "evaluator"
    IMPORTER = "importer"


class PluginNotFound(NotFoundError):
    """Raised when a plugin lookup does not resolve."""

    def __init__(self, plugin: uuid.UUID | str) -> None:
        """Initialize the error.

        Args:
            plugin: Id or name of the missing plugin.
        """
        super().__init__(f"Plugin {plugin} was not found")


class DuplicatePluginName(ConflictError):
    """Raised when a plugin name is already registered for its kind."""

    def __init__(self, kind: PluginKind, name: str) -> None:
        """Initialize the error.

        Args:
            kind: Kind the name is already registered under.
            name: Name that is already registered.
        """
        super().__init__(
            f"{kind.value.capitalize()} name '{name}' is already registered"
        )


class ReservedPluginName(ValidationError):
    """Raised when a plugin name uses the reserved namespace."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that uses the reserved namespace.
        """
        super().__init__(
            f"Plugin name '{name}' uses the reserved namespace '{RESERVED_NAMESPACE}'"
        )


class DuplicatePluginVersion(ConflictError):
    """Raised when a plugin version number is already registered."""

    def __init__(self, plugin_id: uuid.UUID, version: int) -> None:
        """Initialize the error.

        Args:
            plugin_id: Id of the plugin.
            version: Version number that is already registered.
        """
        super().__init__(
            f"Version {version} of plugin {plugin_id} is already registered"
        )


class PluginInUse(ConflictError):
    """Raised when a plugin version is referenced by a stored evaluation."""

    def __init__(self, plugin_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            plugin_id: Id of the plugin in use.
        """
        super().__init__(f"Plugin {plugin_id} is in use by a stored evaluation")


class PluginVersionNotFound(NotFoundError):
    """Raised when a plugin version lookup does not resolve."""

    def __init__(self, plugin_id: uuid.UUID, version: int) -> None:
        """Initialize the error.

        Args:
            plugin_id: Id of the plugin.
            version: Missing version number.
        """
        super().__init__(f"Version {version} of plugin {plugin_id} was not found")


class PluginVersionIdNotFound(NotFoundError):
    """Raised when a plugin version lookup by id does not resolve."""

    def __init__(self, plugin_version_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            plugin_version_id: Id of the missing plugin version.
        """
        super().__init__(f"Plugin version {plugin_version_id} was not found")


class InvalidPluginProvider(ValidationError):
    """Raised when an evaluator plugin carries a provider."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Evaluator plugins do not carry a provider")


class InvalidPluginAgentScope(ValidationError):
    """Raised when an importer plugin carries an agent id."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Importer plugins do not carry an agent id")


class InvalidPluginRequirement(ValidationError):
    """Raised when a package plugin requirement fails validation."""


class InvalidPluginEntrypoint(ValidationError):
    """Raised when a plugin entrypoint fails validation."""


def validate_requirement(value: str) -> str:
    """Validate a package plugin requirement as an exact pin.

    Args:
        value: Requirement string to validate.

    Raises:
        InvalidPluginRequirement: ``value`` is not a valid PEP 508
            requirement, exceeds the length limit, references a URL, carries
            a marker, or does not pin exactly one non-wildcard ``==``
            version.

    Returns:
        Validated requirement.
    """
    if len(value) > MAX_REQUIREMENT_LENGTH:
        raise InvalidPluginRequirement(
            f"Requirement exceeds {MAX_REQUIREMENT_LENGTH} characters"
        )
    try:
        parsed = Requirement(value)
    except InvalidRequirement as exc:
        raise InvalidPluginRequirement(
            f"Requirement '{value}' is not a valid PEP 508 requirement"
        ) from exc
    if parsed.url is not None:
        raise InvalidPluginRequirement(
            f"Requirement '{value}' must not reference a URL"
        )
    if parsed.marker is not None:
        raise InvalidPluginRequirement(f"Requirement '{value}' must not carry a marker")
    specifiers = list(parsed.specifier)
    if len(specifiers) != 1 or specifiers[0].operator != "==":
        raise InvalidPluginRequirement(
            f"Requirement '{value}' must pin exactly one '==' version"
        )
    if "*" in specifiers[0].version:
        raise InvalidPluginRequirement(
            f"Requirement '{value}' version must not contain '*'"
        )
    return value


PluginRequirement = Annotated[str, AfterValidator(validate_requirement)]


def validate_package_entrypoint(value: str) -> str:
    """Validate a package plugin entrypoint as a module:attribute reference.

    Args:
        value: Entrypoint string to validate.

    Raises:
        InvalidPluginEntrypoint: ``value`` is not a well-formed
            ``module:attribute`` reference.

    Returns:
        Validated entrypoint.
    """
    try:
        parse_source_ref(value)
    except ValueError as exc:
        raise InvalidPluginEntrypoint(str(exc)) from exc
    return value


PackageEntrypoint = Annotated[str, AfterValidator(validate_package_entrypoint)]


def validate_script_entrypoint(value: str) -> str:
    """Validate a script plugin entrypoint as a bare attribute name.

    Args:
        value: Entrypoint string to validate.

    Raises:
        InvalidPluginEntrypoint: ``value`` is empty or contains a colon.

    Returns:
        Validated entrypoint.
    """
    if not value or ":" in value:
        raise InvalidPluginEntrypoint(
            f"Invalid script entrypoint '{value}', expected a bare attribute name"
        )
    return value


ScriptEntrypoint = Annotated[str, AfterValidator(validate_script_entrypoint)]


class ScriptPluginSource(FrozenModel):
    """Script plugin source."""

    type: Literal["script"] = "script"
    blob_id: uuid.UUID
    entrypoint: ScriptEntrypoint


class PackagePluginSource(FrozenModel):
    """Package plugin source."""

    type: Literal["package"] = "package"
    requirement: PluginRequirement
    entrypoint: PackageEntrypoint


PluginSource = Annotated[
    ScriptPluginSource | PackagePluginSource, Field(discriminator="type")
]


class Plugin(DomainModel):
    """Plugin."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID | None
    kind: PluginKind
    name: NamespacedName
    description: str | None = None
    provider: str | None = None
    logo_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    latest_version: int = 0
    agent_id: uuid.UUID | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def _check_provider(self) -> "Plugin":
        """Reject a provider on an evaluator plugin.

        Raises:
            InvalidPluginProvider: The kind is evaluator and provider is set.

        Returns:
            The validated plugin.
        """
        if self.kind is PluginKind.EVALUATOR and self.provider is not None:
            raise InvalidPluginProvider
        return self

    @model_validator(mode="after")
    def _check_agent_id(self) -> "Plugin":
        """Reject an agent id on an importer plugin.

        Raises:
            InvalidPluginAgentScope: The kind is importer and agent_id is
                set.

        Returns:
            The validated plugin.
        """
        if self.kind is PluginKind.IMPORTER and self.agent_id is not None:
            raise InvalidPluginAgentScope
        return self

    def update_description(self, description: str | None) -> None:
        """Set a new plugin description.

        Args:
            description: New description.
        """
        self.description = description

    def update_logo_url(self, logo_url: str | None) -> None:
        """Set a new plugin logo URL.

        Args:
            logo_url: New logo URL.
        """
        self.logo_url = logo_url

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new plugin metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata


class PluginVersion(DomainModel):
    """Plugin version."""

    id: uuid.UUID = Field(default_factory=uuid7)
    plugin_id: uuid.UUID
    version: int
    display_version: VersionName | None = None
    source: PluginSource
    created: datetime | None = None
    updated: datetime | None = None

    def update_display_version(self, display_version: VersionName | None) -> None:
        """Set a new display version.

        Args:
            display_version: New display version.
        """
        self.display_version = display_version
