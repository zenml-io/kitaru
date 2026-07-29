"""Importer and evaluator registry entities."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import Field, PositiveInt, model_validator

from kitaru.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name
from kitaru.source_refs import parse_source_ref


class PluginKind(StrEnum):
    """Registered plugin kind."""

    EVALUATOR = "evaluator"
    IMPORTER = "importer"


class ScriptPluginSource(FrozenModel):
    """Single-file plugin source."""

    type: Literal["script"] = "script"
    blob_id: uuid.UUID
    entrypoint: str


class PackagePluginSource(FrozenModel):
    """Exact-pinned installable package source."""

    type: Literal["package"] = "package"
    requirement: str
    entrypoint: str

    @model_validator(mode="after")
    def _validate_source(self) -> "PackagePluginSource":
        try:
            requirement = Requirement(self.requirement)
        except InvalidRequirement as exc:
            raise ValueError("Package requirement is invalid") from exc
        if len(self.requirement) > 255:
            raise ValueError("Package requirement exceeds 255 characters")
        if requirement.url or requirement.marker:
            raise ValueError("Package requirement must not use a URL or marker")
        specifiers = list(requirement.specifier)
        if (
            len(specifiers) != 1
            or specifiers[0].operator != "=="
            or "*" in specifiers[0].version
        ):
            raise ValueError("Package requirement must contain one exact == pin")
        try:
            parse_source_ref(self.entrypoint)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self


PluginSource = Annotated[
    ScriptPluginSource | PackagePluginSource, Field(discriminator="type")
]


class PluginNotFound(NotFoundError):
    """Raised when a registry plugin lookup does not resolve."""

    def __init__(self, plugin: uuid.UUID | str) -> None:
        super().__init__(f"Plugin {plugin} was not found")


class PluginVersionNotFound(NotFoundError):
    """Raised when a registry version lookup does not resolve."""

    def __init__(self, version: uuid.UUID | int) -> None:
        super().__init__(f"Plugin version {version} was not found")


class DuplicatePluginName(ConflictError):
    """Raised when a name is already registered for a plugin kind."""

    def __init__(self, kind: PluginKind, name: str) -> None:
        super().__init__(f"{kind.value.title()} name '{name}' is already registered")


class PluginInUse(ConflictError):
    """Raised when a plugin or version has dependent rows."""

    def __init__(self, plugin_id: uuid.UUID) -> None:
        super().__init__(f"Plugin {plugin_id} is in use")


class InvalidPlugin(ValidationError):
    """Raised when a plugin violates kind-specific rules."""


class Plugin(DomainModel):
    """Registered importer or evaluator."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    kind: PluginKind
    name: Name
    description: str | None = None
    provider: str | None = None
    metadata: dict = Field(default_factory=dict)
    latest_version: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def _check_provider(self) -> "Plugin":
        if self.kind is PluginKind.EVALUATOR and self.provider is not None:
            raise InvalidPlugin("Evaluators cannot declare a provider")
        return self

    def update_description(self, description: str | None) -> None:
        """Set the plugin description."""
        self.description = description

    def update_metadata(self, metadata: dict) -> None:
        """Replace plugin metadata."""
        self.metadata = metadata


class PluginVersion(DomainModel):
    """Immutable plugin code reference with a mutable display label."""

    id: uuid.UUID = Field(default_factory=uuid7)
    plugin_id: uuid.UUID
    version: PositiveInt
    display_version: str | None = None
    source: PluginSource
    created: datetime | None = None
    updated: datetime | None = None

    def update_display_version(self, display_version: str | None) -> None:
        """Set the human-readable version."""
        self.display_version = display_version
