"""Tag entities and errors."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class TagResourceType(StrEnum):
    """Resource kinds tags may reference."""

    SESSION = "session"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    EXPERIMENT_RUN = "experiment_run"


class TagNotFound(NotFoundError):
    """Raised when a tag lookup does not resolve."""

    def __init__(self, tag: uuid.UUID | str) -> None:
        super().__init__(f"Tag {tag} was not found")


class DuplicateTagName(ConflictError):
    """Raised when a tag name is already registered."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Tag name '{name}' is already registered")


class DuplicateTagLink(ConflictError):
    """Raised when the same tag is linked twice."""


class Tag(DomainModel):
    """Reusable resource tag."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set the tag name."""
        self.name = name


class TagLink(DomainModel):
    """Polymorphic tag-to-resource link."""

    id: uuid.UUID = Field(default_factory=uuid7)
    tag_id: uuid.UUID
    resource_type: TagResourceType
    resource_id: uuid.UUID
    created: datetime | None = None
    updated: datetime | None = None
