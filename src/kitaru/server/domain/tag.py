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
"""Tag and tag link entities and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class TagNotFound(NotFoundError):
    """Raised when a tag lookup does not resolve."""

    def __init__(self, tag_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            tag_id: Id of the missing tag.
        """
        super().__init__(f"Tag {tag_id} was not found")


class DuplicateTagName(ConflictError):
    """Raised when a tag name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Tag name '{name}' is already registered")


class TagLinkNotFound(NotFoundError):
    """Raised when a tag link lookup does not resolve."""

    def __init__(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Initialize the error.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.
        """
        super().__init__(
            f"Tag link {tag_id}/{resource_type}/{resource_id} was not found"
        )


class DuplicateTagLink(ConflictError):
    """Raised when a tag is already linked to a resource."""

    def __init__(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Initialize the error.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.
        """
        super().__init__(
            f"Tag {tag_id} is already linked to {resource_type} {resource_id}"
        )


class Tag(DomainModel):
    """Tag."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set a new tag name.

        Args:
            name: New name.
        """
        self.name = name


class TagLink(DomainModel):
    """Tag link."""

    id: uuid.UUID = Field(default_factory=uuid7)
    tag_id: uuid.UUID
    resource_type: TagResourceType
    resource_id: uuid.UUID
    created: datetime | None = None
    updated: datetime | None = None
