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
"""Tag repository interface."""

import uuid
from typing import Protocol

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.tag import Tag, TagLink


class TagRepository(Protocol):
    """Tag persistence operations."""

    async def create(self, tag: Tag) -> Tag:
        """Persist a new tag.

        Args:
            tag: Tag to store.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with timestamps set.
        """
        ...

    async def get(self, tag_id: uuid.UUID) -> Tag:
        """Load a tag by id.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.

        Returns:
            Stored tag.
        """
        ...

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the next cursor.
        """
        ...

    async def update(self, tag: Tag) -> Tag:
        """Persist changes to an existing tag.

        Args:
            tag: Tag with modified fields.

        Raises:
            TagNotFound: No tag has this id.
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with the updated timestamp renewed.
        """
        ...

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag and its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        ...

    async def create_link(self, link: TagLink) -> TagLink:
        """Persist a new tag link.

        Args:
            link: Tag link to store.

        Raises:
            TagNotFound: No tag has the link's tag id.
            DuplicateTagLink: The tag is already linked to the resource.

        Returns:
            Stored tag link with timestamps set.
        """
        ...

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Delete a tag link by tag and resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            TagLinkNotFound: No link matches the tag and resource.
        """
        ...
