"""Tag repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType


class TagRepository(Protocol):
    """Tag and tag-link persistence operations."""

    async def create(self, tag: Tag) -> Tag: ...
    async def get(self, tag_id: uuid.UUID) -> Tag: ...
    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]: ...
    async def update(self, tag: Tag) -> Tag: ...
    async def delete(self, tag_id: uuid.UUID) -> None: ...
    async def create_link(self, link: TagLink) -> TagLink: ...
    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None: ...
