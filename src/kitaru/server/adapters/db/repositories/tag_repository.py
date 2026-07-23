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
"""SQL tag repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.tag import (
    TAG_LINK_UNIQUE_CONSTRAINT,
    TAG_NAME_UNIQUE_CONSTRAINT,
    TagLinkSchema,
    TagSchema,
)
from kitaru.server.application.models.tags import TagFilter
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
    TagResourceType,
)


class SQLTagRepository:
    """Tag repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, tag: Tag) -> Tag:
        """Persist a new tag.

        Args:
            tag: Tag to store.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with timestamps set.
        """
        row = TagSchema.from_domain(tag)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == TAG_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateTagName(tag.name) from exc
            raise
        return row.to_domain()

    async def get(self, tag_id: uuid.UUID) -> Tag:
        """Load a tag by id.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.

        Returns:
            Stored tag.
        """
        row = await self._session.get(TagSchema, tag_id)
        if row is None:
            raise TagNotFound(tag_id)
        return row.to_domain()

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], int]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the total match count.
        """
        statement = select(TagSchema)
        if tag_filter.name is not None:
            statement = statement.where(col(TagSchema.name) == tag_filter.name)
        if tag_filter.owner_id is not None:
            statement = statement.where(col(TagSchema.owner_id) == tag_filter.owner_id)
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(TagSchema.id),
            page=tag_filter.page,
            page_size=tag_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag by id, including its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        row = await self._session.get(TagSchema, tag_id)
        if row is None:
            raise TagNotFound(tag_id)
        await self._session.delete(row)
        await self._session.flush()

    async def create_link(self, link: TagLink) -> TagLink:
        """Persist a new tag link.

        Args:
            link: Tag link to store.

        Raises:
            DuplicateTagLink: The tag link is already registered.

        Returns:
            Stored tag link with timestamps set.
        """
        row = TagLinkSchema.from_domain(link)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == TAG_LINK_UNIQUE_CONSTRAINT:
                raise DuplicateTagLink(
                    link.tag_id, link.resource_type, link.resource_id
                ) from exc
            raise
        return row.to_domain()

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Delete a tag link.

        Args:
            tag_id: Id of the tag.
            resource_type: Type of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            TagLinkNotFound: No tag link matches.
        """
        statement = select(TagLinkSchema).where(
            col(TagLinkSchema.tag_id) == tag_id,
            col(TagLinkSchema.resource_type) == resource_type.value,
            col(TagLinkSchema.resource_id) == resource_id,
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise TagLinkNotFound(tag_id, resource_type, resource_id)
        await self._session.delete(row)
        await self._session.flush()
