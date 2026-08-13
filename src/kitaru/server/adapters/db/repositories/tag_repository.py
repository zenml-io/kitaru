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
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.tag import (
    TAG_LINK_TAG_ID_FOREIGN_KEY,
    TAG_LINK_UNIQUE_CONSTRAINT,
    TAG_NAME_UNIQUE_CONSTRAINT,
    TagLinkORM,
    TagORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
)

TAG_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": TagORM.id,
    "name": TagORM.name,
}


class SQLTagRepository(BaseSQLRepository[TagORM]):
    """Tag repository backed by the application database."""

    orm_class = TagORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return TagNotFound(entity_id)

    async def create(self, tag: Tag) -> Tag:
        """Persist a new tag.

        Args:
            tag: Tag to store.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with timestamps set.
        """
        row = TagORM.from_domain(tag)
        await self._add(
            row, {TAG_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateTagName(tag.name)}
        )
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
        row = await self._get_row(tag_id)
        return row.to_domain()

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the next cursor.
        """
        statement = select(TagORM)
        if tag_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(tag_filter.expression, TAG_FILTER_BINDINGS)
            )
        rows, next_cursor = await paginate(
            self._session, statement, tag_filter, id_column=TagORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

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
        row = await self._get_row(tag.id)
        row.owner_id = tag.owner_id
        row.name = tag.name
        await self._flush(
            {TAG_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateTagName(tag.name)}
        )
        return row.to_domain()

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag and its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        await self._delete_row(tag_id)

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
        row = TagLinkORM.from_domain(link)
        await self._add(
            row,
            {
                TAG_LINK_UNIQUE_CONSTRAINT: lambda: DuplicateTagLink(
                    link.tag_id, link.resource_type, link.resource_id
                ),
                TAG_LINK_TAG_ID_FOREIGN_KEY: lambda: TagNotFound(link.tag_id),
            },
        )
        return row.to_domain()

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
        statement = select(TagLinkORM).where(
            TagLinkORM.tag_id == tag_id,
            TagLinkORM.resource_type == resource_type.value,
            TagLinkORM.resource_id == resource_id,
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise TagLinkNotFound(tag_id, resource_type, resource_id)
        await self._session.delete(row)
        await self._session.flush()
