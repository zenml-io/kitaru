"""SQL tag repository."""

import uuid

from sqlalchemy import delete, select

from kitaru.server.adapters.db.orm.tag import (
    TAG_LINK_UNIQUE_CONSTRAINT,
    TAG_NAME_UNIQUE_CONSTRAINT,
    TagLinkORM,
    TagORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.base import ConflictError, NotFoundError
from kitaru.server.domain.tag import (
    DuplicateTagName,
    Tag,
    TagLink,
    TagNotFound,
    TagResourceType,
)


class SQLTagRepository(BaseSQLRepository[TagORM]):
    """Tag repository backed by PostgreSQL."""

    orm_class = TagORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return TagNotFound(entity_id)

    async def create(self, tag: Tag) -> Tag:
        row = TagORM.from_domain(tag)
        await self._add(
            row,
            {TAG_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateTagName(tag.name)},
        )
        return row.to_domain()

    async def get(self, tag_id: uuid.UUID) -> Tag:
        return (await self._get_row(tag_id)).to_domain()

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]:
        statement = select(TagORM)
        if tag_filter.name is not None:
            statement = statement.where(TagORM.name == tag_filter.name)
        rows, cursor = await paginate(self._session, statement, tag_filter, TagORM.id)
        return [row.to_domain() for row in rows], cursor

    async def update(self, tag: Tag) -> Tag:
        row = await self._get_row(tag.id)
        row.name = tag.name
        await self._flush(
            {TAG_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateTagName(tag.name)}
        )
        return row.to_domain()

    async def delete(self, tag_id: uuid.UUID) -> None:
        await self._delete_row(tag_id)

    async def create_link(self, link: TagLink) -> TagLink:
        row = TagLinkORM.from_domain(link)
        self._session.add(row)
        await self._flush(
            {
                TAG_LINK_UNIQUE_CONSTRAINT: lambda: ConflictError(
                    "Tag link already exists"
                )
            },
        )
        return row.to_domain()

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        result = await self._session.execute(
            delete(TagLinkORM).where(
                TagLinkORM.tag_id == tag_id,
                TagLinkORM.resource_type == resource_type.value,
                TagLinkORM.resource_id == resource_id,
            )
        )
        if result.rowcount == 0:  # ty: ignore[unresolved-attribute]
            raise TagNotFound(tag_id)
