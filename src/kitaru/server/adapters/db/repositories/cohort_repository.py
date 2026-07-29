"""SQL cohort repository."""

import uuid

from sqlalchemy import select

from kitaru.server.adapters.db.orm.cohort import (
    COHORT_NAME_UNIQUE_CONSTRAINT,
    CohortORM,
    CohortSessionORM,
)
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.cohort import (
    Cohort,
    CohortNotFound,
    DuplicateCohortName,
)


class SQLCohortRepository(BaseSQLRepository[CohortORM]):
    """Cohort repository backed by PostgreSQL."""

    orm_class = CohortORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return CohortNotFound(entity_id)

    async def create(self, cohort: Cohort, session_ids: list[uuid.UUID]) -> Cohort:
        row = CohortORM.from_domain(cohort)
        await self._add(
            row,
            {COHORT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateCohortName(cohort.name)},
        )
        self._session.add_all(
            [
                CohortSessionORM(
                    cohort_id=cohort.id, session_id=session_id, index=index
                )
                for index, session_id in enumerate(session_ids)
            ]
        )
        await self._session.flush()
        return row.to_domain()

    async def get(self, cohort_id: uuid.UUID) -> Cohort:
        return (await self._get_row(cohort_id)).to_domain()

    async def get_session_ids(self, cohort_id: uuid.UUID) -> list[uuid.UUID]:
        await self._get_row(cohort_id)
        statement = (
            select(CohortSessionORM.session_id)
            .where(CohortSessionORM.cohort_id == cohort_id)
            .order_by(CohortSessionORM.index)
        )
        return list((await self._session.scalars(statement)).all())

    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]:
        statement = select(CohortORM)
        if cohort_filter.name is not None:
            statement = statement.where(CohortORM.name == cohort_filter.name)
        if cohort_filter.tag is not None:
            statement = (
                statement.join(
                    TagLinkORM,
                    (TagLinkORM.resource_id == CohortORM.id)
                    & (TagLinkORM.resource_type == "cohort"),
                )
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(TagORM.name == cohort_filter.tag)
            )
        rows, cursor = await paginate(
            self._session, statement, cohort_filter, CohortORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, cohort: Cohort) -> Cohort:
        row = await self._get_row(cohort.id)
        row.name = cohort.name
        row.description = cohort.description
        await self._flush(
            {COHORT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateCohortName(cohort.name)}
        )
        return row.to_domain()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        await self._delete_row(cohort_id)
