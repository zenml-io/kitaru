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
"""SQL cohort repository."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.orm.cohort import (
    COHORT_NAME_UNIQUE_CONSTRAINT,
    CohortORM,
)
from kitaru.server.adapters.db.orm.cohort_session import CohortSessionORM
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.pagination import paginate, paginate_join_by_index
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.cohort import CohortFilter, CohortSessionsFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.cohort import Cohort, CohortNotFound, DuplicateCohortName
from kitaru.server.domain.session import Session


class SQLCohortRepository(BaseSQLRepository[CohortORM]):
    """Cohort repository backed by the application database."""

    orm_class = CohortORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return CohortNotFound(entity_id)

    async def create(self, cohort: Cohort, session_ids: Sequence[uuid.UUID]) -> Cohort:
        """Persist a new cohort with its fixed member sessions, in order.

        Args:
            cohort: Cohort to store.
            session_ids: Ordered member session ids, immutable afterward.

        Raises:
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with timestamps set.
        """
        row = CohortORM.from_domain(cohort)
        await self._add(
            row,
            {COHORT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateCohortName(cohort.name)},
        )
        for index, session_id in enumerate(session_ids):
            self._session.add(
                CohortSessionORM(cohort_id=row.id, session_id=session_id, index=index)
            )
        await self._flush()
        return row.to_domain()

    async def get(self, cohort_id: uuid.UUID) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        row = await self._get_row(cohort_id)
        return row.to_domain()

    async def query(
        self, cohort_filter: CohortFilter
    ) -> tuple[list[Cohort], str | None]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the next cursor.
        """
        statement = select(CohortORM)
        if cohort_filter.name is not None:
            statement = statement.where(CohortORM.name == cohort_filter.name)
        if cohort_filter.tag is not None:
            tag_exists = (
                select(TagLinkORM.id)
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(
                    TagLinkORM.resource_type == TagResourceType.COHORT.value,
                    TagLinkORM.resource_id == CohortORM.id,
                    TagORM.name == cohort_filter.tag,
                )
                .correlate(CohortORM)
            )
            statement = statement.where(tag_exists.exists())
        rows, next_cursor = await paginate(
            self._session, statement, cohort_filter, id_column=CohortORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, cohort: Cohort) -> Cohort:
        """Persist changes to an existing cohort.

        Args:
            cohort: Cohort with modified fields.

        Raises:
            CohortNotFound: No cohort has this id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Stored cohort with the updated timestamp renewed.
        """
        row = await self._get_row(cohort.id)
        row.owner_id = cohort.owner_id
        row.name = cohort.name
        row.description = cohort.description
        row.agent_id = cohort.agent_id
        row.session_count = cohort.session_count
        await self._flush(
            {COHORT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateCohortName(cohort.name)}
        )
        return row.to_domain()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id.

        Deleting a cohort cascades its member links.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        await self._delete_row(cohort_id)

    async def list_sessions(
        self, sessions_filter: CohortSessionsFilter
    ) -> tuple[list[Session], str | None]:
        """List a cohort's member sessions in cohort order.

        Args:
            sessions_filter: Filter and pagination parameters.

        Returns:
            Page of member sessions and the next cursor.
        """
        statement = (
            select(SessionORM, CohortSessionORM.index)
            .join(CohortSessionORM, CohortSessionORM.session_id == SessionORM.id)
            .where(CohortSessionORM.cohort_id == sessions_filter.cohort_id)
        )
        rows, next_cursor = await paginate_join_by_index(
            self._session,
            statement,
            sessions_filter,
            index_column=CohortSessionORM.index,
        )
        return [row.to_domain() for row in rows], next_cursor
