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
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.cohort import (
    COHORT_NAME_UNIQUE_CONSTRAINT,
    CohortORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.cohort import Cohort, CohortNotFound, DuplicateCohortName

COHORT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "agent_id": CohortORM.agent_id,
    "name": CohortORM.name,
    "tag": build_tag_condition_binding(TagResourceType.COHORT, CohortORM.id),
}


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

    async def create(self, cohort: Cohort) -> Cohort:
        """Persist a new cohort.

        Args:
            cohort: Cohort to store.

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
        return row.to_domain()

    async def get(self, cohort_id: uuid.UUID, exclusive: bool = False) -> Cohort:
        """Load a cohort by id.

        Args:
            cohort_id: Id of the cohort.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        row = await self._session.get(
            self.orm_class, cohort_id, with_for_update=exclusive
        )
        if row is None:
            raise CohortNotFound(cohort_id)
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
        if cohort_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    cohort_filter.expression, COHORT_FILTER_BINDINGS
                )
            )
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
        row.metadata_ = cohort.metadata
        row.latest_version = cohort.latest_version
        await self._flush(
            {COHORT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateCohortName(cohort.name)}
        )
        return row.to_domain()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id.

        Deleting a cohort cascades its versions.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        await self._delete_row(cohort_id)
