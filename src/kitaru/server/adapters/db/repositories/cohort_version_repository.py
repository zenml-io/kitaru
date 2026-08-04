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
"""SQL cohort version repository."""

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import select, update

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.cohort import CohortORM
from kitaru.server.adapters.db.orm.cohort_version import CohortVersionORM
from kitaru.server.adapters.db.orm.cohort_version_session import (
    CohortVersionSessionORM,
)
from kitaru.server.adapters.db.orm.experiment_run import (
    EXPERIMENT_RUN_COHORT_VERSION_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.cohort import CohortVersionFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.cohort import CohortNotFound
from kitaru.server.domain.cohort_version import (
    CohortVersion,
    CohortVersionIdNotFound,
    CohortVersionInUse,
    CohortVersionNotFound,
)

COHORT_VERSION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "tag": build_tag_condition_binding(
        TagResourceType.COHORT_VERSION, CohortVersionORM.id
    ),
}


class SQLCohortVersionRepository(BaseSQLRepository[CohortVersionORM]):
    """Cohort version repository backed by the application database."""

    orm_class = CohortVersionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return CohortVersionIdNotFound(entity_id)

    async def create(
        self, version: CohortVersion, session_ids: Sequence[uuid.UUID]
    ) -> CohortVersion:
        """Persist a new cohort version with a server-assigned version number.

        Args:
            version: Cohort version to store.
            session_ids: Ordered member session ids to link.

        Raises:
            CohortNotFound: No cohort has the version's cohort id.

        Returns:
            Stored cohort version with its assigned version number and
            timestamps set.
        """
        result = await self._session.execute(
            update(CohortORM)
            .where(CohortORM.id == version.cohort_id)
            .values(latest_version=CohortORM.latest_version + 1)
            .returning(CohortORM.latest_version)
        )
        version_number = result.scalar_one_or_none()
        if version_number is None:
            raise CohortNotFound(version.cohort_id)
        row = CohortVersionORM.from_domain(version)
        row.version = version_number
        await self._add(row)
        for index, session_id in enumerate(session_ids):
            self._session.add(
                CohortVersionSessionORM(
                    cohort_version_id=row.id, session_id=session_id, index=index
                )
            )
        await self._flush()
        return row.to_domain()

    async def get(
        self, cohort_version_id: uuid.UUID, exclusive: bool = False
    ) -> CohortVersion:
        """Load a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version.
        """
        row = await self._get_row(cohort_version_id, exclusive=exclusive)
        return row.to_domain()

    async def get_by_number(self, cohort_id: uuid.UUID, version: int) -> CohortVersion:
        """Load a cohort version by cohort id and version number.

        Args:
            cohort_id: Id of the cohort.
            version: Version number.

        Raises:
            CohortVersionNotFound: No version with this number exists for
                this cohort.

        Returns:
            Stored cohort version.
        """
        statement = select(CohortVersionORM).where(
            CohortVersionORM.cohort_id == cohort_id, CohortVersionORM.version == version
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise CohortVersionNotFound(cohort_id, version)
        return row.to_domain()

    async def query(
        self, version_filter: CohortVersionFilter
    ) -> tuple[list[CohortVersion], str | None]:
        """Query cohort versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohort versions and the next cursor.
        """
        statement = select(CohortVersionORM).where(
            CohortVersionORM.cohort_id == version_filter.cohort_id
        )
        if version_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    version_filter.expression, COHORT_VERSION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, version_filter, id_column=CohortVersionORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def list_session_ids(self, cohort_version_id: uuid.UUID) -> list[uuid.UUID]:
        """List a version's member session ids, in order.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Ordered member session ids.
        """
        # Read the version row and its links in one statement, so a delete
        # committing between two separate reads cannot produce a silently
        # empty member list.
        statement = (
            select(CohortVersionORM.id, CohortVersionSessionORM.session_id)
            .outerjoin(
                CohortVersionSessionORM,
                CohortVersionSessionORM.cohort_version_id == CohortVersionORM.id,
            )
            .where(CohortVersionORM.id == cohort_version_id)
            .order_by(CohortVersionSessionORM.index)
        )
        rows = (await self._session.execute(statement)).all()
        if not rows:
            raise self._not_found(cohort_version_id)
        return [session_id for _, session_id in rows if session_id is not None]

    async def update(self, version: CohortVersion) -> CohortVersion:
        """Persist changes to an existing cohort version.

        Args:
            version: Cohort version with modified fields.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.

        Returns:
            Stored cohort version with the updated timestamp renewed.
        """
        row = await self._get_row(version.id)
        row.display_version = version.display_version
        await self._flush()
        return row.to_domain()

    async def delete(self, cohort_version_id: uuid.UUID) -> None:
        """Delete a cohort version by id, cascading its member links.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            CohortVersionIdNotFound: No cohort version has this id.
            CohortVersionInUse: An experiment run references this version.
        """
        await self._delete_row(
            cohort_version_id,
            {
                EXPERIMENT_RUN_COHORT_VERSION_ID_FOREIGN_KEY: lambda: (
                    CohortVersionInUse(cohort_version_id)
                )
            },
        )
