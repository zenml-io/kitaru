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

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.cohort import (
    COHORT_AGENT_ID_FOREIGN_KEY,
    COHORT_NAME_UNIQUE_CONSTRAINT,
    CohortSchema,
    CohortSessionSchema,
)
from kitaru.server.adapters.db.schemas.session import SessionSchema
from kitaru.server.adapters.db.schemas.tag import TagLinkSchema
from kitaru.server.adapters.db.tag_filtering import tagged_resource_ids
from kitaru.server.application.models.cohorts import (
    CohortFilter,
    CohortSessionsFilter,
)
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.cohort import (
    Cohort,
    CohortNotFound,
    DuplicateCohortName,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.tag import TagResourceType


class SQLCohortRepository:
    """Cohort repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, cohort: Cohort, session_ids: list[uuid.UUID]) -> Cohort:
        """Persist a new cohort with its ordered membership.

        Args:
            cohort: Cohort to store.
            session_ids: Ids of the member sessions, in position order.

        Raises:
            DuplicateCohortName: The cohort name is already registered.
            AgentNotFound: No agent has the cohort's agent id.

        Returns:
            Stored cohort with timestamps set.
        """
        row = CohortSchema.from_domain(cohort)
        members = [
            CohortSessionSchema(
                cohort_id=cohort.id, session_id=session_id, position=position
            )
            for position, session_id in enumerate(session_ids)
        ]
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                self._session.add_all(members)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == COHORT_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateCohortName(cohort.name) from exc
            if constraint == COHORT_AGENT_ID_FOREIGN_KEY:
                raise AgentNotFound(cohort.agent_id) from exc
            raise
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
        row = await self._session.get(CohortSchema, cohort_id)
        if row is None:
            raise CohortNotFound(cohort_id)
        return row.to_domain()

    async def query(self, cohort_filter: CohortFilter) -> tuple[list[Cohort], int]:
        """Query cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.

        Returns:
            Page of matching cohorts and the total match count.
        """
        statement = select(CohortSchema)
        if cohort_filter.name is not None:
            statement = statement.where(col(CohortSchema.name) == cohort_filter.name)
        if cohort_filter.tag is not None:
            statement = statement.where(
                col(CohortSchema.id).in_(
                    tagged_resource_ids(cohort_filter.tag, TagResourceType.COHORT)
                )
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(CohortSchema.id),
            page=cohort_filter.page,
            page_size=cohort_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def query_sessions(
        self, cohort_id: uuid.UUID, sessions_filter: CohortSessionsFilter
    ) -> tuple[list[Session], int]:
        """Query the member sessions of a cohort ordered by position.

        Args:
            cohort_id: Id of the cohort.
            sessions_filter: Pagination parameters.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Page of member sessions and the total member count.
        """
        cohort_row = await self._session.get(CohortSchema, cohort_id)
        if cohort_row is None:
            raise CohortNotFound(cohort_id)
        statement = (
            select(SessionSchema)
            .join(
                CohortSessionSchema,
                col(CohortSessionSchema.session_id) == col(SessionSchema.id),
            )
            .where(col(CohortSessionSchema.cohort_id) == cohort_id)
        )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(CohortSessionSchema.position),
            page=sessions_filter.page,
            page_size=sessions_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

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
        row = await self._session.get(CohortSchema, cohort.id)
        if row is None:
            raise CohortNotFound(cohort.id)
        row.owner_id = cohort.owner_id
        row.name = cohort.name
        row.description = cohort.description
        row.agent_id = cohort.agent_id
        row.session_count = cohort.session_count
        row.filter_snapshot = cohort.filter_snapshot
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == COHORT_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateCohortName(cohort.name) from exc
            raise
        return row.to_domain()

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort by id, including its membership and tag links.

        Membership cascades through the database, tag links carry no foreign
        key and are removed here.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        row = await self._session.get(CohortSchema, cohort_id)
        if row is None:
            raise CohortNotFound(cohort_id)
        await self._session.execute(
            delete(TagLinkSchema).where(
                col(TagLinkSchema.resource_type) == TagResourceType.COHORT.value,
                col(TagLinkSchema.resource_id) == cohort_id,
            )
        )
        await self._session.delete(row)
        await self._session.flush()
