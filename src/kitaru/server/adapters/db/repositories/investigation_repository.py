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
"""SQL investigation repository."""

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import func, select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.investigation import InvestigationORM
from kitaru.server.adapters.db.orm.investigation_session import InvestigationSessionORM
from kitaru.server.adapters.db.pagination import paginate, paginate_by_index
from kitaru.server.adapters.db.repositories.base import (
    EXCLUSIVE_ROW_LOCK,
    BaseSQLRepository,
)
from kitaru.server.application.models.investigation import (
    InvestigationFilter,
    InvestigationSessionFilter,
)
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationNotFound,
    InvestigationSession,
    InvestigationSessionNotFound,
)

INVESTIGATION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": InvestigationORM.id,
    "agent_id": InvestigationORM.agent_id,
    "status": InvestigationORM.status,
}

INVESTIGATION_SESSION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "verdict": InvestigationSessionORM.verdict,
}

SessionCounts = tuple[int, int]


class SQLInvestigationRepository(BaseSQLRepository[InvestigationORM]):
    """Investigation repository backed by the application database."""

    orm_class = InvestigationORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return InvestigationNotFound(entity_id)

    async def _load_session_counts(
        self, investigation_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, SessionCounts]:
        """Bulk-count linked sessions and non-null verdicts per investigation.

        Args:
            investigation_ids: Ids of the investigations.

        Returns:
            Session and verdict counts keyed by investigation id, missing
            ids omitted.
        """
        if not investigation_ids:
            return {}
        statement = (
            select(
                InvestigationSessionORM.investigation_id,
                func.count(InvestigationSessionORM.id),
                func.count(InvestigationSessionORM.verdict),
            )
            .where(InvestigationSessionORM.investigation_id.in_(investigation_ids))
            .group_by(InvestigationSessionORM.investigation_id)
        )
        rows = (await self._session.execute(statement)).all()
        return {
            investigation_id: (total, completed)
            for investigation_id, total, completed in rows
        }

    async def _to_domain(self, row: InvestigationORM) -> Investigation:
        """Build a domain investigation from a row, loading its counts.

        Args:
            row: Stored investigation row.

        Returns:
            Investigation with timestamps set.
        """
        counts = await self._load_session_counts([row.id])
        return row.to_domain(*counts.get(row.id, (0, 0)))

    async def _get_session_row(
        self, investigation_session_id: uuid.UUID, exclusive: bool = False
    ) -> InvestigationSessionORM:
        """Load an investigation session row by id.

        Args:
            investigation_session_id: Id of the investigation session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session row.
        """
        row = await self._session.get(
            InvestigationSessionORM,
            investigation_session_id,
            with_for_update=dict(EXCLUSIVE_ROW_LOCK) if exclusive else False,
        )
        if row is None:
            raise InvestigationSessionNotFound(investigation_session_id)
        return row

    async def create(
        self, investigation: Investigation, sessions: Sequence[InvestigationSession]
    ) -> Investigation:
        """Persist a new investigation with its linked sessions.

        Args:
            investigation: Investigation to store.
            sessions: Ordered investigation_session rows to link.

        Returns:
            Stored investigation with timestamps set.
        """
        row = InvestigationORM.from_domain(investigation)
        self._session.add(row)
        self._session.add_all(
            [InvestigationSessionORM.from_domain(session) for session in sessions]
        )
        await self._flush()
        completed = sum(1 for session in sessions if session.verdict is not None)
        return row.to_domain(len(sessions), completed)

    async def get(
        self, investigation_id: uuid.UUID, exclusive: bool = False
    ) -> Investigation:
        """Load an investigation by id.

        Args:
            investigation_id: Id of the investigation.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation.
        """
        row = await self._get_row(investigation_id, exclusive=exclusive)
        return await self._to_domain(row)

    async def query(
        self, investigation_filter: InvestigationFilter
    ) -> tuple[list[Investigation], str | None]:
        """Query investigations matching a filter.

        Args:
            investigation_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigations and the next cursor.
        """
        statement = select(InvestigationORM)
        if investigation_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    investigation_filter.expression, INVESTIGATION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session,
            statement,
            investigation_filter,
            id_column=InvestigationORM.id,
        )
        counts = await self._load_session_counts([row.id for row in rows])
        return [row.to_domain(*counts.get(row.id, (0, 0))) for row in rows], next_cursor

    async def update(self, investigation: Investigation) -> Investigation:
        """Persist changes to an existing investigation.

        Args:
            investigation: Investigation with modified fields.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation with the updated timestamp renewed.
        """
        row = await self._get_row(investigation.id)
        row.name = investigation.name
        row.description = investigation.description
        row.status = investigation.status.value
        row.started_at = investigation.started_at
        row.ended_at = investigation.ended_at
        await self._flush()
        return await self._to_domain(row)

    async def delete(self, investigation_id: uuid.UUID) -> None:
        """Delete an investigation by id, cascading its links and answers.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            InvestigationNotFound: No investigation has this id.
        """
        await self._delete_row(investigation_id)

    async def get_session(
        self, investigation_session_id: uuid.UUID, exclusive: bool = False
    ) -> InvestigationSession:
        """Load an investigation session by id.

        Args:
            investigation_session_id: Id of the investigation session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session.
        """
        row = await self._get_session_row(investigation_session_id, exclusive=exclusive)
        return row.to_domain()

    async def get_session_by_session_id(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        exclusive: bool = False,
    ) -> InvestigationSession:
        """Load an investigation session by investigation id and session id.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the linked session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            InvestigationSessionNotFound: No investigation session links this
                investigation and session.

        Returns:
            Stored investigation session.
        """
        statement = select(InvestigationSessionORM).where(
            InvestigationSessionORM.investigation_id == investigation_id,
            InvestigationSessionORM.session_id == session_id,
        )
        if exclusive:
            statement = statement.with_for_update(**EXCLUSIVE_ROW_LOCK)
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise InvestigationSessionNotFound(session_id)
        return row.to_domain()

    async def query_sessions(
        self, session_filter: InvestigationSessionFilter
    ) -> tuple[list[InvestigationSession], str | None]:
        """Query an investigation's sessions, ordered by position ascending.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigation sessions and the next cursor.
        """
        statement = select(InvestigationSessionORM).where(
            InvestigationSessionORM.investigation_id == session_filter.investigation_id
        )
        if session_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    session_filter.expression, INVESTIGATION_SESSION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate_by_index(
            self._session,
            statement,
            session_filter,
            index_column=InvestigationSessionORM.position,
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update_session(
        self, session: InvestigationSession
    ) -> InvestigationSession:
        """Persist changes to an existing investigation session.

        Args:
            session: Investigation session with modified fields.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session with the updated timestamp renewed.
        """
        row = await self._get_session_row(session.id)
        row.verdict = session.verdict.value if session.verdict is not None else None
        await self._flush()
        return row.to_domain()
