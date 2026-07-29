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
"""SQL session repository."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import CursorResult, func, select, update

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.orm.cohort_session import (
    COHORT_SESSION_SESSION_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.session import (
    SESSION_PROVIDER_EXTERNAL_ID_UNIQUE_CONSTRAINT,
    SessionORM,
)
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.base import NotFoundError, ValidationError
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionNotFound,
    SessionRollups,
)


class SQLSessionRepository(BaseSQLRepository[SessionORM]):
    """Session repository backed by the application database."""

    orm_class = SessionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return SessionNotFound(entity_id)

    def _duplicate_external_id(self, session: Session) -> DuplicateSessionExternalId:
        """Build the conflict error for a duplicated provider and external id.

        Args:
            session: Session whose provider and external id collided.

        Returns:
            Conflict error.
        """
        return DuplicateSessionExternalId(session.provider, session.external_id)

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        row = SessionORM.from_domain(session)
        await self._add(
            row,
            {
                SESSION_PROVIDER_EXTERNAL_ID_UNIQUE_CONSTRAINT: lambda: (
                    self._duplicate_external_id(session)
                )
            },
        )
        return row.to_domain()

    async def get(self, session_id: uuid.UUID, exclusive: bool = False) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        row = await self._session.get(
            self.orm_class, session_id, with_for_update=exclusive
        )
        if row is None:
            raise SessionNotFound(session_id)
        return row.to_domain()

    async def query(
        self, session_filter: SessionFilter
    ) -> tuple[list[Session], str | None]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Raises:
            ValidationError: ``has_evaluation`` is set. Evaluation rows do
                not exist yet in this branch.

        Returns:
            Page of matching sessions and the next cursor.
        """
        if session_filter.has_evaluation is not None:
            raise ValidationError("has_evaluation filtering is not available yet")

        statement = select(SessionORM)
        if session_filter.agent_id is not None:
            statement = statement.where(SessionORM.agent_id == session_filter.agent_id)
        if session_filter.agent_version_id is not None:
            statement = statement.where(
                SessionORM.agent_version_id == session_filter.agent_version_id
            )
        if session_filter.task_id is not None:
            statement = statement.where(SessionORM.task_id == session_filter.task_id)
        if session_filter.origin is not None:
            statement = statement.where(
                SessionORM.origin == session_filter.origin.value
            )
        if session_filter.status is not None:
            statement = statement.where(
                SessionORM.status == session_filter.status.value
            )
        if session_filter.provider is not None:
            statement = statement.where(SessionORM.provider == session_filter.provider)
        if session_filter.external_id is not None:
            statement = statement.where(
                SessionORM.external_id == session_filter.external_id
            )
        if session_filter.name is not None:
            statement = statement.where(SessionORM.name == session_filter.name)
        if session_filter.tag is not None:
            tag_exists = (
                select(TagLinkORM.id)
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(
                    TagLinkORM.resource_type == TagResourceType.SESSION.value,
                    TagLinkORM.resource_id == SessionORM.id,
                    TagORM.name == session_filter.tag,
                )
                .correlate(SessionORM)
            )
            statement = statement.where(tag_exists.exists())
        if session_filter.started_after is not None:
            statement = statement.where(
                SessionORM.started_at >= session_filter.started_after
            )
        if session_filter.started_before is not None:
            statement = statement.where(
                SessionORM.started_at <= session_filter.started_before
            )
        if session_filter.ended_after is not None:
            statement = statement.where(
                SessionORM.ended_at >= session_filter.ended_after
            )
        if session_filter.ended_before is not None:
            statement = statement.where(
                SessionORM.ended_at <= session_filter.ended_before
            )
        if session_filter.min_cost is not None:
            statement = statement.where(SessionORM.cost >= session_filter.min_cost)
        if session_filter.max_cost is not None:
            statement = statement.where(SessionORM.cost <= session_filter.max_cost)

        rows, next_cursor = await paginate(
            self._session, statement, session_filter, id_column=SessionORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def get_many(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Session]:
        """Bulk-load sessions by id, keyed by id, missing ids omitted.

        Args:
            session_ids: Ids of the sessions to load.

        Returns:
            Stored sessions keyed by id.
        """
        rows = await self._load_by_ids(list(session_ids))
        return {session_id: row.to_domain() for session_id, row in rows.items()}

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with the updated timestamp renewed.
        """
        row = await self._get_row(session.id)
        tokens = session.tokens
        row.owner_id = session.owner_id
        row.agent_id = session.agent_id
        row.agent_version_id = session.agent_version_id
        row.task_id = session.task_id
        row.origin = session.origin.value
        row.status = session.status.value
        row.name = session.name
        row.inputs = session.inputs
        row.outputs = session.outputs
        row.expected = session.expected
        row.error = session.error
        row.started_at = session.started_at
        row.ended_at = session.ended_at
        row.external_id = session.external_id
        row.metadata_ = session.metadata
        row.provider = session.provider
        row.framework = session.framework
        row.adapter_version = session.adapter_version
        row.cost = session.cost
        row.input_tokens = tokens.input_tokens if tokens is not None else None
        row.output_tokens = tokens.output_tokens if tokens is not None else None
        row.cached_input_tokens = (
            tokens.cached_input_tokens if tokens is not None else None
        )
        row.reasoning_tokens = tokens.reasoning_tokens if tokens is not None else None
        row.llm_call_count = session.llm_call_count
        row.tool_call_count = session.tool_call_count
        await self._flush(
            {
                SESSION_PROVIDER_EXTERNAL_ID_UNIQUE_CONSTRAINT: lambda: (
                    self._duplicate_external_id(session)
                )
            }
        )
        return row.to_domain()

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id.

        Deleting a session cascades its nodes.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session belongs to a cohort and cannot be
                deleted.
        """
        await self._delete_row(
            session_id,
            {COHORT_SESSION_SESSION_ID_FOREIGN_KEY: lambda: SessionInUse(session_id)},
        )

    async def apply_rollups(
        self, session_id: uuid.UUID, deltas: SessionRollups
    ) -> None:
        """Apply rollup deltas to a session's cost, tokens, and call counts.

        This is a Core-level bulk statement rather than an ORM attribute
        mutation, so the ``updated`` timestamp needs to be stamped here
        explicitly, the ``onupdate`` client-side default never fires for it.

        Args:
            session_id: Id of the session.
            deltas: Rollup deltas to add.

        Raises:
            SessionNotFound: No session has this id.
        """
        statement = (
            update(SessionORM)
            .where(SessionORM.id == session_id)
            .values(
                cost=func.coalesce(SessionORM.cost, 0) + deltas.cost,
                input_tokens=func.coalesce(SessionORM.input_tokens, 0)
                + deltas.input_tokens,
                output_tokens=func.coalesce(SessionORM.output_tokens, 0)
                + deltas.output_tokens,
                cached_input_tokens=func.coalesce(SessionORM.cached_input_tokens, 0)
                + deltas.cached_input_tokens,
                reasoning_tokens=func.coalesce(SessionORM.reasoning_tokens, 0)
                + deltas.reasoning_tokens,
                llm_call_count=SessionORM.llm_call_count + deltas.llm_call_count,
                tool_call_count=SessionORM.tool_call_count + deltas.tool_call_count,
                updated=datetime.now(UTC),
            )
        )
        result = await self._session.execute(statement)
        rowcount = result.rowcount if isinstance(result, CursorResult) else 0
        if rowcount == 0:
            raise SessionNotFound(session_id)
        await self._session.flush()
