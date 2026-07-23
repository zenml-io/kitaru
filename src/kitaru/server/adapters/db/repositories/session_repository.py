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

from sqlalchemy import Select, delete, func, literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.session import (
    SESSION_AGENT_ID_FOREIGN_KEY,
    SESSION_AGENT_VERSION_ID_FOREIGN_KEY,
    SESSION_EXTERNAL_ID_UNIQUE_CONSTRAINT,
    SessionSchema,
)
from kitaru.server.adapters.db.schemas.tag import TagLinkSchema, TagSchema
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionNotFound,
)
from kitaru.server.domain.tag import TagResourceType


class SQLSessionRepository:
    """Session repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    def _translate_integrity_error(self, exc: IntegrityError, session: Session) -> None:
        """Translate an integrity error into the matching domain error.

        Args:
            exc: Integrity error raised by a flush.
            session: Session that was written.

        Raises:
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.
            AgentNotFound: No agent has the session's agent id.
            AgentVersionNotFound: No agent version has the session's agent
                version id.
        """
        constraint = violated_constraint(exc)
        if constraint == SESSION_EXTERNAL_ID_UNIQUE_CONSTRAINT:
            assert session.provider is not None
            assert session.external_id is not None
            raise DuplicateSessionExternalId(
                session.provider, session.external_id
            ) from exc
        if constraint == SESSION_AGENT_ID_FOREIGN_KEY:
            raise AgentNotFound(session.agent_id) from exc
        if constraint == SESSION_AGENT_VERSION_ID_FOREIGN_KEY:
            assert session.agent_version_id is not None
            raise AgentVersionNotFound(session.agent_version_id) from exc

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            AgentNotFound: No agent has the session's agent id.
            AgentVersionNotFound: No agent version has the session's agent
                version id.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set.
        """
        row = SessionSchema.from_domain(session)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            self._translate_integrity_error(exc, session)
            raise
        return row.to_domain()

    async def get(self, session_id: uuid.UUID) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        row = await self._session.get(SessionSchema, session_id)
        if row is None:
            raise SessionNotFound(session_id)
        return row.to_domain()

    def _apply_filter(
        self,
        statement: Select[tuple[SessionSchema]],
        session_filter: SessionFilter,
    ) -> Select[tuple[SessionSchema]]:
        """Apply filter conditions to a session select.

        Args:
            statement: Select to filter.
            session_filter: Filter parameters.

        Returns:
            Filtered select.
        """
        if session_filter.agent_id is not None:
            statement = statement.where(
                col(SessionSchema.agent_id) == session_filter.agent_id
            )
        if session_filter.agent_version_id is not None:
            statement = statement.where(
                col(SessionSchema.agent_version_id) == session_filter.agent_version_id
            )
        if session_filter.origin is not None:
            statement = statement.where(
                col(SessionSchema.origin) == session_filter.origin.value
            )
        if session_filter.status is not None:
            statement = statement.where(
                col(SessionSchema.status) == session_filter.status.value
            )
        if session_filter.provider is not None:
            statement = statement.where(
                col(SessionSchema.provider) == session_filter.provider.value
            )
        if session_filter.external_id is not None:
            statement = statement.where(
                col(SessionSchema.external_id) == session_filter.external_id
            )
        if session_filter.name is not None:
            statement = statement.where(col(SessionSchema.name) == session_filter.name)
        if session_filter.tag is not None:
            tagged_ids = (
                select(col(TagLinkSchema.resource_id))
                .join(TagSchema, col(TagLinkSchema.tag_id) == col(TagSchema.id))
                .where(
                    col(TagSchema.name) == session_filter.tag,
                    col(TagLinkSchema.resource_type) == TagResourceType.SESSION.value,
                )
            )
            statement = statement.where(col(SessionSchema.id).in_(tagged_ids))
        if session_filter.started_after is not None:
            statement = statement.where(
                col(SessionSchema.started_at) >= session_filter.started_after
            )
        if session_filter.started_before is not None:
            statement = statement.where(
                col(SessionSchema.started_at) <= session_filter.started_before
            )
        if session_filter.ended_after is not None:
            statement = statement.where(
                col(SessionSchema.ended_at) >= session_filter.ended_after
            )
        if session_filter.ended_before is not None:
            statement = statement.where(
                col(SessionSchema.ended_at) <= session_filter.ended_before
            )
        if session_filter.has_score is not None:
            empty_scores = literal({}, JSONB)
            if session_filter.has_score:
                statement = statement.where(col(SessionSchema.scores) != empty_scores)
            else:
                statement = statement.where(col(SessionSchema.scores) == empty_scores)
        if session_filter.min_cost is not None:
            statement = statement.where(
                col(SessionSchema.cost) >= session_filter.min_cost
            )
        if session_filter.max_cost is not None:
            statement = statement.where(
                col(SessionSchema.cost) <= session_filter.max_cost
            )
        total_tokens = (
            func.coalesce(col(SessionSchema.input_tokens), 0)
            + func.coalesce(col(SessionSchema.output_tokens), 0)
            + func.coalesce(col(SessionSchema.cached_input_tokens), 0)
            + func.coalesce(col(SessionSchema.reasoning_tokens), 0)
        )
        if session_filter.min_total_tokens is not None:
            statement = statement.where(total_tokens >= session_filter.min_total_tokens)
        if session_filter.max_total_tokens is not None:
            statement = statement.where(total_tokens <= session_filter.max_total_tokens)
        return statement

    async def query(self, session_filter: SessionFilter) -> tuple[list[Session], int]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching sessions and the total match count.
        """
        statement = self._apply_filter(select(SessionSchema), session_filter)
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(SessionSchema.id),
            page=session_filter.page,
            page_size=session_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

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
        row = await self._session.get(SessionSchema, session.id)
        if row is None:
            raise SessionNotFound(session.id)
        tokens = session.tokens
        row.owner_id = session.owner_id
        row.agent_id = session.agent_id
        row.agent_version_id = session.agent_version_id
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
        row.provider = session.provider.value if session.provider else None
        row.framework = session.framework
        row.adapter_version = session.adapter_version
        row.log_uri = session.log_uri
        row.cost = session.cost
        row.input_tokens = tokens.input_tokens if tokens else None
        row.output_tokens = tokens.output_tokens if tokens else None
        row.cached_input_tokens = tokens.cached_input_tokens if tokens else None
        row.reasoning_tokens = tokens.reasoning_tokens if tokens else None
        row.scores = session.scores
        row.llm_call_count = session.llm_call_count
        row.tool_call_count = session.tool_call_count
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            self._translate_integrity_error(exc, session)
            raise
        return row.to_domain()

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id, including its nodes and tag links.

        Nodes cascade through the database, tag links carry no foreign key
        and are removed here.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
        """
        row = await self._session.get(SessionSchema, session_id)
        if row is None:
            raise SessionNotFound(session_id)
        await self._session.execute(
            delete(TagLinkSchema).where(
                col(TagLinkSchema.resource_type) == TagResourceType.SESSION.value,
                col(TagLinkSchema.resource_id) == session_id,
            )
        )
        await self._session.delete(row)
        await self._session.flush()
