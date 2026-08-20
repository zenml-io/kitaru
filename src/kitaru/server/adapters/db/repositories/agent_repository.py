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
"""SQL agent repository."""

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.agent import AGENT_NAME_UNIQUE_CONSTRAINT, AgentORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.domain.agent import (
    Agent,
    AgentNotFound,
    DuplicateAgentName,
)
from kitaru.server.domain.base import NotFoundError

AGENT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": AgentORM.id,
    "name": AgentORM.name,
}


class SQLAgentRepository(BaseSQLRepository[AgentORM]):
    """Agent repository backed by the application database."""

    orm_class = AgentORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return AgentNotFound(entity_id)

    async def _get_row(self, entity_id: uuid.UUID, exclusive: bool = False) -> AgentORM:
        """Load a row by id, skipping rows marked deleted.

        Args:
            entity_id: Id of the row.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored row.
        """
        statement = select(AgentORM).where(
            AgentORM.id == entity_id, AgentORM.deleted_at.is_(None)
        )
        if exclusive:
            statement = statement.with_for_update()
        row = await self._session.scalar(statement)
        if row is None:
            raise self._not_found(entity_id)
        return row

    async def create(self, agent: Agent) -> Agent:
        """Persist a new agent.

        Args:
            agent: Agent to store.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with timestamps set.
        """
        row = AgentORM.from_domain(agent)
        await self._add(
            row, {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)}
        )
        return row.to_domain()

    async def get(self, agent_id: uuid.UUID) -> Agent:
        """Load an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored agent.
        """
        row = await self._get_row(agent_id)
        return row.to_domain()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], str | None]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the next cursor.
        """
        statement = select(AgentORM).where(AgentORM.deleted_at.is_(None))
        if agent_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    agent_filter.expression, AGENT_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, agent_filter, id_column=AgentORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, agent: Agent) -> Agent:
        """Persist changes to an existing agent.

        Args:
            agent: Agent with modified fields.

        Raises:
            AgentNotFound: No agent has this id.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with the updated timestamp renewed.
        """
        row = await self._get_row(agent.id)
        row.owner_id = agent.owner_id
        row.name = agent.name
        row.description = agent.description
        row.latest_version = agent.latest_version
        await self._flush(
            {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)}
        )
        return row.to_domain()

    async def mark_deleted(self, agent_id: uuid.UUID) -> None:
        """Mark an agent deleted, hiding it from every read.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
        """
        row = await self._get_row(agent_id)
        row.deleted_at = datetime.now(UTC)
        await self._flush()

    async def list_marked_deleted(self, cutoff: datetime, limit: int) -> list[Agent]:
        """List agents marked deleted before a cutoff, up to a limit.

        Args:
            cutoff: Rows marked deleted before this time are returned.
            limit: Maximum number of rows to return.

        Returns:
            Agents marked deleted before the cutoff.
        """
        statement = (
            select(AgentORM)
            .where(AgentORM.deleted_at < cutoff)
            .order_by(AgentORM.id)
            .limit(limit)
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
        """
        await self._delete_row(agent_id)
