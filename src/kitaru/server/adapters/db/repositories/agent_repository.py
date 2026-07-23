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

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.agent import (
    AGENT_NAME_UNIQUE_CONSTRAINT,
    AgentSchema,
)
from kitaru.server.adapters.db.schemas.agent_version import (
    AGENT_VERSION_AGENT_ID_FOREIGN_KEY,
)
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.domain.agent import (
    Agent,
    AgentInUse,
    AgentNotFound,
    DuplicateAgentName,
)


class SQLAgentRepository:
    """Agent repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, agent: Agent) -> Agent:
        """Persist a new agent.

        Args:
            agent: Agent to store.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with timestamps set.
        """
        row = AgentSchema.from_domain(agent)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == AGENT_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateAgentName(agent.name) from exc
            raise
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
        row = await self._session.get(AgentSchema, agent_id)
        if row is None:
            raise AgentNotFound(agent_id)
        return row.to_domain()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], int]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the total match count.
        """
        statement = select(AgentSchema)
        if agent_filter.name is not None:
            statement = statement.where(col(AgentSchema.name) == agent_filter.name)
        if agent_filter.owner_id is not None:
            statement = statement.where(
                col(AgentSchema.owner_id) == agent_filter.owner_id
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(AgentSchema.id),
            page=agent_filter.page,
            page_size=agent_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

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
        row = await self._session.get(AgentSchema, agent.id)
        if row is None:
            raise AgentNotFound(agent.id)
        row.owner_id = agent.owner_id
        row.name = agent.name
        row.description = agent.description
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == AGENT_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateAgentName(agent.name) from exc
            raise
        return row.to_domain()

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
            AgentInUse: The agent still has versions.
        """
        row = await self._session.get(AgentSchema, agent_id)
        if row is None:
            raise AgentNotFound(agent_id)
        try:
            async with self._session.begin_nested():
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == AGENT_VERSION_AGENT_ID_FOREIGN_KEY:
                raise AgentInUse(agent_id) from exc
            raise
