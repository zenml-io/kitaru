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
"""Agent use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
)
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.agent import Agent


class AgentService:
    """Agent use cases."""

    def __init__(self, repository: AgentRepository) -> None:
        """Initialize the service.

        Args:
            repository: Agent repository.
        """
        self._repository = repository

    async def create_agent(
        self,
        name: str,
        description: str | None,
        actor: AuthContext,
    ) -> Agent:
        """Create an agent owned by the caller.

        Args:
            name: Agent name.
            description: Agent description.
            actor: Caller context.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Created agent.
        """
        owner_id = actor.account.id
        agent = Agent(owner_id=owner_id, name=name, description=description)
        return await self._repository.create(agent)

    async def get_agent(self, agent_id: uuid.UUID, actor: AuthContext) -> Agent:
        """Get an agent by id.

        Args:
            agent_id: Id of the agent.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored agent.
        """
        _ = actor
        return await self._repository.get(agent_id)

    async def list_agents(
        self, agent_filter: AgentFilter, actor: AuthContext
    ) -> tuple[list[Agent], int]:
        """List agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching agents and the total match count.
        """
        _ = actor
        return await self._repository.query(agent_filter)

    async def update_agent(
        self,
        agent_id: uuid.UUID,
        name: str | None,
        description: str | None,
        actor: AuthContext,
    ) -> Agent:
        """Partially update an agent.

        Args:
            agent_id: Id of the agent.
            name: New agent name, unchanged when ``None``.
            description: New agent description, unchanged when ``None``.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Updated agent.
        """
        _ = actor
        agent = await self._repository.get(agent_id)
        if name is not None:
            agent.update_name(name)
        if description is not None:
            agent.update_description(description)
        return await self._repository.update(agent)

    async def delete_agent(self, agent_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an agent.

        Args:
            agent_id: Id of the agent.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
            AgentInUse: The agent still has versions.
        """
        _ = actor
        await self._repository.delete(agent_id)
