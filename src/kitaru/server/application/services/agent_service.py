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

from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.models.agent import (
    AgentCreate,
    AgentFilter,
    AgentUpdate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.agent import Agent


class AgentService:
    """Agent use cases."""

    def __init__(
        self, repository: AgentRepository, analytics: ServerAnalytics | None = None
    ) -> None:
        """Initialize the service.

        Args:
            repository: Agent repository.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._analytics = analytics

    async def create_agent(self, command: AgentCreate, actor: AuthContext) -> Agent:
        """Create an agent owned by the caller.

        Args:
            command: Fields for the new agent.
            actor: Caller context.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Created agent.
        """
        agent = Agent(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
        )
        agent = await self._repository.create(agent)
        if self._analytics is not None:
            self._analytics.track(actor.account.id, AnalyticsEvent.AGENT_CREATED)
        return agent

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
    ) -> tuple[list[Agent], str | None]:
        """List agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching agents and the next cursor.
        """
        _ = actor
        return await self._repository.query(agent_filter)

    async def update_agent(
        self, agent_id: uuid.UUID, command: AgentUpdate, actor: AuthContext
    ) -> Agent:
        """Partially update an agent.

        Args:
            agent_id: Id of the agent.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
            ValidationError: The command clears the agent name.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Updated agent.
        """
        _ = actor
        agent = await self._repository.get(agent_id)
        fields = command.model_fields_set
        if "name" in fields:
            agent.update_name(command.name)
        if "description" in fields:
            agent.update_description(command.description)
        return await self._repository.update(agent)

    async def delete_agent(self, agent_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an agent, hiding it and everything under it.

        Args:
            agent_id: Id of the agent.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
        """
        _ = actor
        await self._repository.mark_deleted(agent_id)
