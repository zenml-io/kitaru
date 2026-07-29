#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Agent use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.models.agent import AgentFilter, AgentUpdate
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.base import ValidationError


class AgentService:
    """Agent use cases."""

    def __init__(self, repository: AgentRepository) -> None:
        self._repository = repository

    async def create_agent(
        self, name: str, description: str | None, actor: AuthContext
    ) -> Agent:
        """Create an agent."""
        return await self._repository.create(
            Agent(
                owner_id=actor.account.id,
                name=name,
                description=description,
            )
        )

    async def get_agent(self, agent_id: uuid.UUID, actor: AuthContext) -> Agent:
        """Get an agent."""
        _ = actor
        return await self._repository.get(agent_id)

    async def list_agents(
        self, agent_filter: AgentFilter, actor: AuthContext
    ) -> tuple[list[Agent], str | None]:
        """List agents."""
        _ = actor
        return await self._repository.query(agent_filter)

    async def update_agent(
        self,
        agent_id: uuid.UUID,
        command: AgentUpdate,
        actor: AuthContext,
    ) -> Agent:
        """Partially update an agent."""
        _ = actor
        agent = await self._repository.get(agent_id)
        if "name" in command.model_fields_set:
            if command.name is None:
                raise ValidationError("Agent name cannot be null")
            agent.update_name(command.name)
        if "description" in command.model_fields_set:
            agent.update_description(command.description)
        return await self._repository.update(agent)

    async def delete_agent(self, agent_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an agent."""
        _ = actor
        await self._repository.get(agent_id)
        await self._repository.delete(agent_id)
