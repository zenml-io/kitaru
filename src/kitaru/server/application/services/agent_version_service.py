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
"""Agent version use cases."""

import uuid

from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.models.agent_version import (
    AgentVersionFilter,
    AgentVersionUpdate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)


class AgentVersionService:
    """Agent version use cases."""

    def __init__(self, repository: AgentVersionRepository) -> None:
        """Initialize the service.

        Args:
            repository: Agent version repository.
        """
        self._repository = repository

    async def create_version(
        self,
        agent_id: uuid.UUID,
        display_version: str | None,
        description: str | None,
        run_spec: RunSpec | None,
        capabilities: AgentCapabilities | None,
        actor: AuthContext,
    ) -> AgentVersion:
        """Create a new version of an agent owned by the caller.

        Args:
            agent_id: Id of the agent this version belongs to.
            display_version: Human-readable designator.
            description: Version description.
            run_spec: Run spec.
            capabilities: Agent capabilities, empty when omitted.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Created agent version with its assigned version number.
        """
        agent_version = AgentVersion(
            owner_id=actor.account.id,
            agent_id=agent_id,
            display_version=display_version,
            description=description,
            run_spec=run_spec,
            capabilities=capabilities
            if capabilities is not None
            else AgentCapabilities(),
        )
        return await self._repository.create(agent_version)

    async def get_version(
        self, agent_version_id: uuid.UUID, actor: AuthContext
    ) -> AgentVersion:
        """Get an agent version by id.

        Args:
            agent_version_id: Id of the agent version.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        _ = actor
        return await self._repository.get(agent_version_id)

    async def list_versions(
        self, agent_version_filter: AgentVersionFilter, actor: AuthContext
    ) -> tuple[list[AgentVersion], str | None]:
        """List the versions of an agent.

        Args:
            agent_version_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching agent versions and the next cursor.
        """
        _ = actor
        return await self._repository.query(agent_version_filter)

    async def update_version(
        self,
        agent_version_id: uuid.UUID,
        command: AgentVersionUpdate,
        actor: AuthContext,
    ) -> AgentVersion:
        """Partially update an agent version.

        Replacing the run spec replaces its secret links too. An explicit
        null on capabilities clears it to empty capabilities.

        Args:
            agent_version_id: Id of the agent version.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Updated agent version.
        """
        _ = actor
        agent_version = await self._repository.get(agent_version_id)
        fields = command.model_fields_set
        if "display_version" in fields:
            agent_version.update_display_version(command.display_version)
        if "description" in fields:
            agent_version.update_description(command.description)
        if "run_spec" in fields:
            agent_version.update_run_spec(command.run_spec)
        if "capabilities" in fields:
            agent_version.update_capabilities(
                command.capabilities
                if command.capabilities is not None
                else AgentCapabilities()
            )
        return await self._repository.update(agent_version)

    async def delete_version(
        self, agent_version_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an agent version.

        Args:
            agent_version_id: Id of the agent version.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.
        """
        _ = actor
        await self._repository.delete(agent_version_id)
