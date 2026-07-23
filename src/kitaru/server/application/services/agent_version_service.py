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

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
)
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.secret_repository import (
    SecretRepository,
)
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)
from kitaru.server.domain.secret import SecretNotFound


class AgentVersionService:
    """Agent version use cases."""

    def __init__(
        self,
        repository: AgentVersionRepository,
        agent_repository: AgentRepository,
        secret_repository: SecretRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Agent version repository.
            agent_repository: Agent repository.
            secret_repository: Secret repository.
        """
        self._repository = repository
        self._agent_repository = agent_repository
        self._secret_repository = secret_repository

    async def _check_secrets_exist(self, run_spec: RunSpec) -> None:
        """Check that every secret a run spec references exists.

        Args:
            run_spec: Run specification to check.

        Raises:
            SecretNotFound: A referenced secret does not exist or is
                internal.
        """
        for secret_id in run_spec.secret_ids:
            secret = await self._secret_repository.get(secret_id)
            if secret.internal:
                raise SecretNotFound(secret_id)

    async def create_version(
        self,
        agent_id: uuid.UUID,
        version: str,
        description: str | None,
        run_spec: RunSpec | None,
        capabilities: AgentCapabilities | None,
        actor: AuthContext,
    ) -> AgentVersion:
        """Create an agent version owned by the caller.

        Args:
            agent_id: Id of the agent.
            version: Version label.
            description: Version description.
            run_spec: Run specification.
            capabilities: Agent capabilities.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
            SecretNotFound: A referenced secret does not exist or is
                internal.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Created agent version.
        """
        await self._agent_repository.get(agent_id)
        if run_spec is not None:
            await self._check_secrets_exist(run_spec)
        owner_id = actor.account.id
        agent_version = AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            version=version,
            description=description,
            run_spec=run_spec,
            capabilities=capabilities or AgentCapabilities(),
        )
        return await self._repository.create(agent_version)

    async def get_version(
        self, version_id: uuid.UUID, actor: AuthContext
    ) -> AgentVersion:
        """Get an agent version by id.

        Args:
            version_id: Id of the agent version.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        _ = actor
        return await self._repository.get(version_id)

    async def list_versions(
        self, version_filter: AgentVersionFilter, actor: AuthContext
    ) -> tuple[list[AgentVersion], int]:
        """List agent versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the filtered agent id.

        Returns:
            Page of matching agent versions and the total match count.
        """
        _ = actor
        if version_filter.agent_id is not None:
            await self._agent_repository.get(version_filter.agent_id)
        return await self._repository.query(version_filter)

    async def update_version(
        self,
        version_id: uuid.UUID,
        description: str | None,
        run_spec: RunSpec | None,
        capabilities: AgentCapabilities | None,
        actor: AuthContext,
    ) -> AgentVersion:
        """Partially update an agent version.

        Args:
            version_id: Id of the agent version.
            description: New version description, unchanged when ``None``.
            run_spec: New run specification, unchanged when ``None``.
            capabilities: New agent capabilities, unchanged when ``None``.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            SecretNotFound: A referenced secret does not exist or is
                internal.

        Returns:
            Updated agent version.
        """
        _ = actor
        agent_version = await self._repository.get(version_id)
        if description is not None:
            agent_version.update_description(description)
        if run_spec is not None:
            await self._check_secrets_exist(run_spec)
            agent_version.update_run_spec(run_spec)
        if capabilities is not None:
            agent_version.update_capabilities(capabilities)
        return await self._repository.update(agent_version)

    async def delete_version(self, version_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an agent version.

        Args:
            version_id: Id of the agent version.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.
        """
        _ = actor
        await self._repository.delete(version_id)
