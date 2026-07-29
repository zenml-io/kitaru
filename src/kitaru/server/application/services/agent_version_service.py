#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Agent-version use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.secret_repository import SecretRepository
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
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.secret import SecretNotFound


class AgentVersionService:
    """Agent-version use cases."""

    def __init__(
        self,
        repository: AgentVersionRepository,
        agent_repository: AgentRepository,
        secret_repository: SecretRepository,
    ) -> None:
        self._repository = repository
        self._agent_repository = agent_repository
        self._secret_repository = secret_repository

    async def _check_secrets_exist(self, run_spec: RunSpec | None) -> None:
        if run_spec is None:
            return
        for secret_id in run_spec.secret_ids:
            secret = await self._secret_repository.get(secret_id)
            if secret.internal:
                raise SecretNotFound(secret_id)

    async def create_version(
        self,
        agent_id: uuid.UUID,
        display_version: str | None,
        description: str | None,
        run_spec: RunSpec | None,
        capabilities: AgentCapabilities | None,
        actor: AuthContext,
    ) -> AgentVersion:
        """Create the next version of an agent."""
        await self._agent_repository.get(agent_id)
        await self._check_secrets_exist(run_spec)
        version = await self._agent_repository.next_version(agent_id)
        return await self._repository.create(
            AgentVersion(
                owner_id=actor.account.id,
                agent_id=agent_id,
                version=version,
                display_version=display_version,
                description=description,
                run_spec=run_spec,
                capabilities=capabilities or AgentCapabilities(),
            )
        )

    async def get_version(
        self, version_id: uuid.UUID, actor: AuthContext
    ) -> AgentVersion:
        """Get an agent version."""
        _ = actor
        return await self._repository.get(version_id)

    async def list_versions(
        self, version_filter: AgentVersionFilter, actor: AuthContext
    ) -> tuple[list[AgentVersion], str | None]:
        """List versions of an existing agent."""
        _ = actor
        await self._agent_repository.get(version_filter.agent_id)
        return await self._repository.query(version_filter)

    async def update_version(
        self,
        version_id: uuid.UUID,
        command: AgentVersionUpdate,
        actor: AuthContext,
    ) -> AgentVersion:
        """Partially update an agent version."""
        _ = actor
        version = await self._repository.get(version_id)
        execution_fields = {"run_spec", "capabilities"} & command.model_fields_set
        frozen = bool(
            execution_fields
            and await self._agent_repository.version_is_frozen(version_id)
        )
        if "display_version" in command.model_fields_set:
            version.update_display_version(command.display_version)
        if "description" in command.model_fields_set:
            version.update_description(command.description)
        if "run_spec" in command.model_fields_set:
            await self._check_secrets_exist(command.run_spec)
            version.update_run_spec(command.run_spec, frozen=frozen)
        if "capabilities" in command.model_fields_set:
            if command.capabilities is None:
                raise ValidationError("Agent version capabilities cannot be null")
            version.update_capabilities(command.capabilities, frozen=frozen)
        return await self._repository.update(version)

    async def delete_version(self, version_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an agent version."""
        _ = actor
        await self._repository.get(version_id)
        await self._repository.delete(version_id)
