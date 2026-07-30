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
"""Cohort use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortUpdate,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort


class CohortService:
    """Cohort namespace use cases."""

    def __init__(
        self,
        repository: CohortRepository,
        agent_repository: AgentRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Cohort repository.
            agent_repository: Agent repository, to validate the owning
                agent exists.
        """
        self._repository = repository
        self._agents = agent_repository

    async def create_cohort(self, command: CohortCreate, actor: AuthContext) -> Cohort:
        """Create a cohort namespace owned by the caller.

        Args:
            command: Fields for the new cohort.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the command's agent id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Created cohort.
        """
        await self._agents.get(command.agent_id)
        cohort = Cohort(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            agent_id=command.agent_id,
            metadata=command.metadata,
        )
        return await self._repository.create(cohort)

    async def get_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> Cohort:
        """Get a cohort by id.

        Args:
            cohort_id: Id of the cohort.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Stored cohort.
        """
        _ = actor
        return await self._repository.get(cohort_id)

    async def list_cohorts(
        self, cohort_filter: CohortFilter, actor: AuthContext
    ) -> tuple[list[Cohort], str | None]:
        """List cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching cohorts and the next cursor.
        """
        _ = actor
        return await self._repository.query(cohort_filter)

    async def update_cohort(
        self, cohort_id: uuid.UUID, command: CohortUpdate, actor: AuthContext
    ) -> Cohort:
        """Partially update a cohort's name, description, and metadata.

        Args:
            cohort_id: Id of the cohort.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
            ValidationError: The command clears the cohort name.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Updated cohort.
        """
        _ = actor
        cohort = await self._repository.get(cohort_id)
        fields = command.model_fields_set
        if "name" in fields:
            if command.name is None:
                raise ValidationError("Cohort name cannot be cleared")
            cohort.update_name(command.name)
        if "description" in fields:
            cohort.update_description(command.description)
        if "metadata" in fields:
            assert command.metadata is not None
            cohort.update_metadata(command.metadata)
        return await self._repository.update(cohort)

    async def delete_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a cohort.

        Deleting a cohort cascades its versions.

        Args:
            cohort_id: Id of the cohort.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        _ = actor
        await self._repository.delete(cohort_id)
