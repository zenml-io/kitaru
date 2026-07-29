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
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import Session


class CohortService:
    """Cohort use cases."""

    def __init__(
        self,
        repository: CohortRepository,
        agent_repository: AgentRepository,
        session_repository: SessionRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Cohort repository.
            agent_repository: Agent repository, to validate the owning
                agent exists.
            session_repository: Session repository, to bulk-validate member
                sessions exist and belong to the owning agent.
        """
        self._repository = repository
        self._agents = agent_repository
        self._sessions = session_repository

    async def create_cohort(
        self,
        name: str,
        description: str | None,
        agent_id: uuid.UUID,
        session_ids: list[uuid.UUID],
        actor: AuthContext,
    ) -> Cohort:
        """Create a cohort as a fixed snapshot of its member sessions.

        Args:
            name: Cohort name.
            description: Cohort description.
            agent_id: Agent the cohort's sessions belong to.
            session_ids: Ordered member session ids.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has this id.
            ValidationError: The member list is empty or has duplicates, or a
                member session is missing or belongs to a different agent.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Created cohort.
        """
        cohort = Cohort(
            owner_id=actor.account.id,
            name=name,
            description=description,
            agent_id=agent_id,
            session_count=len(session_ids),
        )
        cohort.check_members(session_ids)
        await self._agents.get(agent_id)
        sessions_by_id = await self._sessions.get_many(session_ids)
        for session_id in session_ids:
            session = sessions_by_id.get(session_id)
            if session is None:
                raise ValidationError(f"Session {session_id} was not found")
            if session.agent_id != agent_id:
                raise ValidationError(
                    f"Session {session_id} does not belong to agent {agent_id}"
                )
        return await self._repository.create(cohort, session_ids)

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

    async def list_cohort_sessions(
        self, sessions_filter: CohortSessionsFilter, actor: AuthContext
    ) -> tuple[list[Session], str | None]:
        """List a cohort's member sessions in cohort order.

        Args:
            sessions_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of member sessions and the next cursor.
        """
        _ = actor
        return await self._repository.list_sessions(sessions_filter)

    async def update_cohort(
        self, cohort_id: uuid.UUID, command: CohortUpdate, actor: AuthContext
    ) -> Cohort:
        """Partially update a cohort's name and description.

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
        return await self._repository.update(cohort)

    async def delete_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a cohort.

        Deleting a cohort cascades its member links.

        Args:
            cohort_id: Id of the cohort.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
        """
        _ = actor
        await self._repository.delete(cohort_id)
