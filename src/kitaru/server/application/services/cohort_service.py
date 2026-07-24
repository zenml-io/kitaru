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

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import (
    CohortCreate,
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.domain.cohort import Cohort, InvalidCohort
from kitaru.server.domain.session import Session


class CohortService:
    """Cohort use cases."""

    def __init__(
        self,
        repository: CohortRepository,
        session_repository: SessionRepository,
        agent_repository: AgentRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Cohort repository.
            session_repository: Session repository.
            agent_repository: Agent repository.
        """
        self._repository = repository
        self._session_repository = session_repository
        self._agent_repository = agent_repository

    async def create_cohort(self, command: CohortCreate, actor: AuthContext) -> Cohort:
        """Create a cohort owned by the caller.

        The given session ids keep their order as the member positions.
        Membership is immutable after creation.

        Args:
            command: Cohort create command.
            actor: Caller context.

        Raises:
            InvalidCohort: The command violates the membership rules.
            AgentNotFound: No agent has the referenced agent id.
            SessionNotFound: No session has a referenced session id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Created cohort.
        """
        if len(set(command.session_ids)) != len(command.session_ids):
            raise InvalidCohort("Session ids contain duplicates")
        await self._agent_repository.get(command.agent_id)
        sessions = [
            await self._session_repository.get(session_id)
            for session_id in command.session_ids
        ]
        if not sessions:
            raise InvalidCohort("Cohort requires at least one session")
        cohort = Cohort(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            agent_id=command.agent_id,
            session_count=len(sessions),
        )
        cohort.check_members(sessions)
        return await self._repository.create(
            cohort, [session.id for session in sessions]
        )

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
    ) -> tuple[list[Cohort], int]:
        """List cohorts matching a filter.

        Args:
            cohort_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching cohorts and the total match count.
        """
        _ = actor
        return await self._repository.query(cohort_filter)

    async def list_cohort_sessions(
        self,
        cohort_id: uuid.UUID,
        sessions_filter: CohortSessionsFilter,
        actor: AuthContext,
    ) -> tuple[list[Session], int]:
        """List the member sessions of a cohort ordered by position.

        Args:
            cohort_id: Id of the cohort.
            sessions_filter: Pagination parameters.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.

        Returns:
            Page of member sessions and the total member count.
        """
        _ = actor
        return await self._repository.query_sessions(cohort_id, sessions_filter)

    async def update_cohort(
        self,
        cohort_id: uuid.UUID,
        command: CohortUpdate,
        actor: AuthContext,
    ) -> Cohort:
        """Partially update a cohort.

        Fields absent from the command stay unchanged. An explicit null
        clears the description and is rejected for the name.

        Args:
            cohort_id: Id of the cohort.
            command: Cohort update command.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
            InvalidCohort: The name is null.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Updated cohort.
        """
        _ = actor
        cohort = await self._repository.get(cohort_id)
        if "name" in command.model_fields_set:
            if command.name is None:
                raise InvalidCohort("Cohort name cannot be null")
            cohort.update_name(command.name)
        if "description" in command.model_fields_set:
            cohort.update_description(command.description)
        return await self._repository.update(cohort)

    async def delete_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a cohort, including its membership and tag links.

        Args:
            cohort_id: Id of the cohort.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
            CohortInUse: The cohort is referenced by an experiment.
        """
        _ = actor
        await self._repository.delete(cohort_id)
