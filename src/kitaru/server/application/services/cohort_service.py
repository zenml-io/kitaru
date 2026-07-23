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
)
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.domain.cohort import Cohort, InvalidCohort
from kitaru.server.domain.session import Session

# Page size for resolving every session matching a cohort filter.
_FILTER_RESOLUTION_PAGE_SIZE = 1000


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

    async def _resolve_filter(self, session_filter: SessionFilter) -> list[Session]:
        """Resolve every session matching a filter across all pages.

        Args:
            session_filter: Filter selecting the member sessions.

        Returns:
            Matching sessions.
        """
        sessions: list[Session] = []
        page = 1
        while True:
            batch, total = await self._session_repository.query(
                session_filter.model_copy(
                    update={"page": page, "page_size": _FILTER_RESOLUTION_PAGE_SIZE}
                )
            )
            sessions.extend(batch)
            if len(sessions) >= total or not batch:
                return sessions
            page += 1

    async def create_cohort(self, command: CohortCreate, actor: AuthContext) -> Cohort:
        """Create a cohort owned by the caller.

        Membership comes from explicit session ids with an agent id, or from
        a session filter that pins an agent. A filter-created cohort stores
        the filter as its provenance snapshot.

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
        if (command.session_ids is None) == (command.session_filter is None):
            raise InvalidCohort(
                "Cohort creation requires either session ids or a filter"
            )
        filter_snapshot = None
        if command.session_ids is not None:
            if command.agent_id is None:
                raise InvalidCohort(
                    "Cohort creation from session ids requires an agent id"
                )
            if len(set(command.session_ids)) != len(command.session_ids):
                raise InvalidCohort("Session ids contain duplicates")
            agent_id = command.agent_id
            await self._agent_repository.get(agent_id)
            sessions = [
                await self._session_repository.get(session_id)
                for session_id in command.session_ids
            ]
        else:
            assert command.session_filter is not None
            if command.agent_id is not None:
                raise InvalidCohort(
                    "Cohort creation from a filter takes the agent id from the filter"
                )
            if command.session_filter.agent_id is None:
                raise InvalidCohort(
                    "Cohort creation from a filter requires an agent id in the filter"
                )
            agent_id = command.session_filter.agent_id
            await self._agent_repository.get(agent_id)
            sessions = await self._resolve_filter(command.session_filter)
            filter_snapshot = command.session_filter.model_dump(
                mode="json", exclude={"page", "page_size"}, exclude_none=True
            )
        if not sessions:
            raise InvalidCohort("Cohort requires at least one session")
        cohort = Cohort(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            agent_id=agent_id,
            session_count=len(sessions),
            filter_snapshot=filter_snapshot,
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
        name: str | None,
        description: str | None,
        actor: AuthContext,
    ) -> Cohort:
        """Partially update a cohort.

        Args:
            cohort_id: Id of the cohort.
            name: New cohort name, unchanged when ``None``.
            description: New cohort description, unchanged when ``None``.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has this id.
            DuplicateCohortName: The cohort name is already registered.

        Returns:
            Updated cohort.
        """
        _ = actor
        cohort = await self._repository.get(cohort_id)
        if name is not None:
            cohort.update_name(name)
        if description is not None:
            cohort.update_description(description)
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
