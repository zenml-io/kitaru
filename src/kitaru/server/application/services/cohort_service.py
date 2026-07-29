#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Cohort use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.application.pagination import decode_cursor, encode_cursor
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort import Cohort, InvalidCohortMembers
from kitaru.server.domain.session import Session


class CohortService:
    """Cohort use cases."""

    def __init__(
        self,
        repository: CohortRepository,
        session_repository: SessionRepository,
        agent_repository: AgentRepository,
    ) -> None:
        self._repository = repository
        self._session_repository = session_repository
        self._agent_repository = agent_repository

    async def create_cohort(self, command: CohortCreate, actor: AuthContext) -> Cohort:
        """Create an immutable ordered cohort."""
        if not command.session_ids:
            raise InvalidCohortMembers("A cohort must contain at least one session")
        if len(set(command.session_ids)) != len(command.session_ids):
            raise InvalidCohortMembers("Cohort session ids must be unique")
        await self._agent_repository.get(command.agent_id)
        sessions = await self._session_repository.get_many(command.session_ids)
        cohort = Cohort(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            agent_id=command.agent_id,
        )
        cohort.check_members(
            [
                sessions[session_id].agent_id
                for session_id in command.session_ids
                if session_id in sessions
            ],
            len(command.session_ids),
        )
        return await self._repository.create(cohort, command.session_ids)

    async def get_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> Cohort:
        """Get a cohort."""
        _ = actor
        return await self._repository.get(cohort_id)

    async def list_cohorts(
        self, cohort_filter: CohortFilter, actor: AuthContext
    ) -> tuple[list[Cohort], str | None]:
        """List cohorts."""
        _ = actor
        return await self._repository.query(cohort_filter)

    async def list_cohort_sessions(
        self,
        cohort_id: uuid.UUID,
        sessions_filter: CohortSessionsFilter,
        actor: AuthContext,
    ) -> tuple[list[Session], str | None]:
        """List cohort members in their immutable position order."""
        _ = actor
        await self._repository.get(cohort_id)
        session_ids = await self._repository.get_session_ids(cohort_id)
        filter_hash = sessions_filter.compute_filter_hash()
        start = 0
        if sessions_filter.cursor is not None:
            decoded = decode_cursor(sessions_filter.cursor, "index:asc", filter_hash)
            start = int(decoded.id) + 1
        selected_ids = session_ids[start : start + sessions_filter.size]
        sessions = await self._session_repository.get_many(selected_ids)
        page = [sessions[item_id] for item_id in selected_ids if item_id in sessions]
        next_cursor = None
        if start + sessions_filter.size < len(session_ids):
            next_cursor = encode_cursor(
                "index:asc",
                str(start + sessions_filter.size - 1),
                filter_hash,
            )
        return page, next_cursor

    async def update_cohort(
        self,
        cohort_id: uuid.UUID,
        command: CohortUpdate,
        actor: AuthContext,
    ) -> Cohort:
        """Partially update cohort metadata."""
        _ = actor
        cohort = await self._repository.get(cohort_id)
        if "name" in command.model_fields_set:
            if command.name is None:
                raise ValidationError("Cohort name cannot be null")
            cohort.update_name(command.name)
        if "description" in command.model_fields_set:
            cohort.update_description(command.description)
        return await self._repository.update(cohort)

    async def delete_cohort(self, cohort_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a cohort."""
        _ = actor
        await self._repository.get(cohort_id)
        await self._repository.delete(cohort_id)
