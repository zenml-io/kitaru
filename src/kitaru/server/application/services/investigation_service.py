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
"""Investigation use cases."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.investigation import InvestigationSessionVerdict
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.investigation_repository import (
    InvestigationRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.investigation import (
    InvestigationCreate,
    InvestigationFilter,
    InvestigationSessionFilter,
    InvestigationUpdate,
)
from kitaru.server.application.services.analytics_events import (
    build_investigation_created_properties,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.investigation import Investigation, InvestigationSession


class InvestigationService:
    """Investigation use cases."""

    def __init__(
        self,
        repository: InvestigationRepository,
        agent_repository: AgentRepository,
        session_repository: SessionRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Investigation repository.
            agent_repository: Agent repository, to validate the investigated
                agent exists.
            session_repository: Session repository, to validate linked
                sessions exist and belong to the agent.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._agents = agent_repository
        self._sessions = session_repository
        self._analytics = analytics

    async def _validate_sessions(
        self, session_ids: Sequence[uuid.UUID], agent_id: uuid.UUID
    ) -> None:
        """Validate linked sessions exist and belong to the investigated agent.

        Args:
            session_ids: Session ids being linked to the investigation.
            agent_id: Id of the investigation's agent.

        Raises:
            ValidationError: A session id repeats, is missing, or belongs to
                a different agent.
        """
        if len(session_ids) != len(set(session_ids)):
            raise ValidationError("Investigation session list contains duplicate ids")
        sessions_by_id = await self._sessions.get_many(session_ids)
        for session_id in session_ids:
            session = sessions_by_id.get(session_id)
            if session is None:
                raise ValidationError(f"Session {session_id} was not found")
            if session.agent_id != agent_id:
                raise ValidationError(
                    f"Session {session_id} does not belong to agent {agent_id}"
                )

    async def create_investigation(
        self, command: InvestigationCreate, actor: AuthContext
    ) -> Investigation:
        """Create an investigation with its linked sessions in one shot.

        Session position follows the command's session order.

        Args:
            command: Agent and sessions for the new investigation.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the command's agent id.
            ValidationError: A linked session id repeats, is missing, or
                belongs to a different agent.

        Returns:
            Created investigation.
        """
        await self._agents.get(command.agent_id)
        session_ids = [item.session_id for item in command.sessions]
        await self._validate_sessions(session_ids, command.agent_id)
        investigation = Investigation(
            owner_id=actor.account.id,
            agent_id=command.agent_id,
            name=command.name,
            description=command.description,
            total_sessions=len(command.sessions),
            completed_sessions=0,
        )
        sessions = [
            InvestigationSession(
                investigation_id=investigation.id,
                session_id=item.session_id,
                position=position,
                question=item.question,
                view=item.view,
                highlights=item.highlights,
            )
            for position, item in enumerate(command.sessions)
        ]
        investigation = await self._repository.create(investigation, sessions)
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.INVESTIGATION_CREATED,
                build_investigation_created_properties(investigation),
            )
        return investigation

    async def get_investigation(
        self, investigation_id: uuid.UUID, actor: AuthContext
    ) -> Investigation:
        """Get an investigation by id.

        Args:
            investigation_id: Id of the investigation.
            actor: Caller context.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation.
        """
        _ = actor
        return await self._repository.get(investigation_id)

    async def list_investigations(
        self, investigation_filter: InvestigationFilter, actor: AuthContext
    ) -> tuple[list[Investigation], str | None]:
        """List investigations matching a filter.

        Args:
            investigation_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching investigations and the next cursor.
        """
        _ = actor
        return await self._repository.query(investigation_filter)

    async def update_investigation(
        self,
        investigation_id: uuid.UUID,
        command: InvestigationUpdate,
        actor: AuthContext,
    ) -> Investigation:
        """Partially update an investigation's name, description, and status.

        Args:
            investigation_id: Id of the investigation.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            InvestigationNotFound: No investigation has this id.
            IllegalInvestigationStatusTransition: The command moves the
                status backwards.
            ValidationError: The command clears the investigation name or
                status.

        Returns:
            Updated investigation.
        """
        _ = actor
        investigation = await self._repository.get(investigation_id)
        fields = command.model_fields_set
        if "name" in fields:
            if command.name is None:
                raise ValidationError("Investigation name cannot be cleared")
            investigation.update_name(command.name)
        if "description" in fields:
            investigation.update_description(command.description)
        if "status" in fields:
            if command.status is None:
                raise ValidationError("Investigation status cannot be cleared")
            investigation.update_status(command.status, datetime.now(UTC))
        return await self._repository.update(investigation)

    async def delete_investigation(
        self, investigation_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an investigation.

        Deleting an investigation cascades its linked sessions and answers.

        Args:
            investigation_id: Id of the investigation.
            actor: Caller context.

        Raises:
            InvestigationNotFound: No investigation has this id.
        """
        _ = actor
        await self._repository.delete(investigation_id)

    async def list_investigation_sessions(
        self, session_filter: InvestigationSessionFilter, actor: AuthContext
    ) -> tuple[list[InvestigationSession], str | None]:
        """List an investigation's linked sessions, ordered by position ascending.

        Args:
            session_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            InvestigationNotFound: No investigation has the filter's
                investigation id.

        Returns:
            Page of matching investigation sessions and the next cursor.
        """
        _ = actor
        await self._repository.get(session_filter.investigation_id)
        return await self._repository.query_sessions(session_filter)

    async def update_investigation_session_verdict(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        verdict: InvestigationSessionVerdict | None,
        actor: AuthContext,
    ) -> InvestigationSession:
        """Set or clear a linked session's verdict.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the linked session.
            verdict: New verdict, None clears it.
            actor: Caller context.

        Raises:
            InvestigationNotFound: No investigation has this id.
            InvestigationSessionNotFound: No investigation session links this
                investigation and session.

        Returns:
            Updated investigation session.
        """
        _ = actor
        await self._repository.get(investigation_id)
        session = await self._repository.get_session_by_session_id(
            investigation_id, session_id
        )
        session.update_verdict(verdict)
        return await self._repository.update_session(session)
