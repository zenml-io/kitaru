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
"""Session use cases."""

import uuid

from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
)
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
)
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.sessions import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.domain.session import (
    InvalidSession,
    Session,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.session_node import compute_rollups


class SessionService:
    """Session use cases."""

    def __init__(
        self,
        repository: SessionRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        node_repository: SessionNodeRepository,
        replay_repository: ReplayRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Session repository.
            agent_repository: Agent repository.
            agent_version_repository: Agent version repository.
            node_repository: Session node repository.
            replay_repository: Replay repository.
        """
        self._repository = repository
        self._agent_repository = agent_repository
        self._agent_version_repository = agent_version_repository
        self._node_repository = node_repository
        self._replay_repository = replay_repository

    async def create_session(
        self, command: SessionCreate, actor: AuthContext
    ) -> Session:
        """Create a session owned by the caller.

        A set replay id links the session to its replay: the session is
        stored with origin ``replay`` and becomes the replay's result
        session.

        Args:
            command: Session create command.
            actor: Caller context.

        Raises:
            InvalidSession: The command violates the origin rules.
            AgentNotFound: No agent has this id.
            AgentVersionNotFound: No agent version has this id.
            ReplayNotFound: No replay has the referenced replay id.
            ReplayNotActive: The replay is not claimed or running.
            ReplayAlreadyLinked: The replay already has a result session.
            DuplicateSessionExternalId: The provider and external id pair is
                already registered.

        Returns:
            Created session.
        """
        origin = command.origin
        replay = None
        if command.replay_id is not None:
            if command.origin is not SessionOrigin.RECORDED:
                raise InvalidSession(
                    "Sessions linked to a replay require origin 'recorded'"
                )
            replay = await self._replay_repository.get(command.replay_id)
            origin = SessionOrigin.REPLAY
        elif command.origin is SessionOrigin.REPLAY:
            raise InvalidSession("Session origin 'replay' requires a replay id")
        if command.origin is SessionOrigin.RECORDED and command.status not in (
            None,
            SessionStatus.IN_PROGRESS,
        ):
            raise InvalidSession("Recorded sessions must be created in progress")
        await self._agent_repository.get(command.agent_id)
        if command.agent_version_id is not None:
            version = await self._agent_version_repository.get(command.agent_version_id)
            if version.agent_id != command.agent_id:
                raise InvalidSession(
                    f"Agent version {command.agent_version_id} does not "
                    f"belong to agent {command.agent_id}"
                )
        session = Session(
            owner_id=actor.account.id,
            agent_id=command.agent_id,
            agent_version_id=command.agent_version_id,
            origin=origin,
            status=command.status or SessionStatus.IN_PROGRESS,
            name=command.name,
            inputs=command.inputs,
            outputs=command.outputs,
            expected=command.expected,
            error=command.error,
            started_at=command.started_at,
            ended_at=command.ended_at,
            external_id=command.external_id,
            metadata=command.metadata,
            provider=command.provider,
            framework=command.framework,
            adapter_version=command.adapter_version,
            log_uri=command.log_uri,
        )
        if replay is not None:
            replay.link_result_session(session.id)
        session = await self._repository.create(session)
        if replay is not None:
            await self._replay_repository.update(replay)
        return session

    async def get_session(self, session_id: uuid.UUID, actor: AuthContext) -> Session:
        """Get a session by id.

        Args:
            session_id: Id of the session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        _ = actor
        return await self._repository.get(session_id)

    async def list_sessions(
        self, session_filter: SessionFilter, actor: AuthContext
    ) -> tuple[list[Session], int]:
        """List sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the filtered agent id.

        Returns:
            Page of matching sessions and the total match count.
        """
        _ = actor
        if session_filter.agent_id is not None:
            await self._agent_repository.get(session_filter.agent_id)
        return await self._repository.query(session_filter)

    async def update_session(
        self, session_id: uuid.UUID, command: SessionUpdate, actor: AuthContext
    ) -> Session:
        """Partially update a session, finishing it when a status is set.

        A set status finishes the session with the command's outputs, error,
        ended_at, and log_uri, and computes the rollups from its nodes. Name,
        expected, and metadata apply to any session when set.

        Args:
            session_id: Id of the session.
            command: Session update command.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionNotInProgress: The session is not in progress.
            InvalidSession: The new status is not terminal.

        Returns:
            Updated session.
        """
        _ = actor
        session = await self._repository.get(session_id)
        if command.status is not None:
            nodes = await self._node_repository.list_for_session(
                session_id, include_payloads=False
            )
            session.finish(
                status=command.status,
                outputs=command.outputs,
                error=command.error,
                ended_at=command.ended_at,
                log_uri=command.log_uri,
                rollups=compute_rollups(nodes),
            )
        if command.name is not None:
            session.update_name(command.name)
        if command.expected is not None:
            session.update_expected(command.expected)
        if command.metadata is not None:
            session.update_metadata(command.metadata)
        return await self._repository.update(session)

    async def merge_scores(
        self, session_id: uuid.UUID, scores: dict[str, float], actor: AuthContext
    ) -> Session:
        """Merge values into a session's scores map.

        Args:
            session_id: Id of the session.
            scores: Score values by scorer name, latest wins.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Updated session.
        """
        _ = actor
        session = await self._repository.get(session_id)
        session.merge_scores(scores)
        return await self._repository.update(session)

    async def delete_session(self, session_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a session, including its nodes and tag links.

        Args:
            session_id: Id of the session.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session is a member of a cohort or referenced
                by a replay.
        """
        _ = actor
        await self._repository.delete(session_id)
