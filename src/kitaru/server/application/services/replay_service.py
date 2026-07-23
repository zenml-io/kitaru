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
"""Replay use cases."""

import uuid

from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replays import ReplayCreate, ReplayFilter
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
)
from kitaru.server.domain.replay import InvalidReplay, Replay
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import SessionStatus


class ReplayService:
    """Replay use cases."""

    def __init__(
        self,
        repository: ReplayRepository,
        replay_config_repository: ReplayConfigRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Replay repository.
            replay_config_repository: Replay config repository.
            session_repository: Session repository.
            agent_version_repository: Agent version repository.
        """
        self._repository = repository
        self._replay_config_repository = replay_config_repository
        self._session_repository = session_repository
        self._agent_version_repository = agent_version_repository

    async def _resolve_agent_version(
        self, agent_id: uuid.UUID, version_id: uuid.UUID | None
    ) -> AgentVersion:
        """Resolve the agent version a replay executes.

        Args:
            agent_id: Id of the original session's agent.
            version_id: Explicit version id, ``None`` resolves the latest
                runnable version.

        Raises:
            NoRunnableAgentVersion: The agent has no runnable version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidReplay: The explicit version belongs to another agent.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Resolved agent version.
        """
        if version_id is None:
            version = await self._agent_version_repository.get_latest_runnable(agent_id)
            if version is None:
                raise NoRunnableAgentVersion(agent_id)
            return version
        version = await self._agent_version_repository.get(version_id)
        if version.agent_id != agent_id:
            raise InvalidReplay(
                f"Agent version {version_id} does not belong to agent {agent_id}"
            )
        if version.run_spec is None:
            raise AgentVersionNotRunnable(version_id)
        return version

    async def create_replay(
        self, command: ReplayCreate, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig]:
        """Create a standalone replay of one session.

        The inline config is normalized into a replay config row. The tool
        policy defaults to a history policy scoped to the original session.

        Args:
            command: Replay create command.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has the original session id.
            InvalidReplay: The original session is in progress or the
                explicit version belongs to another agent.
            InvalidReplayConfig: A history policy scopes to a cohort.
            NoRunnableAgentVersion: The session's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Created replay and its replay config.
        """
        session = await self._session_repository.get(command.original_session_id)
        if session.status is SessionStatus.IN_PROGRESS:
            raise InvalidReplay(f"Session {session.id} is in progress")
        version = await self._resolve_agent_version(
            session.agent_id, command.agent_version_id
        )
        config = ReplayConfig(
            owner_id=actor.account.id,
            override=command.override,
            tool_policy=command.tool_policy
            or ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=command.scoring_policy,
        )
        config.check_standalone()
        config = await self._replay_config_repository.create(config)
        replay = Replay(
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
        )
        return await self._repository.create(replay), config

    async def get_replay(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig]:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay and its replay config.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        return replay, config

    async def list_replays(
        self, replay_filter: ReplayFilter, actor: AuthContext
    ) -> tuple[list[tuple[Replay, ReplayConfig]], int]:
        """List replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching replays with their replay configs and the
            total match count.
        """
        _ = actor
        replays, total = await self._repository.query(replay_filter)
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays], total
