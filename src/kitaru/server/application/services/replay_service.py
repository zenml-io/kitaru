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
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
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
from kitaru.server.application.models.replays import ReplayCreate, ReplayFilter
from kitaru.server.application.services.agent_version_resolution import (
    resolve_agent_version,
)
from kitaru.server.application.services.scorer_resolution import (
    validate_scoring_policy,
)
from kitaru.server.domain.job import (
    InvalidJob,
    Job,
    JobMissingResultSession,
    ReplayJob,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.replay_diff import ReplayDiff, compute_replay_diff
from kitaru.server.domain.session import SessionStatus


class ReplayService:
    """Replay use cases."""

    def __init__(
        self,
        repository: ReplayRepository,
        job_repository: JobRepository,
        replay_config_repository: ReplayConfigRepository,
        session_repository: SessionRepository,
        session_node_repository: SessionNodeRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Replay repository.
            job_repository: Job repository.
            replay_config_repository: Replay config repository.
            session_repository: Session repository.
            session_node_repository: Session node repository.
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository.
        """
        self._repository = repository
        self._job_repository = job_repository
        self._replay_config_repository = replay_config_repository
        self._session_repository = session_repository
        self._session_node_repository = session_node_repository
        self._agent_version_repository = agent_version_repository
        self._plugin_repository = plugin_repository

    async def create_replay(
        self, command: ReplayCreate, actor: AuthContext
    ) -> tuple[Replay, ReplayJob, ReplayConfig]:
        """Create a standalone replay of one session with its job.

        The inline config is normalized into a replay config row. The tool
        policy defaults to a history policy scoped to the original session.

        Args:
            command: Replay create command.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has the input session id.
            InvalidJob: The input session is in progress or the explicit
                version belongs to another agent.
            InvalidReplayConfig: A history policy scopes to a cohort.
            NoRunnableAgentVersion: The session's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            AgentVersionNotRunnable: The explicit version has no run spec.
            PluginNameNotFound: No scorer has a configured name.
            PluginVersionNotFound: A scorer has no configured version.

        Returns:
            Created replay, its job, and its replay config.
        """
        session = await self._session_repository.get(command.input_session_id)
        if session.status is SessionStatus.IN_PROGRESS:
            raise InvalidJob(f"Session {session.id} is in progress")
        version = await resolve_agent_version(
            self._agent_version_repository, session.agent_id, command.agent_version_id
        )
        await validate_scoring_policy(self._plugin_repository, command.scoring_policy)
        config = ReplayConfig(
            owner_id=actor.account.id,
            override=command.override,
            tool_policy=command.tool_policy
            or ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=command.scoring_policy,
        )
        config.check_standalone()
        config = await self._replay_config_repository.create(config)
        assert version.run_spec is not None
        job: Job = ReplayJob(
            agent_version_id=version.id,
            input_session_id=session.id,
            execution_target=version.run_spec.default_execution_target,
        )
        job = await self._job_repository.create(job)
        assert isinstance(job, ReplayJob)
        replay = await self._repository.create(
            Replay(
                owner_id=actor.account.id,
                job_id=job.id,
                replay_config_id=config.id,
                input_session_id=session.id,
            )
        )
        return replay, job, config

    async def get_replay(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Replay, ReplayJob, ReplayConfig]:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            JobNotFound: No job has the replay's job id.

        Returns:
            Stored replay, its job, and its replay config.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        job = await self._job_repository.get(replay.job_id)
        assert isinstance(job, ReplayJob)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        return replay, job, config

    async def list_replays(
        self, replay_filter: ReplayFilter, actor: AuthContext
    ) -> tuple[list[tuple[Replay, ReplayJob, ReplayConfig]], int]:
        """List replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching replays with their jobs and replay configs
            and the total match count.
        """
        _ = actor
        replays, total = await self._repository.query(replay_filter)
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        items: list[tuple[Replay, ReplayJob, ReplayConfig]] = []
        for replay in replays:
            job = await self._job_repository.get(replay.job_id)
            assert isinstance(job, ReplayJob)
            items.append((replay, job, configs[replay.replay_config_id]))
        return items, total

    async def compute_diff(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> ReplayDiff:
        """Compute the full diff between a replay's sessions.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            JobMissingResultSession: The replay's job has no result
                session.

        Returns:
            Full replay diff.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        job = await self._job_repository.get(replay.job_id)
        if job.result_session_id is None:
            raise JobMissingResultSession(job.id)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        original = await self._session_repository.get(replay.input_session_id)
        result = await self._session_repository.get(job.result_session_id)
        original_nodes = await self._session_node_repository.list_for_session(
            original.id, include_payloads=True
        )
        result_nodes = await self._session_node_repository.list_for_session(
            result.id, include_payloads=True
        )
        return compute_replay_diff(
            replay, config.override, original, result, original_nodes, result_nodes
        )
