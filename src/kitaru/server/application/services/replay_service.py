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

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.replay_config import HistoryScope
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext, TaskPrincipal
from kitaru.server.application.models.replay import (
    ReplayCreate,
    ReplayFilter,
    ReplayWithDetails,
    ToolLookupResult,
)
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.agent_version_resolution import (
    resolve_runnable_agent_version,
)
from kitaru.server.application.services.evaluator_resolution import validate_evaluators
from kitaru.server.application.services.replay_pipeline import create_replay_pipelines
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.replay import Replay, ReplayAccessDenied
from kitaru.server.domain.replay_config import (
    HistoryConfig,
    ReplayConfig,
    default_tool_policy,
)
from kitaru.server.domain.session_node import SessionNode


class ReplayService:
    """Replay use cases."""

    def __init__(
        self,
        repository: ReplayRepository,
        experiment_repository: ExperimentRepository,
        experiment_run_repository: ExperimentRunRepository,
        job_repository: JobRepository,
        task_repository: TaskRepository,
        session_repository: SessionRepository,
        session_node_repository: SessionNodeRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Replay repository.
            experiment_repository: Experiment repository, for replay configs.
            experiment_run_repository: Experiment run repository, for tool
                lookup's cohort-version scope.
            job_repository: Job repository.
            task_repository: Task repository.
            session_repository: Session repository.
            session_node_repository: Session node repository, for tool
                lookup.
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository, for evaluator resolution.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._experiments = experiment_repository
        self._experiment_runs = experiment_run_repository
        self._jobs = job_repository
        self._tasks = task_repository
        self._sessions = session_repository
        self._session_nodes = session_node_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._analytics = analytics

    async def _bundle(self, replays: list[Replay]) -> list[ReplayWithDetails]:
        """Enrich replays with their config and result session id, in bulk.

        Args:
            replays: Replays to enrich.

        Returns:
            Replays paired with their config and result session id, in the
            input order.
        """
        if not replays:
            return []
        configs = await self._experiments.get_many_replay_configs(
            list({replay.replay_config_id for replay in replays})
        )
        agent_tasks = await self._tasks.get_agent_tasks_by_job_ids(
            [replay.job_id for replay in replays]
        )
        return [
            ReplayWithDetails(
                replay=replay,
                config=configs[replay.replay_config_id],
                result_session_id=(
                    agent_tasks[replay.job_id].result_session_id
                    if replay.job_id in agent_tasks
                    else None
                ),
            )
            for replay in replays
        ]

    async def create_replay(
        self, command: ReplayCreate, actor: AuthContext
    ) -> ReplayWithDetails:
        """Create a standalone replay of a recorded or imported session.

        An omitted agent version resolves to the baseline session's recorded
        agent version.

        Args:
            command: Fields for the replay and its config.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has the baseline session id.
            ValidationError: The baseline session carries no agent version
                and none was given, the resolved agent version has no run
                spec, or the config uses cohort-version-scoped history.
            AgentVersionNotFound: No agent version has the resolved id.
            PluginNotFound: An evaluator config names an unknown evaluator.
            PluginVersionNotFound: An evaluator config names an unknown
                version.

        Returns:
            Created replay, paired with its config and result session id.
        """
        baseline = await self._sessions.get(command.baseline_session_id)
        agent_version_id = command.agent_version_id
        if agent_version_id is None:
            if baseline.agent_version_id is None:
                raise ValidationError(
                    f"Session {baseline.id} carries no agent version, "
                    "agent_version_id must be given"
                )
            agent_version_id = baseline.agent_version_id
        agent_version = await resolve_runnable_agent_version(
            agent_version_id, self._agent_versions
        )
        evaluators = await validate_evaluators(command.evaluators, self._plugins, actor)
        config = ReplayConfig(
            owner_id=actor.account.id,
            override=command.override,
            tool_policy=command.tool_policy or default_tool_policy(),
            evaluators=evaluators,
        )
        config.check_standalone()
        config = await self._experiments.create_replay_config(config)
        replays = await create_replay_pipelines(
            baselines=[baseline],
            agent_version_id=agent_version.id,
            config=config,
            evaluate_baselines=command.evaluate_baselines,
            experiment_run_id=None,
            actor=actor,
            replay_repository=self._repository,
            job_repository=self._jobs,
            task_repository=self._tasks,
        )
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.REPLAY_CREATED,
                analytics_events.build_replay_created_properties(command.override),
            )
        return (await self._bundle(replays))[0]

    async def get_replay(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> ReplayWithDetails:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayAccessDenied: The caller's task token names a task outside
                this replay's job.
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay, paired with its config and result session id.
        """
        replay = await self._repository.get(replay_id)
        await self._check_task_access(replay, actor)
        return (await self._bundle([replay]))[0]

    async def list_replays(
        self, replay_filter: ReplayFilter, actor: AuthContext
    ) -> tuple[list[ReplayWithDetails], str | None]:
        """List replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching replays, each paired with its config and result
            session id, and the next cursor.
        """
        _ = actor
        replays, next_cursor = await self._repository.query(replay_filter)
        return await self._bundle(replays), next_cursor

    async def tool_lookup(
        self,
        replay_id: uuid.UUID,
        tool_name: str,
        cache_key: str,
        actor: AuthContext,
    ) -> ToolLookupResult:
        """Search recorded tool-call history for a cached result.

        The tool's config and history scope come from the replay's config,
        never from the caller, so the policy is interpreted in one place.

        Args:
            replay_id: Id of the replay.
            tool_name: Tool being called.
            cache_key: Call cache key.
            actor: Caller context.

        Raises:
            ReplayAccessDenied: The caller's task token names a task outside
                this replay's job.
            ReplayNotFound: No replay has this id.
            ValidationError: The tool's config is not a history config.

        Returns:
            Whether a cached result was found, and the result if so.
        """
        replay = await self._repository.get(replay_id)
        await self._check_task_access(replay, actor)
        config = await self._experiments.get_replay_config(replay.replay_config_id)
        tool_config = config.tool_policy.tools.get(
            tool_name, config.tool_policy.default
        )
        if not isinstance(tool_config, HistoryConfig):
            raise ValidationError(f"Tool '{tool_name}' is not configured for history")
        node = await self._find_history_node(replay, tool_config.scope, cache_key)
        if node is None:
            return ToolLookupResult(found=False, result=None)
        return ToolLookupResult(found=True, result=node.outputs)

    async def _check_task_access(self, replay: Replay, actor: AuthContext) -> None:
        """Require a task principal's task to belong to the replay's job.

        An account principal always passes.

        Args:
            replay: Replay being accessed.
            actor: Caller context.

        Raises:
            ReplayAccessDenied: The caller's task token names a task outside
                this replay's job.
        """
        if not isinstance(actor.principal, TaskPrincipal):
            return
        if actor.principal.job_id != replay.job_id:
            raise ReplayAccessDenied(replay.id)

    async def _find_history_node(
        self, replay: Replay, scope: HistoryScope, cache_key: str
    ) -> SessionNode | None:
        """Search the tool config's history scope for a matching node.

        Args:
            replay: Replay to search history for.
            scope: History scope to search.
            cache_key: Call cache key.

        Returns:
            Highest-id matching node, or ``None`` on a miss.
        """
        if scope is HistoryScope.BASELINE:
            return await self._session_nodes.find_latest_by_cache_key_in_session(
                replay.baseline_session_id, cache_key
            )
        if scope is HistoryScope.AGENT:
            baseline = await self._sessions.get(replay.baseline_session_id)
            return await self._session_nodes.find_latest_by_cache_key_in_agent(
                baseline.agent_id, cache_key
            )
        assert replay.experiment_run_id is not None
        run = await self._experiment_runs.get(replay.experiment_run_id)
        return await self._session_nodes.find_latest_by_cache_key_in_cohort_version(
            run.cohort_version_id, cache_key
        )
