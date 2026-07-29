"""Replay use cases."""

import uuid
from typing import Any

from kitaru.server.application.agent_version_resolution import get_agent_run_spec
from kitaru.server.application.evaluator_resolution import validate_evaluators
from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import (
    SessionNodeRepository,
    SessionRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay import ReplayCreate, ReplayFilter
from kitaru.server.application.replay_pipeline import create_replay_pipeline
from kitaru.server.application.services.job_service import JobService
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    HistoryConfig,
    ReplayConfig,
)
from kitaru.server.domain.session import SessionStatus
from kitaru.server.domain.task import AgentTask


class ReplayService:
    """Create replay pipelines and serve replay-time tool lookups."""

    def __init__(
        self,
        repository: ReplayRepository,
        session_repository: SessionRepository,
        session_node_repository: SessionNodeRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        run_repository: ExperimentRunRepository,
        task_repository: TaskRepository,
        job_service: JobService,
    ) -> None:
        """Initialize the service."""
        self._repository = repository
        self._session_repository = session_repository
        self._session_node_repository = session_node_repository
        self._agent_repository = agent_repository
        self._agent_version_repository = agent_version_repository
        self._plugin_repository = plugin_repository
        self._run_repository = run_repository
        self._task_repository = task_repository
        self._job_service = job_service

    async def create_replay(
        self, command: ReplayCreate, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig, uuid.UUID | None]:
        """Create a standalone replay job."""
        baseline = await self._session_repository.get(command.baseline_session_id)
        if baseline.status is SessionStatus.IN_PROGRESS:
            raise ValidationError(f"Session {baseline.id} is still in progress")
        if command.agent_version_id is None:
            agent = await self._agent_repository.get(baseline.agent_id)
            if agent.latest_version < 1:
                raise ValidationError(f"Agent {agent.id} has no versions")
            version = await self._agent_version_repository.get_by_version(
                agent.id, agent.latest_version
            )
        else:
            version = await self._agent_version_repository.get(command.agent_version_id)
        if version.agent_id != baseline.agent_id:
            raise ValidationError(
                f"Agent version {version.id} does not belong to session agent "
                f"{baseline.agent_id}"
            )
        get_agent_run_spec(version)
        evaluators = await validate_evaluators(
            command.evaluators, self._plugin_repository
        )
        replay, config = await create_replay_pipeline(
            owner_id=actor.account.id,
            baseline_session=baseline,
            agent_version=version,
            evaluators=evaluators,
            evaluate_baselines=command.evaluate_baselines,
            job_service=self._job_service,
            replay_repository=self._repository,
            task_repository=self._task_repository,
            override=command.override,
            tool_policy=command.tool_policy,
        )
        return replay, config, None

    async def get_replay(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig, uuid.UUID | None]:
        """Get a replay, its config, and its result session id."""
        _ = actor
        replay = await self._repository.get(replay_id)
        config = await self._repository.get_config(replay.replay_config_id)
        return replay, config, await self._get_result_session_id(replay)

    async def list_replays(
        self, replay_filter: ReplayFilter, actor: AuthContext
    ) -> tuple[
        list[tuple[Replay, ReplayConfig, uuid.UUID | None]],
        str | None,
    ]:
        """List replays with their configs and result sessions."""
        _ = actor
        replays, cursor = await self._repository.query(replay_filter)
        items = []
        for replay in replays:
            items.append(
                (
                    replay,
                    await self._repository.get_config(replay.replay_config_id),
                    await self._get_result_session_id(replay),
                )
            )
        return items, cursor

    async def tool_lookup(
        self,
        replay_id: uuid.UUID,
        tool_name: str,
        cache_key: str,
        actor: AuthContext,
    ) -> tuple[bool, Any]:
        """Look up a recorded tool result in the configured history scope."""
        _ = actor
        replay = await self._repository.get(replay_id)
        config = await self._repository.get_config(replay.replay_config_id)
        tool_config = config.tool_policy.tools.get(
            tool_name, config.tool_policy.default
        )
        if not isinstance(tool_config, HistoryConfig):
            raise ValidationError(f"Tool {tool_name!r} does not use recorded history")
        if tool_config.scope == "baseline":
            node = await self._session_node_repository.find_tool_result(
                cache_key, session_ids=[replay.baseline_session_id]
            )
        elif tool_config.scope == "cohort":
            if replay.experiment_run_id is None:
                raise ValidationError("Cohort history requires an experiment run")
            run = await self._run_repository.get(replay.experiment_run_id)
            node = await self._session_node_repository.find_tool_result(
                cache_key, cohort_id=run.cohort_id
            )
        else:
            baseline = await self._session_repository.get(replay.baseline_session_id)
            node = await self._session_node_repository.find_tool_result(
                cache_key, agent_id=baseline.agent_id
            )
        return (node is not None, node.outputs if node is not None else None)

    async def _get_result_session_id(self, replay: Replay) -> uuid.UUID | None:
        tasks = await self._task_repository.list_job_tasks(replay.job_id)
        agent_tasks = [task for task in tasks if isinstance(task, AgentTask)]
        return agent_tasks[0].result_session_id if agent_tasks else None
