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
"""Experiment use cases."""

import uuid

from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import CohortSessionsFilter
from kitaru.server.application.models.experiments import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
)
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
    InvalidExperimentRun,
)
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    PassthroughPolicy,
    ReplayConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import Session

# Page size for resolving every member session of the experiment's cohort.
_MEMBER_RESOLUTION_PAGE_SIZE = 1000


class ExperimentService:
    """Experiment use cases."""

    def __init__(
        self,
        repository: ExperimentRepository,
        run_repository: ExperimentRunRepository,
        cohort_repository: CohortRepository,
        agent_version_repository: AgentVersionRepository,
        replay_config_repository: ReplayConfigRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment repository.
            run_repository: Experiment run repository.
            cohort_repository: Cohort repository.
            agent_version_repository: Agent version repository.
            replay_config_repository: Replay config repository.
        """
        self._repository = repository
        self._run_repository = run_repository
        self._cohort_repository = cohort_repository
        self._agent_version_repository = agent_version_repository
        self._replay_config_repository = replay_config_repository

    async def create_experiment(
        self, command: ExperimentCreate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Create an experiment owned by the caller.

        The inline config is normalized into a replay config row. The tool
        policy defaults to passthrough when omitted.

        Args:
            command: Experiment create command.
            actor: Caller context.

        Raises:
            CohortNotFound: No cohort has the referenced cohort id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Created experiment and its replay config.
        """
        await self._cohort_repository.get(command.cohort_id)
        config = await self._replay_config_repository.create(
            ReplayConfig(
                owner_id=actor.account.id,
                override=command.override,
                tool_policy=command.tool_policy
                or ToolPolicyConfig(default=PassthroughPolicy()),
                scoring_policy=command.scoring_policy,
            )
        )
        experiment = Experiment(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            cohort_id=command.cohort_id,
            replay_config_id=config.id,
        )
        return await self._repository.create(experiment), config

    async def get_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Get an experiment by id.

        Args:
            experiment_id: Id of the experiment.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment and its replay config.
        """
        _ = actor
        experiment = await self._repository.get(experiment_id)
        config = await self._replay_config_repository.get(experiment.replay_config_id)
        return experiment, config

    async def list_experiments(
        self, experiment_filter: ExperimentFilter, actor: AuthContext
    ) -> tuple[list[tuple[Experiment, ReplayConfig]], int]:
        """List experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching experiments with their replay configs and the
            total match count.
        """
        _ = actor
        experiments, total = await self._repository.query(experiment_filter)
        configs = await self._replay_config_repository.get_many(
            [experiment.replay_config_id for experiment in experiments]
        )
        return [
            (experiment, configs[experiment.replay_config_id])
            for experiment in experiments
        ], total

    async def update_experiment(
        self, experiment_id: uuid.UUID, command: ExperimentUpdate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Partially update an experiment.

        Name and description update on any experiment. Cohort and config
        changes are rejected once a run exists. A config change inserts a
        new replay config row, repoints the experiment, and deletes the old
        row when nothing else references it.

        Args:
            experiment_id: Id of the experiment.
            command: Experiment update command.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentFrozen: A cohort or config change hits an experiment
                with runs.
            CohortNotFound: No cohort has the referenced cohort id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Updated experiment and its replay config.
        """
        experiment = await self._repository.get(experiment_id)
        if command.name is not None:
            experiment.update_name(command.name)
        if command.description is not None:
            experiment.update_description(command.description)
        old_config_id = experiment.replay_config_id
        config_changed = (
            command.override is not None
            or command.tool_policy is not None
            or command.scoring_policy is not None
        )
        config = None
        if command.cohort_id is not None or config_changed:
            frozen = await self._run_repository.has_runs(experiment_id)
            if command.cohort_id is not None:
                await self._cohort_repository.get(command.cohort_id)
                experiment.update_cohort_id(command.cohort_id, frozen=frozen)
            if config_changed:
                old_config = await self._replay_config_repository.get(old_config_id)
                config = await self._replay_config_repository.create(
                    ReplayConfig(
                        owner_id=actor.account.id,
                        override=command.override
                        if command.override is not None
                        else old_config.override,
                        tool_policy=command.tool_policy or old_config.tool_policy,
                        scoring_policy=command.scoring_policy
                        or old_config.scoring_policy,
                    )
                )
                experiment.update_replay_config_id(config.id, frozen=frozen)
        experiment = await self._repository.update(experiment)
        if config is None:
            config = await self._replay_config_repository.get(old_config_id)
        else:
            await self._replay_config_repository.delete_if_unreferenced(old_config_id)
        return experiment, config

    async def delete_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an experiment, including its tag links.

        Deletes the experiment's replay config when nothing else references
        it.

        Args:
            experiment_id: Id of the experiment.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        _ = actor
        experiment = await self._repository.get(experiment_id)
        await self._repository.delete(experiment_id)
        await self._replay_config_repository.delete_if_unreferenced(
            experiment.replay_config_id
        )

    async def _resolve_agent_version(
        self, agent_id: uuid.UUID, version_id: uuid.UUID | None
    ) -> AgentVersion:
        """Resolve the agent version a run executes.

        Args:
            agent_id: Id of the cohort's agent.
            version_id: Explicit version id, ``None`` resolves the latest
                runnable version.

        Raises:
            NoRunnableAgentVersion: The agent has no runnable version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidExperimentRun: The explicit version belongs to another
                agent.
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
            raise InvalidExperimentRun(
                f"Agent version {version_id} does not belong to agent {agent_id}"
            )
        if version.run_spec is None:
            raise AgentVersionNotRunnable(version_id)
        return version

    async def _resolve_members(self, cohort_id: uuid.UUID) -> list[Session]:
        """Resolve every member session of a cohort across all pages.

        Args:
            cohort_id: Id of the cohort.

        Returns:
            Member sessions in position order.
        """
        sessions: list[Session] = []
        page = 1
        while True:
            batch, total = await self._cohort_repository.query_sessions(
                cohort_id,
                CohortSessionsFilter(page=page, page_size=_MEMBER_RESOLUTION_PAGE_SIZE),
            )
            sessions.extend(batch)
            if len(sessions) >= total or not batch:
                return sessions
            page += 1

    async def start_run(
        self,
        experiment_id: uuid.UUID,
        agent_version_id: uuid.UUID | None,
        score_baselines: bool,
        actor: AuthContext,
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Start an experiment run.

        Creates the run plus one pending replay per cohort session, stamped
        with the experiment's replay config and the resolved agent version.

        Args:
            experiment_id: Id of the experiment.
            agent_version_id: Explicit version id, ``None`` resolves the
                latest runnable version of the cohort's agent.
            score_baselines: Whether the runner also scores originals
                missing scores.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
            NoRunnableAgentVersion: The cohort's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidExperimentRun: The explicit version belongs to another
                agent.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Created experiment run and its progress.
        """
        experiment = await self._repository.get(experiment_id)
        cohort = await self._cohort_repository.get(experiment.cohort_id)
        version = await self._resolve_agent_version(cohort.agent_id, agent_version_id)
        sessions = await self._resolve_members(experiment.cohort_id)
        run = ExperimentRun(
            owner_id=actor.account.id,
            experiment_id=experiment.id,
            agent_version_id=version.id,
            score_baselines=score_baselines,
        )
        replays = [
            Replay(
                experiment_run_id=run.id,
                replay_config_id=experiment.replay_config_id,
                agent_version_id=version.id,
                original_session_id=session.id,
            )
            for session in sessions
        ]
        run = await self._run_repository.create(run, replays)
        progress = ExperimentRunProgress(pending=len(replays), total=len(replays))
        return run, progress
