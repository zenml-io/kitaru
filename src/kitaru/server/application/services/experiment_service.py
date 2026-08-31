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

import contextlib
import uuid
from datetime import UTC, datetime

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_version_repository import (
    CohortVersionRepository,
)
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
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
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.application.models.experiment_run import ExperimentRunCreate
from kitaru.server.application.models.replay import ReplayStatusCounts
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.agent_version_resolution import (
    resolve_runnable_agent_version,
)
from kitaru.server.application.services.evaluator_resolution import (
    validate_evaluators,
)
from kitaru.server.application.services.replay_pipeline import create_replay_pipelines
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayConfigInUse,
    ReplayOverride,
    ToolPolicy,
    default_tool_policy,
)
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition
from kitaru.server.utils import paginate_all


class ExperimentService:
    """Experiment use cases."""

    def __init__(
        self,
        repository: ExperimentRepository,
        plugin_repository: PluginRepository,
        experiment_run_repository: ExperimentRunRepository,
        agent_repository: AgentRepository,
        cohort_version_repository: CohortVersionRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
        replay_repository: ReplayRepository,
        job_repository: JobRepository,
        task_repository: TaskRepository,
        evaluation_repository: EvaluationRepository,
        transitions: TaskTransitions,
        payload_store: PayloadStore,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment and replay config repository.
            plugin_repository: Plugin repository, for evaluator resolution.
            experiment_run_repository: Experiment run repository, for run
                fan-out and delete cascade.
            agent_repository: Agent repository, to validate the owning
                agent exists.
            cohort_version_repository: Cohort version repository, for run
                fan-out.
            session_repository: Session repository, for run fan-out.
            agent_version_repository: Agent version repository.
            replay_repository: Replay repository, for run fan-out, progress,
                and job lookup on delete.
            job_repository: Job repository, for run fan-out.
            task_repository: Task repository, for run fan-out.
            evaluation_repository: Evaluation repository, for run fan-out's
                baseline adoption lookup.
            transitions: Task transition dispatch, for job cancellation.
            payload_store: Payload store, for run fan-out's baseline sessions.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._plugin_repository = plugin_repository
        self._experiment_runs = experiment_run_repository
        self._agents = agent_repository
        self._cohort_versions = cohort_version_repository
        self._sessions = session_repository
        self._agent_versions = agent_version_repository
        self._replays = replay_repository
        self._jobs = job_repository
        self._tasks = task_repository
        self._evaluations = evaluation_repository
        self._transitions = transitions
        self._payload_store = payload_store
        self._analytics = analytics

    async def _create_replay_config(
        self,
        owner_id: uuid.UUID,
        override: ReplayOverride | None,
        tool_policy: ToolPolicy,
        evaluators: list[EvaluatorConfig],
    ) -> ReplayConfig:
        """Build and persist a replay config.

        Args:
            owner_id: Id of the owning account.
            override: Replay override, if any.
            tool_policy: Tool policy.
            evaluators: Resolved evaluator configs.

        Returns:
            Stored replay config with timestamps set.
        """
        config = ReplayConfig(
            owner_id=owner_id,
            override=override,
            tool_policy=tool_policy,
            evaluators=evaluators,
        )
        return await self._repository.create_replay_config(config)

    async def _delete_replay_config_if_unreferenced(
        self, replay_config_id: uuid.UUID
    ) -> None:
        """Delete a replay config unless a replay still references it.

        Args:
            replay_config_id: Id of the replay config.
        """
        with contextlib.suppress(ReplayConfigInUse):
            await self._repository.delete_replay_config(replay_config_id)

    async def create_experiment(
        self, command: ExperimentCreate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Create an experiment and its replay config, owned by the caller.

        Args:
            command: Fields for the new experiment and its replay config.
            actor: Caller context.

        Raises:
            AgentNotFound: No agent has the command's agent id.
            PluginNotFound: An evaluator config names an unknown evaluator.
            PluginVersionNotFound: An evaluator config names an unknown
                version.
            ValidationError: An evaluator config is scoped to another agent,
                or two evaluator configs resolve to the same evaluator
                version.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Created experiment and its replay config.
        """
        await self._agents.get(command.agent_id)
        evaluators = await validate_evaluators(
            command.evaluators, self._plugin_repository, command.agent_id, actor
        )
        config = await self._create_replay_config(
            actor.account.id,
            command.override,
            command.tool_policy or default_tool_policy(),
            evaluators,
        )
        experiment = Experiment(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            agent_id=command.agent_id,
            replay_config_id=config.id,
        )
        experiment = await self._repository.create(experiment)
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.EXPERIMENT_CREATED,
                analytics_events.build_experiment_created_properties(config),
            )
        return experiment, config

    async def get_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Get an experiment and its replay config by id.

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
        config = await self._repository.get_replay_config(experiment.replay_config_id)
        return experiment, config

    async def list_experiments(
        self, experiment_filter: ExperimentFilter, actor: AuthContext
    ) -> tuple[list[tuple[Experiment, ReplayConfig]], str | None]:
        """List experiments matching a filter, each with its replay config.

        Args:
            experiment_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching experiments paired with their replay configs,
            and the next cursor.
        """
        _ = actor
        experiments, next_cursor = await self._repository.query(experiment_filter)
        configs = await self._repository.get_many_replay_configs(
            [experiment.replay_config_id for experiment in experiments]
        )
        # Skip experiments whose replay config a concurrent delete removed
        # between the two reads.
        pairs = [
            (experiment, configs[experiment.replay_config_id])
            for experiment in experiments
            if experiment.replay_config_id in configs
        ]
        return pairs, next_cursor

    async def update_experiment(
        self, experiment_id: uuid.UUID, command: ExperimentUpdate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Partially update an experiment and, if touched, its replay config.

        When the command sets any of ``override``, ``tool_policy``, or
        ``evaluators``, a new replay config row replaces the current one,
        rejected when the experiment already has runs. The old row is
        deleted unless a replay still references it.

        Args:
            experiment_id: Id of the experiment.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
            PluginNotFound: A new evaluator config names an unknown
                evaluator.
            PluginVersionNotFound: A new evaluator config names an unknown
                version.
            ValidationError: The command clears the experiment name, the
                tool policy, or every evaluator, a new evaluator config is
                scoped to another agent, or two evaluator configs resolve to
                the same evaluator version.
            DuplicateExperimentName: The experiment name is already
                registered.
            ExperimentFrozen: The experiment has runs and the command
                touches its replay config.

        Returns:
            Updated experiment and its current replay config.
        """
        experiment = await self._repository.get(experiment_id)
        fields = command.model_fields_set
        if "name" in fields:
            if command.name is None:
                raise ValidationError("Experiment name cannot be cleared")
            experiment.update_name(command.name)
        if "description" in fields:
            experiment.update_description(command.description)

        config_fields = {"override", "tool_policy", "evaluators"} & fields
        if not config_fields:
            experiment = await self._repository.update(experiment)
            config = await self._repository.get_replay_config(
                experiment.replay_config_id
            )
            return experiment, config

        has_runs = await self._experiment_runs.exists_for_experiment(experiment_id)
        current_config = await self._repository.get_replay_config(
            experiment.replay_config_id
        )
        evaluators = current_config.evaluators
        if "evaluators" in config_fields:
            if not command.evaluators:
                raise ValidationError("Experiment evaluators cannot be cleared")
            evaluators = await validate_evaluators(
                command.evaluators, self._plugin_repository, experiment.agent_id, actor
            )
        tool_policy = current_config.tool_policy
        if "tool_policy" in config_fields:
            if command.tool_policy is None:
                raise ValidationError("Experiment tool policy cannot be cleared")
            tool_policy = command.tool_policy
        override = current_config.override
        if "override" in config_fields:
            override = command.override

        new_config = await self._create_replay_config(
            experiment.owner_id, override, tool_policy, evaluators
        )
        old_config_id = experiment.replay_config_id
        experiment.update_replay_config_id(new_config.id, has_runs)
        experiment = await self._repository.update(experiment)
        await self._delete_replay_config_if_unreferenced(old_config_id)
        return experiment, new_config

    async def delete_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an experiment, its runs, their replays, and its replay config.

        The experiment row's own delete cascades the runs and their replays,
        and the jobs that ran them stay in place.

        Args:
            experiment_id: Id of the experiment.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
        """
        _ = actor
        # Lock the experiment so a concurrent start_run, which locks it too,
        # either commits its run before this snapshot and has that run's jobs
        # cancelled, or finds the experiment gone. Without the lock it could
        # slip a run in after the snapshot whose jobs then outlive the cascade.
        experiment = await self._repository.get(experiment_id, exclusive=True)
        runs = await self._experiment_runs.list_by_experiment(experiment_id)
        job_ids: list[uuid.UUID] = []
        for run in runs:
            replays = await self._replays.list_by_experiment_run(run.id)
            job_ids.extend(
                replay.job_id for replay in replays if replay.job_id is not None
            )
        # Settle without publishing JobsSettled. Its subscribers lock the
        # replay and run rows in the opposite order of the delete cascade.
        await self._transitions.request_jobs_cancel(job_ids, dispatch_settled=False)
        await self._repository.delete(experiment_id)
        await self._repository.delete_replay_config(experiment.replay_config_id)

    async def _resolve_cohort_version_sessions(
        self, cohort_version_id: uuid.UUID
    ) -> list[Session]:
        """Read every member session of a cohort version.

        Args:
            cohort_version_id: Id of the cohort version.

        Returns:
            Member sessions.
        """
        membership = FilterCondition(
            field="cohort_version_id", op=FilterOp.EQ, value=cohort_version_id
        )
        # Replay seeding reads each baseline's inputs, so the member
        # sessions are loaded with payloads.
        return await paginate_all(
            lambda cursor: self._sessions.query(
                SessionFilter(expression=membership, cursor=cursor, size=1000),
                include_payloads=True,
            )
        )

    async def start_run(
        self,
        experiment_id: uuid.UUID,
        command: ExperimentRunCreate,
        actor: AuthContext,
    ) -> tuple[ExperimentRun, ReplayStatusCounts]:
        """Start an experiment run, fanning out one replay per cohort version session.

        Every replay points at the experiment's replay config and the run's
        id. The run number is server-assigned per experiment, computed under
        a lock of the experiment row.

        Args:
            experiment_id: Id of the experiment.
            command: Cohort version, agent version, and baseline evaluation
                mode for the run.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
            CohortVersionIdNotFound: No cohort version has the given id.
            ValidationError: The cohort version has no sessions, belongs to
                a cohort of another agent, the resolved agent version has no
                run spec, or the config carries an override or tool policy
                the agent version's replay capabilities do not declare.
            SessionNotEvaluatable: ``baseline_evaluation_mode`` is not
                ``NONE`` and a cohort version session is in progress.
            AgentVersionNotFound: No agent version has the given id.
            AgentVersionAgentMismatch: The agent version belongs to another
                agent.

        Returns:
            Created run and its replay counts by status.
        """
        experiment = await self._repository.get(experiment_id, exclusive=True)
        # Locked so a concurrent delete of this version cannot land between
        # the read and the run row that references it.
        cohort_version = await self._cohort_versions.get(
            command.cohort_version_id, exclusive=True
        )
        if cohort_version.session_count == 0:
            raise ValidationError(f"Cohort version {cohort_version.id} has no sessions")
        cohort_agent_id = await self._cohort_versions.get_agent_id(cohort_version.id)
        if cohort_agent_id != experiment.agent_id:
            raise ValidationError(
                f"Cohort version {cohort_version.id} does not belong to agent "
                f"{experiment.agent_id}"
            )
        agent_version = await resolve_runnable_agent_version(
            command.agent_version_id, self._agent_versions, experiment.agent_id
        )
        config = await self._repository.get_replay_config(experiment.replay_config_id)
        run_spec = agent_version.run_spec
        assert run_spec is not None
        config.check_capabilities(run_spec.replay_capabilities)
        sessions = await self._resolve_cohort_version_sessions(cohort_version.id)

        number = await self._experiment_runs.get_max_number(experiment_id) + 1
        run = ExperimentRun(
            owner_id=actor.account.id,
            experiment_id=experiment_id,
            number=number,
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
            baseline_evaluation_mode=command.baseline_evaluation_mode,
        )
        run.start(datetime.now(UTC))
        run = await self._experiment_runs.create(run)

        await create_replay_pipelines(
            baselines=sessions,
            agent_version_id=agent_version.id,
            config=config,
            baseline_evaluation_mode=command.baseline_evaluation_mode,
            experiment_run_id=run.id,
            actor=actor,
            replay_repository=self._replays,
            job_repository=self._jobs,
            task_repository=self._tasks,
            evaluation_repository=self._evaluations,
            payload_store=self._payload_store,
        )
        counts = await self._replays.count_by_status(run.id)
        return run, counts
