"""Experiment use cases."""

import uuid

from kitaru.server.application.agent_version_resolution import resolve_agent_version
from kitaru.server.application.evaluator_resolution import validate_evaluators
from kitaru.server.application.interfaces.agent_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import CohortRepository
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
    ExperimentRunRepository,
)
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
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.application.replay_pipeline import create_replay_pipeline
from kitaru.server.application.services.job_service import JobService
from kitaru.server.domain.base import ConflictError, ValidationError
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
)
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentService:
    """Create experiments and fan them out over cohort snapshots."""

    def __init__(
        self,
        repository: ExperimentRepository,
        run_repository: ExperimentRunRepository,
        cohort_repository: CohortRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        replay_repository: ReplayRepository,
        task_repository: TaskRepository,
        job_service: JobService,
    ) -> None:
        """Initialize the service."""
        self._repository = repository
        self._run_repository = run_repository
        self._cohort_repository = cohort_repository
        self._session_repository = session_repository
        self._agent_version_repository = agent_version_repository
        self._plugin_repository = plugin_repository
        self._replay_repository = replay_repository
        self._task_repository = task_repository
        self._job_service = job_service

    async def create_experiment(
        self, command: ExperimentCreate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Create an experiment and its reusable replay configuration."""
        evaluators = await validate_evaluators(
            command.evaluators, self._plugin_repository
        )
        config = ReplayConfig(
            owner_id=actor.account.id,
            override=command.override,
            tool_policy=command.tool_policy,
            evaluators=evaluators,
        )
        experiment = Experiment(
            owner_id=actor.account.id,
            name=command.name,
            description=command.description,
            replay_config_id=config.id,
        )
        return await self._repository.create(experiment, config)

    async def get_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Get an experiment with its inline replay configuration."""
        _ = actor
        experiment = await self._repository.get(experiment_id)
        config = await self._repository.get_config(experiment.replay_config_id)
        return experiment, config

    async def list_experiments(
        self, experiment_filter: ExperimentFilter, actor: AuthContext
    ) -> tuple[list[tuple[Experiment, ReplayConfig]], str | None]:
        """List experiments with their inline replay configurations."""
        _ = actor
        experiments, cursor = await self._repository.query(experiment_filter)
        items = [
            (
                experiment,
                await self._repository.get_config(experiment.replay_config_id),
            )
            for experiment in experiments
        ]
        return items, cursor

    async def update_experiment(
        self,
        experiment_id: uuid.UUID,
        command: ExperimentUpdate,
        actor: AuthContext,
    ) -> tuple[Experiment, ReplayConfig]:
        """Partially update a mutable experiment definition."""
        _ = actor
        experiment = await self._repository.get(experiment_id)
        config = await self._repository.get_config(experiment.replay_config_id)
        fields = command.model_fields_set
        if "name" in fields:
            if command.name is None:
                raise ValidationError("Experiment name cannot be null")
            experiment.update_name(command.name)
        if "description" in fields:
            experiment.update_description(command.description)
        config_fields = {"override", "tool_policy", "evaluators"} & fields
        if config_fields:
            runs, _ = await self._run_repository.query(
                ExperimentRunFilter(experiment_id=experiment.id, size=1)
            )
            if runs:
                raise ConflictError(
                    f"Experiment {experiment.id} configuration is frozen"
                )
        if "override" in fields:
            config.override = command.override
        if "tool_policy" in fields:
            if command.tool_policy is None:
                raise ValidationError("Experiment tool policy cannot be null")
            config.tool_policy = command.tool_policy
        if "evaluators" in fields:
            if command.evaluators is None:
                raise ValidationError("Experiment evaluators cannot be null")
            config.evaluators = await validate_evaluators(
                command.evaluators, self._plugin_repository
            )
        return await self._repository.update(experiment, config)

    async def delete_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an experiment."""
        _ = actor
        await self._repository.delete(experiment_id)

    async def start_run(
        self,
        experiment_id: uuid.UUID,
        cohort_id: uuid.UUID,
        agent_version_id: uuid.UUID,
        evaluate_baselines: bool,
        actor: AuthContext,
    ) -> tuple[ExperimentRun, ExperimentRunProgress]:
        """Create one replay pipeline for every cohort member."""
        experiment = await self._repository.get(experiment_id)
        config = await self._repository.get_config(experiment.replay_config_id)
        cohort = await self._cohort_repository.get(cohort_id)
        session_ids = await self._cohort_repository.get_session_ids(cohort.id)
        if not session_ids:
            raise ValidationError("An experiment run requires a non-empty cohort")
        sessions_by_id = await self._session_repository.get_many(session_ids)
        if len(sessions_by_id) != len(session_ids):
            raise ValidationError("One or more cohort sessions were not found")
        version = await resolve_agent_version(
            agent_version_id, self._agent_version_repository
        )
        if version.agent_id != cohort.agent_id:
            raise ValidationError(
                f"Agent version {version.id} does not belong to cohort agent "
                f"{cohort.agent_id}"
            )
        run = ExperimentRun(
            owner_id=actor.account.id,
            experiment_id=experiment.id,
            number=await self._repository.next_run_number(experiment.id),
            cohort_id=cohort.id,
            agent_version_id=version.id,
            evaluate_baselines=evaluate_baselines,
        )
        run.start()
        run = await self._run_repository.create(run)
        for session_id in session_ids:
            await create_replay_pipeline(
                owner_id=actor.account.id,
                baseline_session=sessions_by_id[session_id],
                agent_version=version,
                evaluators=config.evaluators,
                evaluate_baselines=evaluate_baselines,
                job_service=self._job_service,
                replay_repository=self._replay_repository,
                task_repository=self._task_repository,
                override=config.override,
                tool_policy=config.tool_policy,
                experiment_run_id=run.id,
            )
        return run, await self._run_repository.progress(run.id)
