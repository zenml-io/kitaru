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

from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.application.services.evaluator_resolution import (
    validate_evaluators,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
    default_tool_policy,
)


class ExperimentService:
    """Experiment use cases."""

    def __init__(
        self, repository: ExperimentRepository, plugin_repository: PluginRepository
    ) -> None:
        """Initialize the service.

        Args:
            repository: Experiment and replay config repository.
            plugin_repository: Plugin repository, for evaluator resolution.
        """
        self._repository = repository
        self._plugin_repository = plugin_repository

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

    async def create_experiment(
        self, command: ExperimentCreate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Create an experiment and its replay config, owned by the caller.

        Args:
            command: Fields for the new experiment and its replay config.
            actor: Caller context.

        Raises:
            PluginNotFound: An evaluator config names an unknown evaluator.
            PluginVersionNotFound: An evaluator config names an unknown
                version.
            ValidationError: Two evaluator configs resolve to the same
                evaluator version.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Created experiment and its replay config.
        """
        evaluators = await validate_evaluators(
            command.evaluators, self._plugin_repository, actor
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
            replay_config_id=config.id,
        )
        experiment = await self._repository.create(experiment)
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
        pairs = [
            (experiment, configs[experiment.replay_config_id])
            for experiment in experiments
        ]
        return pairs, next_cursor

    async def update_experiment(
        self, experiment_id: uuid.UUID, command: ExperimentUpdate, actor: AuthContext
    ) -> tuple[Experiment, ReplayConfig]:
        """Partially update an experiment and, if touched, its replay config.

        When the command sets any of ``override``, ``tool_policy``, or
        ``evaluators``, a new replay config row replaces the current one and
        the old row is deleted, since nothing else references it yet.

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
                tool policy, or every evaluator, or two evaluator configs
                resolve to the same evaluator version.
            DuplicateExperimentName: The experiment name is already
                registered.

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

        current_config = await self._repository.get_replay_config(
            experiment.replay_config_id
        )
        evaluators = current_config.evaluators
        if "evaluators" in config_fields:
            if not command.evaluators:
                raise ValidationError("Experiment evaluators cannot be cleared")
            evaluators = await validate_evaluators(
                command.evaluators, self._plugin_repository, actor
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
        experiment.update_replay_config_id(new_config.id)
        experiment = await self._repository.update(experiment)
        await self._repository.delete_replay_config(old_config_id)
        return experiment, new_config

    async def delete_experiment(
        self, experiment_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an experiment and its replay config.

        Args:
            experiment_id: Id of the experiment.
            actor: Caller context.

        Raises:
            ExperimentNotFound: No experiment has this id.
        """
        _ = actor
        experiment = await self._repository.get(experiment_id)
        await self._repository.delete(experiment_id)
        await self._repository.delete_replay_config(experiment.replay_config_id)
