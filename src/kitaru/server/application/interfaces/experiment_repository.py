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
"""Experiment and replay config repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentRepository(Protocol):
    """Experiment and replay config persistence operations."""

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with timestamps set.
        """
        ...

    async def get(self, experiment_id: uuid.UUID) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        ...

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the next cursor.
        """
        ...

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        ...

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
        """
        ...

    async def create_replay_config(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        ...

    async def get_replay_config(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        ...

    async def get_many_replay_configs(
        self, config_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id, in one bulk fetch.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Replay configs keyed by id, missing ids omitted.
        """
        ...

    async def delete_replay_config(self, config_id: uuid.UUID) -> None:
        """Delete a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.
        """
        ...
