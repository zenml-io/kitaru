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
"""Experiment repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.experiments import ExperimentFilter
from kitaru.server.domain.experiment import Experiment


class ExperimentRepository(Protocol):
    """Experiment persistence operations."""

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

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
    ) -> tuple[list[Experiment], int]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the total match count.
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
            CohortNotFound: No cohort has the experiment's cohort id.
            ReplayConfigNotFound: No replay config has the experiment's
                replay config id.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        ...

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id, including its tag links.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        ...
