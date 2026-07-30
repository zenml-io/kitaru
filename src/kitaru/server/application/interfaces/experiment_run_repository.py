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
"""Experiment run repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.experiment_run import ExperimentRun


class ExperimentRunRepository(Protocol):
    """Experiment run persistence operations."""

    async def create(self, run: ExperimentRun) -> ExperimentRun:
        """Persist a new experiment run.

        Args:
            run: Experiment run to store.

        Raises:
            DuplicateExperimentRunNumber: The experiment already has a run
                with this number.

        Returns:
            Stored experiment run with timestamps set.
        """
        ...

    async def get(
        self, experiment_run_id: uuid.UUID, exclusive: bool = False
    ) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            experiment_run_id: Id of the run.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run.
        """
        ...

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching runs and the next cursor.
        """
        ...

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        ...

    async def delete(self, experiment_run_id: uuid.UUID) -> None:
        """Delete an experiment run by id.

        Args:
            experiment_run_id: Id of the run.

        Raises:
            ExperimentRunNotFound: No run has this id.
        """
        ...

    async def get_max_number(self, experiment_id: uuid.UUID) -> int:
        """Read the highest run number an experiment has assigned.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Highest assigned run number, or 0 when the experiment has no runs.
        """
        ...

    async def exists_for_experiment(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has any run.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            Whether the experiment has any run.
        """
        ...
