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

from kitaru.server.application.models.experiment_runs import ExperimentRunFilter
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.job import Replay


class ExperimentRunRepository(Protocol):
    """Experiment run persistence operations."""

    async def create(self, run: ExperimentRun, jobs: list[Replay]) -> ExperimentRun:
        """Persist a new experiment run with its jobs as one batch.

        Assigns the next per-experiment run number.

        Args:
            run: Experiment run to store.
            jobs: Jobs to store with the run.

        Raises:
            ExperimentNotFound: No experiment has the run's experiment id.

        Returns:
            Stored experiment run with the number and timestamps set.
        """
        ...

    async def get(self, run_id: uuid.UUID) -> ExperimentRun:
        """Load an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run.
        """
        ...

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], int]:
        """Query experiment runs matching a filter.

        Args:
            run_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiment runs and the total match count.
        """
        ...

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        """Persist changes to an existing experiment run.

        Args:
            run: Experiment run with modified fields.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.

        Returns:
            Stored experiment run with the updated timestamp renewed.
        """
        ...

    async def delete(self, run_id: uuid.UUID) -> None:
        """Delete an experiment run by id, including its jobs and tag links.

        Args:
            run_id: Id of the experiment run.

        Raises:
            ExperimentRunNotFound: No experiment run has this id.
        """
        ...

    async def has_runs(self, experiment_id: uuid.UUID) -> bool:
        """Report whether an experiment has stored runs.

        Args:
            experiment_id: Id of the experiment.

        Returns:
            ``True`` when a stored run belongs to the experiment.
        """
        ...
