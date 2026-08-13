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
"""Replay repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.replay import ReplayFilter, ReplayStatusCounts
from kitaru.server.domain.replay import Replay


class ReplayRepository(Protocol):
    """Replay persistence operations."""

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            DuplicateReplayForBaseline: The run already holds a replay for
                this baseline session.
            ReplayAlreadyExistsForJob: The job already has a replay.

        Returns:
            Stored replay with timestamps set.
        """
        ...

    async def create_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist many new replays in one round trip, skipping constraint translation.

        Args:
            replays: Replays to store.

        Returns:
            Stored replays with timestamps set, in the same order.
        """
        ...

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        ...

    async def get_by_job_id(self, job_id: uuid.UUID) -> Replay | None:
        """Load the replay owning a job, if any.

        Args:
            job_id: Id of the job.

        Returns:
            Stored replay, or ``None`` when the job holds no replay.
        """
        ...

    async def get_many_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Bulk-load the replay of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Replays keyed by job id, jobs without a replay omitted.
        """
        ...

    async def query(
        self, replay_filter: ReplayFilter
    ) -> tuple[list[Replay], str | None]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the next cursor.
        """
        ...

    async def list_by_experiment_run(
        self, experiment_run_id: uuid.UUID
    ) -> list[Replay]:
        """Load every replay of an experiment run.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replays of the run, in creation order.
        """
        ...

    async def update(self, replay: Replay) -> Replay:
        """Persist changes to an existing replay.

        Args:
            replay: Replay with modified fields.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay with the updated timestamp renewed.
        """
        ...

    async def update_many(self, replays: list[Replay]) -> list[Replay]:
        """Persist changes to many existing replays in one round trip.

        Args:
            replays: Replays with modified fields.

        Raises:
            ReplayNotFound: A replay id matches no replay.

        Returns:
            Stored replays with the updated timestamp renewed, in the same
            order.
        """
        ...

    async def count_by_status(self, experiment_run_id: uuid.UUID) -> ReplayStatusCounts:
        """Count an experiment run's replays by status.

        Args:
            experiment_run_id: Id of the run.

        Returns:
            Replay counts by status.
        """
        ...

    async def count_by_status_many(
        self, experiment_run_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayStatusCounts]:
        """Bulk-count replays by status for many experiment runs.

        Args:
            experiment_run_ids: Ids of the runs.

        Returns:
            Replay status counts keyed by run id, missing ids holding zero
            counts.
        """
        ...

    async def exists_for_replay_config(self, replay_config_id: uuid.UUID) -> bool:
        """Report whether any replay references a replay config.

        Args:
            replay_config_id: Id of the replay config.

        Returns:
            Whether a replay references the replay config.
        """
        ...
