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
from typing import Protocol

from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.replay import Replay


class ReplayRepository(Protocol):
    """Replay persistence operations."""

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            JobNotFound: No job has the replay's job id.
            ReplayConfigNotFound: No replay config has the replay's replay
                config id.
            SessionNotFound: No session has the replay's input session id.
            DuplicateReplayJob: The job already has a replay.

        Returns:
            Stored replay with timestamps set.
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

    async def get_by_job(self, job_id: uuid.UUID) -> Replay:
        """Load the replay of a job.

        Args:
            job_id: Id of the job.

        Raises:
            ReplayJobNotFound: The job has no replay.

        Returns:
            Stored replay.
        """
        ...

    async def get_many_by_jobs(
        self, job_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, Replay]:
        """Load replays by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Stored replays keyed by job id, jobs without a replay omitted.
        """
        ...

    async def query(self, replay_filter: ReplayFilter) -> tuple[list[Replay], int]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the total match count.
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
