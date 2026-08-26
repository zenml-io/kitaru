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
"""Job repository interface."""

import uuid
from collections.abc import Sequence
from typing import Protocol

from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.job import Job


class JobRepository(Protocol):
    """Job persistence operations."""

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Returns:
            Stored job with timestamps set.
        """
        ...

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist many new jobs in one round trip.

        Args:
            jobs: Jobs to store.

        Returns:
            Stored jobs with timestamps set, in the same order.
        """
        ...

    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        ...

    async def get_many(self, job_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Job]:
        """Bulk-load jobs by id, keyed by id, missing ids omitted.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Stored jobs keyed by id.
        """
        ...

    async def get_many_locked(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Job]:
        """Bulk-lock and load jobs by id, keyed by id, missing ids omitted.

        Rows are locked in id order in one statement.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Locked jobs keyed by id.
        """
        ...

    async def list_unpropagated_cancel_ids(self, limit: int) -> list[uuid.UUID]:
        """Read the ids of canceling jobs whose live tasks still owe the stamp.

        A live task owes the stamp when it carries no cancel request of its
        own, or when it is still pending and has to move straight to
        canceled. Rows are read without locking.

        Args:
            limit: Maximum number of ids to read.

        Returns:
            Ids of the canceling jobs in ascending order.
        """
        ...

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]:
        """Query jobs matching a filter.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the next cursor.
        """
        ...

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        ...

    async def update_many(self, jobs: list[Job]) -> list[Job]:
        """Persist changes to many existing jobs in one round trip.

        Args:
            jobs: Jobs with modified fields.

        Raises:
            JobNotFound: A job id matches no job.

        Returns:
            Stored jobs with the updated timestamp renewed, in the same order.
        """
        ...

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id, cascading its tasks.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        ...
