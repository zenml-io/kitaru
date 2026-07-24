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
from datetime import datetime
from typing import Protocol

from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.domain.job import Job, JobStatus


class JobRepository(Protocol):
    """Job persistence operations."""

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Raises:
            ExperimentRunNotFound: No experiment run has the job's
                experiment run id.
            ReplayConfigNotFound: No replay config has the job's job
                config id.
            AgentVersionNotFound: No agent version has the job's agent
                version id.
            SessionNotFound: No session has the job's original session
                id.
            DuplicateReplaySession: The run already replays the original
                session.

        Returns:
            Stored job with timestamps set.
        """
        ...

    async def get(self, job_id: uuid.UUID) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        ...

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], int]:
        """Query jobs matching a filter.

        With the staleness context set, the status filter matches claimed
        or running jobs with lost heartbeats as pending, or as timed
        out once the attempt count reached the maximum.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the total match count.
        """
        ...

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.
            SessionNotFound: No session has the job's result session id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        ...

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        ...

    async def requeue_stale(
        self, run_id: uuid.UUID, stale_before: datetime, max_attempts: int
    ) -> None:
        """Requeue or time out a run's jobs with lost heartbeats.

        A claimed or running job whose last heartbeat, or claim when no
        heartbeat arrived yet, is older than the threshold goes back to
        pending with the attempt incremented, or to timed out once the
        attempt count reached the maximum.

        Args:
            run_id: Id of the experiment run.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.
        """
        ...

    async def claim_pending(
        self, run_id: uuid.UUID, worker_id: str, limit: int
    ) -> list[Job]:
        """Atomically claim pending jobs of a run for a worker.

        Rows locked by a concurrent claim are skipped, so parallel workers
        never double-claim.

        Args:
            run_id: Id of the experiment run.
            worker_id: Id of the claiming worker.
            limit: Maximum number of jobs to claim.

        Returns:
            Claimed jobs.
        """
        ...

    async def count_by_status(
        self, run_ids: list[uuid.UUID], stale_before: datetime, max_attempts: int
    ) -> dict[uuid.UUID, dict[JobStatus, int]]:
        """Count jobs by status for a set of experiment runs.

        Claimed or running jobs with lost heartbeats count as pending,
        or as timed out once the attempt count reached the maximum, without
        writing.

        Args:
            run_ids: Ids of the experiment runs.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.

        Returns:
            Job counts by status, keyed by experiment run id.
        """
        ...

    async def references_agent_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored job references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored job references the version.
        """
        ...
