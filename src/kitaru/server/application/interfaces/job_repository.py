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
from datetime import datetime
from typing import Protocol

from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.domain.job import Job, JobStatus, WorkerScope


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
            SessionNotFound: No session has the job's input session id.
            DuplicateReplaySession: The run already replays the input
                session.

        Returns:
            Stored job with timestamps set.
        """
        ...

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist new jobs as one batch.

        Args:
            jobs: Jobs to store.

        Raises:
            DuplicateScoreJob: A parent job already scores an input
                session with a scorer.
            AgentVersionNotFound: No agent version has a job's agent
                version id.
            PluginVersionIdNotFound: No plugin version has a job's plugin
                version id.
            SessionNotFound: No session has a job's input session id.

        Returns:
            Stored jobs with timestamps set.
        """
        ...

    async def get(self, job_id: uuid.UUID, for_update: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            for_update: Lock the row for the transaction.

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

    async def list_children(self, parent_job_id: uuid.UUID) -> list[Job]:
        """Load every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.

        Returns:
            Child jobs in id order.
        """
        ...

    async def list_children_many(
        self, parent_job_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, list[Job]]:
        """Load every job fanned out from a set of parent jobs.

        Args:
            parent_job_ids: Ids of the parent jobs.

        Returns:
            Child jobs keyed by parent job id, parents without children
            omitted.
        """
        ...

    async def delete_children(self, parent_job_id: uuid.UUID) -> None:
        """Delete every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.
        """
        ...

    async def requeue_stale(
        self, stale_before: datetime, max_attempts: int, scope: WorkerScope
    ) -> list[Job]:
        """Requeue or time out jobs with lost heartbeats within a scope.

        A claimed or running job whose last heartbeat, or claim when no
        heartbeat arrived yet, is older than the threshold goes back to
        pending with the attempt incremented, or to timed out once the
        attempt count reached the maximum.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.
            scope: Claim scope.

        Returns:
            Jobs the staleness rule moved.
        """
        ...

    async def claim_pending(
        self, worker_id: uuid.UUID, limit: int, scope: WorkerScope
    ) -> list[Job]:
        """Atomically claim pending jobs within a scope for a worker.

        Rows locked by a concurrent claim are skipped, so parallel workers
        never double-claim. With a job id the scope is that job and its
        children, with an experiment run id it is that run's jobs and
        their children, without either it is pool-target work. Agent
        version ids keep only jobs of those versions and jobs without a
        version, kinds keep only jobs of those kinds.

        Args:
            worker_id: Id of the claiming worker.
            limit: Maximum number of jobs to claim.
            scope: Claim scope.

        Returns:
            Claimed jobs.
        """
        ...

    async def heartbeat_many(
        self,
        worker_id: uuid.UUID,
        job_ids: Sequence[uuid.UUID],
        heartbeat_at: datetime,
    ) -> list[Job]:
        """Record one worker heartbeat on every claimed or running job it owns.

        Reported jobs the worker no longer owns, and jobs that already went
        terminal, stay untouched and are absent from the result.

        Args:
            worker_id: Id of the heartbeating worker.
            job_ids: Ids of the jobs the worker reports.
            heartbeat_at: Time of the heartbeat.

        Returns:
            Jobs the heartbeat reached.
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
