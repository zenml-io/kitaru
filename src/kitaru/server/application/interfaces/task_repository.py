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
"""Task repository interface."""

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.application.models.task import TaskFilter, TaskSettlementStats
from kitaru.server.domain.task import Task


class TaskRepository(Protocol):
    """Task persistence operations."""

    async def create(self, task: Task) -> Task:
        """Persist a new task.

        Args:
            task: Task to store.

        Raises:
            DuplicateEvaluationTask: The job already holds an evaluator task
                for this input session and plugin version.

        Returns:
            Stored task with timestamps set.
        """
        ...

    async def create_many(self, tasks: list[Task]) -> list[Task]:
        """Persist many new tasks in one round trip.

        Args:
            tasks: Tasks to store.

        Returns:
            Stored tasks with timestamps set, in the same order.
        """
        ...

    async def get(self, task_id: uuid.UUID, exclusive: bool = False) -> Task:
        """Load a task by id.

        Args:
            task_id: Id of the task.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task.
        """
        ...

    async def get_many(self, task_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Task]:
        """Bulk-load tasks by id, keyed by id, missing ids omitted.

        Args:
            task_ids: Ids of the tasks to load.

        Returns:
            Stored tasks keyed by id.
        """
        ...

    async def query(self, task_filter: TaskFilter) -> tuple[list[Task], str | None]:
        """Query tasks matching a filter.

        ``stale_before`` matches in-flight tasks whose last heartbeat, or
        claim time when they never heartbeated, is older than the bound.

        Args:
            task_filter: Filter and pagination parameters.

        Returns:
            Page of matching tasks and the next cursor.
        """
        ...

    async def list_by_job(self, job_id: uuid.UUID) -> list[Task]:
        """Load every task of a job, ordered by id.

        Args:
            job_id: Id the tasks belong to.

        Returns:
            Tasks of the job in creation order.
        """
        ...

    async def list_by_jobs(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, list[Task]]:
        """Bulk-load every task of many jobs, grouped by job id in creation order.

        Args:
            job_ids: Ids the tasks belong to.

        Returns:
            Tasks keyed by job id in creation order, jobs without tasks
            omitted.
        """
        ...

    async def count_settlement_stats(self, job_id: uuid.UUID) -> TaskSettlementStats:
        """Count a job's tasks into the stats driving its settlement.

        Args:
            job_id: Id the tasks belong to.

        Returns:
            Task settlement stats, zero counts when the job has no tasks.
        """
        ...

    async def count_settlement_stats_many(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, TaskSettlementStats]:
        """Bulk-count many jobs' tasks into the stats driving their settlement.

        Args:
            job_ids: Ids the tasks belong to.

        Returns:
            Task settlement stats keyed by job id, jobs without tasks
            omitted.
        """
        ...

    async def update(self, task: Task) -> Task:
        """Persist changes to an existing task.

        Args:
            task: Task with modified fields.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task with the updated timestamp renewed.
        """
        ...

    async def claim_pending(
        self, scope: WorkerScope, worker_id: uuid.UUID, limit: int, now: datetime
    ) -> list[Task]:
        """Hand pending tasks matching a scope to a worker, oldest first.

        Rows are locked with ``FOR UPDATE SKIP LOCKED``, so concurrent claims
        never hand the same task to two workers and never block on each
        other.

        Args:
            scope: Claim scope narrowing the queue.
            worker_id: Worker claiming the tasks.
            limit: Maximum number of tasks to claim.
            now: Current time.

        Returns:
            Claimed tasks carrying their incremented attempt.
        """
        ...

    async def claim_stale(self, task_id: uuid.UUID, cutoff: datetime) -> Task | None:
        """Lock one task by id if it is still in flight and older than a cutoff.

        The row is locked with ``FOR UPDATE SKIP LOCKED``, so concurrent
        sweeps take disjoint tasks. Staleness is re-checked on the locked
        row because the candidate read ran unlocked.

        Args:
            task_id: Id of the candidate task.
            cutoff: Bound the last heartbeat must be older than.

        Returns:
            Locked stale task, or ``None`` when it is contended or no longer
            stale.
        """
        ...

    async def list_stale_ids(self, cutoff: datetime, limit: int) -> list[uuid.UUID]:
        """Read the ids of in-flight tasks whose last heartbeat is older than a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of ids to read.

        Returns:
            Ids of the stale tasks in ascending order.
        """
        ...

    async def stamp_heartbeats(
        self, task_ids: Sequence[uuid.UUID], worker_id: uuid.UUID, now: datetime
    ) -> tuple[dict[uuid.UUID, datetime | None], set[uuid.UUID]]:
        """Stamp heartbeat_at on the worker's in-flight tasks among the ids.

        Writes only the heartbeat column, so a stamp cannot overwrite fields
        concurrent writers committed since the caller last read the tasks. A
        row locked by another open transaction is left unstamped rather than
        waited on.

        Args:
            task_ids: Candidate task ids.
            worker_id: Worker that must still hold the tasks.
            now: Current time.

        Returns:
            Cancel request time of the task, falling back to its job's, by id
            for every stamped task, and the owned in-flight candidates whose
            lock was held elsewhere and so were left unstamped.
        """
        ...

    async def lock_by_jobs(
        self, job_ids: Sequence[uuid.UUID], nowait: bool = False
    ) -> None:
        """Lock the jobs' non-terminal task rows in id order.

        Args:
            job_ids: Ids the tasks belong to.
            nowait: Whether to fail instead of waiting when another
                transaction holds one of the rows.
        """
        ...

    async def stamp_cancel_requested(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> None:
        """Stamp cancel_requested_at on the jobs' non-terminal tasks lacking it.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.
        """
        ...

    async def cancel_pending(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> list[Task]:
        """Move each still-pending task of the jobs straight to canceled.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.

        Returns:
            Canceled tasks.
        """
        ...

    async def get_scored_evaluator_version_ids(
        self, input_session_id: uuid.UUID
    ) -> set[uuid.UUID]:
        """Read the evaluator versions that already completed against a session.

        Args:
            input_session_id: Id of the scored session.

        Returns:
            Plugin version ids of every completed evaluator task scoring the
            session.
        """
        ...

    async def get_scored_evaluator_version_ids_many(
        self, input_session_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, set[uuid.UUID]]:
        """Read the evaluator versions that already completed against each session.

        Args:
            input_session_ids: Ids of the scored sessions.

        Returns:
            Plugin version ids of every completed evaluator task scoring the
            session, keyed by session id, sessions without one omitted.
        """
        ...

    async def get_agent_tasks_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Task]:
        """Bulk-load the agent task of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Agent tasks keyed by job id, jobs without an agent task omitted.
        """
        ...
