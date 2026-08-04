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
"""Task status transition dispatch and job settlement."""

import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import (
    EventDispatcher,
    JobsSettled,
    TaskTerminal,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.job import Job
from kitaru.server.domain.task import EvaluationTask, ImportTask, Task


def _settlement_outcome(tasks: list[Task]) -> tuple[JobStatus, str | None]:
    """Decide the terminal status of a drained job.

    Args:
        tasks: Every task of the job, in creation order.

    Returns:
        Terminal job status and the error of the first counted failure.
    """
    for task in tasks:
        if task.counted_hard_failure:
            return JobStatus.FAILED, task.error
    for task in tasks:
        if task.status is TaskStatus.CANCELED:
            return JobStatus.CANCELED, None
    return JobStatus.COMPLETED, None


class TaskTransitions:
    """Single write point for task statuses and the job settlement it drives."""

    def __init__(
        self,
        task_repository: TaskRepository,
        job_repository: JobRepository,
        dispatcher: EventDispatcher,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the dispatch.

        Args:
            task_repository: Task repository.
            job_repository: Job repository.
            dispatcher: Event dispatcher the transitions publish on.
            analytics: Analytics tracker, None skips tracking.
        """
        self._tasks = task_repository
        self._jobs = job_repository
        self._dispatcher = dispatcher
        self._analytics = analytics

    async def apply_status(
        self, task: Task, transition: Callable[[Task], None]
    ) -> Task:
        """Apply a task transition, publish it, and advance the owning job.

        The ordered sequence runs inside the caller's transaction: the
        transition is persisted, a terminal status publishes ``TaskTerminal``
        so subscribers can append work before the job is checked, and the job
        advances afterward, publishing ``JobsSettled`` when it settles.

        Args:
            task: Task to transition.
            transition: Domain method application deciding the new status.

        Raises:
            IllegalTaskStatusTransition: The transition is not allowed from
                the task's current status.

        Returns:
            Stored task carrying its new status.
        """
        stored = await self._write_transition(task, transition)
        job = await self.advance_job(stored.job_id)
        if stored.terminal:
            self._track_task_terminal(stored, job)
        return stored

    async def _write_transition(
        self, task: Task, transition: Callable[[Task], None]
    ) -> Task:
        """Persist a transition and publish TaskTerminal without advancing the job.

        Batch callers move many tasks toward the same status and advance the
        job once after the batch instead of once per task.

        Args:
            task: Task to transition.
            transition: Domain method application deciding the new status.

        Raises:
            IllegalTaskStatusTransition: The transition is not allowed from
                the task's current status.

        Returns:
            Stored task carrying its new status.
        """
        previous_status = task.status
        transition(task)
        stored = await self._tasks.update(task)
        if stored.terminal:
            await self._dispatcher.dispatch(
                TaskTerminal(task=stored, previous_status=previous_status)
            )
        return stored

    async def start_job(self, job_id: uuid.UUID) -> None:
        """Move a pending job to running, leaving an already started job alone.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        job = await self._jobs.get(job_id, exclusive=True)
        if job.status is not JobStatus.PENDING:
            return
        job.start(datetime.now(UTC))
        await self._jobs.update(job)

    async def advance_job(self, job_id: uuid.UUID) -> Job:
        """Propagate an abort failure and settle the job once its tasks drain.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Loaded job, settled if this call drained its tasks.
        """
        tasks = await self._tasks.list_by_job(job_id)
        if any(
            task.counted_hard_failure and task.on_failure is TaskOnFailure.ABORT
            for task in tasks
        ):
            await self._propagate_abort(job_id)
        job = await self._jobs.get(job_id, exclusive=True)
        return await self._settle_drained_job(job)

    async def _settle_drained_job(self, job: Job) -> Job:
        """Settle a locked job once every one of its tasks is terminal.

        Args:
            job: Job loaded under its row lock.

        Returns:
            Job, settled if this call drained its tasks.
        """
        if job.settled:
            return job

        # Read the tasks after the job row lock to prevent race conditions
        # during concurrent task settlements.
        tasks = await self._tasks.list_by_job(job.id)
        if not tasks or not all(task.terminal for task in tasks):
            return job
        status, error = _settlement_outcome(tasks)
        job.settle(status, error, datetime.now(UTC))
        if self._analytics is not None:
            self._analytics.track(
                job.owner_id,
                AnalyticsEvent.JOB_COMPLETED,
                analytics_events.build_job_completed_properties(job, tasks),
            )
        settled = await self._jobs.update(job)
        await self._dispatcher.dispatch(JobsSettled(jobs=[settled]))
        return settled

    async def _cancel_pending_tasks(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> None:
        """Move each still-pending task of the jobs straight to canceled.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.
        """
        canceled = await self._tasks.cancel_pending(job_ids, now)
        for task in canceled:
            await self._dispatcher.dispatch(
                TaskTerminal(task=task, previous_status=TaskStatus.PENDING)
            )

    async def _propagate_abort(self, job_id: uuid.UUID) -> None:
        """Cancel-request every live sibling and cancel the pending ones.

        Args:
            job_id: Id of the job.
        """
        now = datetime.now(UTC)
        await self._tasks.stamp_cancel_requested([job_id], now)
        await self._cancel_pending_tasks([job_id], now)

    async def request_jobs_cancel(self, job_ids: Sequence[uuid.UUID]) -> None:
        """Stamp the cancel request on each job and cancel their pending tasks.

        Every task row is locked in one id-ordered statement before any job
        row, which is the order the reporting and claiming paths take, and
        the order the heartbeat stamp locks task rows in. Locking task rows
        job by job or a job row first deadlocks against a worker reporting
        or heartbeating one of these tasks. The jobs are not settled here,
        so no replay or run lock is taken.

        Args:
            job_ids: Ids of the jobs.

        Raises:
            JobNotFound: A job id matches no job.
        """
        now = datetime.now(UTC)
        await self._tasks.stamp_cancel_requested(job_ids, now)
        await self._cancel_pending_tasks(job_ids, now)
        for job_id in job_ids:
            job = await self._jobs.get(job_id, exclusive=True)
            if job.settled:
                continue
            job.request_cancel(now)
            await self._jobs.update(job)

    async def settle_job_if_drained(self, job_id: uuid.UUID) -> Job:
        """Settle a job once every one of its tasks is terminal.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Loaded job, settled if its tasks have drained.
        """
        job = await self._jobs.get(job_id, exclusive=True)
        return await self._settle_drained_job(job)

    async def settle_jobs_if_drained(self, job_ids: Sequence[uuid.UUID]) -> None:
        """Settle every drained job among many in one bulk read and one bulk write.

        Jobs are locked in id order in one statement. A job that already
        settled, or still has a non-terminal task, is left untouched. The
        newly settled jobs publish a single ``JobsSettled``.

        Args:
            job_ids: Ids of the jobs.
        """
        if not job_ids:
            return
        jobs = await self._jobs.get_many_locked(job_ids)
        tasks_by_job = await self._tasks.list_by_jobs(job_ids)
        settled: list[Job] = []
        for job_id in job_ids:
            job = jobs.get(job_id)
            if job is None or job.settled:
                continue
            tasks = tasks_by_job.get(job_id, [])
            if not tasks or not all(task.terminal for task in tasks):
                continue
            status, error = _settlement_outcome(tasks)
            job.settle(status, error, datetime.now(UTC))
            if self._analytics is not None:
                self._analytics.track(
                    job.owner_id,
                    AnalyticsEvent.JOB_COMPLETED,
                    analytics_events.build_job_completed_properties(job, tasks),
                )
            settled.append(job)
        if not settled:
            return
        stored = await self._jobs.update_many(settled)
        await self._dispatcher.dispatch(JobsSettled(jobs=stored))

    async def cancel_job(self, job_id: uuid.UUID) -> Job:
        """Stamp the cancel request on a job and settle it if that drained it.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job carrying the cancel request.
        """
        await self.request_jobs_cancel([job_id])
        return await self.settle_job_if_drained(job_id)

    def _track_task_terminal(self, task: Task, job: Job) -> None:
        """Track a task's transition to a terminal status by kind.

        Args:
            task: Task that just transitioned to a terminal status.
            job: Owning job of the task.
        """
        if self._analytics is None:
            return
        if isinstance(task, ImportTask):
            self._analytics.track(
                job.owner_id,
                AnalyticsEvent.IMPORT_COMPLETED,
                analytics_events.build_import_completed_properties(task),
            )
        elif isinstance(task, EvaluationTask):
            self._analytics.track(
                job.owner_id,
                AnalyticsEvent.EVALUATION_COMPLETED,
                analytics_events.build_evaluation_completed_properties(task),
            )
