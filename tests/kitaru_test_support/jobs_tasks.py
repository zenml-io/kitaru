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
"""In-memory fakes for jobs and tasks."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.job import Job, JobNotFound
from kitaru.server.domain.task import (
    AgentTask,
    AnalysisTask,
    DuplicateEvaluationTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskNotFound,
)
from kitaru.server.domain.worker import get_effective_selectors
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.sessions import FakeSessionRepository


class FakeJobRepository:
    """In-memory job repository."""

    def __init__(self, tasks: "FakeTaskRepository | None" = None) -> None:
        """Initialize the repository.

        Args:
            tasks: Fake task repository, cascaded on delete.
        """
        self._jobs: dict[uuid.UUID, Job] = {}
        self._tasks = tasks

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Returns:
            Stored job with timestamps set.
        """
        now = datetime.now(UTC)
        stored = job.model_copy(update={"created": now, "updated": now})
        self._jobs[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist many new jobs in one round trip.

        Args:
            jobs: Jobs to store.

        Returns:
            Stored jobs with timestamps set, in the same order.
        """
        return [await self.create(job) for job in jobs]

    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        _ = exclusive
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFound(job_id)
        return job.model_copy()

    async def get_many(self, job_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Job]:
        """Bulk-load jobs by id, keyed by id, missing ids omitted.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Stored jobs keyed by id.
        """
        return {
            job_id: self._jobs[job_id].model_copy()
            for job_id in job_ids
            if job_id in self._jobs
        }

    async def get_many_locked(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Job]:
        """Bulk-lock and load jobs by id, keyed by id, missing ids omitted.

        Row locking has no in-memory counterpart, a single process never
        contends with itself.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Locked jobs keyed by id.
        """
        return await self.get_many(job_ids)

    async def list_expired_pending_ids(
        self, cutoff: datetime, limit: int
    ) -> list[uuid.UUID]:
        """Read the ids of pending jobs older than the cutoff.

        Args:
            cutoff: Jobs created before this are read.
            limit: Maximum number of ids to read.

        Returns:
            Ids of the pending jobs in ascending order.
        """
        expired: list[uuid.UUID] = []
        for job_id in sorted(self._jobs):
            job = self._jobs[job_id]
            if job.status is not JobStatus.PENDING or job.created is None:
                continue
            if job.created < cutoff:
                expired.append(job_id)
        return expired[:limit]

    async def list_unpropagated_cancel_ids(self, limit: int) -> list[uuid.UUID]:
        """Read the ids of canceling jobs whose live tasks still owe the stamp.

        Args:
            limit: Maximum number of ids to read.

        Returns:
            Ids of the canceling jobs in ascending order.
        """
        if self._tasks is None:
            return []
        owing: list[uuid.UUID] = []
        for job_id in sorted(self._jobs):
            job = self._jobs[job_id]
            if job.cancel_requested_at is None or job.settled:
                continue
            tasks = await self._tasks.list_by_job(job_id)
            if any(
                not task.terminal
                and (
                    task.cancel_requested_at is None
                    or task.status is TaskStatus.PENDING
                )
                for task in tasks
            ):
                owing.append(job_id)
        return owing[:limit]

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]:
        """Query jobs matching a filter.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the next cursor.
        """
        jobs = list(self._jobs.values())
        if job_filter.job_ids is not None:
            wanted = set(job_filter.job_ids)
            jobs = [job for job in jobs if job.id in wanted]
        if job_filter.expression is not None:
            jobs = [
                job
                for job in jobs
                if _evaluate_filter_expression(job, job_filter.expression)
            ]
        page, next_cursor = _paginate_fake(jobs, job_filter)
        return [job.model_copy() for job in page], next_cursor

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        stored = self._jobs.get(job.id)
        if stored is None:
            raise JobNotFound(job.id)
        renewed = job.model_copy(
            update={
                "created": stored.created,
                "updated": _renewed_timestamp(stored.updated),
            }
        )
        self._jobs[job.id] = renewed
        return renewed.model_copy()

    async def update_many(self, jobs: list[Job]) -> list[Job]:
        """Persist changes to many existing jobs in one round trip.

        Args:
            jobs: Jobs with modified fields.

        Raises:
            JobNotFound: A job id matches no job.

        Returns:
            Stored jobs with the updated timestamp renewed, in the same order.
        """
        return [await self.update(job) for job in jobs]

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id, cascading its tasks.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        if job_id not in self._jobs:
            raise JobNotFound(job_id)
        del self._jobs[job_id]
        if self._tasks is not None:
            self._tasks.cascade_job_delete(job_id)


class FakeTaskRepository:
    """In-memory task repository."""

    def __init__(self, sessions: FakeSessionRepository | None = None) -> None:
        """Initialize the repository.

        Args:
            sessions: Fake session repository, wired back onto the session
                repository so its delete can clear this task's session
                pointers.
        """
        self._tasks: dict[uuid.UUID, Task] = {}
        self._sessions = sessions
        if sessions is not None:
            sessions._tasks = self
        # Assigned after construction, since the fake job repository takes
        # this repository in its own constructor.
        self.jobs: FakeJobRepository | None = None

    def _unlink_session(self, session_id: uuid.UUID) -> None:
        """Drop a deleted session's input tasks.

        Mirrors the SQL repository's CASCADE foreign key from
        ``task.input_session_id``.

        Args:
            session_id: Id of the session being deleted.
        """
        for task_id, task in list(self._tasks.items()):
            if isinstance(task, EvaluationTask) and task.input_session_id == session_id:
                del self._tasks[task_id]

    def _check_evaluator_pair(self, task: Task) -> None:
        """Mirror the unique (job_id, input_session_id, plugin_version_id) key.

        Args:
            task: Task about to be stored.

        Raises:
            DuplicateEvaluationTask: The job already scores this pair.
        """
        if not isinstance(task, EvaluationTask):
            return
        for other in self._tasks.values():
            if not isinstance(other, EvaluationTask) or other.id == task.id:
                continue
            if (
                other.job_id == task.job_id
                and other.input_session_id == task.input_session_id
                and other.plugin_version_id == task.plugin_version_id
            ):
                raise DuplicateEvaluationTask(
                    task.job_id, task.input_session_id, task.plugin_version_id
                )

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
        self._check_evaluator_pair(task)
        now = datetime.now(UTC)
        stored = task.model_copy(update={"created": now, "updated": now})
        self._tasks[stored.id] = stored
        return stored.model_copy()

    async def create_many(self, tasks: list[Task]) -> list[Task]:
        """Persist many new tasks in one round trip.

        Args:
            tasks: Tasks to store.

        Returns:
            Stored tasks with timestamps set, in the same order.
        """
        return [await self.create(task) for task in tasks]

    async def get(self, task_id: uuid.UUID, exclusive: bool = False) -> Task:
        """Load a task by id.

        Args:
            task_id: Id of the task.
            exclusive: Whether to lock the row, a no-op in memory.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task.
        """
        _ = exclusive
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFound(task_id)
        return task.model_copy()

    async def get_many(self, task_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Task]:
        """Bulk-load tasks by id, keyed by id, missing ids omitted.

        Args:
            task_ids: Ids of the tasks to load.

        Returns:
            Stored tasks keyed by id.
        """
        return {
            task_id: self._tasks[task_id].model_copy()
            for task_id in task_ids
            if task_id in self._tasks
        }

    def _matches_claim(self, task: Task, claim: WorkerClaim) -> bool:
        """Report whether a task's kind and agent version match a claim.

        Args:
            task: Candidate task.
            claim: Claim from the worker's scope.

        Returns:
            Whether the claim covers the task.
        """
        if claim.agent_version_id is not None:
            return (
                isinstance(task, AgentTask)
                and task.agent_version_id == claim.agent_version_id
            )
        return task.kind is claim.kind

    def _matches_residual(self, task: Task, scope: WorkerScope) -> bool:
        """Report whether a task matches a scope's conditions beyond its claims.

        Args:
            task: Candidate task.
            scope: Claim scope narrowing the queue.

        Returns:
            Whether the job pin and every selector match.
        """
        if scope.job_id is not None and task.job_id != scope.job_id:
            return False
        for selector in get_effective_selectors(scope):
            if selector.key not in task.labels:
                if selector.required:
                    return False
                continue
            if task.labels[selector.key] not in selector.values:
                return False
        return True

    async def query(self, task_filter: TaskFilter) -> tuple[list[Task], str | None]:
        """Query tasks matching a filter.

        Args:
            task_filter: Filter and pagination parameters.

        Returns:
            Page of matching tasks and the next cursor.
        """
        tasks = list(self._tasks.values())
        if task_filter.job_id is not None:
            tasks = [task for task in tasks if task.job_id == task_filter.job_id]
        if task_filter.stale_before is not None:
            bound = task_filter.stale_before
            tasks = [task for task in tasks if _is_stale_before(task, bound)]
        if task_filter.expression is not None:
            tasks = [
                task
                for task in tasks
                if _evaluate_filter_expression(task, task_filter.expression)
            ]
        page, next_cursor = _paginate_fake(tasks, task_filter)
        return [task.model_copy() for task in page], next_cursor

    async def list_by_job(self, job_id: uuid.UUID) -> list[Task]:
        """Load every task of a job, ordered by id.

        Args:
            job_id: Id the tasks belong to.

        Returns:
            Tasks of the job in creation order.
        """
        tasks = [task for task in self._tasks.values() if task.job_id == job_id]
        return [task.model_copy() for task in sorted(tasks, key=lambda task: task.id)]

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
        job_id_set = set(job_ids)
        tasks_by_job: dict[uuid.UUID, list[Task]] = {}
        for task in sorted(self._tasks.values(), key=lambda task: task.id):
            if task.job_id in job_id_set:
                tasks_by_job.setdefault(task.job_id, []).append(task.model_copy())
        return tasks_by_job

    async def update(self, task: Task) -> Task:
        """Persist changes to an existing task.

        Args:
            task: Task with modified fields.

        Raises:
            TaskNotFound: No task has this id.

        Returns:
            Stored task with the updated timestamp renewed.
        """
        stored = self._tasks.get(task.id)
        if stored is None:
            raise TaskNotFound(task.id)
        renewed = task.model_copy(
            update={
                "created": stored.created,
                "updated": _renewed_timestamp(stored.updated),
            }
        )
        self._tasks[task.id] = renewed
        return renewed.model_copy()

    async def claim_pending(
        self, scope: WorkerScope, worker_id: uuid.UUID, limit: int, now: datetime
    ) -> list[Task]:
        """Hand pending tasks matching a scope to a worker, oldest first.

        Ordering mirrors the SQL repository: the oldest pending tasks
        matching the scope are handed out. Row locking has no in-memory
        counterpart, a single process never contends with itself.

        Args:
            scope: Claim scope narrowing the queue.
            worker_id: Worker claiming the tasks.
            limit: Maximum number of tasks to claim.
            now: Current time.

        Returns:
            Claimed tasks carrying their incremented attempt.
        """
        pending = sorted(
            (
                task
                for task in self._tasks.values()
                if task.status is TaskStatus.PENDING
                and self._matches_residual(task, scope)
            ),
            key=lambda task: task.id,
        )
        selected = [
            task
            for task in pending
            if any(self._matches_claim(task, claim) for claim in scope.claims)
        ][:limit]
        claimed: list[Task] = []
        for task in selected:
            claimed_task = task.model_copy()
            claimed_task.claim(worker_id, now)
            claimed.append(await self.update(claimed_task))
        return claimed

    async def claim_stale(self, task_id: uuid.UUID, cutoff: datetime) -> Task | None:
        """Lock one task by id if it is still in flight and older than a cutoff.

        Args:
            task_id: Id of the candidate task.
            cutoff: Bound the last heartbeat must be older than.

        Returns:
            Locked stale task, or ``None`` when it is no longer stale.
        """
        task = self._tasks.get(task_id)
        if task is None or task.status not in (TaskStatus.CLAIMED, TaskStatus.RUNNING):
            return None
        if not _is_stale_before(task, cutoff):
            return None
        return task.model_copy()

    async def list_stale_ids(self, cutoff: datetime, limit: int) -> list[uuid.UUID]:
        """Read the ids of in-flight tasks whose last heartbeat is older than a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of ids to read.

        Returns:
            Ids of the stale tasks in ascending order.
        """
        stale = sorted(
            (
                task.id
                for task in self._tasks.values()
                if task.status in (TaskStatus.CLAIMED, TaskStatus.RUNNING)
                and _is_stale_before(task, cutoff)
            ),
        )
        return stale[:limit]

    async def stamp_heartbeats(
        self, task_ids: Sequence[uuid.UUID], worker_id: uuid.UUID, now: datetime
    ) -> tuple[dict[uuid.UUID, datetime | None], set[uuid.UUID]]:
        """Stamp heartbeat_at on the worker's in-flight tasks among the ids.

        Row locking has no in-memory counterpart, so no candidate is ever
        reported skipped.

        Args:
            task_ids: Candidate task ids.
            worker_id: Worker that must still hold the tasks.
            now: Current time.

        Returns:
            Cancel request time of the task, falling back to its job's, by id
            for every stamped task, and the owned in-flight candidates whose
            lock was held elsewhere and so were left unstamped.
        """
        stamped: dict[uuid.UUID, datetime | None] = {}
        for task_id in task_ids:
            task = self._tasks.get(task_id)
            if (
                task is None
                or task.worker_id != worker_id
                or task.status not in (TaskStatus.CLAIMED, TaskStatus.RUNNING)
            ):
                continue
            self._tasks[task_id] = task.model_copy(
                update={
                    "heartbeat_at": now,
                    "updated": _renewed_timestamp(task.updated),
                }
            )
            stamped[task_id] = task.cancel_requested_at
            if stamped[task_id] is None and self.jobs is not None:
                owner = (await self.jobs.get_many([task.job_id])).get(task.job_id)
                if owner is not None:
                    stamped[task_id] = owner.cancel_requested_at
        return stamped, set()

    async def lock_by_jobs(
        self, job_ids: Sequence[uuid.UUID], nowait: bool = False
    ) -> None:
        """Lock the jobs' non-terminal task rows in id order.

        Row locking has no in-memory counterpart, a single process never
        contends with itself.

        Args:
            job_ids: Ids the tasks belong to.
            nowait: Whether to fail instead of waiting when another
                transaction holds one of the rows.
        """
        _ = job_ids, nowait

    async def stamp_cancel_requested(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> None:
        """Stamp cancel_requested_at on the jobs' non-terminal tasks lacking it.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.
        """
        for task_id, task in list(self._tasks.items()):
            if task.job_id not in job_ids or task.terminal:
                continue
            if task.cancel_requested_at is not None:
                continue
            self._tasks[task_id] = task.model_copy(
                update={
                    "cancel_requested_at": now,
                    "updated": _renewed_timestamp(task.updated),
                }
            )

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
        canceled: list[Task] = []
        for task in sorted(self._tasks.values(), key=lambda task: task.id):
            if task.job_id not in job_ids or task.status is not TaskStatus.PENDING:
                continue
            canceled_task = task.model_copy()
            canceled_task.request_cancel(now)
            canceled.append(await self.update(canceled_task))
        return canceled

    def cascade_job_delete(self, job_id: uuid.UUID) -> None:
        """Drop the tasks of a deleted job, mirroring the cascading key.

        Args:
            job_id: Id of the deleted job.
        """
        for task_id, task in list(self._tasks.items()):
            if task.job_id == job_id:
                del self._tasks[task_id]

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
        return {
            task.plugin_version_id
            for task in self._tasks.values()
            if isinstance(task, EvaluationTask)
            and task.input_session_id == input_session_id
            and task.status is TaskStatus.COMPLETED
        }


def _is_stale_before(task: Task, bound: datetime) -> bool:
    """Report whether a task last showed a sign of life before a bound.

    Args:
        task: Task to read.
        bound: Time the last sign of life must precede.

    Returns:
        Whether the task heartbeated, or was claimed, before the bound.
    """
    last_seen = task.heartbeat_at if task.heartbeat_at is not None else task.claimed_at
    return last_seen is not None and last_seen < bound


async def create_job(
    repository: FakeJobRepository,
    owner_id: uuid.UUID,
    kind: JobKind = JobKind.SESSION_RUN,
    status: JobStatus = JobStatus.PENDING,
) -> Job:
    """Store a job in the fake repository.

    Args:
        repository: Fake job repository.
        owner_id: Id of the owning account.
        kind: Job kind.
        status: Job status.

    Returns:
        Stored job.
    """
    return await repository.create(Job(owner_id=owner_id, kind=kind, status=status))


async def create_agent_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    agent_version_id: uuid.UUID | None = None,
    inputs: Any = None,
    labels: dict[str, str] | None = None,
    env: dict[str, str] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.ABORT,
) -> AgentTask:
    """Store an agent task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        agent_version_id: Agent version the task runs.
        inputs: Inputs passed to the agent's command.
        labels: Labels matched by worker scope selectors.
        env: Creator-set process environment extras.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored agent task.
    """
    task = AgentTask(
        job_id=job_id,
        agent_version_id=(
            agent_version_id if agent_version_id is not None else uuid.uuid4()
        ),
        inputs=inputs,
        labels=labels if labels is not None else {},
        env=env if env is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, AgentTask)
    return stored


async def create_evaluation_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    plugin_version_id: uuid.UUID | None = None,
    input_session_id: uuid.UUID | None = None,
    connection_id: uuid.UUID | None = None,
    params: dict[str, Any] | None = None,
    labels: dict[str, str] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.CONTINUE,
) -> EvaluationTask:
    """Store an evaluator task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        plugin_version_id: Evaluator version the task runs.
        input_session_id: Session being scored.
        connection_id: Connection injected into the task environment.
        params: Parameters passed to the evaluator.
        labels: Labels matched by worker scope selectors.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored evaluator task.
    """
    task = EvaluationTask(
        job_id=job_id,
        plugin_version_id=(
            plugin_version_id if plugin_version_id is not None else uuid.uuid4()
        ),
        input_session_id=(
            input_session_id if input_session_id is not None else uuid.uuid4()
        ),
        connection_id=connection_id,
        params=params if params is not None else {},
        labels=labels if labels is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, EvaluationTask)
    return stored


async def create_analysis_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    plugin_version_id: uuid.UUID | None = None,
    agent_id: uuid.UUID | None = None,
    import_id: uuid.UUID | None = None,
    connection_id: uuid.UUID | None = None,
    params: dict[str, Any] | None = None,
    labels: dict[str, str] | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.CONTINUE,
) -> AnalysisTask:
    """Store an analyzer task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        plugin_version_id: Analyzer version the task runs.
        agent_id: Agent the produced insights belong to.
        import_id: Import whose sessions are analyzed.
        connection_id: Connection injected into the task environment.
        params: Parameters passed to the analyzer.
        labels: Labels matched by worker scope selectors.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored analyzer task.
    """
    task = AnalysisTask(
        job_id=job_id,
        plugin_version_id=(
            plugin_version_id if plugin_version_id is not None else uuid.uuid4()
        ),
        agent_id=agent_id if agent_id is not None else uuid.uuid4(),
        import_id=import_id if import_id is not None else uuid.uuid4(),
        connection_id=connection_id,
        params=params if params is not None else {},
        labels=labels if labels is not None else {},
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, AnalysisTask)
    return stored


async def create_import_task(
    repository: FakeTaskRepository,
    job_id: uuid.UUID,
    import_id: uuid.UUID | None = None,
    on_failure: TaskOnFailure = TaskOnFailure.ABORT,
) -> ImportTask:
    """Store an importer task in the fake repository.

    Args:
        repository: Fake task repository.
        job_id: Id of the owning job.
        import_id: Id of the import the task runs.
        on_failure: Effect of a hard failure on the job.

    Returns:
        Stored importer task.
    """
    task = ImportTask(
        job_id=job_id,
        import_id=import_id if import_id is not None else uuid.uuid4(),
        on_failure=on_failure,
    )
    stored = await repository.create(task)
    assert isinstance(stored, ImportTask)
    return stored
