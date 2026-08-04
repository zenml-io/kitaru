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
"""SQL task repository."""

import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime

from sqlalchemy import ColumnElement, func, not_, or_, select, update

from kitaru.api_models.v1.task import TaskKind, TaskStatus
from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.job import JobORM
from kitaru.server.adapters.db.orm.task import (
    TASK_EVALUATOR_PAIR_UNIQUE_CONSTRAINT,
    TERMINAL_STATUS_VALUES,
    TaskORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.task import (
    DuplicateEvaluationTask,
    Task,
    TaskNotFound,
)

IN_FLIGHT_STATUS_VALUES = [TaskStatus.CLAIMED.value, TaskStatus.RUNNING.value]

_LAST_SEEN = func.coalesce(TaskORM.heartbeat_at, TaskORM.claimed_at)

TASK_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "job_id": TaskORM.job_id,
    "kind": TaskORM.kind,
    "status": TaskORM.status,
    "worker_id": TaskORM.worker_id,
}


def _scope_conditions(scope: WorkerScope) -> list[ColumnElement[bool]]:
    """Build the claim conditions a worker scope narrows the queue by.

    An unpinned scope without selectors adds no condition and claims any
    pending task.

    Args:
        scope: Claim scope stored on the worker row.

    Returns:
        Conditions to AND into the claim query.
    """
    conditions: list[ColumnElement[bool]] = []
    if scope.kinds:
        conditions.append(TaskORM.kind.in_([kind.value for kind in scope.kinds]))
    if scope.job_id is not None:
        conditions.append(TaskORM.job_id == scope.job_id)
    for selector in scope.selectors or []:
        matches_value = TaskORM.labels[selector.key].astext.in_(selector.values)
        if selector.required:
            conditions.append(matches_value)
        else:
            conditions.append(
                or_(not_(TaskORM.labels.has_key(selector.key)), matches_value)
            )
    return conditions


class SQLTaskRepository(BaseSQLRepository[TaskORM]):
    """Task repository backed by the application database."""

    orm_class = TaskORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return TaskNotFound(entity_id)

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
        row = TaskORM.from_domain(task)
        await self._add(
            row,
            {
                TASK_EVALUATOR_PAIR_UNIQUE_CONSTRAINT: lambda: DuplicateEvaluationTask(
                    row.job_id, row.input_session_id, row.plugin_version_id
                )
            },
        )
        return row.to_domain()

    async def create_many(self, tasks: list[Task]) -> list[Task]:
        """Persist many new tasks in one round trip.

        Args:
            tasks: Tasks to store.

        Returns:
            Stored tasks with timestamps set, in the same order.
        """
        if not tasks:
            return []
        rows = [TaskORM.from_domain(task) for task in tasks]
        self._session.add_all(rows)
        await self._flush()
        return [row.to_domain() for row in rows]

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
        row = await self._get_row(task_id, exclusive=exclusive)
        return row.to_domain()

    async def get_many(self, task_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Task]:
        """Bulk-load tasks by id, keyed by id, missing ids omitted.

        Args:
            task_ids: Ids of the tasks to load.

        Returns:
            Stored tasks keyed by id.
        """
        rows = await self._load_by_ids(task_ids)
        return {task_id: row.to_domain() for task_id, row in rows.items()}

    async def query(self, task_filter: TaskFilter) -> tuple[list[Task], str | None]:
        """Query tasks matching a filter.

        ``stale_before`` matches in-flight tasks whose last heartbeat, or
        claim time when they never heartbeated, is older than the bound.

        Args:
            task_filter: Filter and pagination parameters.

        Returns:
            Page of matching tasks and the next cursor.
        """
        statement = select(TaskORM)
        if task_filter.job_id is not None:
            statement = statement.where(TaskORM.job_id == task_filter.job_id)
        if task_filter.stale_before is not None:
            statement = statement.where(
                TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES),
                task_filter.stale_before > _LAST_SEEN,
            )
        if task_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(task_filter.expression, TASK_FILTER_BINDINGS)
            )
        rows, next_cursor = await paginate(
            self._session, statement, task_filter, id_column=TaskORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def list_by_job(self, job_id: uuid.UUID) -> list[Task]:
        """Load every task of a job, ordered by id.

        Args:
            job_id: Id the tasks belong to.

        Returns:
            Tasks of the job in creation order.
        """
        statement = (
            select(TaskORM).where(TaskORM.job_id == job_id).order_by(TaskORM.id.asc())
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

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
        if not job_ids:
            return {}
        statement = (
            select(TaskORM)
            .where(TaskORM.job_id.in_(list(job_ids)))
            .order_by(TaskORM.job_id.asc(), TaskORM.id.asc())
        )
        rows = (await self._session.scalars(statement)).all()
        tasks_by_job: dict[uuid.UUID, list[Task]] = {}
        for row in rows:
            tasks_by_job.setdefault(row.job_id, []).append(row.to_domain())
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
        row = await self._get_row(task.id)
        row.apply(task)
        await self._flush()
        return row.to_domain()

    async def claim_pending(
        self, scope: WorkerScope, worker_id: uuid.UUID, limit: int, now: datetime
    ) -> list[Task]:
        """Hand pending tasks matching a scope to a worker, oldest first.

        Args:
            scope: Claim scope narrowing the queue.
            worker_id: Worker claiming the tasks.
            limit: Maximum number of tasks to claim.
            now: Current time.

        Returns:
            Claimed tasks carrying their incremented attempt.
        """
        statement = (
            select(TaskORM)
            .where(
                TaskORM.status == TaskStatus.PENDING.value, *_scope_conditions(scope)
            )
            .order_by(TaskORM.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        if not rows:
            return []
        for row in rows:
            task = row.to_domain()
            task.claim(worker_id, now)
            row.apply(task)
        await self._flush()
        return [row.to_domain() for row in rows]

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
        statement = (
            select(TaskORM)
            .where(
                TaskORM.id == task_id,
                TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES),
                cutoff > _LAST_SEEN,
            )
            .with_for_update(skip_locked=True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain() if row is not None else None

    async def list_stale_ids(self, cutoff: datetime, limit: int) -> list[uuid.UUID]:
        """Read the ids of in-flight tasks whose last heartbeat is older than a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of ids to read.

        Returns:
            Ids of the stale tasks in ascending order.
        """
        statement = (
            select(TaskORM.id)
            .where(TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES), cutoff > _LAST_SEEN)
            .order_by(TaskORM.id.asc())
            .limit(limit)
        )
        return list((await self._session.scalars(statement)).all())

    async def stamp_heartbeats(
        self, task_ids: Sequence[uuid.UUID], worker_id: uuid.UUID, now: datetime
    ) -> dict[uuid.UUID, datetime | None]:
        """Stamp heartbeat_at on the worker's in-flight tasks among the ids.

        Writes only the heartbeat column, so a stamp cannot overwrite fields
        concurrent writers committed since the caller last read the tasks.
        Task rows are locked in id order. The owning job row is joined for
        its own cancel request, which reaches a task before the sweep stamps
        it, and joining leaves that row unlocked.

        Args:
            task_ids: Candidate task ids.
            worker_id: Worker that must still hold the tasks.
            now: Current time.

        Returns:
            Cancel request time of the task, falling back to its job's, by id
            for every stamped task.
        """
        if not task_ids:
            return {}
        locked = (
            select(TaskORM.id)
            .where(
                TaskORM.id.in_(list(task_ids)),
                TaskORM.worker_id == worker_id,
                TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES),
            )
            .order_by(TaskORM.id.asc())
            .with_for_update()
        )
        statement = (
            update(TaskORM)
            .where(TaskORM.id.in_(locked), JobORM.id == TaskORM.job_id)
            .values(heartbeat_at=now, updated=now)
            .returning(
                TaskORM.id,
                func.coalesce(TaskORM.cancel_requested_at, JobORM.cancel_requested_at),
            )
            .execution_options(synchronize_session=False)
        )
        rows = (await self._session.execute(statement)).all()
        await self._session.flush()
        return {task_id: cancel_requested_at for task_id, cancel_requested_at in rows}

    async def lock_by_jobs(
        self, job_ids: Sequence[uuid.UUID], nowait: bool = False
    ) -> None:
        """Lock the jobs' non-terminal task rows in id order.

        Args:
            job_ids: Ids the tasks belong to.
            nowait: Whether to fail instead of waiting when another
                transaction holds one of the rows.

        Raises:
            DBAPIError: ``nowait`` is set and a row is held elsewhere.
        """
        if not job_ids:
            return
        statement = (
            select(TaskORM.id)
            .where(
                TaskORM.job_id.in_(list(job_ids)),
                not_(TaskORM.status.in_(TERMINAL_STATUS_VALUES)),
            )
            .order_by(TaskORM.id.asc())
            .with_for_update(nowait=nowait)
        )
        await self._session.execute(statement)

    async def stamp_cancel_requested(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> None:
        """Stamp cancel_requested_at on the jobs' non-terminal tasks lacking it.

        Rows are locked in id order across all jobs.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.
        """
        if not job_ids:
            return
        locked = (
            select(TaskORM.id)
            .where(
                TaskORM.job_id.in_(list(job_ids)),
                not_(TaskORM.status.in_(TERMINAL_STATUS_VALUES)),
                TaskORM.cancel_requested_at.is_(None),
            )
            .order_by(TaskORM.id.asc())
            .with_for_update()
        )
        statement = (
            update(TaskORM)
            .where(TaskORM.id.in_(locked))
            .values(cancel_requested_at=now, updated=now)
            .execution_options(synchronize_session="fetch")
        )
        await self._session.execute(statement)
        await self._session.flush()

    async def cancel_pending(
        self, job_ids: Sequence[uuid.UUID], now: datetime
    ) -> list[Task]:
        """Move each still-pending task of the jobs straight to canceled.

        Rows are locked in id order across all jobs.

        Args:
            job_ids: Ids the tasks belong to.
            now: Current time.

        Returns:
            Canceled tasks.
        """
        if not job_ids:
            return []
        locked = (
            select(TaskORM.id)
            .where(
                TaskORM.job_id.in_(list(job_ids)),
                TaskORM.status == TaskStatus.PENDING.value,
            )
            .order_by(TaskORM.id.asc())
            .with_for_update()
        )
        statement = (
            update(TaskORM)
            .where(TaskORM.id.in_(locked))
            .values(
                status=TaskStatus.CANCELED.value,
                ended_at=now,
                cancel_requested_at=func.coalesce(TaskORM.cancel_requested_at, now),
                updated=now,
            )
            .returning(TaskORM.id)
            .execution_options(synchronize_session="fetch")
        )
        canceled_ids = (await self._session.scalars(statement)).all()
        await self._session.flush()
        if not canceled_ids:
            return []
        rows = await self._session.scalars(
            select(TaskORM)
            .where(TaskORM.id.in_(canceled_ids))
            .order_by(TaskORM.id.asc())
        )
        return [row.to_domain() for row in rows]

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
        statement = select(TaskORM.plugin_version_id).where(
            TaskORM.input_session_id == input_session_id,
            TaskORM.status == TaskStatus.COMPLETED.value,
            TaskORM.plugin_version_id.is_not(None),
        )
        rows = (await self._session.scalars(statement)).all()
        return {row for row in rows if row is not None}

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
        if not input_session_ids:
            return {}
        statement = select(TaskORM.input_session_id, TaskORM.plugin_version_id).where(
            TaskORM.input_session_id.in_(input_session_ids),
            TaskORM.status == TaskStatus.COMPLETED.value,
            TaskORM.plugin_version_id.is_not(None),
        )
        rows = (await self._session.execute(statement)).all()
        scored: dict[uuid.UUID, set[uuid.UUID]] = {}
        for session_id, plugin_version_id in rows:
            scored.setdefault(session_id, set()).add(plugin_version_id)
        return scored

    async def get_agent_tasks_by_job_ids(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Task]:
        """Bulk-load the agent task of each job, keyed by job id.

        Args:
            job_ids: Ids of the jobs.

        Returns:
            Agent tasks keyed by job id, jobs without an agent task omitted.
        """
        if not job_ids:
            return {}
        statement = select(TaskORM).where(
            TaskORM.job_id.in_(job_ids), TaskORM.kind == TaskKind.AGENT.value
        )
        rows = (await self._session.scalars(statement)).all()
        return {row.job_id: row.to_domain() for row in rows}
