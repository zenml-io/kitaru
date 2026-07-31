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
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import ColumnElement, func, not_, or_, select, update

from kitaru.api_models.v1.task import TaskKind, TaskStatus, WorkerScope
from kitaru.server.adapters.db.orm.task import (
    TASK_EVALUATOR_PAIR_UNIQUE_CONSTRAINT,
    TaskORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.task import (
    TERMINAL_TASK_STATUSES,
    DuplicateEvaluationTask,
    Task,
    TaskNotFound,
)

IN_FLIGHT_STATUS_VALUES = [TaskStatus.CLAIMED.value, TaskStatus.RUNNING.value]
TERMINAL_STATUS_VALUES = [status.value for status in TERMINAL_TASK_STATUSES]

_LAST_SEEN = func.coalesce(TaskORM.heartbeat_at, TaskORM.claimed_at)


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
        row = await self._session.get(
            self.orm_class, task_id, with_for_update=exclusive
        )
        if row is None:
            raise TaskNotFound(task_id)
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
        if task_filter.kind is not None:
            statement = statement.where(TaskORM.kind == task_filter.kind.value)
        if task_filter.status is not None:
            statement = statement.where(TaskORM.status == task_filter.status.value)
        if task_filter.worker_id is not None:
            statement = statement.where(TaskORM.worker_id == task_filter.worker_id)
        if task_filter.stale_before is not None:
            statement = statement.where(
                TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES),
                task_filter.stale_before > _LAST_SEEN,
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

    async def claim_stale(self, cutoff: datetime, limit: int) -> list[Task]:
        """Lock in-flight tasks whose last heartbeat is older than a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of tasks to lock.

        Returns:
            Locked stale tasks.
        """
        statement = (
            select(TaskORM)
            .where(TaskORM.status.in_(IN_FLIGHT_STATUS_VALUES), cutoff > _LAST_SEEN)
            .order_by(TaskORM.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

    async def stamp_heartbeats(
        self, task_ids: Sequence[uuid.UUID], worker_id: uuid.UUID, now: datetime
    ) -> dict[uuid.UUID, datetime | None]:
        """Stamp heartbeat_at on the worker's in-flight tasks among the ids.

        Writes only the heartbeat column, so a stamp cannot overwrite fields
        concurrent writers committed since the caller last read the tasks.
        Rows are locked in id order.

        Args:
            task_ids: Candidate task ids.
            worker_id: Worker that must still hold the tasks.
            now: Current time.

        Returns:
            Cancel request time by id for every stamped task.
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
            .where(TaskORM.id.in_(locked))
            .values(heartbeat_at=now, updated=now)
            .returning(TaskORM.id, TaskORM.cancel_requested_at)
            .execution_options(synchronize_session=False)
        )
        rows = (await self._session.execute(statement)).all()
        await self._session.flush()
        return {task_id: cancel_requested_at for task_id, cancel_requested_at in rows}

    async def stamp_cancel_requested(self, job_id: uuid.UUID, now: datetime) -> None:
        """Stamp cancel_requested_at on the job's non-terminal tasks lacking it.

        Rows are locked in id order.

        Args:
            job_id: Id the tasks belong to.
            now: Current time.
        """
        locked = (
            select(TaskORM.id)
            .where(
                TaskORM.job_id == job_id,
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
