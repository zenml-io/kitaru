"""SQL task queue repository."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, not_, or_, select, update

from kitaru.api_models.v1.task import WorkerScope
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.task import (
    StaleTaskAttempt,
    Task,
    TaskNotFound,
    TaskStatus,
)


class SQLTaskRepository(BaseSQLRepository[TaskORM]):
    """Task queue repository backed by PostgreSQL."""

    orm_class = TaskORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return TaskNotFound(entity_id)

    async def create(self, task: Task) -> Task:
        row = TaskORM.from_domain(task)
        await self._add(row)
        return row.to_domain()

    async def get(self, task_id: uuid.UUID, exclusive: bool = False) -> Task:
        statement = select(TaskORM).where(TaskORM.id == task_id)
        if exclusive:
            statement = statement.with_for_update()
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise TaskNotFound(task_id)
        return row.to_domain()

    async def get_many(self, ids: list[uuid.UUID]) -> dict[uuid.UUID, Task]:
        return {
            row_id: row.to_domain()
            for row_id, row in (await self._load_by_ids(ids)).items()
        }

    async def query(self, task_filter: TaskFilter) -> tuple[list[Task], str | None]:
        statement = select(TaskORM)
        for name, column in (
            ("job_id", TaskORM.job_id),
            ("worker_id", TaskORM.worker_id),
        ):
            value = getattr(task_filter, name)
            if value is not None:
                statement = statement.where(column == value)
        if task_filter.kind is not None:
            statement = statement.where(TaskORM.kind == task_filter.kind.value)
        if task_filter.status is not None:
            statement = statement.where(TaskORM.status == task_filter.status.value)
        if task_filter.stale_before is not None:
            statement = statement.where(
                TaskORM.status.in_(
                    [TaskStatus.CLAIMED.value, TaskStatus.RUNNING.value]
                ),
                func.coalesce(TaskORM.heartbeat_at, TaskORM.claimed_at)
                < task_filter.stale_before,
            )
        rows, cursor = await paginate(self._session, statement, task_filter, TaskORM.id)
        return [row.to_domain() for row in rows], cursor

    async def update(self, task: Task, expected_attempt: int | None = None) -> Task:
        source = TaskORM.from_domain(task)
        values = {
            name: getattr(source, name)
            for name in (
                "status",
                "attempt",
                "worker_id",
                "result_session_id",
                "claimed_at",
                "heartbeat_at",
                "cancel_requested_at",
                "started_at",
                "ended_at",
                "error",
                "result",
            )
        }
        statement = update(TaskORM).where(TaskORM.id == task.id)
        if expected_attempt is not None:
            statement = statement.where(TaskORM.attempt == expected_attempt)
        updated = (
            await self._session.execute(
                statement.values(**values).returning(TaskORM.id)
            )
        ).scalar_one_or_none()
        if updated is None:
            if await self._session.get(TaskORM, task.id) is None:
                raise TaskNotFound(task.id)
            assert expected_attempt is not None
            raise StaleTaskAttempt(task.id, expected_attempt)
        return (await self._get_row(task.id)).to_domain()

    def _scope_conditions(self, scope: WorkerScope) -> list[Any]:
        conditions: list[Any] = []
        if scope.kinds:
            conditions.append(TaskORM.kind.in_([kind.value for kind in scope.kinds]))
        if scope.job_id is not None:
            conditions.append(TaskORM.job_id == scope.job_id)
        for selector in scope.selectors or []:
            matches = TaskORM.labels[selector.key].astext.in_(selector.values)
            if selector.required:
                conditions.append(
                    and_(
                        TaskORM.labels.has_key(  # type: ignore[attr-defined]
                            selector.key
                        ),
                        matches,
                    )
                )
            else:
                conditions.append(
                    or_(
                        not_(
                            TaskORM.labels.has_key(  # type: ignore[attr-defined]
                                selector.key
                            )
                        ),
                        matches,
                    )
                )
        return conditions

    async def claim_pending(
        self, worker_id: uuid.UUID, scope: WorkerScope, max_tasks: int
    ) -> list[Task]:
        statement = (
            select(TaskORM)
            .where(
                TaskORM.status == TaskStatus.PENDING.value,
                *self._scope_conditions(scope),
            )
            .order_by(TaskORM.id)
            .limit(max_tasks)
            .with_for_update(skip_locked=True)
        )
        rows = list((await self._session.scalars(statement)).all())
        now = datetime.now().astimezone()
        tasks: list[Task] = []
        for row in rows:
            task = row.to_domain()
            task.claim(worker_id, now=now)
            row.copy_from_domain(task)
            tasks.append(task)
        await self._session.flush()
        return tasks

    async def list_job_tasks(
        self, job_id: uuid.UUID, exclusive: bool = False
    ) -> list[Task]:
        statement = select(TaskORM).where(TaskORM.job_id == job_id).order_by(TaskORM.id)
        if exclusive:
            statement = statement.with_for_update()
        return [
            row.to_domain() for row in (await self._session.scalars(statement)).all()
        ]

    async def stale(self, stale_before: datetime, limit: int) -> list[Task]:
        rows = (
            await self._session.scalars(
                select(TaskORM)
                .where(
                    TaskORM.status.in_(
                        [TaskStatus.CLAIMED.value, TaskStatus.RUNNING.value]
                    ),
                    func.coalesce(TaskORM.heartbeat_at, TaskORM.claimed_at)
                    < stale_before,
                )
                .order_by(TaskORM.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
        return [row.to_domain() for row in rows]

    async def heartbeat(
        self, worker_id: uuid.UUID, task_ids: list[uuid.UUID], now: datetime
    ) -> list[uuid.UUID]:
        if not task_ids:
            return []
        rows = {
            row.id: row
            for row in (
                await self._session.scalars(
                    select(TaskORM).where(TaskORM.id.in_(task_ids))
                )
            ).all()
        }
        cancel: list[uuid.UUID] = []
        for task_id in task_ids:
            row = rows.get(task_id)
            if (
                row is None
                or row.worker_id != worker_id
                or row.status
                not in {
                    TaskStatus.CLAIMED.value,
                    TaskStatus.RUNNING.value,
                }
                or row.cancel_requested_at is not None
            ):
                cancel.append(task_id)
                continue
            row.heartbeat_at = now
        await self._session.flush()
        return cancel

    async def completed_evaluator_exists(
        self, session_id: uuid.UUID, plugin_version_id: uuid.UUID
    ) -> bool:
        count = await self._session.scalar(
            select(func.count())
            .select_from(TaskORM)
            .where(
                TaskORM.kind == "evaluator",
                TaskORM.input_session_id == session_id,
                TaskORM.plugin_version_id == plugin_version_id,
                TaskORM.status == TaskStatus.COMPLETED.value,
            )
        )
        return bool(count)
