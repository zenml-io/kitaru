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
"""SQL worker repository."""

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime

from sqlalchemy import CursorResult, delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.task import IN_FLIGHT_PREDICATE, TaskORM
from kitaru.server.adapters.db.orm.worker import (
    WORKER_NAME_UNIQUE_CONSTRAINT,
    WorkerORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.worker import Worker, WorkerNotFound

WORKER_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "name": WorkerORM.name,
}


class SQLWorkerRepository(BaseSQLRepository[WorkerORM]):
    """Worker repository backed by the application database."""

    orm_class = WorkerORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return WorkerNotFound(entity_id)

    async def register(self, worker: Worker) -> Worker:
        """Persist a worker, refreshing an existing row with the same name.

        The insert races no fallback lookup: a concurrent registration under
        the same name resolves through the database's own conflict handling,
        never through a read-then-write from this repository.

        Args:
            worker: Worker to store or refresh.

        Returns:
            Stored worker with its id, created, and updated timestamp set.
        """
        now = datetime.now(UTC)
        statement = insert(WorkerORM).values(**WorkerORM.column_values(worker))
        # The client-side onupdate hook on TimestampMixin.updated never fires
        # for this Core-level statement, so the conflict branch renews it
        # explicitly. owner_id, id, and created stay out of the SET clause so
        # re-registration keeps the original owner and creation time.
        statement = statement.on_conflict_do_update(
            constraint=WORKER_NAME_UNIQUE_CONSTRAINT,
            set_={
                "pool_id": statement.excluded.pool_id,
                "scope": statement.excluded.scope,
                "runtime": statement.excluded.runtime,
                "last_seen_at": statement.excluded.last_seen_at,
                "metadata": statement.excluded["metadata"],
                "updated": now,
            },
        ).returning(WorkerORM)
        row = (
            await self._session.scalars(
                statement, execution_options={"populate_existing": True}
            )
        ).one()
        return row.to_domain()

    async def get(self, worker_id: uuid.UUID) -> Worker:
        """Load a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        row = await self._get_row(worker_id)
        return row.to_domain()

    async def update_last_seen_at(self, worker_id: uuid.UUID, now: datetime) -> None:
        """Stamp the time the worker was last seen.

        This is a Core-level statement rather than an ORM attribute mutation,
        so the ``updated`` timestamp needs to be stamped here explicitly, the
        ``onupdate`` client-side default never fires for it.

        Args:
            worker_id: Id of the worker.
            now: Current time.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        statement = (
            update(WorkerORM)
            .where(WorkerORM.id == worker_id)
            .values(last_seen_at=now, updated=now)
            .returning(WorkerORM.id)
            .execution_options(synchronize_session="fetch")
        )
        updated = (await self._session.scalars(statement)).one_or_none()
        if updated is None:
            raise WorkerNotFound(worker_id)
        await self._session.flush()

    async def query(
        self, worker_filter: WorkerFilter
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the next cursor.
        """
        statement = select(WorkerORM)
        if worker_filter.seen_after is not None:
            statement = statement.where(
                WorkerORM.last_seen_at >= worker_filter.seen_after
            )
        if worker_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    worker_filter.expression, WORKER_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, worker_filter, id_column=WorkerORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def count_live_by_pool(self, pool_id: uuid.UUID, cutoff: datetime) -> int:
        """Count the pool's workers last seen at or after a cutoff.

        Args:
            pool_id: Id of the worker pool.
            cutoff: Bound the last heartbeat must be at or after.

        Returns:
            Count of live workers in the pool.
        """
        statement = select(func.count(WorkerORM.id)).where(
            WorkerORM.pool_id == pool_id, WorkerORM.last_seen_at >= cutoff
        )
        result = await self._session.scalar(statement)
        return result if result is not None else 0

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        await self._delete_row(worker_id)

    async def delete_stale(self, cutoff: datetime, limit: int) -> int:
        """Delete workers last seen before a cutoff with no in-flight task.

        The staleness check and the delete run as one statement, so a
        worker claimed between the check and the delete is never removed.
        Terminal tasks referencing a pruned worker keep their rows and lose
        the reference through the foreign key's SET NULL.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of workers to delete.

        Returns:
            Number of deleted workers.
        """
        in_flight = select(TaskORM.id).where(
            TaskORM.worker_id == WorkerORM.id, text(IN_FLIGHT_PREDICATE)
        )
        stale_ids = (
            select(WorkerORM.id)
            .where(WorkerORM.last_seen_at < cutoff, ~in_flight.exists())
            .order_by(WorkerORM.id.asc())
            .limit(limit)
        )
        statement = delete(WorkerORM).where(WorkerORM.id.in_(stale_ids))
        result = await self._session.execute(statement)
        return result.rowcount if isinstance(result, CursorResult) else 0
