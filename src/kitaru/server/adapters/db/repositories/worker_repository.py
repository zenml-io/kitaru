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
from datetime import datetime

from sqlalchemy import select, update

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.worker import WorkerORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.worker import Worker, WorkerNotFound

WORKER_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": WorkerORM.id,
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
        """Persist a new worker.

        Args:
            worker: Worker to store.

        Returns:
            Stored worker with its id, created, and updated timestamp set.
        """
        row = WorkerORM.from_domain(worker)
        await self._add(row)
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
        self, worker_filter: WorkerFilter, live_cutoff: datetime | None
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.
            live_cutoff: Bound the last heartbeat must be at or after, None
                keeps stale workers.

        Returns:
            Page of matching workers and the next cursor.
        """
        statement = select(WorkerORM)
        if live_cutoff is not None:
            statement = statement.where(WorkerORM.last_seen_at >= live_cutoff)
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

    async def list_live(self, cutoff: datetime) -> list[Worker]:
        """List workers seen at or after a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be at or after.

        Returns:
            Live workers in id order.
        """
        statement = (
            select(WorkerORM)
            .where(WorkerORM.last_seen_at >= cutoff)
            .order_by(WorkerORM.id)
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        await self._delete_row(worker_id)
