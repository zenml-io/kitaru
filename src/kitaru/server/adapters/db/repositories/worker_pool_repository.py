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
"""SQL worker pool repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.worker_pool import (
    WORKER_POOL_NAME_UNIQUE_CONSTRAINT,
    WorkerPoolORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.worker_pool import WorkerPoolFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.worker_pool import (
    DuplicateWorkerPoolName,
    WorkerPool,
    WorkerPoolNotFound,
)

WORKER_POOL_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "name": WorkerPoolORM.name,
}


class SQLWorkerPoolRepository(BaseSQLRepository[WorkerPoolORM]):
    """Worker pool repository backed by the application database."""

    orm_class = WorkerPoolORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return WorkerPoolNotFound(entity_id)

    async def create(self, worker_pool: WorkerPool) -> WorkerPool:
        """Persist a new worker pool.

        Args:
            worker_pool: Worker pool to store.

        Raises:
            DuplicateWorkerPoolName: The worker pool name is already registered.

        Returns:
            Stored worker pool with timestamps set.
        """
        row = WorkerPoolORM.from_domain(worker_pool)
        await self._add(
            row,
            {
                WORKER_POOL_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateWorkerPoolName(
                    worker_pool.name
                )
            },
        )
        return row.to_domain()

    async def get(self, worker_pool_id: uuid.UUID) -> WorkerPool:
        """Load a worker pool by id.

        Args:
            worker_pool_id: Id of the worker pool.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.

        Returns:
            Stored worker pool.
        """
        row = await self._get_row(worker_pool_id)
        return row.to_domain()

    async def get_by_name(self, name: str) -> WorkerPool:
        """Load a worker pool by name.

        Args:
            name: Worker pool name.

        Raises:
            WorkerPoolNotFound: No worker pool has this name.

        Returns:
            Stored worker pool.
        """
        statement = select(WorkerPoolORM).where(WorkerPoolORM.name == name)
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise WorkerPoolNotFound(name)
        return row.to_domain()

    async def query(
        self, worker_pool_filter: WorkerPoolFilter
    ) -> tuple[list[WorkerPool], str | None]:
        """Query worker pools matching a filter.

        Args:
            worker_pool_filter: Filter and pagination parameters.

        Returns:
            Page of matching worker pools and the next cursor.
        """
        statement = select(WorkerPoolORM)
        if worker_pool_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    worker_pool_filter.expression, WORKER_POOL_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, worker_pool_filter, id_column=WorkerPoolORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, worker_pool: WorkerPool) -> WorkerPool:
        """Persist changes to an existing worker pool.

        Args:
            worker_pool: Worker pool with modified fields.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
            DuplicateWorkerPoolName: The worker pool name is already registered.

        Returns:
            Stored worker pool with the updated timestamp renewed.
        """
        row = await self._get_row(worker_pool.id)
        row.name = worker_pool.name
        row.scope = worker_pool.scope.model_dump(mode="json")
        await self._flush(
            {
                WORKER_POOL_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateWorkerPoolName(
                    worker_pool.name
                )
            }
        )
        return row.to_domain()

    async def delete(self, worker_pool_id: uuid.UUID) -> None:
        """Delete a worker pool by id.

        Args:
            worker_pool_id: Id of the worker pool.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
        """
        await self._delete_row(worker_pool_id)
