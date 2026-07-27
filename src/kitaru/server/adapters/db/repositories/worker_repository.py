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
from datetime import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.worker import (
    WORKER_NAME_UNIQUE_CONSTRAINT,
    WorkerSchema,
)
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.domain.worker import (
    DuplicateWorkerName,
    Worker,
    WorkerNotFound,
)


class SQLWorkerRepository:
    """Worker repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, worker: Worker) -> Worker:
        """Persist a new worker.

        Args:
            worker: Worker to store.

        Raises:
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with timestamps set.
        """
        row = WorkerSchema.from_domain(worker)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == WORKER_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateWorkerName(worker.name) from exc
            raise
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
        row = await self._session.get(WorkerSchema, worker_id)
        if row is None:
            raise WorkerNotFound(worker_id)
        return row.to_domain()

    async def get_by_name(self, name: str) -> Worker:
        """Load a worker by name.

        Args:
            name: Name of the worker.

        Raises:
            WorkerNotFound: No worker has this name.

        Returns:
            Stored worker.
        """
        statement = select(WorkerSchema).where(col(WorkerSchema.name) == name)
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise WorkerNotFound(name)
        return row.to_domain()

    async def query(self, worker_filter: WorkerFilter) -> tuple[list[Worker], int]:
        """Query workers matching a filter.

        The agent version id filter matches workers whose scope pins the
        version, including workers pinning no version.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the total match count.
        """
        statement = select(WorkerSchema)
        if worker_filter.name is not None:
            statement = statement.where(col(WorkerSchema.name) == worker_filter.name)
        if worker_filter.agent_version_id is not None:
            pinned = col(WorkerSchema.scope)["agent_version_ids"]
            statement = statement.where(
                or_(
                    pinned.is_(None),
                    func.jsonb_typeof(pinned) == "null",
                    pinned.contains([str(worker_filter.agent_version_id)]),
                )
            )
        if worker_filter.seen_after is not None:
            statement = statement.where(
                col(WorkerSchema.last_seen_at) >= worker_filter.seen_after
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(WorkerSchema.id),
            page=worker_filter.page,
            page_size=worker_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, worker: Worker) -> Worker:
        """Persist changes to an existing worker.

        Args:
            worker: Worker with modified fields.

        Raises:
            WorkerNotFound: No worker has this id.
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with the updated timestamp renewed.
        """
        row = await self._session.get(WorkerSchema, worker.id)
        if row is None:
            raise WorkerNotFound(worker.id)
        row.owner_id = worker.owner_id
        row.name = worker.name
        row.scope = worker.scope.model_dump(mode="json")
        row.last_seen_at = worker.last_seen_at
        row.metadata_ = worker.metadata
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == WORKER_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateWorkerName(worker.name) from exc
            raise
        return row.to_domain()

    async def touch(self, worker_id: uuid.UUID, last_seen_at: datetime) -> None:
        """Record a worker sighting, bumping only the last seen time.

        Args:
            worker_id: Id of the worker.
            last_seen_at: Time of the sighting.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        row = await self._session.get(WorkerSchema, worker_id)
        if row is None:
            raise WorkerNotFound(worker_id)
        row.last_seen_at = last_seen_at
        await self._session.flush()

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        row = await self._session.get(WorkerSchema, worker_id)
        if row is None:
            raise WorkerNotFound(worker_id)
        await self._session.delete(row)
        await self._session.flush()
