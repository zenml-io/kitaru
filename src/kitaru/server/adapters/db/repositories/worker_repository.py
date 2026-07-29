"""SQL worker repository."""

import uuid
from typing import cast

from sqlalchemy import Table, select
from sqlalchemy.dialects.postgresql import insert

from kitaru.server.adapters.db.orm.worker import WorkerORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.worker import Worker, WorkerNotFound


class SQLWorkerRepository(BaseSQLRepository[WorkerORM]):
    """Worker repository backed by PostgreSQL."""

    orm_class = WorkerORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return WorkerNotFound(entity_id)

    async def upsert(self, worker: Worker) -> Worker:
        row = WorkerORM.from_domain(worker)
        table = cast(Table, WorkerORM.__table__)
        statement = (
            insert(table)
            .values(
                id=row.id,
                owner_id=row.owner_id,
                name=row.name,
                scope=row.scope,
                runtime=row.runtime,
                last_seen_at=row.last_seen_at,
                metadata=row.metadata_,
            )
            .on_conflict_do_update(
                index_elements=[table.c["name"]],
                set_={
                    "owner_id": row.owner_id,
                    "scope": row.scope,
                    "runtime": row.runtime,
                    "last_seen_at": row.last_seen_at,
                    "metadata": row.metadata_,
                },
            )
            .returning(table.c["id"])
        )
        worker_id = (await self._session.execute(statement)).scalar_one()
        return (await self._get_row(worker_id)).to_domain()

    async def get(self, worker_id: uuid.UUID) -> Worker:
        return (await self._get_row(worker_id)).to_domain()

    async def get_by_name(self, name: str) -> Worker:
        row = (
            await self._session.scalars(select(WorkerORM).where(WorkerORM.name == name))
        ).one_or_none()
        if row is None:
            raise WorkerNotFound(name)
        return row.to_domain()

    async def query(
        self, worker_filter: WorkerFilter
    ) -> tuple[list[Worker], str | None]:
        statement = select(WorkerORM)
        if worker_filter.name is not None:
            statement = statement.where(WorkerORM.name == worker_filter.name)
        if worker_filter.seen_after is not None:
            statement = statement.where(
                WorkerORM.last_seen_at >= worker_filter.seen_after
            )
        rows, cursor = await paginate(
            self._session, statement, worker_filter, WorkerORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, worker: Worker) -> Worker:
        row = await self._get_row(worker.id)
        source = WorkerORM.from_domain(worker)
        row.scope = source.scope
        row.runtime = source.runtime
        row.last_seen_at = source.last_seen_at
        row.metadata_ = source.metadata_
        await self._session.flush()
        return row.to_domain()

    async def delete(self, worker_id: uuid.UUID) -> None:
        await self._delete_row(worker_id)
