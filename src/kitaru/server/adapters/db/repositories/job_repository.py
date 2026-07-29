"""SQL job repository."""

import uuid

from sqlalchemy import select

from kitaru.server.adapters.db.orm.job import JobORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.job import Job, JobNotFound


class SQLJobRepository(BaseSQLRepository[JobORM]):
    """Job repository backed by PostgreSQL."""

    orm_class = JobORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return JobNotFound(entity_id)

    async def create(self, job: Job) -> Job:
        row = JobORM.from_domain(job)
        await self._add(row)
        return row.to_domain()

    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job:
        statement = select(JobORM).where(JobORM.id == job_id)
        if exclusive:
            statement = statement.with_for_update()
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise JobNotFound(job_id)
        return row.to_domain()

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]:
        statement = select(JobORM)
        if job_filter.status is not None:
            statement = statement.where(JobORM.status == job_filter.status.value)
        rows, cursor = await paginate(self._session, statement, job_filter, JobORM.id)
        return [row.to_domain() for row in rows], cursor

    async def query_ids(
        self, job_ids: list[uuid.UUID], job_filter: JobFilter
    ) -> tuple[list[Job], str | None]:
        """Query a bounded set of jobs."""
        statement = select(JobORM).where(JobORM.id.in_(job_ids))
        if job_filter.status is not None:
            statement = statement.where(JobORM.status == job_filter.status.value)
        rows, cursor = await paginate(self._session, statement, job_filter, JobORM.id)
        return [row.to_domain() for row in rows], cursor

    async def update(self, job: Job) -> Job:
        row = await self._get_row(job.id)
        source = JobORM.from_domain(job)
        for name in (
            "status",
            "cancel_requested_at",
            "started_at",
            "ended_at",
            "error",
        ):
            setattr(row, name, getattr(source, name))
        await self._session.flush()
        return row.to_domain()

    async def delete(self, job_id: uuid.UUID) -> None:
        await self._delete_row(job_id)
