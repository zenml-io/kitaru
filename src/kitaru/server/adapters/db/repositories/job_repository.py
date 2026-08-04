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
"""SQL job repository."""

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import select

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.job import JobORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.job import Job, JobNotFound

JOB_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "kind": JobORM.kind,
    "status": JobORM.status,
}


class SQLJobRepository(BaseSQLRepository[JobORM]):
    """Job repository backed by the application database."""

    orm_class = JobORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return JobNotFound(entity_id)

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Returns:
            Stored job with timestamps set.
        """
        row = JobORM.from_domain(job)
        await self._add(row)
        return row.to_domain()

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist many new jobs in one round trip.

        Args:
            jobs: Jobs to store.

        Returns:
            Stored jobs with timestamps set, in the same order.
        """
        if not jobs:
            return []
        rows = [JobORM.from_domain(job) for job in jobs]
        self._session.add_all(rows)
        await self._flush()
        return [row.to_domain() for row in rows]

    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        row = await self._session.get(self.orm_class, job_id, with_for_update=exclusive)
        if row is None:
            raise JobNotFound(job_id)
        return row.to_domain()

    async def get_many(self, job_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Job]:
        """Bulk-load jobs by id, keyed by id, missing ids omitted.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Stored jobs keyed by id.
        """
        rows = await self._load_by_ids(job_ids)
        return {job_id: row.to_domain() for job_id, row in rows.items()}

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]:
        """Query jobs matching a filter.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the next cursor.
        """
        statement = select(JobORM)
        if job_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(job_filter.expression, JOB_FILTER_BINDINGS)
            )
        rows, next_cursor = await paginate(
            self._session, statement, job_filter, id_column=JobORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        row = await self._get_row(job.id)
        row.apply(job)
        await self._flush()
        return row.to_domain()

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id, cascading its tasks.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        await self._delete_row(job_id)
