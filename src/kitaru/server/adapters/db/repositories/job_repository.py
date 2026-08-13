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

from sqlalchemy import not_, or_, select

from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.job import JobORM
from kitaru.server.adapters.db.orm.task import TERMINAL_STATUS_VALUES, TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.job import TERMINAL_JOB_STATUSES, Job, JobNotFound

JOB_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": JobORM.id,
    "kind": JobORM.kind,
    "status": JobORM.status,
}

TERMINAL_JOB_STATUS_VALUES = [status.value for status in TERMINAL_JOB_STATUSES]


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
        row = await self._get_row(job_id, exclusive=exclusive)
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

    async def get_many_locked(
        self, job_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Job]:
        """Bulk-lock and load jobs by id, keyed by id, missing ids omitted.

        Rows are locked in id order.

        Args:
            job_ids: Ids of the jobs to load.

        Returns:
            Locked jobs keyed by id.
        """
        rows = await self._load_by_ids(job_ids, exclusive=True)
        return {job_id: row.to_domain() for job_id, row in rows.items()}

    async def list_unpropagated_cancel_ids(self, limit: int) -> list[uuid.UUID]:
        """Read the ids of canceling jobs whose live tasks still owe the stamp.

        A live task owes the stamp when it carries no cancel request of its
        own, or when it is still pending and has to move straight to
        canceled. Rows are read without locking.

        Args:
            limit: Maximum number of ids to read.

        Returns:
            Ids of the canceling jobs in ascending order.
        """
        owing = select(TaskORM.id).where(
            TaskORM.job_id == JobORM.id,
            not_(TaskORM.status.in_(TERMINAL_STATUS_VALUES)),
            or_(
                TaskORM.cancel_requested_at.is_(None),
                TaskORM.status == TaskStatus.PENDING.value,
            ),
        )
        statement = (
            select(JobORM.id)
            .where(
                JobORM.cancel_requested_at.is_not(None),
                not_(JobORM.status.in_(TERMINAL_JOB_STATUS_VALUES)),
                owing.exists(),
            )
            .order_by(JobORM.id.asc())
            .limit(limit)
        )
        return list((await self._session.scalars(statement)).all())

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

    async def update_many(self, jobs: list[Job]) -> list[Job]:
        """Persist changes to many existing jobs in one round trip.

        Every job row is already loaded and locked in the session's identity
        map by ``get_many_locked``, so ``_get_row`` resolves from memory and
        the changes flush as one batch.

        Args:
            jobs: Jobs with modified fields.

        Raises:
            JobNotFound: A job id matches no job.

        Returns:
            Stored jobs with the updated timestamp renewed, in the same order.
        """
        if not jobs:
            return []
        rows = []
        for job in jobs:
            row = await self._get_row(job.id)
            row.apply(job)
            rows.append(row)
        await self._flush()
        return [row.to_domain() for row in rows]

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id, cascading its tasks.

        The job's task rows are locked in id order before the job row, since
        the delete's cascade would otherwise lock them unordered after it.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        await self._lock_task_rows([job_id])
        await self._delete_row(job_id)

    async def delete_many(self, job_ids: Sequence[uuid.UUID]) -> None:
        """Delete many jobs by id, cascading their tasks.

        The jobs' task rows are locked in id order across all jobs before
        the job rows, which are deleted in ascending id order.

        Args:
            job_ids: Ids of the jobs.

        Raises:
            JobNotFound: A job id matches no job.
        """
        if not job_ids:
            return
        await self._lock_task_rows(job_ids)
        for job_id in sorted(job_ids):
            await self._delete_row(job_id)

    async def _lock_task_rows(self, job_ids: Sequence[uuid.UUID]) -> None:
        """Lock the jobs' task rows in id order.

        Args:
            job_ids: Ids the tasks belong to.
        """
        statement = (
            select(TaskORM.id)
            .where(TaskORM.job_id.in_(list(job_ids)))
            .order_by(TaskORM.id.asc())
            .with_for_update()
        )
        await self._session.execute(statement)
