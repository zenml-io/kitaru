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
"""SQL import job repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.schemas.import_job import ImportJobSchema
from kitaru.server.domain.import_job import (
    ImportJob,
    ImportJobNotFound,
    ImportJobStatus,
)


class SQLImportJobRepository:
    """Import job repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository."""
        self._session = session

    async def create(self, job: ImportJob) -> ImportJob:
        """Persist a new job."""
        row = ImportJobSchema.from_domain(job)
        self._session.add(row)
        await self._session.flush()
        return row.to_domain()

    async def get(self, job_id: uuid.UUID) -> ImportJob:
        """Load a job by id."""
        row = await self._session.get(ImportJobSchema, job_id)
        if row is None:
            raise ImportJobNotFound(job_id)
        return row.to_domain()

    async def update(self, job: ImportJob) -> ImportJob:
        """Persist job changes."""
        row = await self._session.get(ImportJobSchema, job.id)
        if row is None:
            raise ImportJobNotFound(job.id)
        replacement = ImportJobSchema.from_domain(job)
        for field in (
            "agent_version_id",
            "importer_id",
            "importer_version",
            "source_instance",
            "filename",
            "content",
            "status",
            "worker_id",
            "started_at",
            "ended_at",
            "source_session_count",
            "imported_count",
            "deduplicated_count",
            "failed_count",
            "session_ids",
            "errors",
            "error",
        ):
            setattr(row, field, getattr(replacement, field))
        await self._session.flush()
        return row.to_domain()

    async def claim_next(self, worker_id: str) -> ImportJob | None:
        """Claim the oldest pending job."""
        statement = (
            select(ImportJobSchema)
            .where(col(ImportJobSchema.status) == ImportJobStatus.PENDING.value)
            .order_by(col(ImportJobSchema.created))
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        row = (await self._session.execute(statement)).scalar_one_or_none()
        if row is None:
            return None
        job = row.to_domain()
        job.start(worker_id)
        return await self.update(job)
