"""Job repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.job import Job


class JobRepository(Protocol):
    """Job persistence operations."""

    async def create(self, job: Job) -> Job: ...
    async def get(self, job_id: uuid.UUID, exclusive: bool = False) -> Job: ...
    async def query(self, job_filter: JobFilter) -> tuple[list[Job], str | None]: ...
    async def query_ids(
        self, job_ids: list[uuid.UUID], job_filter: JobFilter
    ) -> tuple[list[Job], str | None]: ...
    async def update(self, job: Job) -> Job: ...
    async def delete(self, job_id: uuid.UUID) -> None: ...
