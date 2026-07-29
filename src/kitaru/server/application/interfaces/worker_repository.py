"""Worker repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker


class WorkerRepository(Protocol):
    """Worker registration persistence operations."""

    async def upsert(self, worker: Worker) -> Worker: ...
    async def get(self, worker_id: uuid.UUID) -> Worker: ...
    async def get_by_name(self, name: str) -> Worker: ...
    async def query(
        self, worker_filter: WorkerFilter
    ) -> tuple[list[Worker], str | None]: ...
    async def update(self, worker: Worker) -> Worker: ...
    async def delete(self, worker_id: uuid.UUID) -> None: ...
