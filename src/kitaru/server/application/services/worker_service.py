"""Worker registration use cases."""

import uuid
from datetime import UTC, datetime
from typing import Any

from kitaru.api_models.v1.task import WorkerScope
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker, WorkerRuntime


class WorkerService:
    """Worker registration, reads, and deletion."""

    def __init__(
        self,
        repository: WorkerRepository,
        liveness_timeout_seconds: float = 60.0,
    ) -> None:
        self._repository = repository
        self._liveness_timeout_seconds = liveness_timeout_seconds

    async def register_worker(
        self,
        name: str,
        scope: WorkerScope,
        runtime: WorkerRuntime,
        metadata: dict[str, Any],
        actor: AuthContext,
    ) -> tuple[Worker, bool]:
        """Atomically register or refresh a worker by name."""
        worker = Worker(
            owner_id=actor.account.id,
            name=name,
            scope=scope,
            runtime=runtime,
            last_seen_at=datetime.now(UTC),
            metadata=metadata,
        )
        registered = await self._repository.upsert(worker)
        return registered, self._is_live(registered)

    async def get_worker(
        self, worker_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Worker, bool]:
        """Get a worker."""
        _ = actor
        worker = await self._repository.get(worker_id)
        return worker, self._is_live(worker)

    async def list_workers(
        self, worker_filter: WorkerFilter, actor: AuthContext
    ) -> tuple[list[tuple[Worker, bool]], str | None]:
        """List workers."""
        _ = actor
        workers, cursor = await self._repository.query(worker_filter)
        return [(worker, self._is_live(worker)) for worker in workers], cursor

    async def delete_worker(self, worker_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a worker."""
        _ = actor
        await self._repository.delete(worker_id)

    def _is_live(self, worker: Worker) -> bool:
        """Compute liveness from the worker's most recent check-in."""
        return worker.is_live(self._liveness_timeout_seconds)
