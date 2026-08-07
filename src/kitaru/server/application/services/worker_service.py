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
"""Worker use cases."""

import uuid
from datetime import UTC, datetime, timedelta

from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.server.application.interfaces.worker_pool_repository import (
    WorkerPoolRepository,
)
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker, WorkerAccessDenied


class WorkerService:
    """Worker use cases."""

    def __init__(
        self,
        repository: WorkerRepository,
        worker_pool_repository: WorkerPoolRepository,
        retention_seconds: int,
        sweep_batch_limit: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Worker repository.
            worker_pool_repository: Worker pool repository.
            retention_seconds: Retention window in seconds before a dead
                worker is pruned.
            sweep_batch_limit: Maximum workers pruned per sweep tick.
        """
        self._repository = repository
        self._worker_pools = worker_pool_repository
        self._retention_seconds = retention_seconds
        self._sweep_batch_limit = sweep_batch_limit

    async def register_worker(
        self,
        name: str,
        scope: WorkerScope,
        runtime: WorkerRuntime,
        metadata: dict[str, str],
        pool: str | None,
        actor: AuthContext,
    ) -> Worker:
        """Register a worker, refreshing an existing row with the same name.

        Args:
            name: Worker name.
            scope: Claim scope the worker reports.
            runtime: Runtime the worker reports.
            metadata: Arbitrary metadata.
            pool: Pool the worker joins by name, None for an ad-hoc scope.
            actor: Caller context.

        Raises:
            WorkerPoolNotFound: No worker pool has this name.

        Returns:
            Stored worker.
        """
        pool_id = None
        if pool is not None:
            pool_id = (await self._worker_pools.get_by_name(pool)).id
        worker = Worker(
            owner_id=actor.account.id,
            name=name,
            pool_id=pool_id,
            scope=scope,
            runtime=runtime,
            metadata=metadata,
            last_seen_at=datetime.now(UTC),
        )
        return await self._repository.register(worker)

    async def get_worker(self, worker_id: uuid.UUID, actor: AuthContext) -> Worker:
        """Get a worker by id.

        An account principal reads any worker. A worker principal reads only
        itself.

        Args:
            worker_id: Id of the worker.
            actor: Caller context.

        Raises:
            WorkerAccessDenied: The caller's worker token names a different
                worker.
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        if isinstance(actor.principal, WorkerPrincipal) and (
            actor.principal.worker_id != worker_id
        ):
            raise WorkerAccessDenied(worker_id)
        return await self._repository.get(worker_id)

    async def list_workers(
        self, worker_filter: WorkerFilter, actor: AuthContext
    ) -> tuple[list[Worker], str | None]:
        """List workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching workers and the next cursor.
        """
        _ = actor
        return await self._repository.query(worker_filter)

    async def delete_worker(self, worker_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a worker.

        Args:
            worker_id: Id of the worker.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        _ = actor
        await self._repository.delete(worker_id)

    async def prune_dead_workers(self, now: datetime) -> int:
        """Delete workers past the retention window with no in-flight task.

        Args:
            now: Current time.

        Returns:
            Number of deleted workers.
        """
        cutoff = now - timedelta(seconds=self._retention_seconds)
        return await self._repository.delete_stale(cutoff, self._sweep_batch_limit)
