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
from datetime import UTC, datetime
from typing import Any

from kitaru.server.application.interfaces.worker_repository import (
    WorkerRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.domain.job import WorkerScope
from kitaru.server.domain.worker import DuplicateWorkerName, Worker


class WorkerService:
    """Worker use cases."""

    def __init__(
        self, repository: WorkerRepository, liveness_timeout_seconds: int
    ) -> None:
        """Initialize the service.

        Args:
            repository: Worker repository.
            liveness_timeout_seconds: Seconds after which a worker counts
                as dead.
        """
        self._repository = repository
        self.liveness_timeout_seconds = liveness_timeout_seconds

    async def register_worker(
        self,
        name: str,
        scope: WorkerScope,
        metadata: dict[str, Any],
        actor: AuthContext,
    ) -> Worker:
        """Register a worker owned by the caller, upserting by name.

        A worker already registered under the name gets its claim scope
        and metadata replaced and its last seen time bumped. A concurrent
        registration of the same name falls back to the update path.

        Args:
            name: Worker name.
            scope: Claim scope.
            metadata: Worker metadata.
            actor: Caller context.

        Raises:
            WorkerNotFound: The worker was deleted between the duplicate
                name failure and the fallback lookup.

        Returns:
            Registered worker.
        """
        worker = Worker(
            owner_id=actor.account.id,
            name=name,
            scope=scope,
            last_seen_at=datetime.now(UTC),
            metadata=metadata,
        )
        try:
            return await self._repository.create(worker)
        except DuplicateWorkerName:
            existing = await self._repository.get_by_name(name)
            existing.refresh(scope=scope, metadata=metadata)
            return await self._repository.update(existing)

    async def get_worker(self, worker_id: uuid.UUID, actor: AuthContext) -> Worker:
        """Get a worker by id.

        Args:
            worker_id: Id of the worker.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        _ = actor
        return await self._repository.get(worker_id)

    async def list_workers(
        self, worker_filter: WorkerFilter, actor: AuthContext
    ) -> tuple[list[Worker], int]:
        """List workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching workers and the total match count.
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
