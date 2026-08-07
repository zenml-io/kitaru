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
"""Worker pool use cases."""

import uuid

from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.application.interfaces.worker_pool_repository import (
    WorkerPoolRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.worker_pool import (
    WorkerPoolFilter,
    WorkerPoolUpdate,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.worker_pool import WorkerPool


class WorkerPoolService:
    """Worker pool use cases."""

    def __init__(self, repository: WorkerPoolRepository) -> None:
        """Initialize the service.

        Args:
            repository: Worker pool repository.
        """
        self._repository = repository

    async def create_worker_pool(
        self, name: str, scope: WorkerScope, actor: AuthContext
    ) -> WorkerPool:
        """Create a worker pool owned by the caller.

        Args:
            name: Worker pool name.
            scope: Tasks the pool's workers claim.
            actor: Caller context.

        Raises:
            DuplicateWorkerPoolName: The worker pool name is already registered.
            WorkerPoolScopePinsJob: The scope names a job.

        Returns:
            Created worker pool.
        """
        worker_pool = WorkerPool(owner_id=actor.account.id, name=name, scope=scope)
        return await self._repository.create(worker_pool)

    async def get_worker_pool(
        self, worker_pool_id: uuid.UUID, actor: AuthContext
    ) -> WorkerPool:
        """Get a worker pool by id.

        Args:
            worker_pool_id: Id of the worker pool.
            actor: Caller context.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.

        Returns:
            Stored worker pool.
        """
        _ = actor
        return await self._repository.get(worker_pool_id)

    async def list_worker_pools(
        self, worker_pool_filter: WorkerPoolFilter, actor: AuthContext
    ) -> tuple[list[WorkerPool], str | None]:
        """List worker pools matching a filter.

        Args:
            worker_pool_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching worker pools and the next cursor.
        """
        _ = actor
        return await self._repository.query(worker_pool_filter)

    async def update_worker_pool(
        self,
        worker_pool_id: uuid.UUID,
        command: WorkerPoolUpdate,
        actor: AuthContext,
    ) -> WorkerPool:
        """Partially update a worker pool.

        Args:
            worker_pool_id: Id of the worker pool.
            command: Fields to change, built from the request's set fields.
            actor: Caller context.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
            ValidationError: The command clears the name or the scope.
            WorkerPoolScopePinsJob: The new scope names a job.
            DuplicateWorkerPoolName: The worker pool name is already registered.

        Returns:
            Updated worker pool.
        """
        _ = actor
        worker_pool = await self._repository.get(worker_pool_id)
        fields = command.model_fields_set
        if "name" in fields:
            if command.name is None:
                raise ValidationError("Worker pool name cannot be cleared")
            worker_pool.update_name(command.name)
        if "scope" in fields:
            if command.scope is None:
                raise ValidationError("Worker pool scope cannot be cleared")
            worker_pool.update_scope(command.scope)
        return await self._repository.update(worker_pool)

    async def delete_worker_pool(
        self, worker_pool_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete a worker pool.

        Args:
            worker_pool_id: Id of the worker pool.
            actor: Caller context.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
        """
        _ = actor
        await self._repository.delete(worker_pool_id)
