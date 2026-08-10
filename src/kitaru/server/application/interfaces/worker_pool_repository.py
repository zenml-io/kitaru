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
"""Worker pool repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.worker_pool import WorkerPoolFilter
from kitaru.server.domain.worker_pool import WorkerPool


class WorkerPoolRepository(Protocol):
    """Worker pool persistence operations."""

    async def create(self, worker_pool: WorkerPool) -> WorkerPool:
        """Persist a new worker pool.

        Args:
            worker_pool: Worker pool to store.

        Raises:
            DuplicateWorkerPoolName: The worker pool name is already registered.

        Returns:
            Stored worker pool with timestamps set.
        """
        ...

    async def get(self, worker_pool_id: uuid.UUID) -> WorkerPool:
        """Load a worker pool by id.

        Args:
            worker_pool_id: Id of the worker pool.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.

        Returns:
            Stored worker pool.
        """
        ...

    async def get_by_name(self, name: str) -> WorkerPool:
        """Load a worker pool by name.

        Args:
            name: Worker pool name.

        Raises:
            WorkerPoolNotFound: No worker pool has this name.

        Returns:
            Stored worker pool.
        """
        ...

    async def query(
        self, worker_pool_filter: WorkerPoolFilter
    ) -> tuple[list[WorkerPool], str | None]:
        """Query worker pools matching a filter.

        Args:
            worker_pool_filter: Filter and pagination parameters.

        Returns:
            Page of matching worker pools and the next cursor.
        """
        ...

    async def update(self, worker_pool: WorkerPool) -> WorkerPool:
        """Persist changes to an existing worker pool.

        Args:
            worker_pool: Worker pool with modified fields.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
            DuplicateWorkerPoolName: The worker pool name is already registered.

        Returns:
            Stored worker pool with the updated timestamp renewed.
        """
        ...

    async def delete(self, worker_pool_id: uuid.UUID) -> None:
        """Delete a worker pool by id.

        Args:
            worker_pool_id: Id of the worker pool.

        Raises:
            WorkerPoolNotFound: No worker pool has this id.
            WorkerPoolInUse: A worker references the worker pool.
        """
        ...
