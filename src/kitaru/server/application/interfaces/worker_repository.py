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
"""Worker repository interface."""

import uuid
from datetime import datetime
from typing import Protocol

from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import LiveWorkerStats, Worker


class WorkerRepository(Protocol):
    """Worker persistence operations."""

    async def register(self, worker: Worker) -> Worker:
        """Persist a worker, refreshing an existing row with the same name.

        Args:
            worker: Worker to store or refresh.

        Returns:
            Stored worker with its id, created, and updated timestamp set.
        """
        ...

    async def get(self, worker_id: uuid.UUID) -> Worker:
        """Load a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Stored worker.
        """
        ...

    async def update_last_seen_at(self, worker_id: uuid.UUID, now: datetime) -> None:
        """Stamp the time the worker was last seen.

        Args:
            worker_id: Id of the worker.
            now: Current time.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        ...

    async def query(
        self, worker_filter: WorkerFilter
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the next cursor.
        """
        ...

    async def count_live_by_pool(
        self, pool_id: uuid.UUID, cutoff: datetime
    ) -> LiveWorkerStats:
        """Count the pool's live workers and sum their concurrency.

        Args:
            pool_id: Id of the worker pool.
            cutoff: Bound the last heartbeat must be at or after.

        Returns:
            Live worker count and summed concurrency in the pool.
        """
        ...

    async def delete(self, worker_id: uuid.UUID) -> None:
        """Delete a worker by id.

        Args:
            worker_id: Id of the worker.

        Raises:
            WorkerNotFound: No worker has this id.
        """
        ...

    async def delete_stale(self, cutoff: datetime, limit: int) -> int:
        """Delete workers last seen before a cutoff with no in-flight task.

        Terminal tasks referencing a pruned worker keep their rows and lose
        the reference through the foreign key's SET NULL.

        Args:
            cutoff: Bound the last heartbeat must be older than.
            limit: Maximum number of workers to delete.

        Returns:
            Number of deleted workers.
        """
        ...
