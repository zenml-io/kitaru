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
from kitaru.server.domain.worker import Worker


class WorkerRepository(Protocol):
    """Worker persistence operations."""

    async def register(self, worker: Worker) -> Worker:
        """Persist a new worker.

        Args:
            worker: Worker to store.

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
        self, worker_filter: WorkerFilter, live_cutoff: datetime | None
    ) -> tuple[list[Worker], str | None]:
        """Query workers matching a filter.

        Args:
            worker_filter: Filter and pagination parameters.
            live_cutoff: Bound the last heartbeat must be at or after, None
                keeps stale workers.

        Returns:
            Page of matching workers and the next cursor.
        """
        ...

    async def list_live(self, cutoff: datetime) -> list[Worker]:
        """List workers seen at or after a cutoff.

        Args:
            cutoff: Bound the last heartbeat must be at or after.

        Returns:
            Live workers in id order.
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
