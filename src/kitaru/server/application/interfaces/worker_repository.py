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

from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.domain.worker import Worker


class WorkerRepository(Protocol):
    """Worker persistence operations."""

    async def create(self, worker: Worker) -> Worker:
        """Persist a new worker.

        Args:
            worker: Worker to store.

        Raises:
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with timestamps set.
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

    async def get_by_name(self, name: str) -> Worker:
        """Load a worker by name.

        Args:
            name: Name of the worker.

        Raises:
            WorkerNotFound: No worker has this name.

        Returns:
            Stored worker.
        """
        ...

    async def query(self, worker_filter: WorkerFilter) -> tuple[list[Worker], int]:
        """Query workers matching a filter.

        The agent id filter matches workers serving the agent, including
        workers serving all agents.

        Args:
            worker_filter: Filter and pagination parameters.

        Returns:
            Page of matching workers and the total match count.
        """
        ...

    async def update(self, worker: Worker) -> Worker:
        """Persist changes to an existing worker.

        Args:
            worker: Worker with modified fields.

        Raises:
            WorkerNotFound: No worker has this id.
            DuplicateWorkerName: The worker name is already registered.

        Returns:
            Stored worker with the updated timestamp renewed.
        """
        ...

    async def touch(self, worker_id: uuid.UUID, last_seen_at: datetime) -> None:
        """Record a worker sighting, bumping only the last seen time.

        Args:
            worker_id: Id of the worker.
            last_seen_at: Time of the sighting.

        Raises:
            WorkerNotFound: No worker has this id.
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
