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

from kitaru.api_models.v1.task import WorkerScope
from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.worker import Worker, WorkerAccessDenied


class WorkerService:
    """Worker use cases."""

    def __init__(self, repository: WorkerRepository) -> None:
        """Initialize the service.

        Args:
            repository: Worker repository.
        """
        self._repository = repository

    async def register_worker(
        self,
        name: str,
        scope: WorkerScope,
        runtime: WorkerRuntime,
        metadata: dict[str, str],
        actor: AuthContext,
    ) -> Worker:
        """Register a worker, refreshing an existing row with the same name.

        Args:
            name: Worker name.
            scope: Claim scope the worker reports.
            runtime: Runtime the worker reports.
            metadata: Arbitrary metadata.
            actor: Caller context.

        Returns:
            Stored worker.
        """
        worker = Worker(
            owner_id=actor.account.id,
            name=name,
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
