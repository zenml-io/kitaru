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

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.analytics_events import (
    build_worker_registered_properties,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.worker import Worker, WorkerAccessDenied


class WorkerService:
    """Worker use cases."""

    def __init__(
        self,
        repository: WorkerRepository,
        liveness_timeout_seconds: int,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Worker repository.
            liveness_timeout_seconds: Liveness window in seconds.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._liveness_timeout_seconds = liveness_timeout_seconds
        self._analytics = analytics

    async def register_worker(
        self,
        name: str,
        scope: WorkerScope,
        runtime: WorkerRuntime,
        metadata: dict[str, str],
        actor: AuthContext,
    ) -> Worker:
        """Register a worker.

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
        stored = await self._repository.register(worker)
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.WORKER_REGISTERED,
                build_worker_registered_properties(stored),
            )
        return stored

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

    async def renew_worker(self, worker_id: uuid.UUID, actor: AuthContext) -> None:
        """Stamp a worker as seen ahead of issuing it a fresh token.

        Args:
            worker_id: Id of the worker.
            actor: Caller context.

        Raises:
            WorkerAccessDenied: The worker belongs to another account.
            WorkerNotFound: No worker has this id.
        """
        worker = await self._repository.get(worker_id)
        if worker.owner_id != actor.account.id:
            raise WorkerAccessDenied(worker_id)
        await self._repository.update_last_seen_at(worker_id, datetime.now(UTC))

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
        live_cutoff = None
        if not worker_filter.include_stale:
            live_cutoff = datetime.now(UTC) - timedelta(
                seconds=self._liveness_timeout_seconds
            )
        return await self._repository.query(worker_filter, live_cutoff)

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
