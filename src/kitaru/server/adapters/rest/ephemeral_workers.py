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
"""Ephemeral worker start scheduling."""

import logging
import uuid
from importlib.metadata import version

from fastapi import BackgroundTasks
from pydantic import SecretStr

from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.ephemeral_workers import EphemeralWorkers
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.models.worker import EphemeralWorkerSpec
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import (
    WorkerService,
    get_ephemeral_scope,
)
from kitaru.server.domain.worker import Worker, scope_covers
from kitaru.server.utils import paginate_all

logger = logging.getLogger(__name__)

SANDBOX_TAG_PREFIX = "kitaru/"


class EphemeralWorkerStarter:
    """Ephemeral worker start scheduling for one request."""

    def __init__(
        self,
        job_service: JobService,
        worker_service: WorkerService,
        auth_service: AuthService,
        ephemeral_workers: EphemeralWorkers | None,
        settings: APISettings,
        server_id: uuid.UUID | None,
        background_tasks: BackgroundTasks,
    ) -> None:
        """Initialize the starter.

        Args:
            job_service: Job service.
            worker_service: Worker service.
            auth_service: Authentication service for the current request.
            ephemeral_workers: Ephemeral worker backend, None when none is
                configured.
            settings: API settings for this process.
            server_id: Persisted server id, None before startup resolved it.
            background_tasks: Tasks run after the response is sent.
        """
        self._job_service = job_service
        self._worker_service = worker_service
        self._auth_service = auth_service
        self._ephemeral_workers = ephemeral_workers
        self._settings = settings
        self._server_id = server_id
        self._background_tasks = background_tasks

    async def start(self, job_id: uuid.UUID, actor: AuthContext) -> None:
        """Register a worker for the job and start it after the response.

        No configured backend suppresses the start, as does a live worker
        whose scope covers the job's tasks or any task the ephemeral scope
        would not claim.

        Args:
            job_id: Id of the job the worker drains.
            actor: Caller context.
        """
        if self._ephemeral_workers is None:
            return
        tasks = await paginate_all(
            lambda cursor: self._job_service.list_job_tasks(
                job_id, TaskFilter(cursor=cursor), actor=actor
            )
        )
        tasks = [task for task in tasks if not task.terminal]
        if not tasks:
            return
        scope = get_ephemeral_scope(job_id, self._settings.EPHEMERAL_WORKER.selectors)
        if not all(scope_covers(scope, task) for task in tasks):
            return
        if await self._worker_service.is_covered(tasks):
            return
        worker = await self._worker_service.register_ephemeral_worker(
            job_id,
            WorkerRuntime(platform=self._settings.EPHEMERAL_WORKER.backend.value),
            self._settings.EPHEMERAL_WORKER.selectors,
            actor=actor,
        )
        issued_token = self._auth_service.issue_worker_token(
            worker_id=worker.id,
            account_id=actor.account.id,
            timeout_seconds=self._settings.EPHEMERAL_WORKER.timeout_seconds,
        )
        spec = EphemeralWorkerSpec(
            worker_id=worker.id,
            name=worker.name,
            worker_token=SecretStr(issued_token.token),
            server_url=self._settings.SERVER_URL,
            job_id=job_id,
            tags=_get_tags(worker, actor, self._server_id),
        )
        # Background tasks run after the route commits, so the worker is
        # persisted in the DB by then.
        self._background_tasks.add_task(_start_worker, self._ephemeral_workers, spec)


def _get_tags(
    worker: Worker, actor: AuthContext, server_id: uuid.UUID | None
) -> dict[str, str]:
    """Build the tags the backend attaches to the worker's sandbox.

    Args:
        worker: Registered worker.
        actor: Caller context.
        server_id: Persisted server id, None before startup resolved it.

    Returns:
        Tags.
    """
    tags = {
        f"{SANDBOX_TAG_PREFIX}worker_id": str(worker.id),
        f"{SANDBOX_TAG_PREFIX}job_id": str(worker.scope.job_id),
        f"{SANDBOX_TAG_PREFIX}account_id": str(actor.account.id),
        f"{SANDBOX_TAG_PREFIX}server_version": version("kitaru"),
    }
    if server_id is not None:
        tags[f"{SANDBOX_TAG_PREFIX}server_id"] = str(server_id)
    return tags


async def _start_worker(
    ephemeral_workers: EphemeralWorkers, spec: EphemeralWorkerSpec
) -> None:
    """Start a worker, logging a failure instead of raising.

    Args:
        ephemeral_workers: Ephemeral worker backend.
        spec: Ephemeral worker spec.
    """
    try:
        await ephemeral_workers.start(spec)
    except Exception:
        logger.exception(
            "Failed to start worker %s for job %s.", spec.worker_id, spec.job_id
        )
