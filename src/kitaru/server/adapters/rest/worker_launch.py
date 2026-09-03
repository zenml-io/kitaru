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
"""Ephemeral worker launch scheduling."""

import logging

from fastapi import BackgroundTasks
from pydantic import SecretStr

from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.worker_launcher import WorkerLauncher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.models.worker import WorkerLaunch
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.job import Job

logger = logging.getLogger(__name__)


async def schedule_worker_launch(
    job: Job,
    job_service: JobService,
    worker_service: WorkerService,
    auth_service: AuthService,
    launcher: WorkerLauncher,
    settings: APISettings,
    background_tasks: BackgroundTasks,
    actor: AuthContext,
) -> None:
    """Register a worker for the job and launch it after the response.

    A job whose tasks are all covered by live workers suppresses the launch.

    Args:
        job: Created job.
        job_service: Job service.
        worker_service: Worker service.
        auth_service: Authentication service for the current request.
        launcher: Worker launcher.
        settings: API settings for this process.
        background_tasks: Tasks run after the response is sent.
        actor: Caller context.
    """
    tasks, _ = await job_service.list_job_tasks(job.id, TaskFilter(), actor=actor)
    if await worker_service.is_covered(tasks):
        return
    worker = await worker_service.register_ephemeral_worker(
        job.id,
        WorkerRuntime(platform=settings.WORKER_LAUNCHER.backend.value),
        actor=actor,
    )
    issued = auth_service.issue_worker_token(
        worker_id=worker.id,
        account_id=actor.account.id,
        timeout_seconds=settings.WORKER_LAUNCHER.timeout_seconds,
    )
    launch = WorkerLaunch(
        worker_id=worker.id,
        worker_token=SecretStr(issued.token),
        server_url=settings.SERVER_URL,
        job_id=job.id,
    )
    background_tasks.add_task(_launch_worker, launcher, launch)


async def _launch_worker(launcher: WorkerLauncher, launch: WorkerLaunch) -> None:
    """Launch a worker, logging a failure instead of raising.

    Args:
        launcher: Worker launcher.
        launch: Worker launch.
    """
    try:
        await launcher.launch(launch)
    except Exception:
        # Nothing retries here, a lost launch is left to the liveness window.
        logger.exception(
            "Failed to launch worker %s for job %s.", launch.worker_id, launch.job_id
        )
