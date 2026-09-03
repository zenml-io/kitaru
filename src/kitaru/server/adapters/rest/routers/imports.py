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
"""Import routes."""

import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, status
from pydantic import SecretStr

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_app_settings,
    get_auth_service,
    get_job_service,
    get_worker_launcher,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.imports import import_create_to_command
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.worker_launcher import WorkerLauncher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.models.worker import WorkerLaunch
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.job import Job

logger = logging.getLogger(__name__)

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_import(
    body: ImportCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    worker_service: Annotated[WorkerService, Depends(get_worker_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    launcher: Annotated[WorkerLauncher | None, Depends(get_worker_launcher)],
    background_tasks: BackgroundTasks,
) -> JobResponse:
    """Import sessions from a payload blob, as a job holding one importer task.

    Clients observe HTTP 201 on success and 404 when the importer, the
    version, the payload blob, or the agent does not exist. With a worker
    launcher configured and no live worker covering the job's task, a worker
    pinned to the job is registered and launched after the response is sent.

    Args:
        body: Import create request.
        service: Job service.
        worker_service: Worker service.
        auth_service: Authentication service for the current request.
        actor: Caller context.
        settings: API settings for this process.
        launcher: Worker launcher, None when no backend is configured.
        background_tasks: Tasks run after the response is sent.

    Returns:
        Created job.
    """
    command = import_create_to_command(body)
    job = await service.create_import(command, actor=actor)
    if launcher is not None:
        await _schedule_worker_launch(
            job,
            service,
            worker_service,
            auth_service,
            launcher,
            settings,
            background_tasks,
            actor,
        )
    return job_to_response(job)


async def _schedule_worker_launch(
    job: Job,
    job_service: JobService,
    worker_service: WorkerService,
    auth_service: AuthService,
    launcher: WorkerLauncher,
    settings: APISettings,
    background_tasks: BackgroundTasks,
    actor: AuthContext,
) -> None:
    """Register a worker for the job's task and launch it after the response.

    A live worker whose scope covers the task suppresses the launch.

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
    # An import job holds exactly one task.
    tasks, _ = await job_service.list_job_tasks(job.id, TaskFilter(), actor=actor)
    task = tasks[0]
    if await worker_service.is_covered(task):
        return
    worker = await worker_service.register_ephemeral_worker(
        task,
        WorkerRuntime(platform=settings.WORKER_LAUNCHER.backend.value),
        actor=actor,
    )
    issued = auth_service.issue_worker_token(
        worker_id=worker.id,
        account_id=actor.account.id,
        lifetime_seconds=_get_worker_token_lifetime_seconds(settings),
    )
    launch = WorkerLaunch(
        worker_id=worker.id,
        worker_token=SecretStr(issued.token),
        server_url=settings.SERVER_URL,
        job_id=job.id,
    )
    # The route class commits the session before the response goes out and
    # Starlette runs background tasks after it, so the worker row is
    # committed before the sandbox starts.
    background_tasks.add_task(_launch_worker, launcher, launch)


def _get_worker_token_lifetime_seconds(settings: APISettings) -> int:
    """Compute the lifetime of an ephemeral worker token.

    Args:
        settings: API settings for this process.

    Returns:
        Sandbox timeout plus the task token expiry leeway, in seconds.
    """
    # Modal is the only launcher backend, and settings validation requires
    # its sub-model to be set under that backend.
    modal = settings.WORKER_LAUNCHER.modal
    assert modal is not None
    return modal.timeout_seconds + settings.TASK_TOKEN_EXPIRY_LEEWAY_SECONDS


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
