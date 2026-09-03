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

from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, status

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.job import JobResponse
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_app_settings,
    get_auth_service,
    get_ephemeral_workers,
    get_job_service,
    get_worker_service,
)
from kitaru.server.adapters.rest.ephemeral_workers import start_ephemeral_worker
from kitaru.server.adapters.rest.mapping.imports import import_create_to_command
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.ephemeral_workers import EphemeralWorkers
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import WorkerService

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
    ephemeral_workers: Annotated[
        EphemeralWorkers | None, Depends(get_ephemeral_workers)
    ],
    background_tasks: BackgroundTasks,
) -> JobResponse:
    """Import sessions from a payload blob, as a job holding one importer task.

    Clients observe HTTP 201 on success and 404 when the importer, the
    version, the payload blob, or the agent does not exist. With an ephemeral
    worker backend configured and no live worker covering the job's task, a
    worker pinned to the job is registered and started after the response is
    sent.

    Args:
        body: Import create request.
        service: Job service.
        worker_service: Worker service.
        auth_service: Authentication service for the current request.
        actor: Caller context.
        settings: API settings for this process.
        ephemeral_workers: Ephemeral worker backend, None when none is configured.
        background_tasks: Tasks run after the response is sent.

    Returns:
        Created job.
    """
    command = import_create_to_command(body)
    job = await service.create_import(command, actor=actor)
    if ephemeral_workers is not None:
        await start_ephemeral_worker(
            job,
            service,
            worker_service,
            auth_service,
            ephemeral_workers,
            settings,
            background_tasks,
            actor,
        )
    return job_to_response(job)
