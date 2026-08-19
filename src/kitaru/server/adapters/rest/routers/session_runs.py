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
"""Session run routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.server.adapters.rest.dependencies import authorize, get_job_service
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.adapters.rest.mapping.session_runs import (
    session_run_create_to_command,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_session_run(
    body: SessionRunCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Run an agent version once, as a job holding one agent task.

    Clients observe HTTP 201 on success, 404 when no agent version has this
    id, and 422 when the agent version carries no run spec.

    Args:
        body: Session run create request.
        service: Job service.
        actor: Caller context.

    Returns:
        Created job.
    """
    command = session_run_create_to_command(body)
    job = await service.create_session_run(command, actor=actor)
    return job_to_response(job)
