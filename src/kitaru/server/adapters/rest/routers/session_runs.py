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

from kitaru.api_models.v1.jobs import JobResponse
from kitaru.api_models.v1.session_runs import SessionRunCreateRequest
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.adapters.rest.mapping.session_runs import (
    session_run_create_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_session_run(
    body: SessionRunCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Create a session run of one agent version.

    Clients observe HTTP 201 on success, 404 when no agent version has
    the referenced id, 409 when no runnable agent version resolves or an
    on demand run resolves to a version without an image, and 422 when
    the version belongs to another agent or the input is invalid.

    Args:
        body: Session run create request.
        service: Job service.
        actor: Caller context.

    Returns:
        Created job.
    """
    job = await service.create_session_run(
        session_run_create_to_command(body), actor=actor
    )
    return job_to_response(job, None)
