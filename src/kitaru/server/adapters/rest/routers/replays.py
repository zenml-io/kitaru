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
"""Replay routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.jobs import JobResponse, ReplayCreateRequest
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.jobs import (
    job_to_response,
    replay_create_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_replay(
    body: ReplayCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Create a standalone replay of one session with an inline config.

    The tool policy defaults to a history policy scoped to the original
    session.

    Clients observe HTTP 201 on success, 404 when no session or agent
    version has the referenced id, 409 when no runnable agent version
    resolves, and 422 when the original session is in progress, the
    version belongs to another agent, a history policy scopes to a
    cohort, or the input is invalid.

    Args:
        body: Replay create request.
        service: Job service.
        actor: Caller context.

    Returns:
        Created job.
    """
    job, config = await service.create_replay(
        replay_create_to_command(body), actor=actor
    )
    return job_to_response(job, config)
