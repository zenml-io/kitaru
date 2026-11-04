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
"""Analysis routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.analysis import AnalysisCreateRequest
from kitaru.api_models.v1.job import JobResponse
from kitaru.server.adapters.rest.dependencies import authorize, get_job_service
from kitaru.server.adapters.rest.mapping.analyses import analysis_create_to_command
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_analyses(
    body: AnalysisCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Run analyzers over one agent's selected sessions, as one job.

    Clients observe HTTP 201 on success, 404 when an analyzer, version, or
    connection does not exist, and 422 when no finished session matches, more
    sessions match than the cap allows, or the input is invalid.

    Args:
        body: Analysis create request.
        service: Job service.
        actor: Caller context.

    Returns:
        Created job.
    """
    job = await service.create_analyses(analysis_create_to_command(body), actor=actor)
    return job_to_response(job)
