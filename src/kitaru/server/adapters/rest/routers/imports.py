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

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.jobs import JobResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.imports import (
    import_create_to_command,
)
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_import(
    body: ImportCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Create an import of one payload blob.

    The named importer resolves to a plugin version that the job pins.

    Clients observe HTTP 201 on success, 404 when no importer has the
    name, the importer has no such version, no agent has the id, or no
    blob has the payload blob id, and 422 when the input is invalid or
    the importer reads from no known provider.

    Args:
        body: Import create request.
        service: Job service.
        actor: Caller context.

    Returns:
        Created job.
    """
    job = await service.create_import(import_create_to_command(body), actor=actor)
    return job_to_response(job)
