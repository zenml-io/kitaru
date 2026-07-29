"""Import command routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.job import JobResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_job_service
from kitaru.server.adapters.rest.mapping.jobs import job_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.job import ImportCreate
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_import(
    body: ImportCreateRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Create an import job; clients observe 201, 404, or 422."""
    command = ImportCreate.model_validate(body.model_dump(mode="python"))
    return job_to_response(await service.create_import(command, actor=actor))
