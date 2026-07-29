"""Job routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.api_models.v1.task import TaskResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_job_service
from kitaru.server.adapters.rest.mapping.jobs import (
    job_list_params_to_filter,
    job_tasks_params_to_filter,
    job_to_response,
)
from kitaru.server.adapters.rest.mapping.tasks import task_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_jobs(
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[JobListParams, Query()],
) -> Page[JobResponse]:
    """List jobs; clients observe 200 or 422."""
    items, cursor = await service.list_jobs(
        job_list_params_to_filter(params), actor=actor
    )
    return Page[JobResponse](
        items=[job_to_response(item) for item in items], next_cursor=cursor
    )


@router.get("/{job_id}")
async def get_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Get a job; clients observe 200 or 404."""
    return job_to_response(await service.get_job(job_id, actor=actor))


@router.get("/{job_id}/tasks")
async def list_job_tasks(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[JobTasksListParams, Query()],
) -> Page[TaskResponse]:
    """List a job's tasks; clients observe 200, 404, or 422."""
    items, cursor = await service.list_job_tasks(
        job_tasks_params_to_filter(job_id, params), actor=actor
    )
    return Page[TaskResponse](
        items=[task_to_response(item) for item in items], next_cursor=cursor
    )


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Cancel a job; clients observe 200 or 404."""
    return job_to_response(await service.cancel_job(job_id, actor=actor))


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a job; clients observe 204, 404, or 409."""
    await service.delete_job(job_id, actor=actor)
