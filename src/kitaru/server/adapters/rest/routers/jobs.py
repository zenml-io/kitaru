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
"""Job routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.api_models.v1.task import TaskResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_worker,
    get_job_service,
)
from kitaru.server.adapters.rest.mapping.jobs import (
    job_list_params_to_filter,
    job_tasks_list_params_to_filter,
    job_to_response,
)
from kitaru.server.adapters.rest.mapping.tasks import task_to_response
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import JobService

router = APIRouter(route_class=KitaruAPIRoute)


@router.get("")
async def list_jobs(
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[JobListParams, Query()],
) -> Page[JobResponse]:
    """List jobs.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Job service.
        actor: Caller context.
        params: Job list params.

    Returns:
        Page of jobs.
    """
    job_filter = job_list_params_to_filter(params)
    jobs, next_cursor = await service.list_jobs(job_filter, actor=actor)
    return Page[JobResponse](
        items=[job_to_response(job) for job in jobs], next_cursor=next_cursor
    )


@router.get("/{job_id}", responses=error_responses(404))
async def get_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_worker)],
) -> JobResponse:
    """Get a job by id.

    Clients observe HTTP 200 on success and 404 when no job has this id.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Stored job.
    """
    job = await service.get_job(job_id, actor=actor)
    return job_to_response(job)


@router.get("/{job_id}/tasks", responses=error_responses(404))
async def list_job_tasks(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[JobTasksListParams, Query()],
) -> Page[TaskResponse]:
    """List the tasks of a job.

    Clients observe HTTP 200 on success and 404 when no job has this id.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.
        params: Job tasks list params.

    Returns:
        Page of tasks.
    """
    task_filter = job_tasks_list_params_to_filter(params)
    tasks, next_cursor = await service.list_job_tasks(job_id, task_filter, actor=actor)
    return Page[TaskResponse](
        items=[task_to_response(task) for task in tasks], next_cursor=next_cursor
    )


@router.post("/{job_id}/cancel", responses=error_responses(404, 409))
async def cancel_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> JobResponse:
    """Request cancellation of a job.

    Clients observe HTTP 200 on success, 404 when no job has this id, and
    409 when the job already settled.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.

    Returns:
        Job carrying the cancel request.
    """
    job = await service.cancel_job(job_id, actor=actor)
    return job_to_response(job)


@router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404, 409),
)
async def delete_job(
    job_id: uuid.UUID,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a settled job, cascading its tasks.

    Clients observe HTTP 204 on success, 404 when no job has this id, and
    409 when the job has not settled.

    Args:
        job_id: Id of the job.
        service: Job service.
        actor: Caller context.
    """
    await service.delete_job(job_id, actor=actor)
