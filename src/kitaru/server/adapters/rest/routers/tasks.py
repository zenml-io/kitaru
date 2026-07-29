"""Task routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.task import (
    TaskClaimRequest,
    TaskClaimResponse,
    TaskListParams,
    TaskResponse,
    TaskSpecResponse,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_task_service
from kitaru.server.adapters.rest.mapping.tasks import (
    task_list_params_to_filter,
    task_spec_to_response,
    task_to_response,
    task_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.task_service import TaskService

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[TaskListParams, Query()],
) -> Page[TaskResponse]:
    """List tasks; clients observe 200 or 422."""
    items, cursor = await service.list_tasks(
        task_list_params_to_filter(params), actor=actor
    )
    return Page[TaskResponse](
        items=[task_to_response(item) for item in items], next_cursor=cursor
    )


@router.post("/claim")
async def claim_tasks(
    body: TaskClaimRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TaskClaimResponse:
    """Claim tasks; clients observe 200, 404, or 422."""
    items = await service.claim_tasks(body.worker_id, body.max_tasks, actor=actor)
    return TaskClaimResponse(
        tasks=[
            TaskWithSpec(task=task_to_response(task), spec=task_spec_to_response(spec))
            for task, spec in items
        ]
    )


@router.get("/{task_id}")
async def get_task(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TaskResponse:
    """Get a task; clients observe 200 or 404."""
    return task_to_response(await service.get_task(task_id, actor=actor))


@router.get("/{task_id}/spec")
async def get_task_spec(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TaskSpecResponse:
    """Get a resolved task specification; clients observe 200 or 404."""
    return task_spec_to_response(await service.get_spec(task_id, actor=actor))


@router.patch("/{task_id}")
async def update_task(
    task_id: uuid.UUID,
    body: TaskUpdateRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> TaskResponse:
    """Update a task attempt; clients observe 200, 404, 409, 413, or 422."""
    return task_to_response(
        await service.update_task(task_id, task_update_to_command(body), actor=actor)
    )
