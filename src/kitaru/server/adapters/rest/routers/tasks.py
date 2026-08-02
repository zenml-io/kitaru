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
)
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.jwt import TaskSubject
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_task_only,
    authorize_with_task,
    authorize_worker_only,
    get_auth_service,
    get_task_service,
)
from kitaru.server.adapters.rest.mapping.tasks import (
    claimed_tasks_to_response,
    spec_to_response,
    task_list_params_to_filter,
    task_to_response,
    task_update_to_command,
)
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    WorkerAuthContext,
)
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.domain.task import EvaluationTask

router = APIRouter(route_class=CommitRoute)


@router.get("")
async def list_tasks(
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[TaskListParams, Query()],
) -> Page[TaskResponse]:
    """List tasks.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Task service.
        actor: Caller context.
        params: Task list params.

    Returns:
        Page of tasks.
    """
    task_filter = task_list_params_to_filter(params)
    tasks, next_cursor = await service.list_tasks(task_filter, actor=actor)
    return Page[TaskResponse](
        items=[task_to_response(task) for task in tasks], next_cursor=next_cursor
    )


@router.post("/claim")
async def claim_tasks(
    body: TaskClaimRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    actor: Annotated[WorkerAuthContext, Depends(authorize_worker_only)],
) -> TaskClaimResponse:
    """Claim pending tasks matching the worker's stored scope.

    Clients observe HTTP 200 on success, 403 when the caller holds no worker
    token, 404 when no worker has the claiming principal's id, and 422 on
    invalid input.

    Args:
        body: Task claim request.
        service: Task service.
        auth_service: Authentication service for the current request.
        actor: Caller context.

    Returns:
        Claimed tasks with their execution specs and a task token each.
    """
    claimed = await service.claim_tasks(body.max_tasks, actor=actor)
    worker_id = actor.principal.worker_id
    tokens = {
        item.task.id: auth_service.issue_task_token(
            TaskSubject(
                task_id=item.task.id,
                attempt=item.task.attempt,
                worker_id=worker_id,
                account_id=item.job_owner_id,
                input_session_id=item.task.input_session_id
                if isinstance(item.task, EvaluationTask)
                else None,
            ),
            timeout_seconds=item.spec.timeout_seconds,
        ).token
        for item in claimed
    }
    return claimed_tasks_to_response(claimed, tokens)


@router.get("/{task_id}")
async def get_task(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> TaskResponse:
    """Get a task by id.

    Clients observe HTTP 200 on success, 403 when a task token names a
    different task, and 404 when no task has this id.

    Args:
        task_id: Id of the task.
        service: Task service.
        actor: Caller context.

    Returns:
        Stored task.
    """
    task = await service.get_task(task_id, actor=actor)
    return task_to_response(task)


@router.get("/{task_id}/spec")
async def get_task_spec(
    task_id: uuid.UUID,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> TaskSpecResponse:
    """Get the execution spec of a task.

    Clients observe HTTP 200 on success, 403 when a task token names a
    different task, and 404 when no task has this id or the spec references
    a missing resource.

    Args:
        task_id: Id of the task.
        service: Task service.
        actor: Caller context.

    Returns:
        Execution spec.
    """
    spec = await service.get_spec(task_id, actor=actor)
    return spec_to_response(spec)


@router.patch("/{task_id}")
async def update_task(
    task_id: uuid.UUID,
    body: TaskUpdateRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[TaskAuthContext, Depends(authorize_task_only)],
) -> TaskResponse:
    """Apply an executor transition to a task.

    Clients observe HTTP 200 on success, 403 when the caller holds no task
    token for this task, 404 when no task has this id, 409 when the attempt
    does not match or the transition is illegal, 413 when the result exceeds
    the size cap, and 422 when the body carries no status.

    Args:
        task_id: Id of the task.
        body: Task update request.
        service: Task service.
        actor: Caller context.

    Returns:
        Task carrying its new status.
    """
    command = task_update_to_command(body)
    task = await service.update_task(task_id, command, actor=actor)
    return task_to_response(task)
