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
"""Worker pool routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.worker_pool import (
    WorkerPoolCreateRequest,
    WorkerPoolListParams,
    WorkerPoolResponse,
    WorkerPoolUpdateRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_pool_service
from kitaru.server.adapters.rest.mapping.worker_pools import (
    worker_pool_list_params_to_filter,
    worker_pool_to_response,
    worker_pool_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_pool_service import WorkerPoolService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_worker_pool(
    body: WorkerPoolCreateRequest,
    service: Annotated[WorkerPoolService, Depends(get_worker_pool_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerPoolResponse:
    """Create a worker pool.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 when the scope names a job or on other invalid
    input.

    Args:
        body: Worker pool create request.
        service: Worker pool service.
        actor: Caller context.

    Returns:
        Created worker pool.
    """
    worker_pool = await service.create_worker_pool(
        name=body.name, scope=body.scope, actor=actor
    )
    return worker_pool_to_response(worker_pool)


@router.get("")
async def list_worker_pools(
    service: Annotated[WorkerPoolService, Depends(get_worker_pool_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[WorkerPoolListParams, Query()],
) -> Page[WorkerPoolResponse]:
    """List worker pools.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Worker pool service.
        actor: Caller context.
        params: Worker pool list params.

    Returns:
        Page of worker pools.
    """
    worker_pool_filter = worker_pool_list_params_to_filter(params)
    worker_pools, next_cursor = await service.list_worker_pools(
        worker_pool_filter, actor=actor
    )
    return Page[WorkerPoolResponse](
        items=[worker_pool_to_response(worker_pool) for worker_pool in worker_pools],
        next_cursor=next_cursor,
    )


@router.get("/{pool_id}")
async def get_worker_pool(
    pool_id: uuid.UUID,
    service: Annotated[WorkerPoolService, Depends(get_worker_pool_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerPoolResponse:
    """Get a worker pool by id.

    Clients observe HTTP 200 on success and 404 when no worker pool has
    this id.

    Args:
        pool_id: Id of the worker pool.
        service: Worker pool service.
        actor: Caller context.

    Returns:
        Stored worker pool.
    """
    worker_pool = await service.get_worker_pool(pool_id, actor=actor)
    return worker_pool_to_response(worker_pool)


@router.patch("/{pool_id}")
async def update_worker_pool(
    pool_id: uuid.UUID,
    body: WorkerPoolUpdateRequest,
    service: Annotated[WorkerPoolService, Depends(get_worker_pool_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerPoolResponse:
    """Update a worker pool.

    Clients observe HTTP 200 on success, 404 when no worker pool has this
    id, 409 when the new name is already registered, and 422 when the
    update clears the name or scope, or the new scope names a job.

    Args:
        pool_id: Id of the worker pool.
        body: Worker pool update request.
        service: Worker pool service.
        actor: Caller context.

    Returns:
        Updated worker pool.
    """
    command = worker_pool_update_to_command(body)
    worker_pool = await service.update_worker_pool(pool_id, command, actor=actor)
    return worker_pool_to_response(worker_pool)


@router.delete("/{pool_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker_pool(
    pool_id: uuid.UUID,
    service: Annotated[WorkerPoolService, Depends(get_worker_pool_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a worker pool.

    Clients observe HTTP 204 on success, 404 when no worker pool has this
    id, and 409 when a worker still references it.

    Args:
        pool_id: Id of the worker pool.
        service: Worker pool service.
        actor: Caller context.
    """
    await service.delete_worker_pool(pool_id, actor=actor)
