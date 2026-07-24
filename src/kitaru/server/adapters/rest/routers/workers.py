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
"""Worker routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.workers import (
    WorkerCreateRequest,
    WorkerResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.workers import worker_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService

router = APIRouter()


@router.post("")
async def register_worker(
    body: WorkerCreateRequest,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerResponse:
    """Register a worker, upserting by name.

    A worker already registered under the name gets its served agents and
    metadata replaced and its last seen time bumped, so clients observe
    HTTP 200 instead of 201 on success, and 422 on invalid input.

    Args:
        body: Worker create request.
        service: Worker service.
        actor: Caller context.

    Returns:
        Registered worker.
    """
    worker = await service.register_worker(
        name=body.name,
        agent_ids=body.agent_ids,
        metadata=body.metadata,
        actor=actor,
    )
    return worker_to_response(worker, service.liveness_timeout_seconds)


@router.get("")
async def list_workers(
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[WorkerResponse]:
    """List workers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Worker service.
        actor: Caller context.
        name: Filter on worker name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of workers.
    """
    worker_filter = WorkerFilter(name=name, page=page, page_size=page_size)
    workers, total = await service.list_workers(worker_filter, actor=actor)
    return Page[WorkerResponse](
        items=[
            worker_to_response(worker, service.liveness_timeout_seconds)
            for worker in workers
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{worker_id}")
async def get_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerResponse:
    """Get a worker by id.

    Clients observe HTTP 200 on success and 404 when no worker has this
    id.

    Args:
        worker_id: Id of the worker.
        service: Worker service.
        actor: Caller context.

    Returns:
        Stored worker.
    """
    worker = await service.get_worker(worker_id, actor=actor)
    return worker_to_response(worker, service.liveness_timeout_seconds)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a worker.

    Clients observe HTTP 204 on success and 404 when no worker has this
    id.

    Args:
        worker_id: Id of the worker.
        service: Worker service.
        actor: Caller context.
    """
    await service.delete_worker(worker_id, actor=actor)
