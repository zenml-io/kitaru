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
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.jobs import worker_scope_to_domain
from kitaru.server.adapters.rest.mapping.workers import worker_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import WorkerService

router = APIRouter()


@router.post("")
async def register_worker(
    body: WorkerCreateRequest,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerResponse:
    """Register a worker, upserting by name.

    A worker already registered under the name gets its claim scope and
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
        scope=worker_scope_to_domain(body.scope),
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


@router.post("/{worker_id}/heartbeat")
async def heartbeat_worker(
    worker_id: uuid.UUID,
    body: WorkerHeartbeatRequest,
    service: Annotated[JobService, Depends(get_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerHeartbeatResponse:
    """Record one worker heartbeat on the jobs it reports as in flight.

    The heartbeat reaches only claimed or running jobs the worker owns and
    bumps the worker's last seen time. Reported jobs it did not reach, and
    jobs whose experiment run is canceling, come back for the worker to
    abandon.

    Clients observe HTTP 200 on success, 404 when no worker has this id,
    and 422 on invalid input.

    Args:
        worker_id: Id of the worker.
        body: Worker heartbeat request.
        service: Job service.
        actor: Caller context.

    Returns:
        Job ids the worker should stop working on.
    """
    abandon = await service.heartbeat_worker(worker_id, body.job_ids, actor=actor)
    return WorkerHeartbeatResponse(abandon=abandon)


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
