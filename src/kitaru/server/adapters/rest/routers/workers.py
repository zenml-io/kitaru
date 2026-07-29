"""Worker routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.worker import (
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerListParams,
    WorkerResponse,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_task_service,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.workers import (
    worker_list_params_to_filter,
    worker_runtime_to_domain,
    worker_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.worker_service import WorkerService

router = APIRouter(route_class=CommitRoute)


@router.post("")
async def register_worker(
    body: WorkerCreateRequest,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerResponse:
    """Register a worker; clients observe 200 or 422."""
    worker, live = await service.register_worker(
        name=body.name,
        scope=body.scope,
        runtime=worker_runtime_to_domain(body.runtime),
        metadata=body.metadata,
        actor=actor,
    )
    return worker_to_response(worker, live)


@router.get("")
async def list_workers(
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[WorkerListParams, Query()],
) -> Page[WorkerResponse]:
    """List workers; clients observe 200 or 422."""
    items, cursor = await service.list_workers(
        worker_list_params_to_filter(params), actor=actor
    )
    return Page[WorkerResponse](
        items=[worker_to_response(worker, live) for worker, live in items],
        next_cursor=cursor,
    )


@router.get("/{worker_id}")
async def get_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerResponse:
    """Get a worker; clients observe 200 or 404."""
    worker, live = await service.get_worker(worker_id, actor=actor)
    return worker_to_response(worker, live)


@router.post("/{worker_id}/heartbeat")
async def heartbeat_worker(
    worker_id: uuid.UUID,
    body: WorkerHeartbeatRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerHeartbeatResponse:
    """Heartbeat worker tasks; clients observe 200 or 404."""
    cancel_ids = await service.heartbeat_worker(worker_id, body.task_ids, actor=actor)
    return WorkerHeartbeatResponse(cancel_task_ids=cancel_ids)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a worker; clients observe 204, 404, or 409."""
    await service.delete_worker(worker_id, actor=actor)
