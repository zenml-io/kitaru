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
from datetime import UTC, datetime
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
    get_app_settings,
    get_task_service,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.workers import (
    worker_list_params_to_filter,
    worker_to_response,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.worker_service import WorkerService

router = APIRouter(route_class=CommitRoute)


@router.post("")
async def register_worker(
    body: WorkerCreateRequest,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> WorkerResponse:
    """Register a worker, upserting by name.

    Re-registration refreshes the scope, runtime, and metadata and stamps
    last_seen_at, keeping the id and created time. Clients observe HTTP 200
    on both the first registration and every re-registration, and 422 on
    invalid input.

    Args:
        body: Worker create request.
        service: Worker service.
        actor: Caller context.
        settings: API settings for this process.

    Returns:
        Stored worker.
    """
    worker = await service.register_worker(
        name=body.name,
        scope=body.scope,
        runtime=body.runtime,
        metadata=body.metadata,
        actor=actor,
    )
    return worker_to_response(
        worker, datetime.now(UTC), settings.WORKER_LIVENESS_TIMEOUT_SECONDS
    )


@router.get("")
async def list_workers(
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    params: Annotated[WorkerListParams, Query()],
) -> Page[WorkerResponse]:
    """List workers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Worker service.
        actor: Caller context.
        settings: API settings for this process.
        params: Worker list params.

    Returns:
        Page of workers.
    """
    worker_filter = worker_list_params_to_filter(params)
    workers, next_cursor = await service.list_workers(worker_filter, actor=actor)
    now = datetime.now(UTC)
    return Page[WorkerResponse](
        items=[
            worker_to_response(worker, now, settings.WORKER_LIVENESS_TIMEOUT_SECONDS)
            for worker in workers
        ],
        next_cursor=next_cursor,
    )


@router.get("/{worker_id}")
async def get_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> WorkerResponse:
    """Get a worker by id.

    Clients observe HTTP 200 on success and 404 when no worker has this id.

    Args:
        worker_id: Id of the worker.
        service: Worker service.
        actor: Caller context.
        settings: API settings for this process.

    Returns:
        Stored worker.
    """
    worker = await service.get_worker(worker_id, actor=actor)
    return worker_to_response(
        worker, datetime.now(UTC), settings.WORKER_LIVENESS_TIMEOUT_SECONDS
    )


@router.post("/{worker_id}/heartbeat")
async def heartbeat_worker(
    worker_id: uuid.UUID,
    body: WorkerHeartbeatRequest,
    service: Annotated[TaskService, Depends(get_task_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerHeartbeatResponse:
    """Report the tasks a worker currently holds.

    Clients observe HTTP 200 on success and 404 when no worker has this id.

    Args:
        worker_id: Id of the worker.
        body: Worker heartbeat request.
        service: Task service.
        actor: Caller context.

    Returns:
        Held tasks the worker should stop running.
    """
    cancel_task_ids = await service.heartbeat_worker(
        worker_id, body.task_ids, actor=actor
    )
    return WorkerHeartbeatResponse(cancel_task_ids=cancel_task_ids)


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_worker(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a worker.

    Clients observe HTTP 204 on success and 404 when no worker has this id.

    Args:
        worker_id: Id of the worker.
        service: Worker service.
        actor: Caller context.
    """
    await service.delete_worker(worker_id, actor=actor)
