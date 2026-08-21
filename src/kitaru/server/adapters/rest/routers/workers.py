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

from fastapi import APIRouter, Depends, Header, Query, status
from packaging.version import InvalidVersion, Version

from kitaru.analytics.source import AnalyticsSource
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.worker import (
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerListParams,
    WorkerRegistrationResponse,
    WorkerResponse,
    WorkerTokenResponse,
)
from kitaru.headers import CLIENT_HEADER
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_worker,
    authorize_worker_only,
    get_app_settings,
    get_auth_service,
    get_task_service,
    get_worker_service,
)
from kitaru.server.adapters.rest.mapping.workers import (
    worker_list_params_to_filter,
    worker_to_response,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext, WorkerAuthContext
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.client_identity import parse_client_identity
from kitaru.server.domain.worker import WorkerClientUnsupported

router = APIRouter(route_class=KitaruAPIRoute)

LAST_UNSUPPORTED_WORKER_CLIENT_VERSION = "0.22.2"


def _check_worker_client(client: str) -> None:
    """Reject an SDK older than the oldest version that renews its token.

    A caller that is not the Python SDK, or reports no readable version, is
    left alone.

    Args:
        client: Client identification header value.

    Raises:
        WorkerClientUnsupported: The client predates token renewal.
    """
    identity = parse_client_identity(client)
    if (
        identity is None
        or identity.source is not AnalyticsSource.PYTHON
        or identity.version is None
    ):
        return
    try:
        client_version = Version(identity.version)
    except InvalidVersion:
        return
    if client_version <= Version(LAST_UNSUPPORTED_WORKER_CLIENT_VERSION):
        raise WorkerClientUnsupported(
            identity.version, LAST_UNSUPPORTED_WORKER_CLIENT_VERSION
        )


@router.post("")
async def register_worker(
    body: WorkerCreateRequest,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    client: Annotated[str, Header(alias=CLIENT_HEADER)] = "",
) -> WorkerRegistrationResponse:
    """Register a worker.

    Every registration creates a new worker, names are labels and need not
    be unique. Clients observe HTTP 200 on success, 426 from an SDK that
    renews by re-registering, and 422 on invalid input.

    Args:
        body: Worker create request.
        service: Worker service.
        auth_service: Authentication service for the current request.
        actor: Caller context.
        settings: API settings for this process.
        client: Client identification header value.

    Returns:
        Stored worker with a bearer token scoped to it.
    """
    _check_worker_client(client)
    worker = await service.register_worker(
        name=body.name,
        scope=body.scope,
        runtime=body.runtime,
        metadata=body.metadata,
        actor=actor,
    )
    issued = auth_service.issue_worker_token(
        worker_id=worker.id, account_id=actor.account.id
    )
    return WorkerRegistrationResponse(
        worker=worker_to_response(
            worker, datetime.now(UTC), settings.WORKER_LIVENESS_TIMEOUT_SECONDS
        ),
        token=issued.token,
        token_expires_at=issued.expires_at,
    )


@router.post("/{worker_id}/token")
async def renew_worker_token(
    worker_id: uuid.UUID,
    service: Annotated[WorkerService, Depends(get_worker_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> WorkerTokenResponse:
    """Issue a fresh token for a registered worker.

    Renewal stamps last_seen_at. Clients observe HTTP 200 on success, 403
    when the worker belongs to another account, and 404 when no worker has
    this id.

    Args:
        worker_id: Id of the worker.
        service: Worker service.
        auth_service: Authentication service for the current request.
        actor: Caller context.

    Returns:
        Bearer token scoped to the worker.
    """
    await service.renew_worker(worker_id, actor=actor)
    issued = auth_service.issue_worker_token(
        worker_id=worker_id, account_id=actor.account.id
    )
    return WorkerTokenResponse(token=issued.token, token_expires_at=issued.expires_at)


@router.get("")
async def list_workers(
    service: Annotated[WorkerService, Depends(get_worker_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    params: Annotated[WorkerListParams, Query()],
) -> Page[WorkerResponse]:
    """List workers.

    Workers past the liveness window are left out unless include_stale is
    set. Clients observe HTTP 200 on success and 422 on invalid pagination
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
    actor: Annotated[AuthContext, Depends(authorize_with_worker)],
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> WorkerResponse:
    """Get a worker by id.

    Clients observe HTTP 200 on success, 403 when a worker token names a
    different worker, and 404 when no worker has this id.

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
    actor: Annotated[WorkerAuthContext, Depends(authorize_worker_only)],
) -> WorkerHeartbeatResponse:
    """Report the tasks a worker currently holds.

    Clients observe HTTP 200 on success, 403 when the caller holds no worker
    token for this worker, and 404 when no worker has this id.

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
