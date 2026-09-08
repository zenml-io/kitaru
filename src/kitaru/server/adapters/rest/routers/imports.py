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
"""Import routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.imports import (
    ImportCreateRequest,
    ImportListParams,
    ImportResponse,
)
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_app_settings,
    get_auth_service,
    get_ephemeral_workers,
    get_import_service,
    get_job_service,
    get_server_id_state,
    get_worker_service,
)
from kitaru.server.adapters.rest.ephemeral_workers import start_ephemeral_worker
from kitaru.server.adapters.rest.mapping.imports import (
    import_create_to_command,
    import_list_params_to_filter,
    import_to_response,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.ephemeral_workers import EphemeralWorkers
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.import_service import ImportService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.worker_service import WorkerService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_import(
    body: ImportCreateRequest,
    service: Annotated[ImportService, Depends(get_import_service)],
    job_service: Annotated[JobService, Depends(get_job_service)],
    worker_service: Annotated[WorkerService, Depends(get_worker_service)],
    auth_service: Annotated[AuthService, Depends(get_auth_service)],
    ephemeral_workers: Annotated[
        EphemeralWorkers | None, Depends(get_ephemeral_workers)
    ],
    settings: Annotated[APISettings, Depends(get_app_settings)],
    server_id: Annotated[uuid.UUID | None, Depends(get_server_id_state)],
    background_tasks: BackgroundTasks,
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImportResponse:
    """Import sessions from a payload blob, as a job holding one importer task.

    Clients observe HTTP 201 on success, 404 when the importer, the version,
    the payload blob, the agent, the agent version, or an evaluator does not
    exist, and 422 when the agent version belongs to another agent, an
    evaluator is scoped to another agent, or an evaluator version repeats.

    Args:
        body: Import create request.
        service: Import service.
        job_service: Job service.
        worker_service: Worker service.
        auth_service: Authentication service for the current request.
        ephemeral_workers: Ephemeral worker backend, None when none is
            configured.
        settings: API settings for this process.
        server_id: Persisted server id, None before startup resolved it.
        background_tasks: Tasks run after the response is sent.
        actor: Caller context.

    Returns:
        Created import.
    """
    command = import_create_to_command(body)
    import_ = await service.create_import(command, actor=actor)
    if ephemeral_workers is not None and import_.job_id is not None:
        await start_ephemeral_worker(
            import_.job_id,
            job_service,
            worker_service,
            auth_service,
            ephemeral_workers,
            settings,
            server_id,
            background_tasks,
            actor,
        )
    return import_to_response(import_)


@router.get("")
async def list_imports(
    service: Annotated[ImportService, Depends(get_import_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ImportListParams, Query()],
) -> Page[ImportResponse]:
    """List imports.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Import service.
        actor: Caller context.
        params: Import list params.

    Returns:
        Page of imports.
    """
    import_filter = import_list_params_to_filter(params)
    imports, next_cursor = await service.list_imports(import_filter, actor=actor)
    return Page[ImportResponse](
        items=[import_to_response(import_) for import_ in imports],
        next_cursor=next_cursor,
    )


@router.get("/{import_id}", responses=error_responses(404))
async def get_import(
    import_id: uuid.UUID,
    service: Annotated[ImportService, Depends(get_import_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImportResponse:
    """Get an import by id.

    Clients observe HTTP 200 on success and 404 when no import has this id.

    Args:
        import_id: Id of the import.
        service: Import service.
        actor: Caller context.

    Returns:
        Stored import.
    """
    import_ = await service.get_import(import_id, actor=actor)
    return import_to_response(import_)
