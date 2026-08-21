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
"""Evaluator routes.

Every handler is a one-liner into the shared, kind-parametrized
orchestration functions in ``routers/plugins.py``, since evaluators and
importers are both plugin resources.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionResponse,
    EvaluatorVersionUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_evaluator_service
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.adapters.rest.routers import plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
@idempotent
async def create_evaluator(
    body: EvaluatorCreateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Create an evaluator.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Evaluator create request.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Created evaluator.
    """
    return await plugins.create_plugin(service, body, EvaluatorResponse, actor=actor)


@router.get("")
async def list_evaluators(
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[EvaluatorListParams, Query()],
) -> Page[EvaluatorResponse]:
    """List evaluators.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Evaluator service.
        actor: Caller context.
        params: Evaluator list params.

    Returns:
        Page of evaluators.
    """
    return await plugins.list_plugins(
        service, params, EvaluatorResponse, actor=actor, filter_=params.filter
    )


@router.get("/{evaluator_id}")
async def get_evaluator(
    evaluator_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Get an evaluator by id.

    Clients observe HTTP 200 on success and 404 when no evaluator has this
    id.

    Args:
        evaluator_id: Id of the evaluator.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Stored evaluator.
    """
    return await plugins.get_plugin(
        service, evaluator_id, EvaluatorResponse, actor=actor
    )


@router.patch("/{evaluator_id}")
async def update_evaluator(
    evaluator_id: uuid.UUID,
    body: EvaluatorUpdateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Update an evaluator.

    Clients observe HTTP 200 on success, 404 when no evaluator has this id,
    and 422 on invalid input.

    Args:
        evaluator_id: Id of the evaluator.
        body: Evaluator update request.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Updated evaluator.
    """
    return await plugins.update_plugin(
        service, evaluator_id, body, EvaluatorResponse, actor=actor
    )


@router.delete("/{evaluator_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_evaluator(
    evaluator_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an evaluator, cascading its versions.

    Clients observe HTTP 204 on success and 404 when no evaluator has this
    id.

    Args:
        evaluator_id: Id of the evaluator.
        service: Evaluator service.
        actor: Caller context.
    """
    await plugins.delete_plugin(service, evaluator_id, actor=actor)


@router.post("/{evaluator_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_evaluator_version(
    evaluator_id: uuid.UUID,
    body: EvaluatorVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Create an evaluator version.

    Clients observe HTTP 201 on success, 404 when no evaluator has this id
    or a script source names an unknown blob, and 422 on invalid input.

    Args:
        evaluator_id: Id of the evaluator.
        body: Evaluator version create request.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Created evaluator version.
    """
    return await plugins.create_version(
        service,
        evaluator_id,
        body.source,
        body.display_version,
        EvaluatorVersionResponse,
        actor=actor,
    )


@router.get("/{evaluator_id}/versions")
async def list_evaluator_versions(
    evaluator_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[EvaluatorVersionResponse]:
    """List an evaluator's versions.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        evaluator_id: Id of the evaluator.
        service: Evaluator service.
        actor: Caller context.
        params: List params.

    Returns:
        Page of evaluator versions.
    """
    return await plugins.list_versions(
        service, evaluator_id, params, EvaluatorVersionResponse, actor=actor
    )


@router.get("/{evaluator_id}/versions/{version}")
async def get_evaluator_version(
    evaluator_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Get an evaluator version by version number.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this evaluator.

    Args:
        evaluator_id: Id of the evaluator.
        version: Version number.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Stored evaluator version.
    """
    return await plugins.get_version(
        service, evaluator_id, version, EvaluatorVersionResponse, actor=actor
    )


@router.patch("/{evaluator_id}/versions/{version}")
async def update_evaluator_version(
    evaluator_id: uuid.UUID,
    version: int,
    body: EvaluatorVersionUpdateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Update an evaluator version's display version.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this evaluator.

    Args:
        evaluator_id: Id of the evaluator.
        version: Version number.
        body: Evaluator version update request.
        service: Evaluator service.
        actor: Caller context.

    Returns:
        Updated evaluator version.
    """
    return await plugins.update_version(
        service,
        evaluator_id,
        version,
        body.display_version,
        EvaluatorVersionResponse,
        actor=actor,
    )
