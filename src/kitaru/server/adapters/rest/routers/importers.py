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
"""Importer routes.

Every handler is a one-liner into the shared, kind-parametrized
orchestration functions in ``routers/plugins.py``, since evaluators and
importers are both plugin resources.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterResponse,
    ImporterUpdateRequest,
    ImporterVersionCreateRequest,
    ImporterVersionResponse,
    ImporterVersionUpdateRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_importer_service
from kitaru.server.adapters.rest.routers import plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_importer(
    body: ImporterCreateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Create an importer.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Importer create request.
        service: Importer service.
        actor: Caller context.

    Returns:
        Created importer.
    """
    return await plugins.create_plugin(service, body, ImporterResponse, actor=actor)


@router.get("")
async def list_importers(
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ImporterListParams, Query()],
) -> Page[ImporterResponse]:
    """List importers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Importer service.
        actor: Caller context.
        params: Importer list params.

    Returns:
        Page of importers.
    """
    return await plugins.list_plugins(
        service,
        params,
        ImporterResponse,
        actor=actor,
        name=params.name,
        provider=params.provider,
    )


@router.get("/{importer_id}")
async def get_importer(
    importer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Get an importer by id.

    Clients observe HTTP 200 on success and 404 when no importer has this
    id.

    Args:
        importer_id: Id of the importer.
        service: Importer service.
        actor: Caller context.

    Returns:
        Stored importer.
    """
    return await plugins.get_plugin(service, importer_id, ImporterResponse, actor=actor)


@router.patch("/{importer_id}")
async def update_importer(
    importer_id: uuid.UUID,
    body: ImporterUpdateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Update an importer.

    Clients observe HTTP 200 on success, 404 when no importer has this id,
    and 422 on invalid input.

    Args:
        importer_id: Id of the importer.
        body: Importer update request.
        service: Importer service.
        actor: Caller context.

    Returns:
        Updated importer.
    """
    return await plugins.update_plugin(
        service, importer_id, body, ImporterResponse, actor=actor
    )


@router.delete("/{importer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_importer(
    importer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an importer, cascading its versions.

    Clients observe HTTP 204 on success and 404 when no importer has this
    id.

    Args:
        importer_id: Id of the importer.
        service: Importer service.
        actor: Caller context.
    """
    await plugins.delete_plugin(service, importer_id, actor=actor)


@router.post("/{importer_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_importer_version(
    importer_id: uuid.UUID,
    body: ImporterVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Create an importer version.

    Clients observe HTTP 201 on success, 404 when no importer has this id
    or a script source names an unknown blob, and 422 on invalid input.

    Args:
        importer_id: Id of the importer.
        body: Importer version create request.
        service: Importer service.
        actor: Caller context.

    Returns:
        Created importer version.
    """
    return await plugins.create_version(
        service,
        importer_id,
        body.source,
        body.display_version,
        ImporterVersionResponse,
        actor=actor,
    )


@router.get("/{importer_id}/versions")
async def list_importer_versions(
    importer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[ImporterVersionResponse]:
    """List an importer's versions.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        importer_id: Id of the importer.
        service: Importer service.
        actor: Caller context.
        params: List params.

    Returns:
        Page of importer versions.
    """
    return await plugins.list_versions(
        service, importer_id, params, ImporterVersionResponse, actor=actor
    )


@router.get("/{importer_id}/versions/{version}")
async def get_importer_version(
    importer_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Get an importer version by version number.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this importer.

    Args:
        importer_id: Id of the importer.
        version: Version number.
        service: Importer service.
        actor: Caller context.

    Returns:
        Stored importer version.
    """
    return await plugins.get_version(
        service, importer_id, version, ImporterVersionResponse, actor=actor
    )


@router.patch("/{importer_id}/versions/{version}")
async def update_importer_version(
    importer_id: uuid.UUID,
    version: int,
    body: ImporterVersionUpdateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Update an importer version's display version.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this importer.

    Args:
        importer_id: Id of the importer.
        version: Version number.
        body: Importer version update request.
        service: Importer service.
        actor: Caller context.

    Returns:
        Updated importer version.
    """
    return await plugins.update_version(
        service,
        importer_id,
        version,
        body.display_version,
        ImporterVersionResponse,
        actor=actor,
    )
