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
"""Importer routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.importers import (
    ImporterCreateRequest,
    ImporterResponse,
    ImporterVersionCreateRequest,
    ImporterVersionResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_importer_service,
)
from kitaru.server.adapters.rest.mapping.importers import (
    importer_to_response,
    importer_version_to_response,
)
from kitaru.server.adapters.rest.mapping.plugins import format_to_domain
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.application.services.plugin_service import PluginService

router = APIRouter()


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
        service: Plugin service.
        actor: Caller context.

    Returns:
        Created importer.
    """
    plugin = await service.create_plugin(
        name=body.name,
        provider=body.provider,
        metadata=body.metadata,
        actor=actor,
    )
    return importer_to_response(plugin)


@router.get("")
async def list_importers(
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    provider: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ImporterResponse]:
    """List importers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Plugin service.
        actor: Caller context.
        name: Filter on importer name.
        provider: Filter on the provider the importer reads from.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of importers.
    """
    plugin_filter = PluginFilter(
        kind=service.kind,
        name=name,
        provider=provider,
        page=page,
        page_size=page_size,
    )
    plugins, total = await service.list_plugins(plugin_filter, actor=actor)
    return Page[ImporterResponse](
        items=[importer_to_response(plugin) for plugin in plugins],
        total=total,
        page=page,
        page_size=page_size,
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
        service: Plugin service.
        actor: Caller context.

    Returns:
        Stored importer.
    """
    plugin = await service.get_plugin(importer_id, actor=actor)
    return importer_to_response(plugin)


@router.delete("/{importer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_importer(
    importer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an importer and its versions.

    Clients observe HTTP 204 on success and 404 when no importer has this
    id.

    Args:
        importer_id: Id of the importer.
        service: Plugin service.
        actor: Caller context.
    """
    await service.delete_plugin(importer_id, actor=actor)


@router.post("/{importer_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_importer_version(
    importer_id: uuid.UUID,
    body: ImporterVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Create an importer version under the next version number.

    Clients observe HTTP 201 on success, 404 when no importer has this id
    or the code blob does not exist, and 422 on invalid input.

    Args:
        importer_id: Id of the importer.
        body: Importer version create request.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Created importer version.
    """
    version = await service.create_version(
        importer_id,
        format=format_to_domain(body.format),
        blob_id=body.blob_id,
        entrypoint=body.entrypoint,
        actor=actor,
    )
    return importer_version_to_response(version)


@router.get("/{importer_id}/versions")
async def list_importer_versions(
    importer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ImporterVersionResponse]:
    """List the versions of an importer.

    Clients observe HTTP 200 on success, 404 when no importer has this
    id, and 422 on invalid pagination parameters.

    Args:
        importer_id: Id of the importer.
        service: Plugin service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of importer versions.
    """
    version_filter = PluginVersionFilter(
        plugin_id=importer_id, page=page, page_size=page_size
    )
    versions, total = await service.list_versions(version_filter, actor=actor)
    return Page[ImporterVersionResponse](
        items=[importer_version_to_response(version) for version in versions],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{importer_id}/versions/{version}")
async def get_importer_version(
    importer_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Get an importer version by version number.

    Clients observe HTTP 200 on success and 404 when no importer has this
    id or the importer has no such version.

    Args:
        importer_id: Id of the importer.
        version: Version number.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Stored importer version.
    """
    stored = await service.get_version(importer_id, version, actor=actor)
    return importer_version_to_response(stored)
