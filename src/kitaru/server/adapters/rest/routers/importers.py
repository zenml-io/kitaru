"""Importer registry routes."""

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
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_importer_service,
)
from kitaru.server.adapters.rest.routers import plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.plugin import PluginKind

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_importer(
    body: ImporterCreateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Create an importer; clients observe 201, 409, or 422."""
    return await plugins.create_plugin(body, service, actor, ImporterResponse)


@router.get("")
async def list_importers(
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ImporterListParams, Query()],
) -> Page[ImporterResponse]:
    """List importers; clients observe 200 or 422."""
    return await plugins.list_plugins(
        params, service, actor, PluginKind.IMPORTER, ImporterResponse
    )


@router.get("/{plugin_id}")
async def get_importer(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Get an importer; clients observe 200 or 404."""
    return await plugins.get_plugin(plugin_id, service, actor, ImporterResponse)


@router.patch("/{plugin_id}")
async def update_importer(
    plugin_id: uuid.UUID,
    body: ImporterUpdateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterResponse:
    """Update an importer; clients observe 200 or 404."""
    return await plugins.update_plugin(
        plugin_id, body, service, actor, ImporterResponse
    )


@router.delete("/{plugin_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_importer(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an importer; clients observe 204, 404, or 409."""
    await plugins.delete_plugin(plugin_id, service, actor)


@router.post("/{plugin_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_importer_version(
    plugin_id: uuid.UUID,
    body: ImporterVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Create an importer version; clients observe 201, 404, or 422."""
    return await plugins.create_plugin_version(
        plugin_id, body, service, actor, ImporterVersionResponse
    )


@router.get("/{plugin_id}/versions")
async def list_importer_versions(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[ImporterVersionResponse]:
    """List importer versions; clients observe 200, 404, or 422."""
    return await plugins.list_plugin_versions(
        plugin_id, params, service, actor, ImporterVersionResponse
    )


@router.get("/{plugin_id}/versions/{version}")
async def get_importer_version(
    plugin_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Get an importer version; clients observe 200 or 404."""
    return await plugins.get_plugin_version(
        plugin_id, version, service, actor, ImporterVersionResponse
    )


@router.patch("/{plugin_id}/versions/{version}")
async def update_importer_version(
    plugin_id: uuid.UUID,
    version: int,
    body: ImporterVersionUpdateRequest,
    service: Annotated[PluginService, Depends(get_importer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImporterVersionResponse:
    """Update an importer version; clients observe 200, 404, or 409."""
    return await plugins.update_plugin_version(
        plugin_id, version, body, service, actor, ImporterVersionResponse
    )
