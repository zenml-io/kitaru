"""Evaluator registry routes."""

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
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluator_service,
)
from kitaru.server.adapters.rest.routers import plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.plugin import PluginKind

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_evaluator(
    body: EvaluatorCreateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Create an evaluator; clients observe 201, 409, or 422."""
    return await plugins.create_plugin(body, service, actor, EvaluatorResponse)


@router.get("")
async def list_evaluators(
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[EvaluatorListParams, Query()],
) -> Page[EvaluatorResponse]:
    """List evaluators; clients observe 200 or 422."""
    return await plugins.list_plugins(
        params, service, actor, PluginKind.EVALUATOR, EvaluatorResponse
    )


@router.get("/{plugin_id}")
async def get_evaluator(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Get an evaluator; clients observe 200 or 404."""
    return await plugins.get_plugin(plugin_id, service, actor, EvaluatorResponse)


@router.patch("/{plugin_id}")
async def update_evaluator(
    plugin_id: uuid.UUID,
    body: EvaluatorUpdateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorResponse:
    """Update an evaluator; clients observe 200 or 404."""
    return await plugins.update_plugin(
        plugin_id, body, service, actor, EvaluatorResponse
    )


@router.delete("/{plugin_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_evaluator(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an evaluator; clients observe 204, 404, or 409."""
    await plugins.delete_plugin(plugin_id, service, actor)


@router.post("/{plugin_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_evaluator_version(
    plugin_id: uuid.UUID,
    body: EvaluatorVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Create an evaluator version; clients observe 201, 404, or 422."""
    return await plugins.create_plugin_version(
        plugin_id, body, service, actor, EvaluatorVersionResponse
    )


@router.get("/{plugin_id}/versions")
async def list_evaluator_versions(
    plugin_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[EvaluatorVersionResponse]:
    """List evaluator versions; clients observe 200, 404, or 422."""
    return await plugins.list_plugin_versions(
        plugin_id, params, service, actor, EvaluatorVersionResponse
    )


@router.get("/{plugin_id}/versions/{version}")
async def get_evaluator_version(
    plugin_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Get an evaluator version; clients observe 200 or 404."""
    return await plugins.get_plugin_version(
        plugin_id, version, service, actor, EvaluatorVersionResponse
    )


@router.patch("/{plugin_id}/versions/{version}")
async def update_evaluator_version(
    plugin_id: uuid.UUID,
    version: int,
    body: EvaluatorVersionUpdateRequest,
    service: Annotated[PluginService, Depends(get_evaluator_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> EvaluatorVersionResponse:
    """Update an evaluator version; clients observe 200, 404, or 409."""
    return await plugins.update_plugin_version(
        plugin_id, version, body, service, actor, EvaluatorVersionResponse
    )
