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
"""Analyzer routes.

Every handler is a one-liner into the shared, kind-parametrized
orchestration functions in ``routers/plugins.py``, since analyzers,
evaluators, and importers are all plugin resources.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status

from kitaru.api_models.v1.analyzer import (
    AnalyzerCreateRequest,
    AnalyzerListParams,
    AnalyzerResponse,
    AnalyzerUpdateRequest,
    AnalyzerVersionCreateRequest,
    AnalyzerVersionResponse,
    AnalyzerVersionUpdateRequest,
)
from kitaru.api_models.v1.base import ListParams, Page
from kitaru.server.adapters.rest.dependencies import authorize, get_analyzer_service
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.adapters.rest.routers import plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_analyzer(
    body: AnalyzerCreateRequest,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerResponse:
    """Create an analyzer.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Analyzer create request.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Created analyzer.
    """
    return await plugins.create_plugin(service, body, AnalyzerResponse, actor=actor)


@router.get("")
async def list_analyzers(
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AnalyzerListParams, Query()],
) -> Page[AnalyzerResponse]:
    """List analyzers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Analyzer service.
        actor: Caller context.
        params: Analyzer list params.

    Returns:
        Page of analyzers.
    """
    return await plugins.list_plugins(
        service, params, AnalyzerResponse, actor=actor, filter_=params.filter
    )


@router.get("/{analyzer_id}", responses=error_responses(404))
async def get_analyzer(
    analyzer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerResponse:
    """Get an analyzer by id.

    Clients observe HTTP 200 on success and 404 when no analyzer has this
    id.

    Args:
        analyzer_id: Id of the analyzer.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Stored analyzer.
    """
    return await plugins.get_plugin(service, analyzer_id, AnalyzerResponse, actor=actor)


@router.patch("/{analyzer_id}", responses=error_responses(404))
async def update_analyzer(
    analyzer_id: uuid.UUID,
    body: AnalyzerUpdateRequest,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerResponse:
    """Update an analyzer.

    Clients observe HTTP 200 on success, 404 when no analyzer has this id,
    and 422 on invalid input.

    Args:
        analyzer_id: Id of the analyzer.
        body: Analyzer update request.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Updated analyzer.
    """
    return await plugins.update_plugin(
        service, analyzer_id, body, AnalyzerResponse, actor=actor
    )


@router.delete(
    "/{analyzer_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404),
)
async def delete_analyzer(
    analyzer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an analyzer, cascading its versions.

    Clients observe HTTP 204 on success and 404 when no analyzer has this
    id.

    Args:
        analyzer_id: Id of the analyzer.
        service: Analyzer service.
        actor: Caller context.
    """
    await plugins.delete_plugin(service, analyzer_id, actor=actor)


@router.post(
    "/{analyzer_id}/versions",
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(400, 404, 409),
)
@idempotent
async def create_analyzer_version(
    analyzer_id: uuid.UUID,
    body: AnalyzerVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerVersionResponse:
    """Create an analyzer version.

    Clients observe HTTP 201 on success, 404 when no analyzer has this id
    or a script source names an unknown blob, and 422 on invalid input.

    Args:
        analyzer_id: Id of the analyzer.
        body: Analyzer version create request.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Created analyzer version.
    """
    return await plugins.create_version(
        service,
        analyzer_id,
        body.source,
        body.display_version,
        AnalyzerVersionResponse,
        actor=actor,
    )


@router.get("/{analyzer_id}/versions")
async def list_analyzer_versions(
    analyzer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[AnalyzerVersionResponse]:
    """List an analyzer's versions.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        analyzer_id: Id of the analyzer.
        service: Analyzer service.
        actor: Caller context.
        params: List params.

    Returns:
        Page of analyzer versions.
    """
    return await plugins.list_versions(
        service, analyzer_id, params, AnalyzerVersionResponse, actor=actor
    )


@router.get("/{analyzer_id}/versions/{version}", responses=error_responses(404))
async def get_analyzer_version(
    analyzer_id: uuid.UUID,
    version: Annotated[int, Path(ge=1, le=plugins.INT32_MAX)],
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerVersionResponse:
    """Get an analyzer version by version number.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this analyzer.

    Args:
        analyzer_id: Id of the analyzer.
        version: Version number.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Stored analyzer version.
    """
    return await plugins.get_version(
        service, analyzer_id, version, AnalyzerVersionResponse, actor=actor
    )


@router.patch("/{analyzer_id}/versions/{version}", responses=error_responses(404))
async def update_analyzer_version(
    analyzer_id: uuid.UUID,
    version: Annotated[int, Path(ge=1, le=plugins.INT32_MAX)],
    body: AnalyzerVersionUpdateRequest,
    service: Annotated[PluginService, Depends(get_analyzer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AnalyzerVersionResponse:
    """Update an analyzer version's display version.

    Clients observe HTTP 200 on success and 404 when no version with this
    number exists for this analyzer.

    Args:
        analyzer_id: Id of the analyzer.
        version: Version number.
        body: Analyzer version update request.
        service: Analyzer service.
        actor: Caller context.

    Returns:
        Updated analyzer version.
    """
    return await plugins.update_version(
        service,
        analyzer_id,
        version,
        body.display_version,
        AnalyzerVersionResponse,
        actor=actor,
    )
