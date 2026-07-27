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
"""Scorer routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.scorers import (
    ScorerCreateRequest,
    ScorerResponse,
    ScorerVersionCreateRequest,
    ScorerVersionResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_scorer_service,
)
from kitaru.server.adapters.rest.mapping.plugins import format_to_domain
from kitaru.server.adapters.rest.mapping.scorers import (
    scorer_to_response,
    scorer_version_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.application.services.plugin_service import PluginService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_scorer(
    body: ScorerCreateRequest,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ScorerResponse:
    """Create a scorer.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Scorer create request.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Created scorer.
    """
    plugin = await service.create_plugin(
        name=body.name, provider=None, metadata={}, actor=actor
    )
    return scorer_to_response(plugin)


@router.get("")
async def list_scorers(
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ScorerResponse]:
    """List scorers.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Plugin service.
        actor: Caller context.
        name: Filter on scorer name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of scorers.
    """
    plugin_filter = PluginFilter(
        kind=service.kind, name=name, page=page, page_size=page_size
    )
    plugins, total = await service.list_plugins(plugin_filter, actor=actor)
    return Page[ScorerResponse](
        items=[scorer_to_response(plugin) for plugin in plugins],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{scorer_id}")
async def get_scorer(
    scorer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ScorerResponse:
    """Get a scorer by id.

    Clients observe HTTP 200 on success and 404 when no scorer has this
    id.

    Args:
        scorer_id: Id of the scorer.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Stored scorer.
    """
    plugin = await service.get_plugin(scorer_id, actor=actor)
    return scorer_to_response(plugin)


@router.delete("/{scorer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_scorer(
    scorer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a scorer and its versions.

    Clients observe HTTP 204 on success and 404 when no scorer has this
    id.

    Args:
        scorer_id: Id of the scorer.
        service: Plugin service.
        actor: Caller context.
    """
    await service.delete_plugin(scorer_id, actor=actor)


@router.post("/{scorer_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_scorer_version(
    scorer_id: uuid.UUID,
    body: ScorerVersionCreateRequest,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ScorerVersionResponse:
    """Create a scorer version under the next version number.

    Clients observe HTTP 201 on success, 404 when no scorer has this id
    or the code blob does not exist, and 422 on invalid input.

    Args:
        scorer_id: Id of the scorer.
        body: Scorer version create request.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Created scorer version.
    """
    version = await service.create_version(
        scorer_id,
        format=format_to_domain(body.format),
        blob_id=body.blob_id,
        entrypoint=body.entrypoint,
        actor=actor,
    )
    return scorer_version_to_response(version)


@router.get("/{scorer_id}/versions")
async def list_scorer_versions(
    scorer_id: uuid.UUID,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ScorerVersionResponse]:
    """List the versions of a scorer.

    Clients observe HTTP 200 on success, 404 when no scorer has this id,
    and 422 on invalid pagination parameters.

    Args:
        scorer_id: Id of the scorer.
        service: Plugin service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of scorer versions.
    """
    version_filter = PluginVersionFilter(
        plugin_id=scorer_id, page=page, page_size=page_size
    )
    versions, total = await service.list_versions(version_filter, actor=actor)
    return Page[ScorerVersionResponse](
        items=[scorer_version_to_response(version) for version in versions],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{scorer_id}/versions/{version}")
async def get_scorer_version(
    scorer_id: uuid.UUID,
    version: int,
    service: Annotated[PluginService, Depends(get_scorer_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ScorerVersionResponse:
    """Get a scorer version by version number.

    Clients observe HTTP 200 on success and 404 when no scorer has this
    id or the scorer has no such version.

    Args:
        scorer_id: Id of the scorer.
        version: Version number.
        service: Plugin service.
        actor: Caller context.

    Returns:
        Stored scorer version.
    """
    stored = await service.get_version(scorer_id, version, actor=actor)
    return scorer_version_to_response(stored)
