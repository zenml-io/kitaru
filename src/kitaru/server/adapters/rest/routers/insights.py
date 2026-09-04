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
"""Insight routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.insight import (
    InsightBatchCreateRequest,
    InsightListParams,
    InsightResponse,
    InsightUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_insight_service
from kitaru.server.adapters.rest.mapping.insights import (
    created_insight_to_response,
    insight_batch_create_to_command,
    insight_list_params_to_filter,
    insight_to_response,
    insight_update_to_command,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.insight_service import InsightService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404)
)
@idempotent
async def create_insights(
    body: InsightBatchCreateRequest,
    service: Annotated[InsightService, Depends(get_insight_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[InsightResponse]:
    """Create a batch of insights for one agent in one shot.

    Clients observe HTTP 201 on success, 404 when the agent does not exist,
    and 422 on validation.

    Args:
        body: Insight batch create request.
        service: Insight service.
        actor: Caller context.

    Returns:
        Created insights in input order.
    """
    command = insight_batch_create_to_command(body)
    insights = await service.create_insights(command, actor=actor)
    return [created_insight_to_response(insight) for insight in insights]


@router.get("")
async def list_insights(
    service: Annotated[InsightService, Depends(get_insight_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[InsightListParams, Query()],
) -> Page[InsightResponse]:
    """List insights.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Insight service.
        actor: Caller context.
        params: Insight list params.

    Returns:
        Page of insights.
    """
    insight_filter = insight_list_params_to_filter(params)
    items, next_cursor = await service.list_insights(insight_filter, actor=actor)
    return Page[InsightResponse](
        items=[insight_to_response(item) for item in items],
        next_cursor=next_cursor,
    )


@router.get("/{insight_id}", responses=error_responses(404))
async def get_insight(
    insight_id: uuid.UUID,
    service: Annotated[InsightService, Depends(get_insight_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InsightResponse:
    """Get an insight by id.

    Clients observe HTTP 200 on success and 404 when no insight has this id.

    Args:
        insight_id: Id of the insight.
        service: Insight service.
        actor: Caller context.

    Returns:
        Stored insight.
    """
    insight = await service.get_insight(insight_id, actor=actor)
    return insight_to_response(insight)


@router.patch("/{insight_id}", responses=error_responses(404))
async def update_insight(
    insight_id: uuid.UUID,
    body: InsightUpdateRequest,
    service: Annotated[InsightService, Depends(get_insight_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InsightResponse:
    """Update an insight's title and description.

    Clients observe HTTP 200 on success, 404 when no insight has this id,
    and 422 when the update clears the insight title.

    Args:
        insight_id: Id of the insight.
        body: Insight update request.
        service: Insight service.
        actor: Caller context.

    Returns:
        Updated insight.
    """
    command = insight_update_to_command(body)
    insight = await service.update_insight(insight_id, command, actor=actor)
    return insight_to_response(insight)


@router.delete(
    "/{insight_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404),
)
async def delete_insight(
    insight_id: uuid.UUID,
    service: Annotated[InsightService, Depends(get_insight_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an insight.

    Clients observe HTTP 204 on success and 404 when no insight has this id.

    Args:
        insight_id: Id of the insight.
        service: Insight service.
        actor: Caller context.
    """
    await service.delete_insight(insight_id, actor=actor)
