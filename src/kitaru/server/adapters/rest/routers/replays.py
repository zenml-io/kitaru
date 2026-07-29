"""Replay routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_replay_service
from kitaru.server.adapters.rest.mapping.replays import (
    replay_create_to_command,
    replay_list_params_to_filter,
    replay_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.replay_service import ReplayService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_replay(
    body: ReplayCreateRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayResponse:
    """Create a replay; clients observe 201, 404, or 422."""
    replay, config, result_session_id = await service.create_replay(
        replay_create_to_command(body), actor=actor
    )
    return replay_to_response(replay, config, result_session_id)


@router.get("")
async def list_replays(
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ReplayListParams, Query()],
) -> Page[ReplayResponse]:
    """List replays; clients observe 200 or 422."""
    items, cursor = await service.list_replays(
        replay_list_params_to_filter(params), actor=actor
    )
    return Page[ReplayResponse](
        items=[
            replay_to_response(item, config, result_session_id)
            for item, config, result_session_id in items
        ],
        next_cursor=cursor,
    )


@router.get("/{replay_id}")
async def get_replay(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayResponse:
    """Get a replay; clients observe 200 or 404."""
    replay, config, result_session_id = await service.get_replay(replay_id, actor=actor)
    return replay_to_response(replay, config, result_session_id)


@router.post("/{replay_id}/tool-lookup")
async def tool_lookup(
    replay_id: uuid.UUID,
    body: ToolLookupRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ToolLookupResponse:
    """Look up a replay tool result; clients observe 200 or 404."""
    found, result = await service.tool_lookup(
        replay_id, body.tool_name, body.cache_key, actor=actor
    )
    return ToolLookupResponse(found=found, result=result)
