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
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_replay_service,
)
from kitaru.server.adapters.rest.mapping.replays import (
    replay_create_to_command,
    replay_list_params_to_filter,
    replay_to_response,
    tool_lookup_result_to_response,
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
    """Create a standalone replay of a recorded or imported session.

    Clients observe HTTP 201 on success, 404 when the baseline session or
    the resolved agent version or an evaluator config does not exist, and
    422 when the baseline session carries no agent version and none was
    given, the resolved agent version has no run spec, the tool policy uses
    cohort-version-scoped history, or an evaluator version repeats.

    Args:
        body: Replay create request.
        service: Replay service.
        actor: Caller context.

    Returns:
        Created replay.
    """
    command = replay_create_to_command(body)
    bundle = await service.create_replay(command, actor=actor)
    return replay_to_response(bundle.replay, bundle.config, bundle.result_session_id)


@router.get("")
async def list_replays(
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ReplayListParams, Query()],
) -> Page[ReplayResponse]:
    """List replays.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Replay service.
        actor: Caller context.
        params: Replay list params.

    Returns:
        Page of replays.
    """
    replay_filter = replay_list_params_to_filter(params)
    bundles, next_cursor = await service.list_replays(replay_filter, actor=actor)
    return Page[ReplayResponse](
        items=[
            replay_to_response(bundle.replay, bundle.config, bundle.result_session_id)
            for bundle in bundles
        ],
        next_cursor=next_cursor,
    )


@router.get("/{replay_id}")
async def get_replay(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> ReplayResponse:
    """Get a replay by id.

    Clients observe HTTP 200 on success, 403 when a task token names a task
    outside this replay's job, and 404 when no replay has this id.

    Args:
        replay_id: Id of the replay.
        service: Replay service.
        actor: Caller context.

    Returns:
        Stored replay.
    """
    bundle = await service.get_replay(replay_id, actor=actor)
    return replay_to_response(bundle.replay, bundle.config, bundle.result_session_id)


@router.post("/{replay_id}/tool-lookup")
async def tool_lookup(
    replay_id: uuid.UUID,
    body: ToolLookupRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> ToolLookupResponse:
    """Search recorded tool-call history for a cached result.

    Clients observe HTTP 200 on success, 403 when a task token names a task
    outside this replay's job, 404 when no replay has this id, and 422 when
    the tool is not configured for history or an occurrence was given for a
    non-baseline history scope.

    Args:
        replay_id: Id of the replay.
        body: Tool lookup request.
        service: Replay service.
        actor: Caller context.

    Returns:
        Whether a cached result was found, and the result if so.
    """
    result = await service.tool_lookup(
        replay_id, body.tool_name, body.cache_key, body.occurrence, actor=actor
    )
    return tool_lookup_result_to_response(result)
