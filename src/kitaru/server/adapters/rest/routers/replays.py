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
from kitaru.api_models.v1.replays import (
    ReplayCreateRequest,
    ReplayDiffResponse,
    ReplayHeartbeatResponse,
    ReplayResponse,
    ReplaySpecResponse,
    ReplayStatus,
    ReplayUpdateRequest,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_replay_service,
)
from kitaru.server.adapters.rest.mapping.replays import (
    replay_create_to_command,
    replay_diff_to_response,
    replay_spec_to_response,
    replay_status_to_domain,
    replay_to_response,
    replay_update_to_command,
    tool_lookup_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.application.services.replay_service import ReplayService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_replay(
    body: ReplayCreateRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayResponse:
    """Create a standalone replay of one session with an inline config.

    The tool policy defaults to a history policy scoped to the original
    session.

    Clients observe HTTP 201 on success, 404 when no session or agent
    version has the referenced id, 409 when no runnable agent version
    resolves, and 422 when the original session is in progress, the
    version belongs to another agent, a history policy scopes to a
    cohort, or the input is invalid.

    Args:
        body: Replay create request.
        service: Replay service.
        actor: Caller context.

    Returns:
        Created replay.
    """
    replay, config = await service.create_replay(
        replay_create_to_command(body), actor=actor
    )
    return replay_to_response(replay, config)


@router.get("")
async def list_replays(
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    original_session_id: uuid.UUID | None = None,
    replay_status: Annotated[ReplayStatus | None, Query(alias="status")] = None,
    standalone: bool | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[ReplayResponse]:
    """List replays.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Replay service.
        actor: Caller context.
        original_session_id: Filter on the replayed session id.
        replay_status: Filter on replay status.
        standalone: Filter on standalone replays.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of replays.
    """
    replay_filter = ReplayFilter(
        original_session_id=original_session_id,
        status=replay_status_to_domain(replay_status),
        standalone=standalone,
        page=page,
        page_size=page_size,
    )
    replays, total = await service.list_replays(replay_filter, actor=actor)
    return Page[ReplayResponse](
        items=[replay_to_response(replay, config) for replay, config in replays],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{replay_id}")
async def get_replay(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayResponse:
    """Get a replay by id.

    Clients observe HTTP 200 on success and 404 when no replay has this
    id.

    Args:
        replay_id: Id of the replay.
        service: Replay service.
        actor: Caller context.

    Returns:
        Stored replay.
    """
    replay, config = await service.get_replay(replay_id, actor=actor)
    return replay_to_response(replay, config)


@router.get("/{replay_id}/spec")
async def get_replay_spec(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplaySpecResponse:
    """Resolve the spec a runner executes a replay with.

    The spec includes the resolved secret environment of the run spec's
    secrets.

    Clients observe HTTP 200 on success, 404 when no replay has this id or
    a run spec secret was deleted, and 409 when the stamped agent version
    has no run spec.

    Args:
        replay_id: Id of the replay.
        service: Replay service.
        actor: Caller context.

    Returns:
        Resolved replay spec.
    """
    spec = await service.get_spec(replay_id, actor=actor)
    return replay_spec_to_response(spec)


@router.patch("/{replay_id}")
async def update_replay(
    replay_id: uuid.UUID,
    body: ReplayUpdateRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayResponse:
    """Transition a replay through the runner status updates.

    Completing stores the scoring result and the computed diff summary.
    The transition that makes the last replay of a run terminal also
    finalizes the run.

    Clients observe HTTP 200 on success, 404 when no replay has this id,
    409 when the transition is illegal or completing without a linked
    result session, and 422 when completing without a scoring result or
    failing without an error.

    Args:
        replay_id: Id of the replay.
        body: Replay update request.
        service: Replay service.
        actor: Caller context.

    Returns:
        Updated replay.
    """
    replay, config = await service.update_replay(
        replay_id, replay_update_to_command(body), actor=actor
    )
    return replay_to_response(replay, config)


@router.post("/{replay_id}/heartbeat")
async def heartbeat_replay(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayHeartbeatResponse:
    """Record a worker heartbeat on a replay.

    Clients observe HTTP 200 on success, 404 when no replay has this id,
    and 409 when the replay is neither claimed, running, nor canceled.

    Args:
        replay_id: Id of the replay.
        service: Replay service.
        actor: Caller context.

    Returns:
        Heartbeat response with the cancellation flag.
    """
    canceled = await service.heartbeat_replay(replay_id, actor=actor)
    return ReplayHeartbeatResponse(canceled=canceled)


@router.post("/{replay_id}/tool-lookup")
async def tool_lookup(
    replay_id: uuid.UUID,
    body: ToolLookupRequest,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ToolLookupResponse:
    """Resolve a history tool policy lookup within its scope.

    Clients observe HTTP 200 on success, including misses, 404 when no
    replay has this id, and 422 when the cache key does not match the tool
    name and inputs, the tool resolves to no history policy, or a
    standalone replay scopes to a cohort.

    Args:
        replay_id: Id of the replay.
        body: Tool lookup request.
        service: Replay service.
        actor: Caller context.

    Returns:
        Tool lookup response.
    """
    node = await service.tool_lookup(
        replay_id,
        tool_name=body.tool_name,
        inputs=body.inputs,
        cache_key=body.cache_key,
        actor=actor,
    )
    return tool_lookup_to_response(node)


@router.get("/{replay_id}/diff")
async def get_replay_diff(
    replay_id: uuid.UUID,
    service: Annotated[ReplayService, Depends(get_replay_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ReplayDiffResponse:
    """Compute the full diff between a replay's sessions.

    Clients observe HTTP 200 on success, 404 when no replay has this id,
    and 409 when the replay has no result session yet.

    Args:
        replay_id: Id of the replay.
        service: Replay service.
        actor: Caller context.

    Returns:
        Computed replay diff.
    """
    diff = await service.compute_diff(replay_id, actor=actor)
    return replay_diff_to_response(diff)
