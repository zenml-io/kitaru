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
    ReplayResponse,
    ReplayStatus,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_replay_service,
)
from kitaru.server.adapters.rest.mapping.replays import (
    replay_create_to_command,
    replay_status_to_domain,
    replay_to_response,
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
