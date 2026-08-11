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
"""Investigation and investigation session routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationResponse,
    InvestigationSessionResponse,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationUpdateRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_investigation_service,
)
from kitaru.server.adapters.rest.mapping.investigations import (
    investigation_create_to_command,
    investigation_list_params_to_filter,
    investigation_session_list_params_to_filter,
    investigation_session_to_response,
    investigation_to_response,
    investigation_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_investigation(
    body: InvestigationCreateRequest,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InvestigationResponse:
    """Create an investigation with its questions and linked sessions in one shot.

    Clients observe HTTP 201 on success, 404 when the agent does not exist,
    and 422 when a linked session id repeats, is missing, or belongs to a
    different agent, or the questions contain a duplicate key.

    Args:
        body: Investigation create request.
        service: Investigation service.
        actor: Caller context.

    Returns:
        Created investigation.
    """
    command = investigation_create_to_command(body)
    investigation = await service.create_investigation(command, actor=actor)
    return investigation_to_response(investigation)


@router.get("")
async def list_investigations(
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[InvestigationListParams, Query()],
) -> Page[InvestigationResponse]:
    """List investigations.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Investigation service.
        actor: Caller context.
        params: Investigation list params.

    Returns:
        Page of investigations.
    """
    investigation_filter = investigation_list_params_to_filter(params)
    items, next_cursor = await service.list_investigations(
        investigation_filter, actor=actor
    )
    return Page[InvestigationResponse](
        items=[investigation_to_response(item) for item in items],
        next_cursor=next_cursor,
    )


@router.get("/{investigation_id}")
async def get_investigation(
    investigation_id: uuid.UUID,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InvestigationResponse:
    """Get an investigation by id.

    Clients observe HTTP 200 on success and 404 when no investigation has
    this id.

    Args:
        investigation_id: Id of the investigation.
        service: Investigation service.
        actor: Caller context.

    Returns:
        Stored investigation.
    """
    investigation = await service.get_investigation(investigation_id, actor=actor)
    return investigation_to_response(investigation)


@router.patch("/{investigation_id}")
async def update_investigation(
    investigation_id: uuid.UUID,
    body: InvestigationUpdateRequest,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InvestigationResponse:
    """Update an investigation's name, description, and status.

    Clients observe HTTP 200 on success, 404 when no investigation has this
    id, 409 when the update moves the status backwards, and 422 when the
    update clears the investigation name or status.

    Args:
        investigation_id: Id of the investigation.
        body: Investigation update request.
        service: Investigation service.
        actor: Caller context.

    Returns:
        Updated investigation.
    """
    command = investigation_update_to_command(body)
    investigation = await service.update_investigation(
        investigation_id, command, actor=actor
    )
    return investigation_to_response(investigation)


@router.delete("/{investigation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_investigation(
    investigation_id: uuid.UUID,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an investigation, cascading its linked sessions and answers.

    Clients observe HTTP 204 on success and 404 when no investigation has
    this id.

    Args:
        investigation_id: Id of the investigation.
        service: Investigation service.
        actor: Caller context.
    """
    await service.delete_investigation(investigation_id, actor=actor)


@router.get("/{investigation_id}/sessions")
async def list_investigation_sessions(
    investigation_id: uuid.UUID,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[InvestigationSessionsListParams, Query()],
) -> Page[InvestigationSessionResponse]:
    """List an investigation's linked sessions, ordered by position ascending.

    Clients observe HTTP 200 on success, 404 when no investigation has this
    id, and 422 on invalid pagination parameters.

    Args:
        investigation_id: Id of the investigation.
        service: Investigation service.
        actor: Caller context.
        params: Investigation sessions list params.

    Returns:
        Page of investigation sessions.
    """
    session_filter = investigation_session_list_params_to_filter(
        investigation_id, params
    )
    items, next_cursor = await service.list_investigation_sessions(
        session_filter, actor=actor
    )
    return Page[InvestigationSessionResponse](
        items=[investigation_session_to_response(item) for item in items],
        next_cursor=next_cursor,
    )


@router.patch("/{investigation_id}/sessions/{session_id}")
async def update_investigation_session_verdict(
    investigation_id: uuid.UUID,
    session_id: uuid.UUID,
    body: InvestigationSessionUpdateRequest,
    service: Annotated[InvestigationService, Depends(get_investigation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> InvestigationSessionResponse:
    """Set or clear a linked session's verdict.

    Clients observe HTTP 200 on success and 404 when no investigation has
    this id or no investigation session links this investigation and
    session.

    Args:
        investigation_id: Id of the investigation.
        session_id: Id of the linked session.
        body: Investigation session update request.
        service: Investigation service.
        actor: Caller context.

    Returns:
        Updated investigation session.
    """
    session = await service.update_investigation_session_verdict(
        investigation_id, session_id, body.verdict, actor=actor
    )
    return investigation_session_to_response(session)
