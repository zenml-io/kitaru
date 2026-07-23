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
"""Session routes."""

import uuid
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from pydantic import AwareDatetime

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.session_nodes import (
    SessionNodeBatchRequest,
    SessionNodeResponse,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionProvider,
    SessionResponse,
    SessionScoresRequest,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.session_nodes import (
    node_upsert_to_command,
    session_node_to_response,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    origin_to_domain,
    provider_to_domain,
    session_create_to_command,
    session_to_response,
    session_update_to_command,
    status_to_domain,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_session(
    body: SessionCreateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Create a session.

    Clients observe HTTP 201 on success, 404 when no agent or agent version
    has the referenced id, 409 when the provider and external id pair is
    already registered, and 422 on invalid input.

    Args:
        body: Session create request.
        service: Session service.
        actor: Caller context.

    Returns:
        Created session.
    """
    session = await service.create_session(session_create_to_command(body), actor=actor)
    return session_to_response(session)


@router.get("")
async def list_sessions(
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    agent_id: uuid.UUID | None = None,
    agent_version_id: uuid.UUID | None = None,
    origin: SessionOrigin | None = None,
    status: SessionStatus | None = None,
    provider: SessionProvider | None = None,
    external_id: str | None = None,
    name: str | None = None,
    tag: str | None = None,
    started_after: AwareDatetime | None = None,
    started_before: AwareDatetime | None = None,
    ended_after: AwareDatetime | None = None,
    ended_before: AwareDatetime | None = None,
    has_score: bool | None = None,
    min_cost: Decimal | None = None,
    max_cost: Decimal | None = None,
    min_total_tokens: int | None = None,
    max_total_tokens: int | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[SessionResponse]:
    """List sessions.

    Clients observe HTTP 200 on success, 404 when no agent has the filtered
    agent id, and 422 on invalid filter or pagination parameters.

    Args:
        service: Session service.
        actor: Caller context.
        agent_id: Filter on agent id.
        agent_version_id: Filter on agent version id.
        origin: Filter on session origin.
        status: Filter on session status.
        provider: Filter on session provider.
        external_id: Filter on external id.
        name: Filter on session name.
        tag: Filter on attached tag name.
        started_after: Earliest start time.
        started_before: Latest start time.
        ended_after: Earliest end time.
        ended_before: Latest end time.
        has_score: Filter on the presence of scores.
        min_cost: Lowest cost.
        max_cost: Highest cost.
        min_total_tokens: Lowest total token count.
        max_total_tokens: Highest total token count.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of sessions.
    """
    session_filter = SessionFilter(
        agent_id=agent_id,
        agent_version_id=agent_version_id,
        origin=origin_to_domain(origin) if origin else None,
        status=status_to_domain(status),
        provider=provider_to_domain(provider),
        external_id=external_id,
        name=name,
        tag=tag,
        started_after=started_after,
        started_before=started_before,
        ended_after=ended_after,
        ended_before=ended_before,
        has_score=has_score,
        min_cost=min_cost,
        max_cost=max_cost,
        min_total_tokens=min_total_tokens,
        max_total_tokens=max_total_tokens,
        page=page,
        page_size=page_size,
    )
    sessions, total = await service.list_sessions(session_filter, actor=actor)
    return Page[SessionResponse](
        items=[session_to_response(session) for session in sessions],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{session_id}")
async def get_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Get a session by id.

    Clients observe HTTP 200 on success and 404 when no session has this
    id.

    Args:
        session_id: Id of the session.
        service: Session service.
        actor: Caller context.

    Returns:
        Stored session.
    """
    session = await service.get_session(session_id, actor=actor)
    return session_to_response(session)


@router.patch("/{session_id}")
async def update_session(
    session_id: uuid.UUID,
    body: SessionUpdateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Update a session, finishing it when a status is set.

    A set status finishes the session with the request's outputs, error,
    ended_at, and log_uri, and computes the rollups from its nodes. Name,
    expected, and metadata apply to any session.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    409 when a status is set but the session is not in progress, and 422 on
    invalid input.

    Args:
        session_id: Id of the session.
        body: Session update request.
        service: Session service.
        actor: Caller context.

    Returns:
        Updated session.
    """
    session = await service.update_session(
        session_id, session_update_to_command(body), actor=actor
    )
    return session_to_response(session)


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a session, including its nodes and tag links.

    Clients observe HTTP 204 on success and 404 when no session has this
    id.

    Args:
        session_id: Id of the session.
        service: Session service.
        actor: Caller context.
    """
    await service.delete_session(session_id, actor=actor)


@router.post("/{session_id}/nodes")
async def upsert_session_nodes(
    session_id: uuid.UUID,
    body: SessionNodeBatchRequest,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[SessionNodeResponse]:
    """Upsert a batch of session nodes.

    Nodes upsert on their client-generated id, so retries are idempotent.
    Nodes must arrive parent-before-child within and across batches.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    409 when the session does not accept node ingest or a sequence or
    external id is already registered, and 422 when a node references an
    unknown parent or the input is invalid.

    Args:
        session_id: Id of the session.
        body: Session node batch request.
        service: Session node service.
        actor: Caller context.

    Returns:
        Stored nodes in batch order.
    """
    nodes = await service.ingest_nodes(
        session_id,
        [node_upsert_to_command(node) for node in body.nodes],
        actor=actor,
    )
    return [session_node_to_response(node, include_payloads=True) for node in nodes]


@router.get("/{session_id}/nodes")
async def list_session_nodes(
    session_id: uuid.UUID,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    include_payloads: bool = False,
) -> list[SessionNodeResponse]:
    """List the nodes of a session ordered by sequence.

    Inputs, outputs, and attributes are null unless ``include_payloads`` is
    set.

    Clients observe HTTP 200 on success and 404 when no session has this
    id.

    Args:
        session_id: Id of the session.
        service: Session node service.
        actor: Caller context.
        include_payloads: Whether to include inputs, outputs, and
            attributes.

    Returns:
        Nodes ordered by sequence.
    """
    nodes = await service.list_nodes(session_id, include_payloads, actor=actor)
    return [
        session_node_to_response(node, include_payloads=include_payloads)
        for node in nodes
    ]


@router.post("/{session_id}/scores")
async def merge_session_scores(
    session_id: uuid.UUID,
    body: SessionScoresRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Merge values into a session's scores map, latest wins per name.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    and 422 on invalid input.

    Args:
        session_id: Id of the session.
        body: Session scores request.
        service: Session service.
        actor: Caller context.

    Returns:
        Updated session.
    """
    session = await service.merge_scores(session_id, body.scores, actor=actor)
    return session_to_response(session)
