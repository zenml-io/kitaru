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
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionEvaluationsRequest,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import (
    merged_evaluation_to_response,
    session_evaluations_request_to_merges,
)
from kitaru.server.adapters.rest.mapping.session_nodes import (
    session_node_batch_to_upserts,
    session_node_list_params_to_filter,
    session_node_to_response,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    session_create_to_command,
    session_list_params_to_filter,
    session_to_response,
    session_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_session(
    body: SessionCreateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionResponse:
    """Create a session.

    A task principal's session is always linked to its own task, regardless
    of the request's task_id. Clients observe HTTP 201 on success, 409 when
    the provider and external id pair is already registered, and 422 on
    invalid input.

    Args:
        body: Session create request.
        service: Session service.
        actor: Caller context.

    Returns:
        Created session.
    """
    command = session_create_to_command(body)
    session = await service.create_session(command, actor=actor)
    return session_to_response(session)


@router.get("")
async def list_sessions(
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[SessionListParams, Query()],
) -> Page[SessionResponse]:
    """List sessions.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Session service.
        actor: Caller context.
        params: Session list params.

    Returns:
        Page of sessions.
    """
    session_filter = session_list_params_to_filter(params)
    sessions, next_cursor = await service.list_sessions(session_filter, actor=actor)
    return Page[SessionResponse](
        items=[session_to_response(session) for session in sessions],
        next_cursor=next_cursor,
    )


@router.get("/{session_id}")
async def get_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
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
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionResponse:
    """Update a session.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    409 when the update moves a terminal session back to in_progress, and
    422 on invalid input, including an attempt to clear the status.

    Args:
        session_id: Id of the session.
        body: Session update request.
        service: Session service.
        actor: Caller context.

    Returns:
        Updated session.
    """
    command = session_update_to_command(body)
    session = await service.update_session(session_id, command, actor=actor)
    return session_to_response(session)


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a session.

    Deleting a session cascades its nodes. Clients observe HTTP 204 on
    success and 404 when no session has this id.

    Args:
        session_id: Id of the session.
        service: Session service.
        actor: Caller context.
    """
    await service.delete_session(session_id, actor=actor)


@router.post("/{session_id}/nodes")
async def ingest_session_nodes(
    session_id: uuid.UUID,
    body: SessionNodeBatchRequest,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> list[SessionNodeResponse]:
    """Ingest a batch of session nodes.

    An index already stored is replaced whole, matching the upsert
    semantics of ``POST /v1/workers``. Clients observe HTTP 200 on success,
    404 when no session has this id, 409 when the session does not
    currently accept node ingestion, and 422 when a parent_index does not
    resolve.

    Args:
        session_id: Id of the session to ingest into.
        body: Session node batch request.
        service: Session node service.
        actor: Caller context.

    Returns:
        Stored nodes in batch order, with inputs, outputs, and attributes
        populated.
    """
    batch = session_node_batch_to_upserts(body)
    nodes = await service.ingest_nodes(session_id, batch, actor=actor)
    index_by_id = await service.get_index_by_id(session_id, actor=actor)
    return [
        session_node_to_response(node, index_by_id, include_payloads=True)
        for node in nodes
    ]


@router.get("/{session_id}/nodes")
async def list_session_nodes(
    session_id: uuid.UUID,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
    params: Annotated[SessionNodeListParams, Query()],
) -> Page[SessionNodeResponse]:
    """List the nodes of a session, ordered by index ascending.

    Clients observe HTTP 200 on success, 403 when a task token neither owns
    nor reads this session, and 422 on invalid pagination parameters.

    Args:
        session_id: Id of the session.
        service: Session node service.
        actor: Caller context.
        params: Session node list params.

    Returns:
        Page of session nodes, ordered by index.
    """
    session_node_filter = session_node_list_params_to_filter(session_id, params)
    nodes, next_cursor = await service.list_nodes(session_node_filter, actor=actor)
    index_by_id = await service.get_index_by_id(session_id, actor=actor)
    return Page[SessionNodeResponse](
        items=[
            session_node_to_response(
                node, index_by_id, include_payloads=session_node_filter.include_payloads
            )
            for node in nodes
        ],
        next_cursor=next_cursor,
    )


@router.post("/{session_id}/evaluations")
async def merge_session_evaluations(
    session_id: uuid.UUID,
    body: SessionEvaluationsRequest,
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> list[EvaluationResponse]:
    """Merge manual evaluations into a session.

    A resent name overwrites its score, value, data type, and explanation.
    Clients observe HTTP 200 on success, 404 when no session has this id,
    and 422 when the request names the same evaluation twice.

    Args:
        session_id: Id of the session to merge evaluations into.
        body: Session evaluations request.
        service: Evaluation service.
        actor: Caller context.

    Returns:
        Stored evaluations in request order.
    """
    commands = session_evaluations_request_to_merges(body)
    evaluations = await service.merge_evaluations(session_id, commands, actor=actor)
    return [merged_evaluation_to_response(evaluation) for evaluation in evaluations]
