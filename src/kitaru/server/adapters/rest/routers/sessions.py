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
    SessionDetailResponse,
    SessionEvaluationsRequest,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
    SessionNodeListParams,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import (
    created_evaluation_to_response,
    session_evaluations_request_to_creates,
)
from kitaru.server.adapters.rest.mapping.session_nodes import (
    referenced_parent_ids,
    session_node_batch_to_upserts,
    session_node_list_params_to_filter,
    session_node_to_response,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    session_create_to_command,
    session_list_params_to_filter,
    session_to_detail_response,
    session_to_response,
    session_update_to_command,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_session(
    body: SessionCreateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionResponse:
    """Create a session.

    A task principal's session is always linked to its own task, regardless
    of the request's task_id. Clients observe HTTP 201 on success, 409 when
    the imported_from and external id pair is already registered, and 422 on
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
) -> Page[SessionDetailResponse] | Page[SessionResponse]:
    """List sessions.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Session service.
        actor: Caller context.
        params: Session list params.

    Returns:
        Page of sessions, with payloads when include_payloads is set.
    """
    session_filter = session_list_params_to_filter(params)
    sessions, next_cursor = await service.list_sessions(
        session_filter, include_payloads=params.include_payloads, actor=actor
    )
    if params.include_payloads:
        return Page[SessionDetailResponse](
            items=[session_to_detail_response(session) for session in sessions],
            next_cursor=next_cursor,
        )
    return Page[SessionResponse](
        items=[session_to_response(session) for session in sessions],
        next_cursor=next_cursor,
    )


@router.get("/{session_id}", responses=error_responses(404))
async def get_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionDetailResponse:
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
    return session_to_detail_response(session)


@router.patch("/{session_id}", responses=error_responses(404, 409))
async def update_session(
    session_id: uuid.UUID,
    body: SessionUpdateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionResponse:
    """Update a session.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    409 when the session is no longer in progress, and 422 on invalid
    input, including an attempt to clear the status.

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


@router.delete(
    "/{session_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404, 409),
)
async def delete_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a session.

    Deleting a session cascades its nodes. Clients observe HTTP 204 on
    success, 404 when no session has this id, and 409 when the session is
    referenced by a cohort version, investigation, or replay.

    Args:
        session_id: Id of the session.
        service: Session service.
        actor: Caller context.
    """
    await service.delete_session(session_id, actor=actor)


@router.post("/{session_id}/nodes", responses=error_responses(404, 409))
async def ingest_session_nodes(
    session_id: uuid.UUID,
    body: SessionNodeBatchRequest,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> list[SessionNodeResponse]:
    """Ingest a batch of session nodes.

    An index already stored is replaced whole, matching the upsert
    semantics of ``POST /api/v1/workers``. Clients observe HTTP 200 on success,
    404 when no session has this id, 409 when the session does not
    currently accept node ingestion, and 422 when a parent_index does not
    resolve.

    Args:
        session_id: Id of the session to ingest into.
        body: Session node batch request.
        service: Session node service.
        actor: Caller context.

    Returns:
        Stored nodes in batch order, with reasoning, inputs, outputs, and
        attributes null.
    """
    batch = session_node_batch_to_upserts(body)
    nodes = await service.ingest_nodes(session_id, batch, actor=actor)
    index_by_id = await service.get_indexes_by_ids(
        session_id, referenced_parent_ids(nodes), actor=actor
    )
    return [
        session_node_to_response(node, index_by_id, include_payloads=False)
        for node in nodes
    ]


@router.get("/{session_id}/nodes", responses=error_responses(404))
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
    index_by_id = await service.get_indexes_by_ids(
        session_id, referenced_parent_ids(nodes), actor=actor
    )
    return Page[SessionNodeResponse](
        items=[
            session_node_to_response(
                node, index_by_id, include_payloads=session_node_filter.include_payloads
            )
            for node in nodes
        ],
        next_cursor=next_cursor,
    )


@router.get("/{session_id}/full", responses=error_responses(404))
async def get_session_with_nodes(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    node_service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> SessionWithNodesResponse:
    """Get a session together with every one of its nodes.

    The node list is not paginated, so one call carries a whole session.

    Clients observe HTTP 200 on success, 403 when a task token neither owns
    nor reads this session, and 404 when no session has this id.

    Args:
        session_id: Id of the session.
        service: Session service.
        node_service: Session node service.
        actor: Caller context.

    Returns:
        Session with every node, ordered by index.
    """
    session = await service.get_session(session_id, actor=actor)
    nodes = await node_service.list_all_nodes(
        session_id, include_payloads=True, actor=actor
    )
    index_by_id = await node_service.get_indexes_by_ids(
        session_id, referenced_parent_ids(nodes), actor=actor
    )
    return SessionWithNodesResponse(
        session=session_to_detail_response(session),
        nodes=[
            session_node_to_response(node, index_by_id, include_payloads=True)
            for node in nodes
        ],
    )


@router.post("/{session_id}/evaluations", responses=error_responses(404, 409))
async def create_session_evaluations(
    session_id: uuid.UUID,
    body: SessionEvaluationsRequest,
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> list[EvaluationResponse]:
    """Create manual evaluations on a session.

    Clients observe HTTP 200 on success, 404 when no session has this id,
    409 when the session is not finished or an evaluation name already
    exists for the session, and 422 when the request names the same
    evaluation twice.

    Args:
        session_id: Id of the session to create evaluations on.
        body: Session evaluations request.
        service: Evaluation service.
        actor: Caller context.

    Returns:
        Stored evaluations in request order.
    """
    commands = session_evaluations_request_to_creates(body)
    evaluations = await service.create_evaluations(session_id, commands, actor=actor)
    return [created_evaluation_to_response(evaluation) for evaluation in evaluations]
