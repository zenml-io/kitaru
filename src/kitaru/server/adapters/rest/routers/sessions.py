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
    get_evaluation_service,
    get_session_node_service,
    get_session_service,
)
from kitaru.server.adapters.rest.mapping.evaluations import evaluation_to_response
from kitaru.server.adapters.rest.mapping.session_nodes import (
    session_node_to_command,
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
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Create a session; clients observe 201, 404, 409, or 422."""
    return session_to_response(
        await service.create_session(session_create_to_command(body), actor=actor)
    )


@router.get("")
async def list_sessions(
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[SessionListParams, Query()],
) -> Page[SessionResponse]:
    """List sessions; clients observe 200 or 422."""
    items, cursor = await service.list_sessions(
        session_list_params_to_filter(params), actor=actor
    )
    return Page[SessionResponse](
        items=[session_to_response(item) for item in items], next_cursor=cursor
    )


@router.get("/{session_id}")
async def get_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Get a session; clients observe 200 or 404."""
    return session_to_response(await service.get_session(session_id, actor=actor))


@router.patch("/{session_id}")
async def update_session(
    session_id: uuid.UUID,
    body: SessionUpdateRequest,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> SessionResponse:
    """Update a session; clients observe 200, 404, 409, or 422."""
    return session_to_response(
        await service.update_session(
            session_id, session_update_to_command(body), actor=actor
        )
    )


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: uuid.UUID,
    service: Annotated[SessionService, Depends(get_session_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a session; clients observe 204, 404, or 409."""
    await service.delete_session(session_id, actor=actor)


@router.post("/{session_id}/nodes")
async def ingest_session_nodes(
    session_id: uuid.UUID,
    body: SessionNodeBatchRequest,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[SessionNodeResponse]:
    """Ingest session nodes; clients observe 200, 404, or 422."""
    nodes, index_by_id = await service.ingest_nodes(
        session_id,
        [session_node_to_command(item) for item in body.nodes],
        actor=actor,
    )
    return [
        session_node_to_response(node, index_by_id, include_payloads=True)
        for node in nodes
    ]


@router.get("/{session_id}/nodes")
async def list_session_nodes(
    session_id: uuid.UUID,
    service: Annotated[SessionNodeService, Depends(get_session_node_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[SessionNodeListParams, Query()],
) -> Page[SessionNodeResponse]:
    """List session nodes; clients observe 200, 404, or 422."""
    nodes, cursor, index_by_id = await service.list_nodes(
        session_id,
        include_payloads=params.include_payloads,
        cursor=params.cursor,
        size=params.size,
        actor=actor,
    )
    return Page[SessionNodeResponse](
        items=[
            session_node_to_response(
                node, index_by_id, include_payloads=params.include_payloads
            )
            for node in nodes
        ],
        next_cursor=cursor,
    )


@router.post("/{session_id}/evaluations")
async def merge_session_evaluations(
    session_id: uuid.UUID,
    body: SessionEvaluationsRequest,
    service: Annotated[EvaluationService, Depends(get_evaluation_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[EvaluationResponse]:
    """Merge manual evaluations; clients observe 200, 404, or 422."""
    items = await service.merge_evaluations(
        session_id,
        [item.model_dump(mode="python") for item in body.evaluations],
        actor=actor,
    )
    return [evaluation_to_response(item) for item in items]
