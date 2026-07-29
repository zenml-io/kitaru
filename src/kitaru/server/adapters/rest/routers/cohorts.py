"""Cohort routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.session import SessionResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_cohort_service
from kitaru.server.adapters.rest.mapping.cohorts import (
    cohort_create_to_command,
    cohort_list_params_to_filter,
    cohort_sessions_params_to_filter,
    cohort_to_response,
    cohort_update_to_command,
)
from kitaru.server.adapters.rest.mapping.sessions import session_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.cohort_service import CohortService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_cohort(
    body: CohortCreateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Create a cohort; clients observe 201, 404, 409, or 422."""
    return cohort_to_response(
        await service.create_cohort(cohort_create_to_command(body), actor=actor)
    )


@router.get("")
async def list_cohorts(
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[CohortListParams, Query()],
) -> Page[CohortResponse]:
    """List cohorts; clients observe 200 or 422."""
    items, cursor = await service.list_cohorts(
        cohort_list_params_to_filter(params), actor=actor
    )
    return Page[CohortResponse](
        items=[cohort_to_response(item) for item in items], next_cursor=cursor
    )


@router.get("/{cohort_id}")
async def get_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Get a cohort; clients observe 200 or 404."""
    return cohort_to_response(await service.get_cohort(cohort_id, actor=actor))


@router.get("/{cohort_id}/sessions")
async def list_cohort_sessions(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[SessionResponse]:
    """List cohort sessions; clients observe 200, 404, or 422."""
    items, cursor = await service.list_cohort_sessions(
        cohort_id, cohort_sessions_params_to_filter(cohort_id, params), actor=actor
    )
    return Page[SessionResponse](
        items=[session_to_response(item) for item in items], next_cursor=cursor
    )


@router.patch("/{cohort_id}")
async def update_cohort(
    cohort_id: uuid.UUID,
    body: CohortUpdateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Update a cohort; clients observe 200, 404, 409, or 422."""
    return cohort_to_response(
        await service.update_cohort(
            cohort_id, cohort_update_to_command(body), actor=actor
        )
    )


@router.delete("/{cohort_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a cohort; clients observe 204, 404, or 409."""
    await service.delete_cohort(cohort_id, actor=actor)
