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
"""Cohort routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.cohorts import (
    CohortCreateRequest,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.sessions import SessionResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_cohort_service,
)
from kitaru.server.adapters.rest.mapping.cohorts import (
    cohort_create_to_command,
    cohort_to_response,
)
from kitaru.server.adapters.rest.mapping.sessions import session_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import (
    CohortFilter,
    CohortSessionsFilter,
)
from kitaru.server.application.services.cohort_service import CohortService

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_cohort(
    body: CohortCreateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Create a cohort from explicit session ids or a session filter.

    Explicit session ids require an agent id and keep their order as the
    member positions. A filter must pin an agent, resolves every matching
    session, and is stored as the cohort's provenance snapshot. Membership
    is immutable after creation.

    Clients observe HTTP 201 on success, 404 when no agent or session has a
    referenced id, 409 when the name is already registered, and 422 when
    the membership is empty, a member belongs to another agent or is in
    progress, or the input is invalid.

    Args:
        body: Cohort create request.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Created cohort.
    """
    cohort = await service.create_cohort(cohort_create_to_command(body), actor=actor)
    return cohort_to_response(cohort)


@router.get("")
async def list_cohorts(
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    name: str | None = None,
    tag: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[CohortResponse]:
    """List cohorts.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Cohort service.
        actor: Caller context.
        name: Filter on cohort name.
        tag: Filter on attached tag name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of cohorts.
    """
    cohort_filter = CohortFilter(name=name, tag=tag, page=page, page_size=page_size)
    cohorts, total = await service.list_cohorts(cohort_filter, actor=actor)
    return Page[CohortResponse](
        items=[cohort_to_response(cohort) for cohort in cohorts],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{cohort_id}")
async def get_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Get a cohort by id.

    Clients observe HTTP 200 on success and 404 when no cohort has this
    id.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Stored cohort.
    """
    cohort = await service.get_cohort(cohort_id, actor=actor)
    return cohort_to_response(cohort)


@router.get("/{cohort_id}/sessions")
async def list_cohort_sessions(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[SessionResponse]:
    """List the member sessions of a cohort ordered by position.

    Clients observe HTTP 200 on success, 404 when no cohort has this id,
    and 422 on invalid pagination parameters.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of member sessions.
    """
    sessions_filter = CohortSessionsFilter(page=page, page_size=page_size)
    sessions, total = await service.list_cohort_sessions(
        cohort_id, sessions_filter, actor=actor
    )
    return Page[SessionResponse](
        items=[session_to_response(session) for session in sessions],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.patch("/{cohort_id}")
async def update_cohort(
    cohort_id: uuid.UUID,
    body: CohortUpdateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Update a cohort's name and description.

    Clients observe HTTP 200 on success, 404 when no cohort has this id,
    409 when the new name is already registered, and 422 on invalid input.

    Args:
        cohort_id: Id of the cohort.
        body: Cohort update request.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Updated cohort.
    """
    cohort = await service.update_cohort(
        cohort_id, name=body.name, description=body.description, actor=actor
    )
    return cohort_to_response(cohort)


@router.delete("/{cohort_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a cohort, including its membership and tag links.

    Clients observe HTTP 204 on success and 404 when no cohort has this
    id.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.
    """
    await service.delete_cohort(cohort_id, actor=actor)
