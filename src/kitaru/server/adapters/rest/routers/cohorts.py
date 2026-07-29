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
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortSessionsListParams,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.session import SessionResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_cohort_service
from kitaru.server.adapters.rest.mapping.cohorts import (
    cohort_create_to_command,
    cohort_list_params_to_filter,
    cohort_sessions_list_params_to_filter,
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
    """Create a cohort as a fixed snapshot of its member sessions.

    Clients observe HTTP 201 on success, 404 when the agent does not exist,
    409 when the cohort name is already registered, and 422 when the member
    list is empty, has duplicates, or references a session that is missing
    or belongs to a different agent.

    Args:
        body: Cohort create request.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Created cohort.
    """
    command = cohort_create_to_command(body)
    cohort = await service.create_cohort(
        name=command.name,
        description=command.description,
        agent_id=command.agent_id,
        session_ids=command.session_ids,
        actor=actor,
    )
    return cohort_to_response(cohort)


@router.get("")
async def list_cohorts(
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[CohortListParams, Query()],
) -> Page[CohortResponse]:
    """List cohorts.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Cohort service.
        actor: Caller context.
        params: Cohort list params.

    Returns:
        Page of cohorts.
    """
    cohort_filter = cohort_list_params_to_filter(params)
    cohorts, next_cursor = await service.list_cohorts(cohort_filter, actor=actor)
    return Page[CohortResponse](
        items=[cohort_to_response(cohort) for cohort in cohorts],
        next_cursor=next_cursor,
    )


@router.get("/{cohort_id}")
async def get_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Get a cohort by id.

    Clients observe HTTP 200 on success and 404 when no cohort has this id.

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
    params: Annotated[CohortSessionsListParams, Query()],
) -> Page[SessionResponse]:
    """List a cohort's member sessions in cohort order.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.
        params: Cohort sessions list params.

    Returns:
        Page of member sessions, in cohort order.
    """
    sessions_filter = cohort_sessions_list_params_to_filter(cohort_id, params)
    sessions, next_cursor = await service.list_cohort_sessions(
        sessions_filter, actor=actor
    )
    return Page[SessionResponse](
        items=[session_to_response(session) for session in sessions],
        next_cursor=next_cursor,
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
    409 when the new name is already registered, and 422 when the update
    clears the name.

    Args:
        cohort_id: Id of the cohort.
        body: Cohort update request.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Updated cohort.
    """
    command = cohort_update_to_command(body)
    cohort = await service.update_cohort(cohort_id, command, actor=actor)
    return cohort_to_response(cohort)


@router.delete("/{cohort_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a cohort.

    Deleting a cohort cascades its member links. Clients observe HTTP 204 on
    success and 404 when no cohort has this id.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.
    """
    await service.delete_cohort(cohort_id, actor=actor)
