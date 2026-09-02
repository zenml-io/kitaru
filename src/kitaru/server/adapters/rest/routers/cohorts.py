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
    CohortUpdateRequest,
)
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionListParams,
    CohortVersionResponse,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_cohort_service,
    get_cohort_version_service,
)
from kitaru.server.adapters.rest.mapping.cohort_versions import (
    cohort_version_create_to_command,
    cohort_version_list_params_to_filter,
    cohort_version_to_response,
)
from kitaru.server.adapters.rest.mapping.cohorts import (
    cohort_create_to_command,
    cohort_list_params_to_filter,
    cohort_to_response,
    cohort_update_to_command,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute, idempotent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)

router = APIRouter(route_class=KitaruAPIRoute)


@router.post(
    "", status_code=status.HTTP_201_CREATED, responses=error_responses(400, 404, 409)
)
@idempotent
async def create_cohort(
    body: CohortCreateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Create a cohort namespace.

    Clients observe HTTP 201 on success, 404 when the agent does not exist,
    409 when the cohort name is already registered, and 422 on invalid
    input.

    Args:
        body: Cohort create request.
        service: Cohort service.
        actor: Caller context.

    Returns:
        Created cohort.
    """
    command = cohort_create_to_command(body)
    cohort = await service.create_cohort(command, actor=actor)
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


@router.get("/{cohort_id}", responses=error_responses(404))
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


@router.patch("/{cohort_id}", responses=error_responses(404, 409))
async def update_cohort(
    cohort_id: uuid.UUID,
    body: CohortUpdateRequest,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortResponse:
    """Update a cohort's name, description, and metadata.

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


@router.delete(
    "/{cohort_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404, 409),
)
async def delete_cohort(
    cohort_id: uuid.UUID,
    service: Annotated[CohortService, Depends(get_cohort_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a cohort.

    Deleting a cohort cascades its versions. Clients observe HTTP 204 on
    success, 404 when no cohort has this id, and 409 when an experiment
    run references one of its versions.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort service.
        actor: Caller context.
    """
    await service.delete_cohort(cohort_id, actor=actor)


@router.post(
    "/{cohort_id}/versions",
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(400, 404, 409),
)
@idempotent
async def create_cohort_version(
    cohort_id: uuid.UUID,
    body: CohortVersionCreateRequest,
    service: Annotated[CohortVersionService, Depends(get_cohort_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortVersionResponse:
    """Create a new version of a cohort from a membership delta.

    Clients observe HTTP 201 on success, 404 when no cohort has this id or
    no cohort version has the baseline id, and 422 when the baseline belongs
    to a different cohort, the delta removes a session absent from the base
    version, adds a session already present, repeats a session id, or an
    added session is missing or belongs to a different agent.

    Args:
        cohort_id: Id of the cohort.
        body: Cohort version create request.
        service: Cohort version service.
        actor: Caller context.

    Returns:
        Created cohort version.
    """
    command = cohort_version_create_to_command(body)
    version = await service.create_version(cohort_id, command, actor=actor)
    return cohort_version_to_response(version)


@router.get("/{cohort_id}/versions", responses=error_responses(404))
async def list_cohort_versions(
    cohort_id: uuid.UUID,
    service: Annotated[CohortVersionService, Depends(get_cohort_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[CohortVersionListParams, Query()],
) -> Page[CohortVersionResponse]:
    """List the versions of a cohort.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        cohort_id: Id of the cohort.
        service: Cohort version service.
        actor: Caller context.
        params: Cohort version list params.

    Returns:
        Page of cohort versions.
    """
    version_filter = cohort_version_list_params_to_filter(cohort_id, params)
    versions, next_cursor = await service.list_versions(version_filter, actor=actor)
    return Page[CohortVersionResponse](
        items=[cohort_version_to_response(version) for version in versions],
        next_cursor=next_cursor,
    )
