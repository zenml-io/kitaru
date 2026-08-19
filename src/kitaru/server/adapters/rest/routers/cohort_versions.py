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
"""Cohort version routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.cohort_version import (
    CohortVersionResponse,
    CohortVersionUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_cohort_version_service,
)
from kitaru.server.adapters.rest.mapping.cohort_versions import (
    cohort_version_to_response,
    cohort_version_update_to_command,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)

router = APIRouter(route_class=KitaruAPIRoute)


@router.get("/{cohort_version_id}")
async def get_cohort_version(
    cohort_version_id: uuid.UUID,
    service: Annotated[CohortVersionService, Depends(get_cohort_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortVersionResponse:
    """Get a cohort version by id.

    Clients observe HTTP 200 on success and 404 when no cohort version has
    this id.

    Args:
        cohort_version_id: Id of the cohort version.
        service: Cohort version service.
        actor: Caller context.

    Returns:
        Stored cohort version.
    """
    version = await service.get_version(cohort_version_id, actor=actor)
    return cohort_version_to_response(version)


@router.patch("/{cohort_version_id}")
async def update_cohort_version(
    cohort_version_id: uuid.UUID,
    body: CohortVersionUpdateRequest,
    service: Annotated[CohortVersionService, Depends(get_cohort_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> CohortVersionResponse:
    """Update a cohort version.

    Clients observe HTTP 200 on success, 404 when no cohort version has this
    id, and 422 on invalid input.

    Args:
        cohort_version_id: Id of the cohort version.
        body: Cohort version update request.
        service: Cohort version service.
        actor: Caller context.

    Returns:
        Updated cohort version.
    """
    command = cohort_version_update_to_command(body)
    version = await service.update_version(cohort_version_id, command, actor=actor)
    return cohort_version_to_response(version)


@router.delete("/{cohort_version_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cohort_version(
    cohort_version_id: uuid.UUID,
    service: Annotated[CohortVersionService, Depends(get_cohort_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a cohort version.

    Clients observe HTTP 204 on success and 404 when no cohort version has
    this id.

    Args:
        cohort_version_id: Id of the cohort version.
        service: Cohort version service.
        actor: Caller context.
    """
    await service.delete_version(cohort_version_id, actor=actor)
