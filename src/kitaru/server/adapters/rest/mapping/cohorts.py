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
"""Cohort DTO conversions."""

from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortUpdate,
)
from kitaru.server.domain.cohort import Cohort


def cohort_create_to_command(body: CohortCreateRequest) -> CohortCreate:
    """Convert a cohort create request to its application command.

    Args:
        body: Cohort create request.

    Returns:
        Create command.
    """
    return CohortCreate(
        name=body.name,
        description=body.description,
        agent_id=body.agent_id,
        metadata=body.metadata,
    )


def cohort_to_response(cohort: Cohort) -> CohortResponse:
    """Convert a cohort entity to its response DTO.

    Args:
        cohort: Stored cohort.

    Returns:
        Cohort response.
    """
    assert cohort.created is not None
    assert cohort.updated is not None
    return CohortResponse(
        id=cohort.id,
        owner_id=cohort.owner_id,
        name=cohort.name,
        description=cohort.description,
        agent_id=cohort.agent_id,
        metadata=cohort.metadata,
        latest_version=cohort.latest_version,
        created=cohort.created,
        updated=cohort.updated,
    )


def cohort_list_params_to_filter(params: CohortListParams) -> CohortFilter:
    """Convert cohort list params to the application filter.

    Args:
        params: Cohort list params.

    Returns:
        Cohort filter.
    """
    return CohortFilter(
        agent_id=params.agent_id,
        name=params.name,
        tag=params.tag,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def cohort_update_to_command(body: CohortUpdateRequest) -> CohortUpdate:
    """Convert a cohort update request to its application command.

    Args:
        body: Cohort update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    return CohortUpdate(**body.model_dump(exclude_unset=True))
