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

from kitaru.api_models.v1.cohorts import (
    CohortCreateRequest,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import set_fields
from kitaru.server.application.models.cohorts import CohortCreate, CohortUpdate
from kitaru.server.domain.cohort import Cohort


def cohort_create_to_command(body: CohortCreateRequest) -> CohortCreate:
    """Convert a cohort create request to its command.

    Args:
        body: Cohort create request.

    Returns:
        Cohort create command.
    """
    return CohortCreate(
        name=body.name,
        description=body.description,
        agent_id=body.agent_id,
        session_ids=body.session_ids,
    )


def cohort_update_to_command(body: CohortUpdateRequest) -> CohortUpdate:
    """Convert a cohort update request to its command.

    Only fields set on the request are set on the command, so an absent
    field stays distinguishable from an explicit null.

    Args:
        body: Cohort update request.

    Returns:
        Cohort update command.
    """
    return CohortUpdate(**set_fields(body))


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
        session_count=cohort.session_count,
        created=cohort.created,
        updated=cohort.updated,
    )
