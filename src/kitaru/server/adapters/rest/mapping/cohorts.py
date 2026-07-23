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
    CohortSessionFilter,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    origin_to_domain,
    provider_to_domain,
    status_to_domain,
)
from kitaru.server.application.models.cohorts import CohortCreate
from kitaru.server.application.models.sessions import SessionFilter
from kitaru.server.domain.cohort import Cohort


def session_filter_to_domain(body: CohortSessionFilter) -> SessionFilter:
    """Convert a cohort session filter DTO to the application filter.

    Args:
        body: Cohort session filter.

    Returns:
        Session filter with default pagination.
    """
    return SessionFilter(
        agent_id=body.agent_id,
        agent_version_id=body.agent_version_id,
        origin=origin_to_domain(body.origin) if body.origin else None,
        status=status_to_domain(body.status),
        provider=provider_to_domain(body.provider),
        external_id=body.external_id,
        name=body.name,
        tag=body.tag,
        started_after=body.started_after,
        started_before=body.started_before,
        ended_after=body.ended_after,
        ended_before=body.ended_before,
        has_score=body.has_score,
        min_cost=body.min_cost,
        max_cost=body.max_cost,
        min_total_tokens=body.min_total_tokens,
        max_total_tokens=body.max_total_tokens,
    )


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
        session_filter=session_filter_to_domain(body.filter) if body.filter else None,
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
        session_count=cohort.session_count,
        filter_snapshot=cohort.filter_snapshot,
        created=cohort.created,
        updated=cohort.updated,
    )
