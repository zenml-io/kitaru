"""Cohort DTO conversions."""

import uuid

from kitaru.api_models.v1.base import ListParams
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortSessionsFilter,
    CohortUpdate,
)
from kitaru.server.domain.cohort import Cohort


def cohort_to_response(cohort: Cohort) -> CohortResponse:
    """Convert a cohort entity to its response."""
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


def cohort_create_to_command(body: CohortCreateRequest) -> CohortCreate:
    """Convert a cohort create body."""
    return CohortCreate(
        name=body.name,
        description=body.description,
        agent_id=body.agent_id,
        session_ids=body.session_ids,
    )


def cohort_update_to_command(body: CohortUpdateRequest) -> CohortUpdate:
    """Convert a cohort PATCH body while preserving unset fields."""
    return to_partial(CohortUpdate, body)


def cohort_list_params_to_filter(params: CohortListParams) -> CohortFilter:
    """Convert cohort list query parameters."""
    return CohortFilter(**params.model_dump(mode="python"))


def cohort_sessions_params_to_filter(
    cohort_id: uuid.UUID, params: ListParams
) -> CohortSessionsFilter:
    """Convert cohort session pagination parameters."""
    return CohortSessionsFilter(
        cohort_id=cohort_id,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
