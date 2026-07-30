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
"""Cohort version DTO conversions."""

import uuid

from kitaru.api_models.v1.base import ListParams
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionResponse,
    CohortVersionUpdateRequest,
)
from kitaru.server.application.models.cohort import (
    CohortVersionCreate,
    CohortVersionFilter,
    CohortVersionUpdate,
)
from kitaru.server.domain.cohort_version import CohortVersion


def cohort_version_create_to_command(
    body: CohortVersionCreateRequest,
) -> CohortVersionCreate:
    """Convert a cohort version create request to its application command.

    Args:
        body: Cohort version create request.

    Returns:
        Create command.
    """
    return CohortVersionCreate(
        add_session_ids=body.add_session_ids,
        remove_session_ids=body.remove_session_ids,
        display_version=body.display_version,
    )


def cohort_version_to_response(version: CohortVersion) -> CohortVersionResponse:
    """Convert a cohort version entity to its response DTO.

    Args:
        version: Stored cohort version.

    Returns:
        Cohort version response.
    """
    assert version.created is not None
    assert version.updated is not None
    return CohortVersionResponse(
        id=version.id,
        owner_id=version.owner_id,
        cohort_id=version.cohort_id,
        version=version.version,
        display_version=version.display_version,
        session_count=version.session_count,
        created=version.created,
        updated=version.updated,
    )


def cohort_version_list_params_to_filter(
    cohort_id: uuid.UUID, params: ListParams
) -> CohortVersionFilter:
    """Convert list params to the application filter scoped to one cohort.

    Args:
        cohort_id: Id of the cohort whose versions to list.
        params: List params.

    Returns:
        Cohort version filter.
    """
    return CohortVersionFilter(
        cohort_id=cohort_id,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def cohort_version_update_to_command(
    body: CohortVersionUpdateRequest,
) -> CohortVersionUpdate:
    """Convert a cohort version update request to its application command.

    Args:
        body: Cohort version update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    return CohortVersionUpdate(**body.model_dump(exclude_unset=True))
