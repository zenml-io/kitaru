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
"""Investigation and investigation session DTO conversions."""

import uuid

from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationResponse,
    InvestigationSessionResponse,
    InvestigationSessionsListParams,
    InvestigationUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.investigation import (
    InvestigationCreate,
    InvestigationFilter,
    InvestigationSessionFilter,
    InvestigationSessionInput,
    InvestigationUpdate,
)
from kitaru.server.domain.investigation import Investigation, InvestigationSession


def investigation_create_to_command(
    body: InvestigationCreateRequest,
) -> InvestigationCreate:
    """Convert an investigation create request to its application command.

    Args:
        body: Investigation create request.

    Returns:
        Create command.
    """
    return InvestigationCreate(
        agent_id=body.agent_id,
        name=body.name,
        description=body.description,
        questions=body.questions,
        sessions=[
            InvestigationSessionInput(session_id=item.session_id, view=item.view)
            for item in body.sessions
        ],
    )


def investigation_to_response(investigation: Investigation) -> InvestigationResponse:
    """Convert an investigation entity to its response DTO.

    Args:
        investigation: Stored investigation.

    Returns:
        Investigation response.
    """
    assert investigation.created is not None
    assert investigation.updated is not None
    return InvestigationResponse(
        id=investigation.id,
        owner_id=investigation.owner_id,
        agent_id=investigation.agent_id,
        name=investigation.name,
        description=investigation.description,
        status=investigation.status,
        questions=investigation.questions,
        started_at=investigation.started_at,
        ended_at=investigation.ended_at,
        metadata=investigation.metadata,
        total_sessions=investigation.total_sessions,
        completed_sessions=investigation.completed_sessions,
        created=investigation.created,
        updated=investigation.updated,
    )


def investigation_list_params_to_filter(
    params: InvestigationListParams,
) -> InvestigationFilter:
    """Convert investigation list params to the application filter.

    Args:
        params: Investigation list params.

    Returns:
        Investigation filter.
    """
    return InvestigationFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def investigation_update_to_command(
    body: InvestigationUpdateRequest,
) -> InvestigationUpdate:
    """Convert an investigation update request to its application command.

    Args:
        body: Investigation update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    return InvestigationUpdate(**body.model_dump(exclude_unset=True))


def investigation_session_list_params_to_filter(
    investigation_id: uuid.UUID, params: InvestigationSessionsListParams
) -> InvestigationSessionFilter:
    """Convert list params to the application filter scoped to one investigation.

    Args:
        investigation_id: Id of the investigation whose sessions to list.
        params: Investigation sessions list params.

    Returns:
        Investigation session filter.
    """
    return InvestigationSessionFilter(
        investigation_id=investigation_id,
        cursor=params.cursor,
        size=params.size,
    )


def investigation_session_to_response(
    session: InvestigationSession,
) -> InvestigationSessionResponse:
    """Convert an investigation session entity to its response DTO.

    Args:
        session: Stored investigation session.

    Returns:
        Investigation session response.
    """
    assert session.created is not None
    assert session.updated is not None
    return InvestigationSessionResponse(
        id=session.id,
        investigation_id=session.investigation_id,
        session_id=session.session_id,
        position=session.position,
        verdict=session.verdict,
        view=session.view,
        created=session.created,
        updated=session.updated,
    )
