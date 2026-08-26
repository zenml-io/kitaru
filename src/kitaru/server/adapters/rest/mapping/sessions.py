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
"""Session DTO conversions."""

from typing import Any

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionDetailResponse,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.domain.session import Session


def session_create_to_command(body: SessionCreateRequest) -> SessionCreate:
    """Convert a session create request to its application command.

    Args:
        body: Session create request.

    Returns:
        Create command.
    """
    return SessionCreate(
        agent_id=body.agent_id,
        agent_version_id=body.agent_version_id,
        origin=body.origin,
        status=body.status,
        name=body.name,
        input_text_selector=body.input_text_selector,
        output_text_selector=body.output_text_selector,
        inputs=body.inputs,
        outputs=body.outputs,
        error=body.error,
        started_at=body.started_at,
        ended_at=body.ended_at,
        external_id=body.external_id,
        metadata=body.metadata,
        imported_from=body.imported_from,
        framework=body.framework,
        adapter_version=body.adapter_version,
    )


def session_to_response(session: Session) -> SessionResponse:
    """Convert a session entity to its response DTO.

    Args:
        session: Stored session.

    Returns:
        Session response.
    """
    assert session.created is not None
    assert session.updated is not None
    return SessionResponse(
        id=session.id,
        owner_id=session.owner_id,
        agent_id=session.agent_id,
        number=session.number,
        agent_version_id=session.agent_version_id,
        task_id=session.task_id,
        origin=session.origin,
        status=session.status,
        name=session.name,
        error=session.error,
        started_at=session.started_at,
        ended_at=session.ended_at,
        external_id=session.external_id,
        metadata=session.metadata,
        imported_from=session.imported_from,
        framework=session.framework,
        adapter_version=session.adapter_version,
        cost=session.cost,
        tokens=session.tokens,
        llm_call_count=session.llm_call_count,
        tool_call_count=session.tool_call_count,
        created=session.created,
        updated=session.updated,
    )


def session_to_detail_response(session: Session) -> SessionDetailResponse:
    """Convert a session entity to its detail response DTO.

    Args:
        session: Stored session with payloads resolved.

    Returns:
        Session detail response.
    """
    return SessionDetailResponse(
        **dict(session_to_response(session)),
        input_text_selector=session.input_text_selector,
        output_text_selector=session.output_text_selector,
        inputs=session.inputs.value if session.inputs is not None else None,
        outputs=session.outputs.value if session.outputs is not None else None,
    )


def session_list_params_to_filter(params: SessionListParams) -> SessionFilter:
    """Convert session list params to the application filter.

    Args:
        params: Session list params.

    Returns:
        Session filter.
    """
    return SessionFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def session_update_to_command(body: SessionUpdateRequest) -> SessionUpdate:
    """Convert a session update request to its application command.

    Args:
        body: Session update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    fields = body.model_fields_set
    values: dict[str, Any] = {}
    for field in (
        "status",
        "outputs",
        "output_text_selector",
        "error",
        "ended_at",
        "name",
    ):
        if field in fields:
            values[field] = getattr(body, field)
    if "metadata" in fields:
        values["metadata"] = body.metadata
    return SessionUpdate.model_validate(values)
