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
"""Session node DTO conversions."""

import uuid

from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.session_node import (
    SessionNodeFilter,
    SessionNodeUpsert,
)
from kitaru.server.domain.session_node import SessionNode


def session_node_create_to_upsert(body: SessionNodeCreateRequest) -> SessionNodeUpsert:
    """Convert a session node create request to its application command.

    Args:
        body: Session node create request.

    Returns:
        Upsert command.
    """
    return SessionNodeUpsert(
        external_id=body.external_id,
        parent_external_id=body.parent_external_id,
        links=body.links,
        trace_id=body.trace_id,
        node_type=body.node_type,
        name=body.name,
        status=body.status,
        error=body.error,
        started_at=body.started_at,
        ended_at=body.ended_at,
        input_text_selector=body.input_text_selector,
        output_text_selector=body.output_text_selector,
        system_prompt_selector=body.system_prompt_selector,
        reasoning=body.reasoning,
        inputs=body.inputs,
        outputs=body.outputs,
        requested_model=body.requested_model,
        model=body.model,
        model_provider=body.model_provider,
        tokens=body.tokens,
        cost=body.cost,
        model_params=body.model_params,
        tool_name=body.tool_name,
        subagent_id=body.subagent_id,
        attributes=body.attributes,
        metadata=body.metadata,
    )


def session_node_batch_to_upserts(
    batch: SessionNodeBatchRequest,
) -> list[SessionNodeUpsert]:
    """Convert a session node batch request to its application commands.

    Args:
        batch: Session node batch request.

    Returns:
        Upsert commands in batch order.
    """
    return [session_node_create_to_upsert(node) for node in batch.nodes]


def session_node_to_response(
    node: SessionNode, include_payloads: bool
) -> SessionNodeResponse:
    """Convert a session node entity to its response DTO.

    Args:
        node: Stored session node.
        include_payloads: Whether to populate inputs, outputs, and
            attributes.

    Returns:
        Session node response.
    """
    return SessionNodeResponse(
        id=node.id,
        session_id=node.session_id,
        external_id=node.external_id,
        parent_external_id=node.parent_external_id,
        links=node.links,
        trace_id=node.trace_id,
        node_type=node.node_type,
        name=node.name,
        status=node.status,
        error=node.error,
        started_at=node.started_at,
        ended_at=node.ended_at,
        input_text_selector=node.input_text_selector,
        output_text_selector=node.output_text_selector,
        system_prompt_selector=node.system_prompt_selector,
        reasoning=(
            node.reasoning.value
            if include_payloads and node.reasoning is not None
            else None
        ),
        inputs=(
            node.inputs.value if include_payloads and node.inputs is not None else None
        ),
        outputs=(
            node.outputs.value
            if include_payloads and node.outputs is not None
            else None
        ),
        requested_model=node.requested_model,
        model=node.model,
        model_provider=node.model_provider,
        tokens=node.tokens,
        cost=node.cost,
        model_params=node.model_params,
        tool_name=node.tool_name,
        subagent_id=node.subagent_id,
        cache_key=node.cache_key,
        attributes=(
            node.attributes.value
            if include_payloads and node.attributes is not None
            else None
        ),
        metadata=node.metadata,
    )


def session_node_list_params_to_filter(
    session_id: uuid.UUID, params: SessionNodeListParams
) -> SessionNodeFilter:
    """Convert list params to the application filter scoped to one session.

    Args:
        session_id: Id of the session whose nodes to list.
        params: Session node list params.

    Returns:
        Session node filter.
    """
    return SessionNodeFilter(
        session_id=session_id,
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        include_payloads=params.include_payloads,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
