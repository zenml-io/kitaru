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

import kitaru.api_models.v1.session_nodes as node_models
from kitaru.api_models.v1.session_nodes import (
    SessionNodeCreateRequest,
    SessionNodeResponse,
)
from kitaru.server.adapters.rest.mapping.sessions import (
    token_usage_to_domain,
    token_usage_to_response,
)
from kitaru.server.application.models.session_nodes import SessionNodeUpsert
from kitaru.server.domain.session_node import NodeStatus, NodeType, SessionNode


def node_upsert_to_command(body: SessionNodeCreateRequest) -> SessionNodeUpsert:
    """Convert a session node create request to its upsert command.

    Args:
        body: Session node create request.

    Returns:
        Session node upsert command.
    """
    return SessionNodeUpsert(
        id=body.id,
        parent_id=body.parent_id,
        secondary_parent_ids=body.secondary_parent_ids,
        sequence=body.sequence,
        external_id=body.external_id,
        trace_id=body.trace_id,
        node_type=NodeType(body.node_type.value),
        name=body.name,
        status=NodeStatus(body.status.value),
        error=body.error,
        started_at=body.started_at,
        ended_at=body.ended_at,
        inputs=body.inputs,
        outputs=body.outputs,
        requested_model=body.requested_model,
        model=body.model,
        provider=body.provider,
        tokens=token_usage_to_domain(body.tokens),
        cost=body.cost,
        model_params=body.model_params,
        tool_name=body.tool_name,
        subagent_id=body.subagent_id,
        attributes=body.attributes,
        metadata=body.metadata,
    )


def session_node_to_response(
    node: SessionNode, include_payloads: bool
) -> SessionNodeResponse:
    """Convert a session node entity to its response DTO.

    Args:
        node: Stored session node.
        include_payloads: Whether to include inputs, outputs, and
            attributes.

    Returns:
        Session node response.
    """
    assert node.created is not None
    assert node.updated is not None
    return SessionNodeResponse(
        id=node.id,
        session_id=node.session_id,
        key=node.key,
        parent_id=node.parent_id,
        secondary_parent_ids=node.secondary_parent_ids,
        sequence=node.sequence,
        external_id=node.external_id,
        trace_id=node.trace_id,
        node_type=node_models.NodeType(node.node_type.value),
        name=node.name,
        status=node_models.NodeStatus(node.status.value),
        error=node.error,
        started_at=node.started_at,
        ended_at=node.ended_at,
        inputs=node.inputs if include_payloads else None,
        outputs=node.outputs if include_payloads else None,
        requested_model=node.requested_model,
        model=node.model,
        provider=node.provider,
        tokens=token_usage_to_response(node.tokens),
        cost=node.cost,
        model_params=node.model_params,
        tool_name=node.tool_name,
        cache_key=node.cache_key,
        subagent_id=node.subagent_id,
        attributes=node.attributes if include_payloads else None,
        metadata=node.metadata,
        created=node.created,
        updated=node.updated,
    )
