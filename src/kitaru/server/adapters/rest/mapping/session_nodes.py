"""Session node DTO conversions."""

import uuid

from kitaru.api_models.v1.session import TokenUsage as TokenUsageDTO
from kitaru.api_models.v1.session_node import (
    SessionNodeCreateRequest,
    SessionNodeResponse,
)
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.domain.session_node import SessionNode


def session_node_to_response(
    node: SessionNode,
    index_by_id: dict[uuid.UUID, int],
    include_payloads: bool,
) -> SessionNodeResponse:
    """Convert a node using the complete id-to-index lookup for its session."""
    tokens = (
        TokenUsageDTO.model_validate(node.tokens.model_dump())
        if node.tokens is not None
        else None
    )
    return SessionNodeResponse(
        id=node.id,
        session_id=node.session_id,
        index=node.index,
        parent_index=(
            index_by_id[node.parent_id] if node.parent_id is not None else None
        ),
        parent_id=node.parent_id,
        secondary_parent_indexes=[
            index_by_id[parent_id] for parent_id in node.secondary_parent_ids
        ],
        secondary_parent_ids=node.secondary_parent_ids,
        external_id=node.external_id,
        trace_id=node.trace_id,
        node_type=node.node_type,
        name=node.name,
        status=node.status,
        error=node.error,
        started_at=node.started_at,
        ended_at=node.ended_at,
        inputs=node.inputs if include_payloads else None,
        outputs=node.outputs if include_payloads else None,
        requested_model=node.requested_model,
        model=node.model,
        provider=node.provider,
        tokens=tokens,
        cost=node.cost,
        model_params=node.model_params,
        tool_name=node.tool_name,
        subagent_id=node.subagent_id,
        attributes=node.attributes if include_payloads else None,
        metadata=node.metadata,
        cache_key=node.cache_key,
    )


def session_node_to_command(
    body: SessionNodeCreateRequest,
) -> SessionNodeUpsert:
    """Convert a node upsert body."""
    return SessionNodeUpsert.model_validate(body.model_dump(mode="python"))
