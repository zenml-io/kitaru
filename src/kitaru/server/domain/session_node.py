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
"""Session node entity and errors."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.domain.base import DomainModel, ValidationError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import SessionRollups


class SessionNodeParentNotFound(ValidationError):
    """Raised when a node's parent_index does not match a stored or batched node."""

    def __init__(self, index: int, parent_index: int) -> None:
        """Initialize the error.

        Args:
            index: Index of the node whose parent reference did not resolve.
            parent_index: Parent index that did not resolve.
        """
        super().__init__(
            f"Node {index} references parent_index {parent_index}, which does "
            "not match a stored or batched node"
        )


class SessionNode(DomainModel):
    """Session node."""

    id: uuid.UUID = Field(default_factory=uuid7)
    session_id: uuid.UUID
    parent_id: uuid.UUID | None = None
    secondary_parent_ids: list[uuid.UUID] = Field(default_factory=list)
    index: int
    external_id: str | None = None
    trace_id: str | None = None
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    input_text_selector: str | None = None
    output_text_selector: str | None = None
    system_prompt_selector: str | None = None
    reasoning: str | None = None
    reasoning_blob_id: uuid.UUID | None = None
    inputs: Any = None
    inputs_blob_id: uuid.UUID | None = None
    outputs: Any = None
    outputs_blob_id: uuid.UUID | None = None
    requested_model: str | None = None
    model: str | None = None
    model_provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    cache_key: str | None = None
    subagent_id: str | None = None
    attributes: Any = None
    attributes_blob_id: uuid.UUID | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None


def node_rollup_contribution(node: SessionNode | None) -> SessionRollups:
    """Compute the rollup fields one node contributes to its session.

    Returns:
        Zero contribution when ``node`` is ``None``.
    """
    if node is None:
        return SessionRollups()
    tokens = node.tokens
    return SessionRollups(
        cost=node.cost if node.cost is not None else Decimal(0),
        input_tokens=(tokens.input_tokens or 0) if tokens is not None else 0,
        output_tokens=(tokens.output_tokens or 0) if tokens is not None else 0,
        cached_input_tokens=(tokens.cached_input_tokens or 0)
        if tokens is not None
        else 0,
        reasoning_tokens=(tokens.reasoning_tokens or 0) if tokens is not None else 0,
        llm_call_count=1 if node.node_type == NodeType.LLM_CALL else 0,
        tool_call_count=1 if node.node_type == NodeType.TOOL_CALL else 0,
    )
