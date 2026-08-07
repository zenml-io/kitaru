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
"""Session node API models."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self

from pydantic import AwareDatetime, Field, model_validator

from kitaru.api_models.v1.base import (
    CursorParams,
    JsonValue,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.session import SessionResponse, TokenUsage


class NodeType(StrEnum):
    """Kind of work a session node records."""

    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"
    SUBAGENT_CALL = "subagent_call"
    SPAN = "span"


class NodeStatus(StrEnum):
    """Session node status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class SessionNodeCreateRequest(RequestModel):
    """Session node create request."""

    index: int = Field(description="Position within the session, the wire identity.")
    parent_index: int | None = Field(
        default=None, description="Index of the parent node."
    )
    secondary_parent_indexes: list[int] = Field(
        default_factory=list, description="Indexes of additional parent nodes."
    )
    external_id: str | None = Field(
        default=None, description="Id from the source system."
    )
    trace_id: str | None = Field(default=None, description="Distributed trace id.")
    node_type: NodeType = Field(description="Kind of work the node records.")
    name: str = Field(description="Node name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(default=None, description="Error from a failed node.")
    started_at: AwareDatetime | None = Field(
        default=None, description="Time the node started."
    )
    ended_at: AwareDatetime | None = Field(
        default=None, description="Time the node ended."
    )
    input_text: str | None = Field(
        default=None, description="Primary human-readable input to the node."
    )
    output_text: str | None = Field(
        default=None, description="Primary human-readable output from the node."
    )
    system_prompt: str | None = Field(
        default=None, description="System prompt used by the model call."
    )
    reasoning: str | None = Field(
        default=None, description="Visible reasoning produced by the model call."
    )
    inputs: Any = Field(description="Node inputs.")
    outputs: Any = Field(description="Node outputs.")
    requested_model: str | None = Field(
        default=None, description="Model requested by the call."
    )
    model: str | None = Field(default=None, description="Model that served the call.")
    provider: str | None = Field(default=None, description="Model provider.")
    tokens: TokenUsage | None = Field(default=None, description="Token usage.")
    cost: Decimal | None = Field(default=None, description="Cost of the call.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Parameters passed to the model."
    )
    tool_name: str | None = Field(default=None, description="Tool called.")
    subagent_id: str | None = Field(default=None, description="Subagent invoked.")
    attributes: Any = Field(description="Arbitrary span attributes.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class SessionNodeListParams(CursorParams):
    """Session node list params."""

    include_payloads: bool = Field(
        default=False, description="Include inputs, outputs, and attributes."
    )


class SessionNodeBatchRequest(RequestModel):
    """Session node batch request."""

    nodes: list[SessionNodeCreateRequest] = Field(
        description="Nodes to upsert, parent before child."
    )

    @model_validator(mode="after")
    def _parents_precede_children(self) -> Self:
        """Require every node's parent indexes to precede its own index.

        Raises:
            ValueError: A parent index does not precede its node's index, or
                a node index repeats within the batch.

        Returns:
            The validated batch.
        """
        indexes: set[int] = set()
        for node in self.nodes:
            parent_indexes = [node.parent_index, *node.secondary_parent_indexes]
            if any(
                parent is not None and parent >= node.index for parent in parent_indexes
            ):
                raise ValueError("parent indexes must be less than the node index")
            if node.index in indexes:
                raise ValueError("node indexes must be unique within a batch")
            indexes.add(node.index)
        return self


class SessionNodeResponse(ResponseModel):
    """Session node response."""

    id: uuid.UUID = Field(description="Node id.")
    session_id: uuid.UUID = Field(description="Session this node belongs to.")
    index: int = Field(description="Position within the session.")
    parent_index: int | None = Field(description="Parent node index.")
    secondary_parent_indexes: list[int] = Field(description="Secondary parent indexes.")
    parent_id: uuid.UUID | None = Field(default=None, description="Parent node.")
    secondary_parent_ids: list[uuid.UUID] = Field(
        description="Additional parent nodes."
    )
    external_id: str | None = Field(
        default=None, description="Id from the source system."
    )
    trace_id: str | None = Field(default=None, description="Distributed trace id.")
    node_type: NodeType = Field(description="Kind of work the node records.")
    name: str = Field(description="Node name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(default=None, description="Error from a failed node.")
    started_at: datetime | None = Field(
        default=None, description="Time the node started."
    )
    ended_at: datetime | None = Field(default=None, description="Time the node ended.")
    input_text: str | None = Field(
        default=None, description="Primary human-readable input to the node."
    )
    output_text: str | None = Field(
        default=None, description="Primary human-readable output from the node."
    )
    system_prompt: str | None = Field(
        default=None, description="System prompt used by the model call."
    )
    reasoning: str | None = Field(
        default=None, description="Visible reasoning produced by the model call."
    )
    inputs: Any = Field(
        default=None, description="Node inputs, null unless include_payloads."
    )
    outputs: Any = Field(
        default=None, description="Node outputs, null unless include_payloads."
    )
    requested_model: str | None = Field(
        default=None, description="Model requested by the call."
    )
    model: str | None = Field(default=None, description="Model that served the call.")
    provider: str | None = Field(default=None, description="Model provider.")
    tokens: TokenUsage | None = Field(default=None, description="Token usage.")
    cost: Decimal | None = Field(default=None, description="Cost of the call.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Parameters passed to the model."
    )
    tool_name: str | None = Field(default=None, description="Tool called.")
    subagent_id: str | None = Field(default=None, description="Subagent invoked.")
    cache_key: str | None = Field(
        default=None, description="Cache key for a replayed tool call."
    )
    attributes: Any = Field(
        default=None,
        description="Arbitrary span attributes, null unless include_payloads.",
    )
    metadata: dict[str, JsonValue] = Field(description="Arbitrary metadata.")


class SessionWithNodesResponse(ResponseModel):
    """Session with nodes response."""

    session: SessionResponse = Field(description="Session.")
    nodes: list[SessionNodeResponse] = Field(
        description="Every node of the session, ordered by index ascending."
    )
