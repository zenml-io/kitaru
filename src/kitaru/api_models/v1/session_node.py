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
from typing import Any, Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from kitaru.api_models.v1.base import (
    JsonValue,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.session import SessionDetailResponse, TokenUsage


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

    external_id: str = Field(
        min_length=1, description="Id from the source system, the wire identity."
    )
    parent_external_id: str | None = Field(
        default=None, description="External id of the parent node."
    )
    secondary_parent_external_ids: list[str] = Field(
        default_factory=list, description="External ids of additional parent nodes."
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
    input_text_selector: str | None = Field(
        default=None,
        description="RFC 6901 JSON Pointer selecting display text from node inputs.",
    )
    output_text_selector: str | None = Field(
        default=None,
        description="RFC 6901 JSON Pointer selecting display text from node outputs.",
    )
    system_prompt_selector: str | None = Field(
        default=None,
        description=(
            "RFC 6901 JSON Pointer selecting the system prompt from node inputs."
        ),
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
    model_provider: str | None = Field(default=None, description="Model provider.")
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


class SessionNodeListParams(FilterableListParams):
    """Session node list params."""

    sort: Literal["position:asc"] = Field(
        default="position:asc",
        description="Nodes are ordered by start time, then insertion.",
    )
    include_payloads: bool = Field(
        default=False,
        description="Include reasoning, inputs, outputs, and attributes.",
    )


class SessionNodeBatchRequest(RequestModel):
    """Session node batch request."""

    nodes: list[SessionNodeCreateRequest] = Field(
        max_length=500,
        description="Nodes to upsert, in any order.",
    )

    @model_validator(mode="after")
    def _check_external_ids(self) -> Self:
        """Require unique node external ids and no self-parent.

        A parent external id the batch does not carry is left to the server,
        which links it once the parent lands.

        Raises:
            ValueError: A node names itself as a parent, or a node external
                id repeats within the batch.

        Returns:
            The validated batch.
        """
        seen: set[str] = set()
        for node in self.nodes:
            parents = [node.parent_external_id, *node.secondary_parent_external_ids]
            if node.external_id in parents:
                raise ValueError("a node cannot be its own parent")
            if node.external_id in seen:
                raise ValueError("node external ids must be unique within a batch")
            seen.add(node.external_id)
        return self


class SessionNodeResponse(ResponseModel):
    """Session node response."""

    id: uuid.UUID = Field(description="Node id.")
    session_id: uuid.UUID = Field(description="Session this node belongs to.")
    parent_id: uuid.UUID | None = Field(default=None, description="Parent node.")
    secondary_parent_ids: list[uuid.UUID] = Field(
        description="Additional parent nodes."
    )
    external_id: str = Field(description="Id from the source system.")
    parent_external_id: str | None = Field(
        default=None, description="External id of the parent node."
    )
    secondary_parent_external_ids: list[str] = Field(
        description="External ids of additional parent nodes."
    )
    trace_id: str | None = Field(default=None, description="Distributed trace id.")
    node_type: NodeType = Field(description="Kind of work the node records.")
    name: str = Field(description="Node name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(default=None, description="Error from a failed node.")
    started_at: datetime = Field(description="Time the node started.")
    ended_at: datetime | None = Field(default=None, description="Time the node ended.")
    input_text_selector: str | None = Field(
        default=None,
        description="RFC 6901 JSON Pointer selecting display text from node inputs.",
    )
    output_text_selector: str | None = Field(
        default=None,
        description="RFC 6901 JSON Pointer selecting display text from node outputs.",
    )
    system_prompt_selector: str | None = Field(
        default=None,
        description=(
            "RFC 6901 JSON Pointer selecting the system prompt from node inputs."
        ),
    )
    reasoning: str | None = Field(
        default=None,
        description="Visible reasoning, null unless payloads are included.",
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
    model_provider: str | None = Field(default=None, description="Model provider.")
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

    session: SessionDetailResponse = Field(description="Session.")
    nodes: list[SessionNodeResponse] = Field(
        description="Every node of the session, ordered by position ascending."
    )
