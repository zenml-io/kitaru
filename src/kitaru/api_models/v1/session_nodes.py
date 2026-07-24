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
from typing import Any

from pydantic import AwareDatetime, ConfigDict, Field

from kitaru.api_models.v1.base import JsonValue, RequestModel, ResponseModel
from kitaru.api_models.v1.sessions import TokenUsage


class NodeType(StrEnum):
    """Session node type."""

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

    model_config = ConfigDict(protected_namespaces=())

    id: uuid.UUID = Field(description="Client-generated node id.")
    parent_id: uuid.UUID | None = Field(
        default=None, description="Id of the primary parent, absent for roots."
    )
    secondary_parent_ids: list[uuid.UUID] = Field(
        default_factory=list, description="Extra fan-in parent ids."
    )
    sequence: int = Field(
        ge=0, description="Producer-assigned order within the session."
    )
    external_id: str | None = Field(
        default=None, max_length=255, description="Provider observation or span id."
    )
    trace_id: str | None = Field(
        default=None, max_length=255, description="Provider or OTel trace id."
    )
    node_type: NodeType = Field(description="Node type.")
    name: str = Field(max_length=255, description="Display name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(default=None, description="Error message.")
    started_at: AwareDatetime | None = Field(default=None, description="Start time.")
    ended_at: AwareDatetime | None = Field(default=None, description="End time.")
    inputs: JsonValue = Field(default=None, description="Node inputs.")
    outputs: JsonValue = Field(default=None, description="Node outputs.")
    requested_model: str | None = Field(
        default=None, max_length=255, description="Model asked for."
    )
    model: str | None = Field(
        default=None, max_length=255, description="Model that answered."
    )
    provider: str | None = Field(
        default=None, max_length=64, description="LLM provider."
    )
    tokens: TokenUsage | None = Field(default=None, description="Token usage.")
    cost: Decimal | None = Field(default=None, description="Call cost.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Model parameters."
    )
    tool_name: str | None = Field(
        default=None, max_length=255, description="Name of the called tool."
    )
    subagent_id: str | None = Field(
        default=None, max_length=255, description="Harness id of the subagent."
    )
    attributes: dict[str, JsonValue] = Field(
        default_factory=dict, description="Provider or adapter attributes."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="User metadata."
    )


class SessionNodeBatchRequest(RequestModel):
    """Session node batch request."""

    nodes: list[SessionNodeCreateRequest] = Field(
        description="Nodes in parent-before-child order."
    )


class SessionNodeResponse(ResponseModel):
    """Session node response."""

    model_config = ConfigDict(protected_namespaces=())

    id: uuid.UUID = Field(description="Node id.")
    session_id: uuid.UUID = Field(description="Id of the session.")
    key: str = Field(description="Stable path key, computed by the server.")
    parent_id: uuid.UUID | None = Field(
        description="Id of the primary parent, null for roots."
    )
    secondary_parent_ids: list[uuid.UUID] = Field(
        description="Extra fan-in parent ids."
    )
    sequence: int = Field(description="Producer-assigned order within the session.")
    external_id: str | None = Field(description="Provider observation or span id.")
    trace_id: str | None = Field(description="Provider or OTel trace id.")
    node_type: NodeType = Field(description="Node type.")
    name: str = Field(description="Display name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(description="Error message.")
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    inputs: Any = Field(
        default=None, description="Node inputs, null unless payloads are requested."
    )
    outputs: Any = Field(
        default=None, description="Node outputs, null unless payloads are requested."
    )
    requested_model: str | None = Field(description="Model asked for.")
    model: str | None = Field(description="Model that answered.")
    provider: str | None = Field(description="LLM provider.")
    tokens: TokenUsage | None = Field(description="Token usage.")
    cost: Decimal | None = Field(description="Call cost.")
    model_params: dict[str, Any] | None = Field(description="Model parameters.")
    tool_name: str | None = Field(description="Name of the called tool.")
    cache_key: str | None = Field(
        description="Tool call cache key, computed by the server."
    )
    subagent_id: str | None = Field(description="Harness id of the subagent.")
    attributes: dict[str, Any] | None = Field(
        default=None,
        description="Provider or adapter attributes, null unless payloads "
        "are requested.",
    )
    metadata: dict[str, Any] = Field(description="User metadata.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
