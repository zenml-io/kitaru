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
"""Session node command models."""

import uuid
from decimal import Decimal
from typing import Any

from pydantic import AwareDatetime, ConfigDict, Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.session import TokenUsage
from kitaru.server.domain.session_node import NodeStatus, NodeType


class SessionNodeUpsert(FrozenModel):
    """Session node upsert command."""

    model_config = ConfigDict(protected_namespaces=())

    id: uuid.UUID
    parent_id: uuid.UUID | None = None
    secondary_parent_ids: list[uuid.UUID] = Field(default_factory=list)
    sequence: int = Field(ge=0)
    external_id: str | None = None
    trace_id: str | None = None
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
