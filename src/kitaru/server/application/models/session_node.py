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
"""Session node filter and command models."""

import uuid
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Literal

from pydantic import Field

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import NodeLink, NodeStatus, NodeType
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.filtering import EQUALITY_OPS, FilterField


class SessionNodeFilter(ListFilter):
    """Session node list filter.

    Ordered by position ascending rather than the created-descending
    default, so a client reads a session's nodes in the order they ran.
    """

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"position"})
    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "node_type": FilterField(value_type=NodeType, ops=EQUALITY_OPS),
    }

    session_id: uuid.UUID
    include_payloads: bool = False
    sort: Literal["position:asc"] = "position:asc"


class SessionNodeUpsert(FrozenModel):
    """Session node upsert command."""

    external_id: str
    parent_external_id: str | None = None
    links: list[NodeLink] = Field(default_factory=list)
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
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    model_provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: str | None = None
    attributes: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)
