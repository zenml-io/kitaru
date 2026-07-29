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
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import Field

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class SessionNodeFilter(ListFilter):
    """Session node list filter.

    Ordered by index ascending rather than the created-descending default,
    since a node's wire identity and its position in its session are the
    same thing.
    """

    sortable_fields: ClassVar[frozenset[str]] = frozenset({"index"})

    session_id: uuid.UUID
    include_payloads: bool = False
    sort: str = "index:asc"


class SessionNodeUpsert(FrozenModel):
    """Session node upsert command."""

    index: int
    parent_index: int | None = None
    secondary_parent_indexes: list[int] = Field(default_factory=list)
    external_id: str | None = None
    trace_id: str | None = None
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: uuid.UUID | None = None
    attributes: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)
