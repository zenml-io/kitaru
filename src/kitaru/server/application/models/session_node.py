"""Session-node upsert command."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.session import TokenUsage
from kitaru.server.domain.session_node import NodeStatus, NodeType


class SessionNodeUpsert(FrozenModel):
    """Index-addressed session-node replacement."""

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
    subagent_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
