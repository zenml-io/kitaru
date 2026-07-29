"""Session node entity."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import Field

from kitaru.server.domain.base import DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import TokenUsage


class NodeType(StrEnum):
    """Recorded node kind."""

    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"
    SUBAGENT_CALL = "subagent_call"
    SPAN = "span"


class NodeStatus(StrEnum):
    """Recorded node lifecycle status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class SessionNodeNotFound(NotFoundError):
    """Raised when a session node lookup does not resolve."""

    def __init__(self, node_id: uuid.UUID) -> None:
        super().__init__(f"Session node {node_id} was not found")


class SessionNode(DomainModel):
    """One node in a session execution tree."""

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
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    cache_key: str | None = None
    subagent_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created: datetime | None = None
    updated: datetime | None = None
