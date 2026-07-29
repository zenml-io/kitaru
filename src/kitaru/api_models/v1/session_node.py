"""Session node API models."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import AwareDatetime, Field, model_validator

from kitaru.api_models.v1.base import JsonValue, ListParams, RequestModel, ResponseModel
from kitaru.api_models.v1.session import TokenUsage


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
    """Session node upsert request."""

    index: int = Field(ge=0, description="Session-local node index.")
    parent_index: int | None = Field(default=None, description="Parent node index.")
    secondary_parent_indexes: list[int] = Field(
        default_factory=list, description="Secondary parent indexes."
    )
    external_id: str | None = Field(default=None, description="External node id.")
    trace_id: str | None = Field(default=None, description="External trace id.")
    node_type: NodeType = Field(description="Node type.")
    name: str = Field(description="Node name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(default=None, description="Failure detail.")
    started_at: AwareDatetime | None = Field(default=None, description="Start time.")
    ended_at: AwareDatetime | None = Field(default=None, description="End time.")
    inputs: JsonValue | None = Field(default=None, description="Node inputs.")
    outputs: JsonValue | None = Field(default=None, description="Node outputs.")
    requested_model: str | None = Field(default=None, description="Requested model.")
    model: str | None = Field(default=None, description="Resolved model.")
    provider: str | None = Field(default=None, description="Model provider.")
    tokens: TokenUsage | None = Field(default=None, description="Token usage.")
    cost: Decimal | None = Field(default=None, description="Node cost.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Model parameters."
    )
    tool_name: str | None = Field(default=None, description="Tool name.")
    subagent_id: str | None = Field(default=None, description="Subagent id.")
    attributes: dict[str, JsonValue] = Field(
        default_factory=dict, description="Node attributes."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Node metadata."
    )


class SessionNodeBatchRequest(RequestModel):
    """Ordered node upsert batch."""

    nodes: list[SessionNodeCreateRequest] = Field(
        description="Nodes in parent-first order."
    )

    @model_validator(mode="after")
    def _parents_precede_children(self) -> "SessionNodeBatchRequest":
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


class SessionNodeListParams(ListParams):
    """Session node list params."""

    include_payloads: bool = Field(
        default=False, description="Whether to include payload fields."
    )


class SessionNodeResponse(ResponseModel):
    """Session node response."""

    id: uuid.UUID = Field(description="Node id.")
    session_id: uuid.UUID = Field(description="Session id.")
    index: int = Field(description="Session-local node index.")
    parent_index: int | None = Field(description="Parent node index.")
    parent_id: uuid.UUID | None = Field(description="Parent node id.")
    secondary_parent_indexes: list[int] = Field(description="Secondary parent indexes.")
    secondary_parent_ids: list[uuid.UUID] = Field(description="Secondary parent ids.")
    external_id: str | None = Field(description="External node id.")
    trace_id: str | None = Field(description="External trace id.")
    node_type: NodeType = Field(description="Node type.")
    name: str = Field(description="Node name.")
    status: NodeStatus = Field(description="Node status.")
    error: str | None = Field(description="Failure detail.")
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    inputs: JsonValue | None = Field(description="Node inputs.")
    outputs: JsonValue | None = Field(description="Node outputs.")
    requested_model: str | None = Field(description="Requested model.")
    model: str | None = Field(description="Resolved model.")
    provider: str | None = Field(description="Model provider.")
    tokens: TokenUsage | None = Field(description="Token usage.")
    cost: Decimal | None = Field(description="Node cost.")
    model_params: dict[str, JsonValue] | None = Field(description="Model parameters.")
    tool_name: str | None = Field(description="Tool name.")
    subagent_id: str | None = Field(description="Subagent id.")
    attributes: dict[str, JsonValue] | None = Field(description="Node attributes.")
    metadata: dict[str, JsonValue] = Field(description="Node metadata.")
    cache_key: str | None = Field(description="Tool-call cache key.")
