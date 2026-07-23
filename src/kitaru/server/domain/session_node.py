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
"""Session node entity, key helpers, and errors."""

import uuid
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import ConfigDict, Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.session import SessionRollups, TokenUsage


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


class UnknownParentNode(ValidationError):
    """Raised when a node references a parent that does not exist."""

    def __init__(self, parent_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            parent_id: Id of the missing parent node.
        """
        super().__init__(f"Parent node {parent_id} was not found")


class DuplicateSessionNodeId(ConflictError):
    """Raised when a node id is already registered in another session."""

    def __init__(self, node_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            node_id: Id that is already registered.
        """
        super().__init__(
            f"Session node {node_id} is already registered in another session"
        )


class DuplicateNodeSequence(ConflictError):
    """Raised when a node sequence is already registered in the session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(
            f"A node sequence is already registered in session {session_id}"
        )


class DuplicateNodeExternalId(ConflictError):
    """Raised when a node external id is already registered in the session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(
            f"A node external id is already registered in session {session_id}"
        )


class DuplicateNodeKey(ConflictError):
    """Raised when a node key is already registered in the session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"A node key is already registered in session {session_id}")


class SessionNode(DomainModel):
    """Session node."""

    model_config = ConfigDict(protected_namespaces=())

    id: uuid.UUID = Field(default_factory=uuid7)
    session_id: uuid.UUID
    key: str
    parent_id: uuid.UUID | None = None
    secondary_parent_ids: list[uuid.UUID] = Field(default_factory=list)
    sequence: int = Field(ge=0)
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


def build_node_key(
    parent_key: str | None, node_type: NodeType, name: str, occurrence: int
) -> str:
    """Build the stable path key of a node.

    The segment is ``<node_type>:<name>`` with the occurrence index appended
    as ``#N`` from the second occurrence on, joined to the parent key with
    ``/``.

    Args:
        parent_key: Key of the primary parent, ``None`` for roots.
        node_type: Node type.
        name: Node name.
        occurrence: One-based occurrence index among same-named siblings.

    Returns:
        Path key.
    """
    segment = f"{node_type}:{name}"
    if occurrence > 1:
        segment = f"{segment}#{occurrence}"
    if parent_key is None:
        return segment
    return f"{parent_key}/{segment}"


def _add(total: int | None, value: int | None) -> int | None:
    """Add an optional value to an optional total.

    Args:
        total: Running total.
        value: Value to add.

    Returns:
        New total, ``None`` when both are ``None``.
    """
    if value is None:
        return total
    if total is None:
        return value
    return total + value


def compute_rollups(nodes: Sequence[SessionNode]) -> SessionRollups:
    """Compute session rollups from its nodes.

    Args:
        nodes: Nodes of the session.

    Returns:
        Cost, token usage, and call count rollups.
    """
    cost: Decimal | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None
    llm_call_count = 0
    tool_call_count = 0
    for node in nodes:
        if node.node_type is NodeType.LLM_CALL:
            llm_call_count += 1
        elif node.node_type is NodeType.TOOL_CALL:
            tool_call_count += 1
        if node.cost is not None:
            cost = node.cost if cost is None else cost + node.cost
        if node.tokens is not None:
            input_tokens = _add(input_tokens, node.tokens.input_tokens)
            output_tokens = _add(output_tokens, node.tokens.output_tokens)
            cached_input_tokens = _add(
                cached_input_tokens, node.tokens.cached_input_tokens
            )
            reasoning_tokens = _add(reasoning_tokens, node.tokens.reasoning_tokens)
    return SessionRollups(
        cost=cost,
        tokens=TokenUsage.from_counts(
            input_tokens, output_tokens, cached_input_tokens, reasoning_tokens
        ),
        llm_call_count=llm_call_count,
        tool_call_count=tool_call_count,
    )
