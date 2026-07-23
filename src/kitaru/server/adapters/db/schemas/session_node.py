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
"""Session node ORM table."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import ConfigDict
from sqlalchemy import (
    CHAR,
    BigInteger,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.session import TokenUsage
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)

SESSION_NODE_KEY_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "key"]
)
SESSION_NODE_SEQUENCE_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "sequence"]
)
SESSION_NODE_EXTERNAL_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "external_id"]
)
SESSION_NODE_SESSION_ID_FOREIGN_KEY = foreign_key_name("session_node", ["session_id"])
SESSION_NODE_PARENT_ID_FOREIGN_KEY = foreign_key_name("session_node", ["parent_id"])
SESSION_NODE_CACHE_KEY_INDEX = index_name("session_node", ["cache_key"])

MAX_KEY_LENGTH = 512
MAX_STATUS_LENGTH = 16
MAX_PROVIDER_LENGTH = 64
CACHE_KEY_LENGTH = 64


class SessionNodeSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Session node table."""

    model_config = ConfigDict(protected_namespaces=())  # ty: ignore[invalid-assignment]

    __tablename__ = "session_node"
    __table_args__ = (
        UniqueConstraint("session_id", "key", name=SESSION_NODE_KEY_UNIQUE_CONSTRAINT),
        UniqueConstraint(
            "session_id", "sequence", name=SESSION_NODE_SEQUENCE_UNIQUE_CONSTRAINT
        ),
        UniqueConstraint(
            "session_id",
            "external_id",
            name=SESSION_NODE_EXTERNAL_ID_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=SESSION_NODE_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["parent_id"],
            ["session_node.id"],
            name=SESSION_NODE_PARENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(SESSION_NODE_CACHE_KEY_INDEX, "cache_key"),
    )

    session_id: uuid.UUID = Field(nullable=False)
    key: str = Field(max_length=MAX_KEY_LENGTH, nullable=False)
    parent_id: uuid.UUID | None = Field(default=None)
    secondary_parent_ids: list[str] = Field(
        default_factory=list, sa_type=JSONB, nullable=False
    )
    sequence: int = Field(nullable=False)
    external_id: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    trace_id: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    node_type: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    error: str | None = Field(default=None, sa_type=Text)
    started_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    ended_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    inputs: Any = Field(default=None, sa_type=JSONB, nullable=True)
    outputs: Any = Field(default=None, sa_type=JSONB, nullable=True)
    requested_model: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    model: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    provider: str | None = Field(default=None, max_length=MAX_PROVIDER_LENGTH)
    input_tokens: int | None = Field(default=None, sa_type=BigInteger)
    output_tokens: int | None = Field(default=None, sa_type=BigInteger)
    cached_input_tokens: int | None = Field(default=None, sa_type=BigInteger)
    reasoning_tokens: int | None = Field(default=None, sa_type=BigInteger)
    cost: Decimal | None = Field(
        default=None,
        sa_type=Numeric(12, 6),  # ty: ignore[invalid-argument-type]
    )
    model_params: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    tool_name: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    cache_key: str | None = Field(
        default=None,
        sa_type=CHAR(CACHE_KEY_LENGTH),  # ty: ignore[invalid-argument-type]
    )
    subagent_id: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    attributes: dict[str, Any] = Field(
        default_factory=dict, sa_type=JSONB, nullable=False
    )
    metadata_: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("metadata", JSONB, nullable=False),
    )

    @classmethod
    def from_domain(cls, node: SessionNode) -> "SessionNodeSchema":
        """Build a row from a domain session node.

        Args:
            node: Session node to store.

        Returns:
            Row without timestamps set.
        """
        tokens = node.tokens
        return cls(
            id=node.id,
            session_id=node.session_id,
            key=node.key,
            parent_id=node.parent_id,
            secondary_parent_ids=[
                str(parent_id) for parent_id in node.secondary_parent_ids
            ],
            sequence=node.sequence,
            external_id=node.external_id,
            trace_id=node.trace_id,
            node_type=node.node_type.value,
            name=node.name,
            status=node.status.value,
            error=node.error,
            started_at=node.started_at,
            ended_at=node.ended_at,
            inputs=node.inputs,
            outputs=node.outputs,
            requested_model=node.requested_model,
            model=node.model,
            provider=node.provider,
            input_tokens=tokens.input_tokens if tokens else None,
            output_tokens=tokens.output_tokens if tokens else None,
            cached_input_tokens=tokens.cached_input_tokens if tokens else None,
            reasoning_tokens=tokens.reasoning_tokens if tokens else None,
            cost=node.cost,
            model_params=node.model_params,
            tool_name=node.tool_name,
            cache_key=node.cache_key,
            subagent_id=node.subagent_id,
            attributes=node.attributes,
            metadata_=node.metadata,
        )

    def to_domain(self, include_payloads: bool = True) -> SessionNode:
        """Build a domain session node from this row.

        Args:
            include_payloads: Whether to read inputs, outputs, and
                attributes.

        Returns:
            Session node with timestamps set.
        """
        return SessionNode(
            id=self.id,
            session_id=self.session_id,
            key=self.key,
            parent_id=self.parent_id,
            secondary_parent_ids=[
                uuid.UUID(parent_id) for parent_id in self.secondary_parent_ids
            ],
            sequence=self.sequence,
            external_id=self.external_id,
            trace_id=self.trace_id,
            node_type=NodeType(self.node_type),
            name=self.name,
            status=NodeStatus(self.status),
            error=self.error,
            started_at=self.started_at,
            ended_at=self.ended_at,
            inputs=self.inputs if include_payloads else None,
            outputs=self.outputs if include_payloads else None,
            requested_model=self.requested_model,
            model=self.model,
            provider=self.provider,
            tokens=TokenUsage.from_counts(
                self.input_tokens,
                self.output_tokens,
                self.cached_input_tokens,
                self.reasoning_tokens,
            ),
            cost=self.cost,
            model_params=self.model_params,
            tool_name=self.tool_name,
            cache_key=self.cache_key,
            subagent_id=self.subagent_id,
            attributes=self.attributes if include_payloads else {},
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
