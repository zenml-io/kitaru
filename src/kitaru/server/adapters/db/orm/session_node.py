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

from sqlalchemy import (
    CHAR,
    BigInteger,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.session_node import SessionNode

SESSION_NODE_SESSION_ID_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "index"]
)
SESSION_NODE_SESSION_ID_EXTERNAL_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "external_id"]
)
SESSION_NODE_SESSION_ID_FOREIGN_KEY = foreign_key_name("session_node", ["session_id"])
SESSION_NODE_CACHE_KEY_INDEX = index_name("session_node", ["cache_key"])

NODE_TYPE_LENGTH = 32
NODE_STATUS_LENGTH = 32
CACHE_KEY_LENGTH = 64


class SessionNodeORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session node table."""

    __tablename__ = "session_node"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "index",
            name=SESSION_NODE_SESSION_ID_INDEX_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "session_id",
            "external_id",
            name=SESSION_NODE_SESSION_ID_EXTERNAL_ID_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=SESSION_NODE_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(
            SESSION_NODE_CACHE_KEY_INDEX,
            "cache_key",
            postgresql_where=text("cache_key IS NOT NULL"),
        ),
    )

    session_id: Mapped[uuid.UUID]
    # Validated at ingestion to reference a node of the same session, which
    # a foreign key cannot express.
    parent_id: Mapped[uuid.UUID | None]
    secondary_parent_ids: Mapped[list[str]] = mapped_column(JSONB)
    index: Mapped[int]
    external_id: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str | None] = mapped_column(Text)
    node_type: Mapped[str] = mapped_column(String(NODE_TYPE_LENGTH))
    name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(NODE_STATUS_LENGTH))
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    inputs: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    outputs: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    requested_model: Mapped[str | None] = mapped_column(Text)
    model: Mapped[str | None] = mapped_column(Text)
    provider: Mapped[str | None] = mapped_column(Text)
    input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cached_input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    reasoning_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cost: Mapped[Decimal | None] = mapped_column(Numeric)
    model_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    tool_name: Mapped[str | None] = mapped_column(Text)
    cache_key: Mapped[str | None] = mapped_column(CHAR(CACHE_KEY_LENGTH))
    subagent_id: Mapped[str | None] = mapped_column(String(255))
    attributes: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, node: SessionNode) -> "SessionNodeORM":
        """Build a row from a domain session node.

        The token usage is flattened into the token columns, all null when
        the node carries no token usage.

        Args:
            node: Session node to store.

        Returns:
            Row without timestamps set.
        """
        row = cls(id=node.id)
        row.apply_domain(node)
        return row

    def apply_domain(self, node: SessionNode) -> None:
        """Copy every mutable field of a domain node onto this row.

        Used both to populate a freshly inserted row and to replace an
        existing row whole on an upsert.

        Args:
            node: Session node carrying the desired field values.
        """
        tokens = node.tokens
        self.session_id = node.session_id
        self.parent_id = node.parent_id
        self.secondary_parent_ids = [
            str(parent_id) for parent_id in node.secondary_parent_ids
        ]
        self.index = node.index
        self.external_id = node.external_id
        self.trace_id = node.trace_id
        self.node_type = node.node_type.value
        self.name = node.name
        self.status = node.status.value
        self.error = node.error
        self.started_at = node.started_at
        self.ended_at = node.ended_at
        self.inputs = node.inputs
        self.outputs = node.outputs
        self.requested_model = node.requested_model
        self.model = node.model
        self.provider = node.provider
        self.input_tokens = tokens.input_tokens if tokens is not None else None
        self.output_tokens = tokens.output_tokens if tokens is not None else None
        self.cached_input_tokens = (
            tokens.cached_input_tokens if tokens is not None else None
        )
        self.reasoning_tokens = tokens.reasoning_tokens if tokens is not None else None
        self.cost = node.cost
        self.model_params = node.model_params
        self.tool_name = node.tool_name
        self.cache_key = node.cache_key
        self.subagent_id = node.subagent_id
        self.attributes = node.attributes
        self.metadata_ = node.metadata

    def to_domain(self, include_payloads: bool) -> SessionNode:
        """Build a domain session node from this row.

        Args:
            include_payloads: Whether to read the inputs, outputs, and
                attributes columns. When ``False``, those columns are never
                touched, so a deferred load never fires.

        Returns:
            Session node with timestamps set.
        """
        has_tokens = any(
            value is not None
            for value in (
                self.input_tokens,
                self.output_tokens,
                self.cached_input_tokens,
                self.reasoning_tokens,
            )
        )
        tokens = (
            TokenUsage(
                input_tokens=self.input_tokens,
                output_tokens=self.output_tokens,
                cached_input_tokens=self.cached_input_tokens,
                reasoning_tokens=self.reasoning_tokens,
            )
            if has_tokens
            else None
        )
        return SessionNode(
            id=self.id,
            session_id=self.session_id,
            parent_id=self.parent_id,
            secondary_parent_ids=[
                uuid.UUID(parent_id) for parent_id in self.secondary_parent_ids
            ],
            index=self.index,
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
            tokens=tokens,
            cost=self.cost,
            model_params=self.model_params,
            tool_name=self.tool_name,
            cache_key=self.cache_key,
            subagent_id=self.subagent_id,
            attributes=self.attributes if include_payloads else None,
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
