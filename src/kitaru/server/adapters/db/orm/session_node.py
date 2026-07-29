"""Session-node ORM table."""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

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
from kitaru.server.domain.session import TokenUsage
from kitaru.server.domain.session_node import NodeStatus, NodeType, SessionNode

SESSION_NODE_SESSION_FOREIGN_KEY = foreign_key_name("session_node", ["session_id"])
SESSION_NODE_PARENT_FOREIGN_KEY = foreign_key_name("session_node", ["parent_id"])
SESSION_NODE_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "index"]
)
SESSION_NODE_EXTERNAL_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session_node", ["session_id", "external_id"]
)
SESSION_NODE_CACHE_KEY_INDEX = index_name("session_node", ["cache_key"])


class SessionNodeORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session execution node table."""

    __tablename__ = "session_node"
    __table_args__ = (
        UniqueConstraint(
            "session_id", "index", name=SESSION_NODE_INDEX_UNIQUE_CONSTRAINT
        ),
        UniqueConstraint(
            "session_id",
            "external_id",
            name=SESSION_NODE_EXTERNAL_UNIQUE_CONSTRAINT,
        ),
        Index(
            SESSION_NODE_CACHE_KEY_INDEX,
            "cache_key",
            postgresql_where=text("cache_key IS NOT NULL"),
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=SESSION_NODE_SESSION_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["parent_id"],
            ["session_node.id"],
            name=SESSION_NODE_PARENT_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    session_id: Mapped[uuid.UUID]
    parent_id: Mapped[uuid.UUID | None]
    secondary_parent_ids: Mapped[list[str]] = mapped_column(JSONB)
    index: Mapped[int]
    external_id: Mapped[str | None] = mapped_column(String(255))
    trace_id: Mapped[str | None] = mapped_column(String(255))
    node_type: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32))
    error: Mapped[str | None]
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    inputs: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    outputs: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    requested_model: Mapped[str | None] = mapped_column(String(255))
    model: Mapped[str | None] = mapped_column(String(255))
    provider: Mapped[str | None] = mapped_column(String(255))
    input_tokens: Mapped[int | None]
    output_tokens: Mapped[int | None]
    cached_input_tokens: Mapped[int | None]
    reasoning_tokens: Mapped[int | None]
    cost: Mapped[Decimal | None] = mapped_column(Numeric)
    model_params: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True))
    tool_name: Mapped[str | None] = mapped_column(String(255))
    cache_key: Mapped[str | None] = mapped_column(String(64))
    subagent_id: Mapped[str | None] = mapped_column(String(255))
    attributes: Mapped[dict] = mapped_column(JSONB)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, node: SessionNode) -> "SessionNodeORM":
        """Build a row from a session node."""
        tokens = node.tokens
        return cls(
            id=node.id,
            session_id=node.session_id,
            parent_id=node.parent_id,
            secondary_parent_ids=[str(value) for value in node.secondary_parent_ids],
            index=node.index,
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
        """Build a session node from this row."""
        token_values = (
            self.input_tokens,
            self.output_tokens,
            self.cached_input_tokens,
            self.reasoning_tokens,
        )
        tokens = None
        if any(value is not None for value in token_values):
            tokens = TokenUsage(
                input_tokens=self.input_tokens,
                output_tokens=self.output_tokens,
                cached_input_tokens=self.cached_input_tokens,
                reasoning_tokens=self.reasoning_tokens,
            )
        return SessionNode(
            id=self.id,
            session_id=self.session_id,
            parent_id=self.parent_id,
            secondary_parent_ids=[
                uuid.UUID(value) for value in self.secondary_parent_ids
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
            attributes=self.attributes if include_payloads else {},
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
