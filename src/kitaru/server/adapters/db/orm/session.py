"""Session ORM table."""

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
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)

SESSION_OWNER_FOREIGN_KEY = foreign_key_name("session", ["owner_id"])
SESSION_AGENT_FOREIGN_KEY = foreign_key_name("session", ["agent_id"])
SESSION_AGENT_VERSION_FOREIGN_KEY = foreign_key_name("session", ["agent_version_id"])
SESSION_TASK_FOREIGN_KEY = foreign_key_name("session", ["task_id"])
SESSION_EXTERNAL_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session", ["provider", "external_id"]
)
SESSION_AGENT_STARTED_INDEX = index_name("session", ["agent_id", "started_at"])
SESSION_STATUS_INDEX = index_name("session", ["status"])
SESSION_TASK_INDEX = index_name("session", ["task_id"])


class SessionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Agent session table."""

    __tablename__ = "session"
    __table_args__ = (
        UniqueConstraint(
            "provider", "external_id", name=SESSION_EXTERNAL_UNIQUE_CONSTRAINT
        ),
        Index(SESSION_AGENT_STARTED_INDEX, "agent_id", "started_at"),
        Index(SESSION_STATUS_INDEX, "status"),
        Index(SESSION_TASK_INDEX, "task_id"),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=SESSION_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=SESSION_AGENT_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=SESSION_AGENT_VERSION_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["task_id"],
            ["task.id"],
            name=SESSION_TASK_FOREIGN_KEY,
            ondelete="SET NULL",
            use_alter=True,
        ),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    agent_version_id: Mapped[uuid.UUID | None]
    task_id: Mapped[uuid.UUID | None]
    origin: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    name: Mapped[str | None] = mapped_column(String(255))
    inputs: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    outputs: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    expected: Mapped[object | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[str | None]
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    external_id: Mapped[str | None] = mapped_column(String(255))
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)
    provider: Mapped[str | None] = mapped_column(String(255))
    framework: Mapped[str | None] = mapped_column(String(255))
    adapter_version: Mapped[str | None] = mapped_column(String(255))
    cost: Mapped[Decimal | None] = mapped_column(Numeric)
    input_tokens: Mapped[int | None]
    output_tokens: Mapped[int | None]
    cached_input_tokens: Mapped[int | None]
    reasoning_tokens: Mapped[int | None]
    llm_call_count: Mapped[int]
    tool_call_count: Mapped[int]

    @classmethod
    def from_domain(cls, session: Session) -> "SessionORM":
        """Build a row from a session."""
        tokens = session.tokens
        return cls(
            id=session.id,
            owner_id=session.owner_id,
            agent_id=session.agent_id,
            agent_version_id=session.agent_version_id,
            task_id=session.task_id,
            origin=session.origin.value,
            status=session.status.value,
            name=session.name,
            inputs=session.inputs,
            outputs=session.outputs,
            expected=session.expected,
            error=session.error,
            started_at=session.started_at,
            ended_at=session.ended_at,
            external_id=session.external_id,
            metadata_=session.metadata,
            provider=session.provider,
            framework=session.framework,
            adapter_version=session.adapter_version,
            cost=session.cost,
            input_tokens=tokens.input_tokens if tokens else None,
            output_tokens=tokens.output_tokens if tokens else None,
            cached_input_tokens=tokens.cached_input_tokens if tokens else None,
            reasoning_tokens=tokens.reasoning_tokens if tokens else None,
            llm_call_count=session.llm_call_count,
            tool_call_count=session.tool_call_count,
        )

    def to_domain(self) -> Session:
        """Build a session from this row."""
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
        return Session(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            agent_version_id=self.agent_version_id,
            task_id=self.task_id,
            origin=SessionOrigin(self.origin),
            status=SessionStatus(self.status),
            name=self.name,
            inputs=self.inputs,
            outputs=self.outputs,
            expected=self.expected,
            error=self.error,
            started_at=self.started_at,
            ended_at=self.ended_at,
            external_id=self.external_id,
            metadata=self.metadata_,
            provider=self.provider,
            framework=self.framework,
            adapter_version=self.adapter_version,
            cost=self.cost,
            tokens=tokens,
            llm_call_count=self.llm_call_count,
            tool_call_count=self.tool_call_count,
            created=self.created,
            updated=self.updated,
        )

    def copy_from_domain(self, session: Session) -> None:
        """Copy mutable fields from a session."""
        source = self.from_domain(session)
        for column in (
            "status",
            "name",
            "outputs",
            "expected",
            "error",
            "ended_at",
            "metadata_",
            "task_id",
            "cost",
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "reasoning_tokens",
            "llm_call_count",
            "tool_call_count",
        ):
            setattr(self, column, getattr(source, column))
