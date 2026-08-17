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
"""Session ORM table."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus, TokenUsage
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
from kitaru.server.domain.session import Session

SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session", ["imported_from", "external_id"]
)
SESSION_AGENT_ID_NUMBER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session", ["agent_id", "number"]
)
SESSION_AGENT_ID_FOREIGN_KEY = foreign_key_name("session", ["agent_id"])
SESSION_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("session", ["agent_version_id"])
SESSION_OWNER_ID_FOREIGN_KEY = foreign_key_name("session", ["owner_id"])
SESSION_TASK_ID_FOREIGN_KEY = foreign_key_name("session", ["task_id"])
SESSION_AGENT_ID_ID_INDEX = index_name("session", ["agent_id", "id"])
SESSION_AGENT_VERSION_ID_ID_INDEX = index_name("session", ["agent_version_id", "id"])
SESSION_STATUS_INDEX = index_name("session", ["status"])
SESSION_TASK_ID_INDEX = index_name("session", ["task_id"])
SESSION_OWNER_ID_INDEX = index_name("session", ["owner_id"])

STATUS_LENGTH = 32
ORIGIN_LENGTH = 32


class SessionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session table."""

    __tablename__ = "session"
    __table_args__ = (
        UniqueConstraint(
            "imported_from",
            "external_id",
            name=SESSION_IMPORTED_FROM_EXTERNAL_ID_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "agent_id",
            "number",
            name=SESSION_AGENT_ID_NUMBER_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=SESSION_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=SESSION_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=SESSION_OWNER_ID_FOREIGN_KEY
        ),
        # Session and task reference each other, so this side of the cycle is
        # created as a separate ALTER statement.
        ForeignKeyConstraint(
            ["task_id"],
            ["task.id"],
            name=SESSION_TASK_ID_FOREIGN_KEY,
            ondelete="SET NULL",
            use_alter=True,
        ),
        Index(SESSION_AGENT_ID_ID_INDEX, "agent_id", "id"),
        Index(SESSION_AGENT_VERSION_ID_ID_INDEX, "agent_version_id", "id"),
        Index(SESSION_STATUS_INDEX, "status"),
        Index(SESSION_TASK_ID_INDEX, "task_id"),
        Index(SESSION_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    number: Mapped[int]
    agent_version_id: Mapped[uuid.UUID | None]
    task_id: Mapped[uuid.UUID | None]
    origin: Mapped[str] = mapped_column(String(ORIGIN_LENGTH))
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    name: Mapped[str | None] = mapped_column(Text)
    inputs: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    outputs: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    external_id: Mapped[str | None] = mapped_column(Text)
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    imported_from: Mapped[str | None] = mapped_column(Text)
    framework: Mapped[str | None] = mapped_column(Text)
    adapter_version: Mapped[str | None] = mapped_column(Text)
    cost: Mapped[Decimal | None] = mapped_column(Numeric)
    input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cached_input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    reasoning_tokens: Mapped[int | None] = mapped_column(BigInteger)
    llm_call_count: Mapped[int]
    tool_call_count: Mapped[int]

    @classmethod
    def from_domain(cls, session: Session) -> "SessionORM":
        """Build a row from a domain session.

        The token usage is flattened into the token columns, all null when
        the session carries no token usage.

        Args:
            session: Session to store.

        Returns:
            Row without timestamps set.
        """
        tokens = session.tokens
        return cls(
            id=session.id,
            owner_id=session.owner_id,
            agent_id=session.agent_id,
            number=session.number,
            agent_version_id=session.agent_version_id,
            task_id=session.task_id,
            origin=session.origin.value,
            status=session.status.value,
            name=session.name,
            inputs=session.inputs,
            outputs=session.outputs,
            error=session.error,
            started_at=session.started_at,
            ended_at=session.ended_at,
            external_id=session.external_id,
            metadata_=session.metadata,
            imported_from=session.imported_from,
            framework=session.framework,
            adapter_version=session.adapter_version,
            cost=session.cost,
            input_tokens=tokens.input_tokens if tokens is not None else None,
            output_tokens=tokens.output_tokens if tokens is not None else None,
            cached_input_tokens=(
                tokens.cached_input_tokens if tokens is not None else None
            ),
            reasoning_tokens=tokens.reasoning_tokens if tokens is not None else None,
            llm_call_count=session.llm_call_count,
            tool_call_count=session.tool_call_count,
        )

    def to_domain(self) -> Session:
        """Build a domain session from this row.

        The token columns collapse back to ``None`` only when every one of
        them is null, matching a session that has never rolled up a node.

        Returns:
            Session with timestamps set.
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
        return Session(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            number=self.number,
            agent_version_id=self.agent_version_id,
            task_id=self.task_id,
            origin=SessionOrigin(self.origin),
            status=SessionStatus(self.status),
            name=self.name,
            inputs=self.inputs,
            outputs=self.outputs,
            error=self.error,
            started_at=self.started_at,
            ended_at=self.ended_at,
            external_id=self.external_id,
            metadata=self.metadata_,
            imported_from=self.imported_from,
            framework=self.framework,
            adapter_version=self.adapter_version,
            cost=self.cost,
            tokens=tokens,
            llm_call_count=self.llm_call_count,
            tool_call_count=self.tool_call_count,
            created=self.created,
            updated=self.updated,
        )
