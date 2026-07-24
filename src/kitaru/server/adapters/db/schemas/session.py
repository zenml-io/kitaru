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
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Numeric,
    Text,
    UniqueConstraint,
    text,
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
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)

SESSION_EXTERNAL_ID_UNIQUE_CONSTRAINT = unique_constraint_name(
    "session", ["provider", "external_id"]
)
SESSION_IMPORT_REVISION_UNIQUE_CONSTRAINT = "uq_session_import_revision"
SESSION_IMPORT_DIGEST_UNIQUE_CONSTRAINT = "uq_session_import_digest"
SESSION_AGENT_ID_FOREIGN_KEY = foreign_key_name("session", ["agent_id"])
SESSION_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("session", ["agent_version_id"])
SESSION_IMPORT_JOB_ID_FOREIGN_KEY = foreign_key_name("session", ["import_job_id"])
SESSION_SUPERSEDES_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "session", ["supersedes_session_id"]
)
SESSION_OWNER_ID_INDEX = index_name("session", ["owner_id"])
SESSION_AGENT_ID_CREATED_INDEX = index_name("session", ["agent_id", "created"])
SESSION_ORIGIN_INDEX = index_name("session", ["origin"])
SESSION_STATUS_INDEX = index_name("session", ["status"])
SESSION_EXTERNAL_ID_INDEX = index_name("session", ["external_id"])
SESSION_SOURCE_IDENTITY_INDEX = index_name(
    "session", ["provider", "source_instance", "external_id"]
)

MAX_STATUS_LENGTH = 16
MAX_PROVIDER_LENGTH = 255
MAX_FRAMEWORK_LENGTH = 64


class SessionSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Session table."""

    __tablename__ = "session"
    __table_args__ = (
        Index(
            SESSION_EXTERNAL_ID_UNIQUE_CONSTRAINT,
            "provider",
            "external_id",
            unique=True,
            postgresql_where=text("source_revision IS NULL"),
        ),
        UniqueConstraint(
            "owner_id",
            "provider",
            "source_instance",
            "external_id",
            "source_revision",
            name=SESSION_IMPORT_REVISION_UNIQUE_CONSTRAINT,
        ),
        UniqueConstraint(
            "owner_id",
            "provider",
            "source_instance",
            "external_id",
            "source_digest",
            name=SESSION_IMPORT_DIGEST_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=SESSION_AGENT_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=SESSION_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["import_job_id"],
            ["import_job.id"],
            name=SESSION_IMPORT_JOB_ID_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["supersedes_session_id"],
            ["session.id"],
            name=SESSION_SUPERSEDES_SESSION_ID_FOREIGN_KEY,
        ),
        Index(SESSION_OWNER_ID_INDEX, "owner_id"),
        Index(SESSION_AGENT_ID_CREATED_INDEX, "agent_id", "created"),
        Index(SESSION_ORIGIN_INDEX, "origin"),
        Index(SESSION_STATUS_INDEX, "status"),
        Index(SESSION_EXTERNAL_ID_INDEX, "external_id"),
        Index(
            SESSION_SOURCE_IDENTITY_INDEX,
            "provider",
            "source_instance",
            "external_id",
        ),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    agent_id: uuid.UUID = Field(nullable=False)
    agent_version_id: uuid.UUID | None = Field(default=None)
    origin: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    name: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    inputs: Any = Field(default=None, sa_type=JSONB, nullable=True)
    outputs: Any = Field(default=None, sa_type=JSONB, nullable=True)
    expected: Any = Field(default=None, sa_type=JSONB, nullable=True)
    error: str | None = Field(default=None, sa_type=Text)
    started_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    ended_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    external_id: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    metadata_: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("metadata", JSONB, nullable=False),
    )
    provider: str | None = Field(default=None, max_length=MAX_PROVIDER_LENGTH)
    source_instance: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)
    source_revision: int | None = Field(default=None)
    source_digest: str | None = Field(default=None, max_length=64)
    source_metadata: dict[str, Any] = Field(
        default_factory=dict, sa_type=JSONB, nullable=False
    )
    replay_readiness: dict[str, Any] | None = Field(default=None, sa_type=JSONB)
    normalization_warnings: list[str] = Field(
        default_factory=list, sa_type=JSONB, nullable=False
    )
    import_job_id: uuid.UUID | None = Field(default=None)
    supersedes_session_id: uuid.UUID | None = Field(default=None)
    framework: str | None = Field(default=None, max_length=MAX_FRAMEWORK_LENGTH)
    adapter_version: str | None = Field(default=None, max_length=MAX_FRAMEWORK_LENGTH)
    log_uri: str | None = Field(default=None, sa_type=Text)
    cost: Decimal | None = Field(
        default=None,
        sa_type=Numeric(12, 6),  # ty: ignore[invalid-argument-type]
    )
    input_tokens: int | None = Field(default=None, sa_type=BigInteger)
    output_tokens: int | None = Field(default=None, sa_type=BigInteger)
    cached_input_tokens: int | None = Field(default=None, sa_type=BigInteger)
    reasoning_tokens: int | None = Field(default=None, sa_type=BigInteger)
    scores: dict[str, float] = Field(
        default_factory=dict, sa_type=JSONB, nullable=False
    )
    llm_call_count: int = Field(default=0, nullable=False)
    tool_call_count: int = Field(default=0, nullable=False)

    @classmethod
    def from_domain(cls, session: Session) -> "SessionSchema":
        """Build a row from a domain session.

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
            agent_version_id=session.agent_version_id,
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
            source_instance=session.source_instance,
            source_revision=session.source_revision,
            source_digest=session.source_digest,
            source_metadata=session.source_metadata,
            replay_readiness=session.replay_readiness,
            normalization_warnings=session.normalization_warnings,
            import_job_id=session.import_job_id,
            supersedes_session_id=session.supersedes_session_id,
            framework=session.framework,
            adapter_version=session.adapter_version,
            log_uri=session.log_uri,
            cost=session.cost,
            input_tokens=tokens.input_tokens if tokens else None,
            output_tokens=tokens.output_tokens if tokens else None,
            cached_input_tokens=tokens.cached_input_tokens if tokens else None,
            reasoning_tokens=tokens.reasoning_tokens if tokens else None,
            scores=session.scores,
            llm_call_count=session.llm_call_count,
            tool_call_count=session.tool_call_count,
        )

    def to_domain(self) -> Session:
        """Build a domain session from this row.

        Returns:
            Session with timestamps set.
        """
        return Session(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            agent_version_id=self.agent_version_id,
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
            source_instance=self.source_instance,
            source_revision=self.source_revision,
            source_digest=self.source_digest,
            source_metadata=self.source_metadata,
            replay_readiness=self.replay_readiness,
            normalization_warnings=self.normalization_warnings,
            import_job_id=self.import_job_id,
            supersedes_session_id=self.supersedes_session_id,
            framework=self.framework,
            adapter_version=self.adapter_version,
            log_uri=self.log_uri,
            cost=self.cost,
            tokens=TokenUsage.from_counts(
                self.input_tokens,
                self.output_tokens,
                self.cached_input_tokens,
                self.reasoning_tokens,
            ),
            llm_call_count=self.llm_call_count,
            tool_call_count=self.tool_call_count,
            scores=self.scores,
            created=self.created,
            updated=self.updated,
        )
