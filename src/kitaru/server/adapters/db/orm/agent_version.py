"""Agent-version ORM tables."""

import uuid

from sqlalchemy import ForeignKeyConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)

AGENT_VERSION_PARENT_FOREIGN_KEY = foreign_key_name("agent_version", ["agent_id"])
AGENT_VERSION_OWNER_FOREIGN_KEY = foreign_key_name("agent_version", ["owner_id"])
AGENT_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version", ["agent_id", "version"]
)
AGENT_VERSION_SECRET_VERSION_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["agent_version_id"]
)
AGENT_VERSION_SECRET_SECRET_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["secret_id"]
)
AGENT_VERSION_SECRET_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version_secret", ["agent_version_id", "index"]
)


class AgentVersionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Agent-version table."""

    __tablename__ = "agent_version"
    __table_args__ = (
        UniqueConstraint("agent_id", "version", name=AGENT_VERSION_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=AGENT_VERSION_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=AGENT_VERSION_PARENT_FOREIGN_KEY
        ),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    version: Mapped[int]
    display_version: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None]
    run_command: Mapped[str | None]
    run_working_dir: Mapped[str | None]
    run_env: Mapped[dict[str, str] | None] = mapped_column(JSONB(none_as_null=True))
    run_timeout_seconds: Mapped[int | None]
    capabilities: Mapped[dict] = mapped_column(JSONB)

    @classmethod
    def from_domain(cls, version: AgentVersion) -> "AgentVersionORM":
        """Build a row from an agent version."""
        run = version.run_spec
        return cls(
            id=version.id,
            owner_id=version.owner_id,
            agent_id=version.agent_id,
            version=version.version,
            display_version=version.display_version,
            description=version.description,
            run_command=run.command if run else None,
            run_working_dir=run.working_dir if run else None,
            run_env=run.env if run else None,
            run_timeout_seconds=run.timeout_seconds if run else None,
            capabilities=version.capabilities.model_dump(mode="json"),
        )

    def to_domain(self, secret_ids: list[uuid.UUID] | None = None) -> AgentVersion:
        """Build an agent version from this row."""
        run_spec = None
        if self.run_command is not None:
            run_spec = RunSpec(
                command=self.run_command,
                working_dir=self.run_working_dir,
                env=self.run_env or {},
                secret_ids=secret_ids or [],
                timeout_seconds=self.run_timeout_seconds or 3600,
            )
        return AgentVersion(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            version=self.version,
            display_version=self.display_version,
            description=self.description,
            run_spec=run_spec,
            capabilities=AgentCapabilities.model_validate(self.capabilities),
            created=self.created,
            updated=self.updated,
        )


class AgentVersionSecretORM(TimestampMixin, Base):
    """Ordered agent-version secret link."""

    __tablename__ = "agent_version_secret"
    __table_args__ = (
        UniqueConstraint(
            "agent_version_id",
            "index",
            name=AGENT_VERSION_SECRET_INDEX_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=AGENT_VERSION_SECRET_VERSION_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["secret_id"],
            ["secret.id"],
            name=AGENT_VERSION_SECRET_SECRET_FOREIGN_KEY,
        ),
    )

    agent_version_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    secret_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    index: Mapped[int]
