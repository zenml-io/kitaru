"""Agent ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, String, UniqueConstraint
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
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.names import MAX_NAME_LENGTH

AGENT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("agent", ["name"])
AGENT_OWNER_FOREIGN_KEY = foreign_key_name("agent", ["owner_id"])


class AgentORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Agent table."""

    __tablename__ = "agent"
    __table_args__ = (
        UniqueConstraint("name", name=AGENT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=AGENT_OWNER_FOREIGN_KEY
        ),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None]
    latest_version: Mapped[int]

    @classmethod
    def from_domain(cls, agent: Agent) -> "AgentORM":
        """Build a row from an agent."""
        return cls(
            id=agent.id,
            owner_id=agent.owner_id,
            name=agent.name,
            description=agent.description,
            latest_version=agent.latest_version,
        )

    def to_domain(self) -> Agent:
        """Build an agent from this row."""
        return Agent(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            latest_version=self.latest_version,
            created=self.created,
            updated=self.updated,
        )
