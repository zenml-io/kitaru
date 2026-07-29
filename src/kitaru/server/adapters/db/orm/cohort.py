"""Cohort ORM tables."""

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
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.names import MAX_NAME_LENGTH

COHORT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("cohort", ["name"])
COHORT_OWNER_FOREIGN_KEY = foreign_key_name("cohort", ["owner_id"])
COHORT_AGENT_FOREIGN_KEY = foreign_key_name("cohort", ["agent_id"])
COHORT_SESSION_COHORT_FOREIGN_KEY = foreign_key_name("cohort_session", ["cohort_id"])
COHORT_SESSION_SESSION_FOREIGN_KEY = foreign_key_name("cohort_session", ["session_id"])
COHORT_SESSION_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_session", ["cohort_id", "index"]
)


class CohortORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Cohort table."""

    __tablename__ = "cohort"
    __table_args__ = (
        UniqueConstraint("name", name=COHORT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=COHORT_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(["agent_id"], ["agent.id"], name=COHORT_AGENT_FOREIGN_KEY),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None]
    agent_id: Mapped[uuid.UUID]
    session_count: Mapped[int]

    @classmethod
    def from_domain(cls, cohort: Cohort) -> "CohortORM":
        """Build a row from a cohort."""
        return cls(
            id=cohort.id,
            owner_id=cohort.owner_id,
            name=cohort.name,
            description=cohort.description,
            agent_id=cohort.agent_id,
            session_count=cohort.session_count,
        )

    def to_domain(self) -> Cohort:
        """Build a cohort from this row."""
        return Cohort(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            agent_id=self.agent_id,
            session_count=self.session_count,
            created=self.created,
            updated=self.updated,
        )


class CohortSessionORM(TimestampMixin, Base):
    """Ordered cohort membership table."""

    __tablename__ = "cohort_session"
    __table_args__ = (
        UniqueConstraint(
            "cohort_id", "index", name=COHORT_SESSION_INDEX_UNIQUE_CONSTRAINT
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name=COHORT_SESSION_COHORT_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=COHORT_SESSION_SESSION_FOREIGN_KEY,
        ),
    )

    cohort_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    index: Mapped[int]
