"""Experiment-run ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, String, UniqueConstraint
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
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)

EXPERIMENT_RUN_OWNER_FOREIGN_KEY = foreign_key_name("experiment_run", ["owner_id"])
EXPERIMENT_RUN_EXPERIMENT_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["experiment_id"]
)
EXPERIMENT_RUN_COHORT_FOREIGN_KEY = foreign_key_name("experiment_run", ["cohort_id"])
EXPERIMENT_RUN_AGENT_VERSION_FOREIGN_KEY = foreign_key_name(
    "experiment_run", ["agent_version_id"]
)
EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT = unique_constraint_name(
    "experiment_run", ["experiment_id", "number"]
)


class ExperimentRunORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Experiment-run table."""

    __tablename__ = "experiment_run"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id",
            "number",
            name=EXPERIMENT_RUN_NUMBER_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["owner_id"],
            ["account.id"],
            name=EXPERIMENT_RUN_OWNER_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["experiment_id"],
            ["experiment.id"],
            name=EXPERIMENT_RUN_EXPERIMENT_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name=EXPERIMENT_RUN_COHORT_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=EXPERIMENT_RUN_AGENT_VERSION_FOREIGN_KEY,
        ),
    )

    owner_id: Mapped[uuid.UUID]
    experiment_id: Mapped[uuid.UUID]
    number: Mapped[int]
    status: Mapped[str] = mapped_column(String(32))
    cohort_id: Mapped[uuid.UUID]
    agent_version_id: Mapped[uuid.UUID]
    evaluate_baselines: Mapped[bool]
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None]

    @classmethod
    def from_domain(cls, run: ExperimentRun) -> "ExperimentRunORM":
        """Build a row from an experiment run."""
        return cls(
            id=run.id,
            owner_id=run.owner_id,
            experiment_id=run.experiment_id,
            number=run.number,
            status=run.status.value,
            cohort_id=run.cohort_id,
            agent_version_id=run.agent_version_id,
            evaluate_baselines=run.evaluate_baselines,
            started_at=run.started_at,
            ended_at=run.ended_at,
            error=run.error,
        )

    def to_domain(self) -> ExperimentRun:
        """Build an experiment run from this row."""
        return ExperimentRun(
            id=self.id,
            owner_id=self.owner_id,
            experiment_id=self.experiment_id,
            number=self.number,
            status=ExperimentRunStatus(self.status),
            cohort_id=self.cohort_id,
            agent_version_id=self.agent_version_id,
            evaluate_baselines=self.evaluate_baselines,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
