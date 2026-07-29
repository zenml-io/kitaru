"""Replay ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, String, UniqueConstraint
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
from kitaru.server.domain.replay import Replay, ReplayStatus

REPLAY_JOB_UNIQUE_CONSTRAINT = unique_constraint_name("replay", ["job_id"])
REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT = unique_constraint_name(
    "replay", ["experiment_run_id", "baseline_session_id"]
)
REPLAY_RUN_STATUS_INDEX = index_name("replay", ["experiment_run_id", "status"])
REPLAY_BASELINE_INDEX = index_name("replay", ["baseline_session_id"])
REPLAY_OWNER_FOREIGN_KEY = foreign_key_name("replay", ["owner_id"])
REPLAY_JOB_FOREIGN_KEY = foreign_key_name("replay", ["job_id"])
REPLAY_RUN_FOREIGN_KEY = foreign_key_name("replay", ["experiment_run_id"])
REPLAY_CONFIG_FOREIGN_KEY = foreign_key_name("replay", ["replay_config_id"])
REPLAY_BASELINE_FOREIGN_KEY = foreign_key_name("replay", ["baseline_session_id"])


class ReplayORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Replay pipeline table."""

    __tablename__ = "replay"
    __table_args__ = (
        UniqueConstraint("job_id", name=REPLAY_JOB_UNIQUE_CONSTRAINT),
        UniqueConstraint(
            "experiment_run_id",
            "baseline_session_id",
            name=REPLAY_RUN_BASELINE_UNIQUE_CONSTRAINT,
        ),
        Index(REPLAY_RUN_STATUS_INDEX, "experiment_run_id", "status"),
        Index(REPLAY_BASELINE_INDEX, "baseline_session_id"),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=REPLAY_OWNER_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name=REPLAY_JOB_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["experiment_run_id"],
            ["experiment_run.id"],
            name=REPLAY_RUN_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["replay_config_id"],
            ["replay_config.id"],
            name=REPLAY_CONFIG_FOREIGN_KEY,
        ),
        ForeignKeyConstraint(
            ["baseline_session_id"],
            ["session.id"],
            name=REPLAY_BASELINE_FOREIGN_KEY,
        ),
    )

    owner_id: Mapped[uuid.UUID]
    job_id: Mapped[uuid.UUID]
    experiment_run_id: Mapped[uuid.UUID | None]
    replay_config_id: Mapped[uuid.UUID]
    baseline_session_id: Mapped[uuid.UUID]
    evaluate_baselines: Mapped[bool]
    status: Mapped[str] = mapped_column(String(32))
    error: Mapped[str | None]

    @classmethod
    def from_domain(cls, replay: Replay) -> "ReplayORM":
        """Build a row from a replay."""
        return cls(
            id=replay.id,
            owner_id=replay.owner_id,
            job_id=replay.job_id,
            experiment_run_id=replay.experiment_run_id,
            replay_config_id=replay.replay_config_id,
            baseline_session_id=replay.baseline_session_id,
            evaluate_baselines=replay.evaluate_baselines,
            status=replay.status.value,
            error=replay.error,
        )

    def to_domain(self) -> Replay:
        """Build a replay from this row."""
        return Replay(
            id=self.id,
            owner_id=self.owner_id,
            job_id=self.job_id,
            experiment_run_id=self.experiment_run_id,
            replay_config_id=self.replay_config_id,
            baseline_session_id=self.baseline_session_id,
            evaluate_baselines=self.evaluate_baselines,
            status=ReplayStatus(self.status),
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
