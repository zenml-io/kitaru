"""Job ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.job import Job, JobStatus

JOB_OWNER_FOREIGN_KEY = foreign_key_name("job", ["owner_id"])
JOB_STATUS_INDEX = index_name("job", ["status"])


class JobORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Generic job table."""

    __tablename__ = "job"
    __table_args__ = (
        Index(JOB_STATUS_INDEX, "status"),
        ForeignKeyConstraint(["owner_id"], ["account.id"], name=JOB_OWNER_FOREIGN_KEY),
    )

    owner_id: Mapped[uuid.UUID]
    status: Mapped[str] = mapped_column(String(32))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None]

    @classmethod
    def from_domain(cls, job: Job) -> "JobORM":
        """Build a row from a job."""
        return cls(
            id=job.id,
            owner_id=job.owner_id,
            status=job.status.value,
            cancel_requested_at=job.cancel_requested_at,
            started_at=job.started_at,
            ended_at=job.ended_at,
            error=job.error,
        )

    def to_domain(self) -> Job:
        """Build a job from this row."""
        return Job(
            id=self.id,
            owner_id=self.owner_id,
            status=JobStatus(self.status),
            cancel_requested_at=self.cancel_requested_at,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
