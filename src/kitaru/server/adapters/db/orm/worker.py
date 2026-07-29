"""Worker ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.task import WorkerScope
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)
from kitaru.server.domain.worker import Worker, WorkerRuntime

WORKER_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("worker", ["name"])
WORKER_OWNER_FOREIGN_KEY = foreign_key_name("worker", ["owner_id"])


class WorkerORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Worker registration table."""

    __tablename__ = "worker"
    __table_args__ = (
        UniqueConstraint("name", name=WORKER_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=WORKER_OWNER_FOREIGN_KEY
        ),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(255))
    scope: Mapped[dict] = mapped_column(JSONB)
    runtime: Mapped[dict] = mapped_column(JSONB)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, worker: Worker) -> "WorkerORM":
        """Build a row from a worker."""
        return cls(
            id=worker.id,
            owner_id=worker.owner_id,
            name=worker.name,
            scope=worker.scope.model_dump(mode="json"),
            runtime=worker.runtime.model_dump(mode="json"),
            last_seen_at=worker.last_seen_at,
            metadata_=worker.metadata,
        )

    def to_domain(self) -> Worker:
        """Build a worker from this row."""
        return Worker(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            scope=WorkerScope.model_validate(self.scope),
            runtime=WorkerRuntime.model_validate(self.runtime),
            last_seen_at=self.last_seen_at,
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
