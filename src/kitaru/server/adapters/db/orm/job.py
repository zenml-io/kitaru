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
"""Job ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.job import Job

KIND_LENGTH = 32
STATUS_LENGTH = 32

JOB_OWNER_ID_FOREIGN_KEY = foreign_key_name("job", ["owner_id"])
JOB_KIND_INDEX = index_name("job", ["kind"])
JOB_STATUS_INDEX = index_name("job", ["status"])


class JobORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Job table."""

    __tablename__ = "job"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=JOB_OWNER_ID_FOREIGN_KEY
        ),
        Index(JOB_KIND_INDEX, "kind"),
        Index(JOB_STATUS_INDEX, "status"),
    )

    owner_id: Mapped[uuid.UUID]
    kind: Mapped[str] = mapped_column(String(KIND_LENGTH))
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)

    @classmethod
    def from_domain(cls, job: Job) -> "JobORM":
        """Build a row from a domain job.

        Args:
            job: Job to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=job.id,
            owner_id=job.owner_id,
            kind=job.kind.value,
            status=job.status.value,
            cancel_requested_at=job.cancel_requested_at,
            started_at=job.started_at,
            ended_at=job.ended_at,
            error=job.error,
        )

    def apply(self, job: Job) -> None:
        """Copy a domain job's mutable fields onto this row.

        Args:
            job: Job with modified fields.
        """
        self.status = job.status.value
        self.cancel_requested_at = job.cancel_requested_at
        self.started_at = job.started_at
        self.ended_at = job.ended_at
        self.error = job.error

    def to_domain(self) -> Job:
        """Build a domain job from this row.

        Returns:
            Job with timestamps set.
        """
        return Job(
            id=self.id,
            owner_id=self.owner_id,
            kind=JobKind(self.kind),
            status=JobStatus(self.status),
            cancel_requested_at=self.cancel_requested_at,
            started_at=self.started_at,
            ended_at=self.ended_at,
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
