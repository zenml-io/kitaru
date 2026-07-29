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
"""Worker ORM table."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.task import WorkerScope
from kitaru.api_models.v1.worker import WorkerRuntime
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
from kitaru.server.domain.names import MAX_NAME_LENGTH
from kitaru.server.domain.worker import Worker

WORKER_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("worker", ["name"])
WORKER_OWNER_ID_FOREIGN_KEY = foreign_key_name("worker", ["owner_id"])
WORKER_OWNER_ID_INDEX = index_name("worker", ["owner_id"])


class WorkerORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Worker table."""

    __tablename__ = "worker"
    __table_args__ = (
        UniqueConstraint("name", name=WORKER_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=WORKER_OWNER_ID_FOREIGN_KEY
        ),
        Index(WORKER_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    scope: Mapped[dict] = mapped_column(JSONB)
    runtime: Mapped[dict] = mapped_column(JSONB)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    metadata_: Mapped[dict[str, str]] = mapped_column("metadata", JSONB)

    @classmethod
    def column_values(cls, worker: Worker) -> dict[str, object]:
        """Map a domain worker to its column values, id included.

        Shared by `from_domain` and the upsert statement in
        `SQLWorkerRepository.register`, so the field list is written once.

        Args:
            worker: Worker to store.

        Returns:
            Column values keyed by column name.
        """
        return {
            "id": worker.id,
            "owner_id": worker.owner_id,
            "name": worker.name,
            "scope": worker.scope.model_dump(mode="json"),
            "runtime": worker.runtime.model_dump(mode="json"),
            "last_seen_at": worker.last_seen_at,
            "metadata_": worker.metadata,
        }

    @classmethod
    def from_domain(cls, worker: Worker) -> "WorkerORM":
        """Build a row from a domain worker.

        Args:
            worker: Worker to store.

        Returns:
            Row without timestamps set.
        """
        return cls(**cls.column_values(worker))

    def to_domain(self) -> Worker:
        """Build a domain worker from this row.

        Returns:
            Worker with timestamps set.
        """
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
