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
from typing import Any

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKeyConstraint,
    Index,
    UniqueConstraint,
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
from kitaru.server.domain.worker import Worker

WORKER_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("worker", ["name"])
WORKER_OWNER_ID_FOREIGN_KEY = foreign_key_name("worker", ["owner_id"])
WORKER_OWNER_ID_INDEX = index_name("worker", ["owner_id"])


class WorkerSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Worker table."""

    __tablename__ = "worker"
    __table_args__ = (
        UniqueConstraint("name", name=WORKER_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=WORKER_OWNER_ID_FOREIGN_KEY
        ),
        Index(WORKER_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    agent_ids: list[str] = Field(default_factory=list, sa_type=JSONB, nullable=False)
    last_seen_at: datetime = Field(
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
        nullable=False,
    )
    metadata_: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column("metadata", JSONB, nullable=False),
    )

    @classmethod
    def from_domain(cls, worker: Worker) -> "WorkerSchema":
        """Build a row from a domain worker.

        Args:
            worker: Worker to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=worker.id,
            owner_id=worker.owner_id,
            name=worker.name,
            agent_ids=[str(agent_id) for agent_id in worker.agent_ids],
            last_seen_at=worker.last_seen_at,
            metadata_=worker.metadata,
        )

    def to_domain(self) -> Worker:
        """Build a domain worker from this row.

        Returns:
            Worker with timestamps set.
        """
        return Worker(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            agent_ids=[uuid.UUID(agent_id) for agent_id in self.agent_ids],
            last_seen_at=self.last_seen_at,
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
