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
"""Worker pool ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.worker import WorkerScope
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
from kitaru.server.domain.worker_pool import WorkerPool

WORKER_POOL_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("worker_pool", ["name"])
WORKER_POOL_OWNER_ID_FOREIGN_KEY = foreign_key_name("worker_pool", ["owner_id"])
WORKER_POOL_OWNER_ID_INDEX = index_name("worker_pool", ["owner_id"])


class WorkerPoolORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Worker pool table."""

    __tablename__ = "worker_pool"
    __table_args__ = (
        UniqueConstraint("name", name=WORKER_POOL_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=WORKER_POOL_OWNER_ID_FOREIGN_KEY
        ),
        Index(WORKER_POOL_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    scope: Mapped[dict] = mapped_column(JSONB)

    @classmethod
    def from_domain(cls, worker_pool: WorkerPool) -> "WorkerPoolORM":
        """Build a row from a domain worker pool.

        Args:
            worker_pool: Worker pool to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=worker_pool.id,
            owner_id=worker_pool.owner_id,
            name=worker_pool.name,
            scope=worker_pool.scope.model_dump(mode="json"),
        )

    def to_domain(self) -> WorkerPool:
        """Build a domain worker pool from this row.

        Returns:
            Worker pool with timestamps set.
        """
        return WorkerPool(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            scope=WorkerScope.model_validate(self.scope),
            created=self.created,
            updated=self.updated,
        )
