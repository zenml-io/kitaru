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
"""Cohort ORM table."""

import uuid
from typing import Any

from sqlalchemy import ForeignKeyConstraint, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
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
COHORT_AGENT_ID_FOREIGN_KEY = foreign_key_name("cohort", ["agent_id"])


class CohortORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Cohort table."""

    __tablename__ = "cohort"
    __table_args__ = (
        UniqueConstraint("name", name=COHORT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=COHORT_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    owner_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None] = mapped_column(Text)
    agent_id: Mapped[uuid.UUID]
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    latest_version: Mapped[int]

    @classmethod
    def from_domain(cls, cohort: Cohort) -> "CohortORM":
        """Build a row from a domain cohort.

        Args:
            cohort: Cohort to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=cohort.id,
            owner_id=cohort.owner_id,
            name=cohort.name,
            description=cohort.description,
            agent_id=cohort.agent_id,
            metadata_=cohort.metadata,
            latest_version=cohort.latest_version,
        )

    def to_domain(self) -> Cohort:
        """Build a domain cohort from this row.

        Returns:
            Cohort with timestamps set.
        """
        return Cohort(
            id=self.id,
            owner_id=self.owner_id,
            name=self.name,
            description=self.description,
            agent_id=self.agent_id,
            metadata=self.metadata_,
            latest_version=self.latest_version,
            created=self.created,
            updated=self.updated,
        )
