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
"""Cohort version ORM table."""

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
from kitaru.server.domain.cohort_version import CohortVersion

COHORT_VERSION_COHORT_ID_VERSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_version", ["cohort_id", "version"]
)
COHORT_VERSION_COHORT_ID_FOREIGN_KEY = foreign_key_name("cohort_version", ["cohort_id"])


class CohortVersionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Cohort version table."""

    __tablename__ = "cohort_version"
    __table_args__ = (
        UniqueConstraint(
            "cohort_id",
            "version",
            name=COHORT_VERSION_COHORT_ID_VERSION_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name=COHORT_VERSION_COHORT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
    )

    owner_id: Mapped[uuid.UUID]
    cohort_id: Mapped[uuid.UUID]
    version: Mapped[int]
    display_version: Mapped[str | None] = mapped_column(String(255))
    session_count: Mapped[int]

    @classmethod
    def from_domain(cls, version: CohortVersion) -> "CohortVersionORM":
        """Build a row from a domain cohort version.

        Args:
            version: Cohort version to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=version.id,
            owner_id=version.owner_id,
            cohort_id=version.cohort_id,
            version=version.version,
            display_version=version.display_version,
            session_count=version.session_count,
        )

    def to_domain(self) -> CohortVersion:
        """Build a domain cohort version from this row.

        Returns:
            Cohort version with timestamps set.
        """
        return CohortVersion(
            id=self.id,
            owner_id=self.owner_id,
            cohort_id=self.cohort_id,
            version=self.version,
            display_version=self.display_version,
            session_count=self.session_count,
            created=self.created,
            updated=self.updated,
        )
