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
"""Cohort ORM tables."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, Text, UniqueConstraint
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
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.names import MAX_NAME_LENGTH

COHORT_NAME_UNIQUE_CONSTRAINT = unique_constraint_name("cohort", ["name"])
COHORT_AGENT_ID_FOREIGN_KEY = foreign_key_name("cohort", ["agent_id"])
COHORT_OWNER_ID_INDEX = index_name("cohort", ["owner_id"])

COHORT_SESSION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_session", ["cohort_id", "session_id"]
)
COHORT_SESSION_POSITION_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_session", ["cohort_id", "position"]
)
COHORT_SESSION_COHORT_ID_FOREIGN_KEY = foreign_key_name("cohort_session", ["cohort_id"])
COHORT_SESSION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "cohort_session", ["session_id"]
)
COHORT_SESSION_SESSION_ID_INDEX = index_name("cohort_session", ["session_id"])


class CohortSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Cohort table."""

    __tablename__ = "cohort"
    __table_args__ = (
        UniqueConstraint("name", name=COHORT_NAME_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=COHORT_AGENT_ID_FOREIGN_KEY
        ),
        Index(COHORT_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    name: str = Field(max_length=MAX_NAME_LENGTH, nullable=False)
    description: str | None = Field(default=None, sa_type=Text)
    agent_id: uuid.UUID = Field(nullable=False)
    session_count: int = Field(nullable=False)

    @classmethod
    def from_domain(cls, cohort: Cohort) -> "CohortSchema":
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
            session_count=cohort.session_count,
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
            session_count=self.session_count,
            created=self.created,
            updated=self.updated,
        )


class CohortSessionSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Cohort session table."""

    __tablename__ = "cohort_session"
    __table_args__ = (
        UniqueConstraint(
            "cohort_id", "session_id", name=COHORT_SESSION_UNIQUE_CONSTRAINT
        ),
        UniqueConstraint(
            "cohort_id", "position", name=COHORT_SESSION_POSITION_UNIQUE_CONSTRAINT
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            ["cohort.id"],
            name=COHORT_SESSION_COHORT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=COHORT_SESSION_SESSION_ID_FOREIGN_KEY,
        ),
        Index(COHORT_SESSION_SESSION_ID_INDEX, "session_id"),
    )

    cohort_id: uuid.UUID = Field(nullable=False)
    session_id: uuid.UUID = Field(nullable=False)
    position: int = Field(nullable=False)
