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
"""Cohort version session link ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, TimestampMixin
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)

COHORT_VERSION_SESSION_COHORT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "cohort_version_session", ["cohort_version_id"]
)
COHORT_VERSION_SESSION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "cohort_version_session", ["session_id"]
)
COHORT_VERSION_SESSION_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_version_session", ["cohort_version_id", "index"]
)
COHORT_VERSION_SESSION_SESSION_ID_COHORT_VERSION_ID_INDEX = index_name(
    "cohort_version_session", ["session_id", "cohort_version_id"]
)


class CohortVersionSessionORM(TimestampMixin, Base):
    """Cohort version session link table, preserving member order.

    Repository-managed. No domain model, the ordered session ids it stores
    round-trip through the cohort version's fixed member listing.
    """

    __tablename__ = "cohort_version_session"
    __table_args__ = (
        ForeignKeyConstraint(
            ["cohort_version_id"],
            ["cohort_version.id"],
            name=COHORT_VERSION_SESSION_COHORT_VERSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=COHORT_VERSION_SESSION_SESSION_ID_FOREIGN_KEY,
        ),
        UniqueConstraint(
            "cohort_version_id",
            "index",
            name=COHORT_VERSION_SESSION_INDEX_UNIQUE_CONSTRAINT,
        ),
        Index(
            COHORT_VERSION_SESSION_SESSION_ID_COHORT_VERSION_ID_INDEX,
            "session_id",
            "cohort_version_id",
        ),
    )

    cohort_version_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    index: Mapped[int]
