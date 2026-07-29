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
"""Cohort session link ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, TimestampMixin
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)

COHORT_SESSION_COHORT_ID_FOREIGN_KEY = foreign_key_name("cohort_session", ["cohort_id"])
COHORT_SESSION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "cohort_session", ["session_id"]
)
COHORT_SESSION_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "cohort_session", ["cohort_id", "index"]
)


class CohortSessionORM(TimestampMixin, Base):
    """Cohort session link table, preserving member order.

    Repository-managed. No domain model, the ordered session ids it stores
    round-trip through the cohort's fixed session listing.
    """

    __tablename__ = "cohort_session"
    __table_args__ = (
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
        UniqueConstraint(
            "cohort_id",
            "index",
            name=COHORT_SESSION_INDEX_UNIQUE_CONSTRAINT,
        ),
    )

    cohort_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    index: Mapped[int]
