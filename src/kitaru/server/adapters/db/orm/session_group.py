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
"""Session group ORM models."""

import uuid

from sqlalchemy import ForeignKeyConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name

SESSION_GROUP_JOB_ID_FOREIGN_KEY = foreign_key_name("session_group", ["job_id"])
SESSION_GROUP_JOB_ID_INDEX = index_name("session_group", ["job_id"])
SESSION_GROUP_SESSION_SESSION_GROUP_ID_FOREIGN_KEY = foreign_key_name(
    "session_group_session", ["session_group_id"]
)
SESSION_GROUP_SESSION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "session_group_session", ["session_id"]
)
SESSION_GROUP_SESSION_SESSION_ID_INDEX = index_name(
    "session_group_session", ["session_id"]
)


class SessionGroupORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Session group table: a fixed set of sessions the tasks of one job read.

    Repository-managed, with no domain model and no API of its own.
    """

    __tablename__ = "session_group"
    __table_args__ = (
        ForeignKeyConstraint(
            ["job_id"],
            ["job.id"],
            name=SESSION_GROUP_JOB_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(SESSION_GROUP_JOB_ID_INDEX, "job_id"),
    )

    job_id: Mapped[uuid.UUID]


class SessionGroupSessionORM(TimestampMixin, Base):
    """Session group membership link table.

    Deleting a session removes it from every group, so a group never names
    a session that no longer exists.
    """

    __tablename__ = "session_group_session"
    __table_args__ = (
        ForeignKeyConstraint(
            ["session_group_id"],
            ["session_group.id"],
            name=SESSION_GROUP_SESSION_SESSION_GROUP_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=SESSION_GROUP_SESSION_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(SESSION_GROUP_SESSION_SESSION_ID_INDEX, "session_id"),
    )

    session_group_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
