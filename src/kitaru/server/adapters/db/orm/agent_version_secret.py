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
"""Agent version secret link ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, TimestampMixin
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)

AGENT_VERSION_SECRET_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["agent_version_id"]
)
AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY = foreign_key_name(
    "agent_version_secret", ["secret_id"]
)
AGENT_VERSION_SECRET_INDEX_UNIQUE_CONSTRAINT = unique_constraint_name(
    "agent_version_secret", ["agent_version_id", "index"]
)


class AgentVersionSecretORM(TimestampMixin, Base):
    """Agent version secret link table, preserving secret order.

    Repository-managed. No domain model, the ordered secret ids it stores
    round-trip through ``RunSpec.secret_ids`` on the owning agent version.
    """

    __tablename__ = "agent_version_secret"
    __table_args__ = (
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=AGENT_VERSION_SECRET_AGENT_VERSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["secret_id"],
            ["secret.id"],
            name=AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY,
        ),
        UniqueConstraint(
            "agent_version_id",
            "index",
            name=AGENT_VERSION_SECRET_INDEX_UNIQUE_CONSTRAINT,
        ),
    )

    agent_version_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    secret_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    index: Mapped[int]
