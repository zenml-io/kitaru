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
"""Insight ORM table."""

import uuid
from typing import Any

from pydantic import TypeAdapter
from sqlalchemy import ForeignKeyConstraint, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.insight import InsightData
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.names import MAX_NAME_LENGTH

_INSIGHT_DATA_ADAPTER: TypeAdapter[InsightData] = TypeAdapter(InsightData)

INSIGHT_OWNER_ID_FOREIGN_KEY = foreign_key_name("insight", ["owner_id"])
INSIGHT_AGENT_ID_FOREIGN_KEY = foreign_key_name("insight", ["agent_id"])
INSIGHT_OWNER_ID_INDEX = index_name("insight", ["owner_id"])
INSIGHT_AGENT_ID_INDEX = index_name("insight", ["agent_id"])

TYPE_LENGTH = 32


class InsightORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Insight table."""

    __tablename__ = "insight"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=INSIGHT_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=INSIGHT_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(INSIGHT_OWNER_ID_INDEX, "owner_id"),
        Index(INSIGHT_AGENT_ID_INDEX, "agent_id"),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    title: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(String(TYPE_LENGTH))
    data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, insight: Insight) -> "InsightORM":
        """Build a row from a domain insight.

        Args:
            insight: Insight to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=insight.id,
            owner_id=insight.owner_id,
            agent_id=insight.agent_id,
            title=insight.title,
            description=insight.description,
            type=insight.data.type,
            data=insight.data.model_dump(mode="json"),
            metadata_=insight.metadata,
        )

    def to_domain(self) -> Insight:
        """Build a domain insight from this row.

        Returns:
            Insight with timestamps set.
        """
        return Insight(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            title=self.title,
            description=self.description,
            data=_INSIGHT_DATA_ADAPTER.validate_python(self.data),
            metadata=self.metadata_,
            created=self.created,
            updated=self.updated,
        )
