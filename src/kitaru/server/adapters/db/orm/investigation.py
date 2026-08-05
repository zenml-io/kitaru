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
"""Investigation ORM table."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.investigation import InvestigationStatus, QuestionItem
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.names import MAX_NAME_LENGTH

INVESTIGATION_OWNER_ID_FOREIGN_KEY = foreign_key_name("investigation", ["owner_id"])
INVESTIGATION_AGENT_ID_FOREIGN_KEY = foreign_key_name("investigation", ["agent_id"])
INVESTIGATION_OWNER_ID_INDEX = index_name("investigation", ["owner_id"])
INVESTIGATION_AGENT_ID_INDEX = index_name("investigation", ["agent_id"])

STATUS_LENGTH = 32


class InvestigationORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Investigation table."""

    __tablename__ = "investigation"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=INVESTIGATION_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"], ["agent.id"], name=INVESTIGATION_AGENT_ID_FOREIGN_KEY
        ),
        Index(INVESTIGATION_OWNER_ID_INDEX, "owner_id"),
        Index(INVESTIGATION_AGENT_ID_INDEX, "agent_id"),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    questions: Mapped[list[Any]] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)

    @classmethod
    def from_domain(cls, investigation: Investigation) -> "InvestigationORM":
        """Build a row from a domain investigation.

        Args:
            investigation: Investigation to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=investigation.id,
            owner_id=investigation.owner_id,
            agent_id=investigation.agent_id,
            name=investigation.name,
            description=investigation.description,
            status=investigation.status.value,
            questions=[
                question.model_dump(mode="json") for question in investigation.questions
            ],
            started_at=investigation.started_at,
            ended_at=investigation.ended_at,
            metadata_=investigation.metadata,
        )

    def to_domain(self, total_sessions: int, completed_sessions: int) -> Investigation:
        """Build a domain investigation from this row.

        Args:
            total_sessions: Linked session count.
            completed_sessions: Completed or skipped linked session count.

        Returns:
            Investigation with timestamps set.
        """
        return Investigation(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            name=self.name,
            description=self.description,
            status=InvestigationStatus(self.status),
            questions=[
                QuestionItem.model_validate(question) for question in self.questions
            ],
            started_at=self.started_at,
            ended_at=self.ended_at,
            metadata=self.metadata_,
            total_sessions=total_sessions,
            completed_sessions=completed_sessions,
            created=self.created,
            updated=self.updated,
        )
