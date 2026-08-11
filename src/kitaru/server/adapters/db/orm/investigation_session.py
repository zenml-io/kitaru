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
"""Investigation session ORM table."""

import uuid
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.investigation import (
    InvestigationSessionVerdict,
    InvestigationSessionView,
)
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
from kitaru.server.domain.investigation import InvestigationSession

INVESTIGATION_SESSION_INVESTIGATION_ID_SESSION_ID_UNIQUE_CONSTRAINT = (
    unique_constraint_name("investigation_session", ["investigation_id", "session_id"])
)
INVESTIGATION_SESSION_INVESTIGATION_ID_FOREIGN_KEY = foreign_key_name(
    "investigation_session", ["investigation_id"]
)
INVESTIGATION_SESSION_SESSION_ID_FOREIGN_KEY = foreign_key_name(
    "investigation_session", ["session_id"]
)
INVESTIGATION_SESSION_SESSION_ID_INDEX = index_name(
    "investigation_session", ["session_id"]
)

VERDICT_LENGTH = 32


class InvestigationSessionORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Investigation session table."""

    __tablename__ = "investigation_session"
    __table_args__ = (
        UniqueConstraint(
            "investigation_id",
            "session_id",
            name=INVESTIGATION_SESSION_INVESTIGATION_ID_SESSION_ID_UNIQUE_CONSTRAINT,
        ),
        ForeignKeyConstraint(
            ["investigation_id"],
            ["investigation.id"],
            name=INVESTIGATION_SESSION_INVESTIGATION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=INVESTIGATION_SESSION_SESSION_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        Index(INVESTIGATION_SESSION_SESSION_ID_INDEX, "session_id"),
    )

    investigation_id: Mapped[uuid.UUID]
    session_id: Mapped[uuid.UUID]
    position: Mapped[int]
    verdict: Mapped[str | None] = mapped_column(String(VERDICT_LENGTH))
    view: Mapped[Any | None] = mapped_column(JSONB(none_as_null=True))

    @classmethod
    def from_domain(cls, session: InvestigationSession) -> "InvestigationSessionORM":
        """Build a row from a domain investigation session.

        Args:
            session: Investigation session to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=session.id,
            investigation_id=session.investigation_id,
            session_id=session.session_id,
            position=session.position,
            verdict=session.verdict.value if session.verdict is not None else None,
            view=(
                session.view.model_dump(mode="json")
                if session.view is not None
                else None
            ),
        )

    def to_domain(self) -> InvestigationSession:
        """Build a domain investigation session from this row.

        Returns:
            Investigation session with timestamps set.
        """
        return InvestigationSession(
            id=self.id,
            investigation_id=self.investigation_id,
            session_id=self.session_id,
            position=self.position,
            verdict=(
                InvestigationSessionVerdict(self.verdict)
                if self.verdict is not None
                else None
            ),
            view=(
                InvestigationSessionView.model_validate(self.view)
                if self.view is not None
                else None
            ),
            created=self.created,
            updated=self.updated,
        )
