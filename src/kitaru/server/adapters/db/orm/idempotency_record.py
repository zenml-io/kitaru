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
"""Idempotency record ORM table."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    check_constraint_name,
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.idempotency import IdempotencyRecord, IdempotencyState

IDEMPOTENCY_RECORD_SCOPE_COLUMNS = [
    "actor_account_id",
    "actor_principal_kind",
    "actor_principal_identity",
    "method",
    "route",
    "caller_key",
]
IDEMPOTENCY_RECORD_SCOPE_UNIQUE = unique_constraint_name(
    "idempotency_record", IDEMPOTENCY_RECORD_SCOPE_COLUMNS
)
IDEMPOTENCY_RECORD_ACCOUNT_FOREIGN_KEY = foreign_key_name(
    "idempotency_record", ["actor_account_id"]
)
IDEMPOTENCY_RECORD_STATE_CHECK = check_constraint_name("idempotency_record", ["state"])
IDEMPOTENCY_RECORD_RESPONSE_CHECK = check_constraint_name(
    "idempotency_record", ["state", "response_status"]
)
IDEMPOTENCY_RECORD_FINGERPRINT_CHECK = check_constraint_name(
    "idempotency_record", ["fingerprint"]
)
IDEMPOTENCY_RECORD_EXPIRY_INDEX = index_name("idempotency_record", ["expires_at", "id"])


class IdempotencyRecordORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Scoped request reservation and replayable response table."""

    __tablename__ = "idempotency_record"
    __table_args__ = (
        ForeignKeyConstraint(
            ["actor_account_id"],
            ["account.id"],
            name=IDEMPOTENCY_RECORD_ACCOUNT_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            *IDEMPOTENCY_RECORD_SCOPE_COLUMNS,
            name=IDEMPOTENCY_RECORD_SCOPE_UNIQUE,
        ),
        CheckConstraint(
            "state IN ('pending', 'completed')",
            name=IDEMPOTENCY_RECORD_STATE_CHECK,
        ),
        CheckConstraint(
            "(state = 'pending' AND response_status IS NULL "
            "AND response_body IS NULL AND response_headers IS NULL "
            "AND completed_at IS NULL AND expires_at IS NULL) OR "
            "(state = 'completed' AND response_status BETWEEN 100 AND 599 "
            "AND response_body IS NOT NULL AND response_headers IS NOT NULL "
            "AND completed_at IS NOT NULL AND expires_at > completed_at)",
            name=IDEMPOTENCY_RECORD_RESPONSE_CHECK,
        ),
        CheckConstraint(
            "fingerprint ~ '^[0-9a-f]{64}$'",
            name=IDEMPOTENCY_RECORD_FINGERPRINT_CHECK,
        ),
        Index(
            IDEMPOTENCY_RECORD_EXPIRY_INDEX,
            "expires_at",
            "id",
            postgresql_where=text("state = 'completed' AND expires_at IS NOT NULL"),
        ),
    )

    actor_account_id: Mapped[uuid.UUID]
    actor_principal_kind: Mapped[str] = mapped_column(String(16))
    actor_principal_identity: Mapped[str] = mapped_column(String(96))
    method: Mapped[str] = mapped_column(String(16))
    route: Mapped[str] = mapped_column(String(255))
    caller_key: Mapped[str] = mapped_column(String(255))
    fingerprint: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16))
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[bytes | None] = mapped_column(LargeBinary)
    response_headers: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @classmethod
    def from_domain(cls, record: IdempotencyRecord) -> "IdempotencyRecordORM":
        """Build a row from a domain record.

        Args:
            record: Record to persist.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=record.id,
            actor_account_id=record.actor_account_id,
            actor_principal_kind=record.actor_principal_kind,
            actor_principal_identity=record.actor_principal_identity,
            method=record.method,
            route=record.route,
            caller_key=record.caller_key,
            fingerprint=record.fingerprint,
            state=record.state.value,
            response_status=record.response_status,
            response_body=record.response_body,
            response_headers=record.response_headers,
            completed_at=record.completed_at,
            expires_at=record.expires_at,
        )

    def to_domain(self) -> IdempotencyRecord:
        """Build a domain record from this row.

        Returns:
            Stored record.
        """
        headers = None
        if self.response_headers is not None:
            headers = {
                str(key): str(value) for key, value in self.response_headers.items()
            }
        return IdempotencyRecord(
            id=self.id,
            actor_account_id=self.actor_account_id,
            actor_principal_kind=self.actor_principal_kind,
            actor_principal_identity=self.actor_principal_identity,
            method=self.method,
            route=self.route,
            caller_key=self.caller_key,
            fingerprint=self.fingerprint,
            state=IdempotencyState(self.state),
            response_status=self.response_status,
            response_body=self.response_body,
            response_headers=headers,
            completed_at=self.completed_at,
            expires_at=self.expires_at,
            created=self.created,
            updated=self.updated,
        )
