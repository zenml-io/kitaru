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
"""Blob ORM table."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    DateTime,
    ForeignKeyConstraint,
    Index,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import Base, UUIDPrimaryKeyMixin
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.blob import Blob

BLOB_SHA256_UNIQUE_CONSTRAINT = unique_constraint_name("blob", ["sha256"])
BLOB_OWNER_ID_FOREIGN_KEY = foreign_key_name("blob", ["owner_id"])
BLOB_OWNER_ID_INDEX = index_name("blob", ["owner_id"])


class BlobORM(UUIDPrimaryKeyMixin, Base):
    """Blob table."""

    __tablename__ = "blob"
    __table_args__ = (
        UniqueConstraint("sha256", name=BLOB_SHA256_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=BLOB_OWNER_ID_FOREIGN_KEY
        ),
        Index(BLOB_OWNER_ID_INDEX, "owner_id"),
    )

    # A blob is immutable and carries no updated timestamp, so only created
    # is declared here instead of using TimestampMixin.
    created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), sort_order=-2
    )
    owner_id: Mapped[uuid.UUID | None]
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int]
    media_type: Mapped[str] = mapped_column(String(255))
    data: Mapped[bytes] = mapped_column(LargeBinary)

    @classmethod
    def from_domain(cls, blob: Blob) -> "BlobORM":
        """Build a row from a domain blob.

        Args:
            blob: Blob to store.

        Returns:
            Row without its timestamp set.
        """
        return cls(
            id=blob.id,
            owner_id=blob.owner_id,
            sha256=blob.sha256,
            size=blob.size,
            media_type=blob.media_type,
            data=blob.data,
        )

    def to_domain(self) -> Blob:
        """Build a domain blob from this row.

        Returns:
            Blob with its timestamp set.
        """
        return Blob(
            id=self.id,
            owner_id=self.owner_id,
            sha256=self.sha256,
            size=self.size,
            media_type=self.media_type,
            data=self.data,
            created=self.created,
        )
