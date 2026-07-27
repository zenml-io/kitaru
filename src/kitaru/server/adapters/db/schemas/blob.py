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

from sqlalchemy import (
    BigInteger,
    ForeignKeyConstraint,
    Index,
    LargeBinary,
    UniqueConstraint,
)
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
from kitaru.server.domain.blob import (
    MAX_BLOB_URI_LENGTH,
    MAX_MEDIA_TYPE_LENGTH,
    SHA256_LENGTH,
    Blob,
)

BLOB_SHA256_UNIQUE_CONSTRAINT = unique_constraint_name("blob", ["sha256"])
BLOB_OWNER_ID_FOREIGN_KEY = foreign_key_name("blob", ["owner_id"])
BLOB_OWNER_ID_INDEX = index_name("blob", ["owner_id"])


class BlobSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Blob table."""

    __tablename__ = "blob"
    __table_args__ = (
        UniqueConstraint("sha256", name=BLOB_SHA256_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=BLOB_OWNER_ID_FOREIGN_KEY
        ),
        Index(BLOB_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(nullable=False)
    sha256: str = Field(max_length=SHA256_LENGTH, nullable=False)
    size: int = Field(sa_type=BigInteger, nullable=False)
    media_type: str = Field(max_length=MAX_MEDIA_TYPE_LENGTH, nullable=False)
    data: bytes | None = Field(default=None, sa_type=LargeBinary)
    uri: str | None = Field(default=None, max_length=MAX_BLOB_URI_LENGTH)

    @classmethod
    def from_domain(cls, blob: Blob) -> "BlobSchema":
        """Build a row from a domain blob.

        Args:
            blob: Blob to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=blob.id,
            owner_id=blob.owner_id,
            sha256=blob.sha256,
            size=blob.size,
            media_type=blob.media_type,
            data=blob.data,
            uri=blob.uri,
        )

    def to_domain(self) -> Blob:
        """Build a domain blob from this row.

        Returns:
            Blob with the creation timestamp set.
        """
        return Blob(
            id=self.id,
            owner_id=self.owner_id,
            sha256=self.sha256,
            size=self.size,
            media_type=self.media_type,
            data=self.data,
            uri=self.uri,
            created=self.created,
        )
