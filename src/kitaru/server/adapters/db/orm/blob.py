"""Blob ORM table."""

import uuid

from sqlalchemy import ForeignKeyConstraint, LargeBinary, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    unique_constraint_name,
)
from kitaru.server.domain.blob import Blob

BLOB_SHA_UNIQUE_CONSTRAINT = unique_constraint_name("blob", ["sha256"])
BLOB_OWNER_FOREIGN_KEY = foreign_key_name("blob", ["owner_id"])


class BlobORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Content-addressed blob table."""

    __tablename__ = "blob"
    __table_args__ = (
        UniqueConstraint("sha256", name=BLOB_SHA_UNIQUE_CONSTRAINT),
        ForeignKeyConstraint(["owner_id"], ["account.id"], name=BLOB_OWNER_FOREIGN_KEY),
    )

    owner_id: Mapped[uuid.UUID]
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int]
    media_type: Mapped[str] = mapped_column(String(255))
    data: Mapped[bytes] = mapped_column(LargeBinary)

    @classmethod
    def from_domain(cls, blob: Blob) -> "BlobORM":
        """Build a row from a blob."""
        assert blob.data is not None
        return cls(
            id=blob.id,
            owner_id=blob.owner_id,
            sha256=blob.sha256,
            size=blob.size,
            media_type=blob.media_type,
            data=blob.data,
        )

    def to_domain(self) -> Blob:
        """Build a blob from this row."""
        return Blob(
            id=self.id,
            owner_id=self.owner_id,
            sha256=self.sha256,
            size=self.size,
            media_type=self.media_type,
            data=self.data,
            created=self.created,
        )
