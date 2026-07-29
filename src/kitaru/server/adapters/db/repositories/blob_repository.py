"""SQL blob repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.blob import (
    BLOB_SHA_UNIQUE_CONSTRAINT,
    BlobORM,
)
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.blob import Blob, BlobInUse, BlobNotFound


class SQLBlobRepository(BaseSQLRepository[BlobORM]):
    """Blob repository backed by PostgreSQL."""

    orm_class = BlobORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return BlobNotFound(entity_id)

    async def create(self, blob: Blob) -> tuple[Blob, bool]:
        row = BlobORM.from_domain(blob)
        try:
            await self._add(row)
        except IntegrityError as exc:
            if violated_constraint(exc) != BLOB_SHA_UNIQUE_CONSTRAINT:
                raise
            return await self.get_by_sha256(blob.sha256), False
        return row.to_domain(), True

    async def get(self, blob_id: uuid.UUID) -> Blob:
        row = (
            await self._session.execute(
                self._metadata_statement().where(BlobORM.id == blob_id)
            )
        ).one_or_none()
        if row is None:
            raise BlobNotFound(blob_id)
        return self._metadata_to_domain(row)

    async def get_content(self, blob_id: uuid.UUID) -> Blob:
        """Load blob metadata and content bytes."""
        return (await self._get_row(blob_id)).to_domain()

    async def get_by_sha256(self, sha256: str) -> Blob:
        row = (
            await self._session.execute(
                self._metadata_statement().where(BlobORM.sha256 == sha256)
            )
        ).one_or_none()
        if row is None:
            raise BlobNotFound(sha256)
        return self._metadata_to_domain(row)

    @staticmethod
    def _metadata_statement():
        """Select blob fields without loading content bytes."""
        return select(
            BlobORM.id,
            BlobORM.owner_id,
            BlobORM.sha256,
            BlobORM.size,
            BlobORM.media_type,
            BlobORM.created,
        )

    @staticmethod
    def _metadata_to_domain(row) -> Blob:
        """Build blob metadata from a projected row."""
        return Blob(
            id=row.id,
            owner_id=row.owner_id,
            sha256=row.sha256,
            size=row.size,
            media_type=row.media_type,
            created=row.created,
        )

    async def delete(self, blob_id: uuid.UUID) -> None:
        try:
            async with self._session.begin_nested():
                await self._delete_row(blob_id)
        except IntegrityError as exc:
            raise BlobInUse(blob_id) from exc
