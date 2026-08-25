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
"""SQL blob repository."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.blob import (
    BLOB_SHA256_MEDIA_TYPE_UNIQUE_CONSTRAINT,
    BlobORM,
)
from kitaru.server.adapters.db.orm.plugin import PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.blob import Blob, BlobInUse, BlobNotFound


class SQLBlobRepository(BaseSQLRepository[BlobORM]):
    """Blob repository backed by the application database."""

    orm_class = BlobORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return BlobNotFound(entity_id)

    async def create(self, blob: Blob) -> tuple[Blob, bool]:
        """Persist a new blob, deduping a concurrent identical upload.

        Args:
            blob: Blob to store.

        Returns:
            Stored blob and whether this call created it.
        """
        row = BlobORM.from_domain(blob)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == BLOB_SHA256_MEDIA_TYPE_UNIQUE_CONSTRAINT:
                return await self._get_by_sha256_and_media_type(
                    blob.sha256, blob.media_type
                ), False
            raise
        return row.to_domain(), True

    async def _get_by_sha256_and_media_type(self, sha256: str, media_type: str) -> Blob:
        """Load a blob by sha256 and media type.

        Args:
            sha256: Content hash.
            media_type: Content media type.

        Returns:
            Stored blob.
        """
        statement = select(BlobORM).where(
            BlobORM.sha256 == sha256, BlobORM.media_type == media_type
        )
        row = (await self._session.execute(statement)).scalar_one()
        return row.to_domain()

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        row = await self._get_row(blob_id)
        return row.to_domain()

    async def get_many(self, blob_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Blob]:
        """Bulk-load blobs by id, keyed by id, missing ids omitted.

        Args:
            blob_ids: Ids of the blobs to load.

        Returns:
            Stored blobs keyed by id.
        """
        rows = await self._load_by_ids(list(blob_ids))
        return {blob_id: row.to_domain() for blob_id, row in rows.items()}

    async def get_many_by_sha256s(
        self, sha256s: Sequence[str]
    ) -> dict[tuple[str, str], Blob]:
        """Bulk-load blobs by content hash, keyed by (sha256, media_type).

        Args:
            sha256s: Content hashes to look up.

        Returns:
            Stored blobs keyed by (sha256, media_type), hashes with no
            matching row omitted.
        """
        if not sha256s:
            return {}
        statement = select(BlobORM).where(BlobORM.sha256.in_(sha256s))
        rows = (await self._session.scalars(statement)).all()
        return {(row.sha256, row.media_type): row.to_domain() for row in rows}

    async def delete(self, blob_id: uuid.UUID) -> None:
        """Delete a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version.
        """
        await self._delete_row(
            blob_id, {PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY: lambda: BlobInUse(blob_id)}
        )
