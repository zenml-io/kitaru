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

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.schemas.blob import (
    BLOB_SHA256_UNIQUE_CONSTRAINT,
    BlobSchema,
)
from kitaru.server.domain.blob import Blob, BlobNotFound, DuplicateBlobContent


class SQLBlobRepository:
    """Blob repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, blob: Blob) -> Blob:
        """Persist a new blob.

        Args:
            blob: Blob to store.

        Raises:
            DuplicateBlobContent: The content hash is already stored.

        Returns:
            Stored blob with timestamps set.
        """
        row = BlobSchema.from_domain(blob)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == BLOB_SHA256_UNIQUE_CONSTRAINT:
                raise DuplicateBlobContent(blob.sha256) from exc
            raise
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
        row = await self._session.get(BlobSchema, blob_id)
        if row is None:
            raise BlobNotFound(blob_id)
        return row.to_domain()

    async def get_hashes(self, blob_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
        """Load blob content hashes by id, without reading the content.

        Args:
            blob_ids: Ids of the blobs.

        Returns:
            Content hashes keyed by blob id, missing ids omitted.
        """
        if not blob_ids:
            return {}
        statement = select(col(BlobSchema.id), col(BlobSchema.sha256)).where(
            col(BlobSchema.id).in_(blob_ids)
        )
        rows = (await self._session.execute(statement)).all()
        return {blob_id: sha256 for blob_id, sha256 in rows}

    async def get_by_sha256(self, sha256: str) -> Blob | None:
        """Load a blob by content hash.

        Args:
            sha256: Hash of the content.

        Returns:
            Stored blob, ``None`` when the content is not stored.
        """
        statement = select(BlobSchema).where(col(BlobSchema.sha256) == sha256)
        row = (await self._session.scalars(statement)).first()
        if row is None:
            return None
        return row.to_domain()
