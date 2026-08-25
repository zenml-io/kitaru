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

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.blob import BLOB_SHA256_UNIQUE_CONSTRAINT, BlobORM
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
            if violated_constraint(exc) == BLOB_SHA256_UNIQUE_CONSTRAINT:
                return await self._get_by_sha256(blob.sha256), False
            raise
        return row.to_domain(), True

    async def _get_by_sha256(self, sha256: str) -> Blob:
        """Load a blob by sha256.

        Args:
            sha256: Content hash.

        Returns:
            Stored blob.
        """
        statement = select(BlobORM).where(BlobORM.sha256 == sha256)
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
