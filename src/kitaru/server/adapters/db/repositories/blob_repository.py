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
from typing import Any

from sqlalchemy import Row, Select, select
from sqlalchemy.exc import IntegrityError

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.blob import BLOB_SHA256_UNIQUE_CONSTRAINT, BlobORM
from kitaru.server.adapters.db.orm.plugin import PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY
from kitaru.server.adapters.db.orm.task import TASK_PAYLOAD_BLOB_ID_FOREIGN_KEY
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
            Stored blob and whether this call created it. A dedup hit
            returns the existing row with its content left unloaded.
        """
        row = BlobORM.from_domain(blob)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == BLOB_SHA256_UNIQUE_CONSTRAINT:
                return await self._get_by_sha256_without_content(blob.sha256), False
            raise
        return row.to_domain(), True

    @staticmethod
    def _select_metadata() -> Select[tuple[uuid.UUID, uuid.UUID, str, int, str, Any]]:
        """Build the select over every blob column except the content.

        Returns:
            Unfiltered metadata select.
        """
        return select(
            BlobORM.id,
            BlobORM.owner_id,
            BlobORM.sha256,
            BlobORM.size,
            BlobORM.media_type,
            BlobORM.created,
        )

    @staticmethod
    def _to_metadata(row: Row[tuple[uuid.UUID, uuid.UUID, str, int, str, Any]]) -> Blob:
        """Build a domain blob from a metadata row.

        Args:
            row: Metadata row.

        Returns:
            Blob with an empty content placeholder.
        """
        return Blob(
            id=row.id,
            owner_id=row.owner_id,
            sha256=row.sha256,
            size=row.size,
            media_type=row.media_type,
            data=b"",
            created=row.created,
        )

    async def _get_by_sha256_without_content(self, sha256: str) -> Blob:
        """Load a blob's metadata by sha256 without its content column.

        Args:
            sha256: Content hash.

        Returns:
            Blob with an empty content placeholder.
        """
        statement = self._select_metadata().where(BlobORM.sha256 == sha256)
        return self._to_metadata((await self._session.execute(statement)).one())

    async def get_metadata(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob's metadata by id, leaving its content unloaded.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Blob with an empty content placeholder.
        """
        statement = self._select_metadata().where(BlobORM.id == blob_id)
        row = (await self._session.execute(statement)).one_or_none()
        if row is None:
            raise BlobNotFound(blob_id)
        return self._to_metadata(row)

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id, content included.

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
            BlobInUse: The blob is referenced by a plugin version or an
                import task.
        """
        row = await self._get_row(blob_id)
        await self._session.delete(row)
        await self._flush(
            {
                PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY: lambda: BlobInUse(blob_id),
                TASK_PAYLOAD_BLOB_ID_FOREIGN_KEY: lambda: BlobInUse(blob_id),
            }
        )
