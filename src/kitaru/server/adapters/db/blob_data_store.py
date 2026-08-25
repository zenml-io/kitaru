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
"""Blob data store backed by the application database."""

from collections.abc import Mapping, Sequence

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.orm.blob_content import BlobContentORM
from kitaru.server.domain.blob import BlobContentNotFound


class DatabaseBlobDataStore:
    """Blob content store backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the data store.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def put(self, sha256: str, data: bytes) -> None:
        """Store content under its hash, idempotent on a repeat hash.

        Args:
            sha256: Content hash.
            data: Content bytes.
        """
        statement = (
            insert(BlobContentORM)
            .values(sha256=sha256, data=data)
            .on_conflict_do_nothing()
        )
        await self._session.execute(statement)

    async def put_many(self, data_by_sha256: Mapping[str, bytes]) -> None:
        """Store many contents under their hashes, idempotent per hash.

        Args:
            data_by_sha256: Content bytes keyed by their sha256.
        """
        if not data_by_sha256:
            return
        statement = (
            insert(BlobContentORM)
            .values(
                [
                    {"sha256": sha256, "data": data}
                    for sha256, data in data_by_sha256.items()
                ]
            )
            .on_conflict_do_nothing()
        )
        await self._session.execute(statement)

    async def get(self, sha256: str) -> bytes:
        """Load content by its hash.

        Args:
            sha256: Content hash.

        Raises:
            BlobContentNotFound: No content is stored under this hash.

        Returns:
            Content bytes.
        """
        statement = select(BlobContentORM.data).where(BlobContentORM.sha256 == sha256)
        data = (await self._session.execute(statement)).scalar_one_or_none()
        if data is None:
            raise BlobContentNotFound(sha256)
        return data

    async def get_many(self, sha256s: Sequence[str]) -> dict[str, bytes]:
        """Load many contents by their hashes.

        Args:
            sha256s: Content hashes.

        Raises:
            BlobContentNotFound: A requested hash has no stored content.

        Returns:
            Content bytes keyed by their sha256.
        """
        if not sha256s:
            return {}
        statement = select(BlobContentORM).where(BlobContentORM.sha256.in_(sha256s))
        rows = (await self._session.scalars(statement)).all()
        data_by_sha256 = {row.sha256: row.data for row in rows}
        for sha256 in sha256s:
            if sha256 not in data_by_sha256:
                raise BlobContentNotFound(sha256)
        return data_by_sha256

    async def delete(self, sha256: str) -> None:
        """Delete content by its hash, idempotent on a missing hash.

        Args:
            sha256: Content hash.
        """
        statement = delete(BlobContentORM).where(BlobContentORM.sha256 == sha256)
        await self._session.execute(statement)
