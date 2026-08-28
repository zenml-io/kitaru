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
"""Blob use cases."""

import hashlib
import uuid
from collections.abc import AsyncIterator

from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.resource_access import check_task_blob_read
from kitaru.server.domain.blob import Blob, BlobTooLarge


class BlobService:
    """Content-addressed blob storage."""

    def __init__(
        self,
        repository: BlobRepository,
        data_stores: BlobDataStores,
        max_size_bytes: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Blob repository.
            data_stores: Content stores keyed by the backend they serve.
            max_size_bytes: Upload size cap in bytes.
        """
        self._repository = repository
        self._data_stores = data_stores
        self._max_size_bytes = max_size_bytes

    async def upload_blob(
        self,
        content: AsyncIterator[bytes],
        media_type: str,
        actor: AuthContext,
    ) -> tuple[Blob, bool]:
        """Upload a blob, deduping identical content and media type.

        The content is hashed and buffered chunk by chunk, so the cap holds
        the whole way through and the buffer is used directly for the store
        put without a second read. The put runs before the registry write,
        so a failed request orphans content instead of dangling a ref.

        Args:
            content: Upload content, read in chunks.
            media_type: Content media type.
            actor: Caller context.

        Raises:
            BlobTooLarge: The upload exceeds the configured size cap.

        Returns:
            Stored blob and whether this call created it.
        """
        hasher = hashlib.sha256()
        buffer = bytearray()
        size = 0
        async for chunk in content:
            size += len(chunk)
            if size > self._max_size_bytes:
                raise BlobTooLarge(self._max_size_bytes)
            hasher.update(chunk)
            buffer.extend(chunk)
        sha256 = hasher.hexdigest()
        await self._data_stores.get_write_store().put(sha256, bytes(buffer))
        blob = Blob(
            owner_id=actor.account.id,
            sha256=sha256,
            size=size,
            media_type=media_type,
            stored_in=self._data_stores.backend,
        )
        return await self._repository.create(blob)

    async def get_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> Blob:
        """Get a blob's metadata by id.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobAccessDenied: A task principal holds no grant for the blob.
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        check_task_blob_read(blob_id, actor)
        return await self._repository.get(blob_id)

    async def download_blob(
        self, blob_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Blob, bytes]:
        """Get a blob's metadata and content by id.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobAccessDenied: A task principal holds no grant for the blob.
            BlobNotFound: No blob has this id.
            RuntimeError: No data store is configured for the blob's backend.

        Returns:
            Stored blob and its content.
        """
        check_task_blob_read(blob_id, actor)
        blob = await self._repository.get(blob_id)
        store = self._data_stores.get_store(blob.stored_in)
        return blob, await store.get(blob.sha256)

    async def delete_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a blob.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version, a
                session, or a session node.
        """
        _ = actor
        await self._repository.delete(blob_id)
