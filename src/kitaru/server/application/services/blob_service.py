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

from kitaru.server.application.interfaces.blob_repository import (
    BlobRepository,
)
from kitaru.server.application.interfaces.blob_storage import BlobStorage
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.blob import (
    Blob,
    BlobTooLarge,
    DuplicateBlobContent,
)


class BlobService:
    """Blob use cases."""

    def __init__(
        self,
        repository: BlobRepository,
        storage: BlobStorage,
        max_size_bytes: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Blob repository.
            storage: Blob content storage.
            max_size_bytes: Size limit for uploaded content.
        """
        self._repository = repository
        self._storage = storage
        self._max_size_bytes = max_size_bytes

    async def upload_blob(
        self, content: bytes, media_type: str, actor: AuthContext
    ) -> tuple[Blob, bool]:
        """Upload content owned by the caller, deduplicating by hash.

        Content already stored under the same hash returns the stored
        blob. A concurrent upload of the same content falls back to it as
        well.

        Args:
            content: Content to upload.
            media_type: Media type of the content.
            actor: Caller context.

        Raises:
            BlobTooLarge: The content exceeds the size limit.

        Returns:
            Stored blob and whether the upload created it.
        """
        if len(content) > self._max_size_bytes:
            raise BlobTooLarge(self._max_size_bytes)
        sha256 = hashlib.sha256(content).hexdigest()
        existing = await self._repository.get_by_sha256(sha256)
        if existing is not None:
            return existing, False
        location = await self._storage.store(sha256, content)
        blob = Blob(
            owner_id=actor.account.id,
            sha256=sha256,
            size=len(content),
            media_type=media_type,
            data=location.data,
            uri=location.uri,
        )
        try:
            return await self._repository.create(blob), True
        except DuplicateBlobContent:
            stored = await self._repository.get_by_sha256(sha256)
            if stored is None:
                raise
            return stored, False

    async def download_blob(
        self, blob_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Blob, bytes]:
        """Download the content of a blob.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob and its content.
        """
        _ = actor
        blob = await self._repository.get(blob_id)
        return blob, await self._storage.load(blob)
