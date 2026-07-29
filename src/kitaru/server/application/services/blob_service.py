#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Content-addressed blob use cases."""

import hashlib
import uuid
from io import BytesIO
from tempfile import SpooledTemporaryFile
from typing import BinaryIO

from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.blob import Blob, BlobNotFound, BlobTooLarge

DEFAULT_MAX_BLOB_SIZE_BYTES = 100 * 1024 * 1024
_HASH_CHUNK_SIZE = 1024 * 1024


class BlobService:
    """Content-addressed blob use cases."""

    def __init__(
        self,
        repository: BlobRepository,
        max_size_bytes: int = DEFAULT_MAX_BLOB_SIZE_BYTES,
    ) -> None:
        self._repository = repository
        self._max_size_bytes = max_size_bytes

    async def upload_blob(
        self, content: BinaryIO | bytes, media_type: str, actor: AuthContext
    ) -> tuple[Blob, bool]:
        """Stream, cap, hash, and deduplicate an uploaded blob."""
        source = BytesIO(content) if isinstance(content, bytes) else content
        source.seek(0)
        digest = hashlib.sha256()
        size = 0
        with SpooledTemporaryFile(
            max_size=self._max_size_bytes, mode="w+b"
        ) as buffered:
            while chunk := source.read(_HASH_CHUNK_SIZE):
                size += len(chunk)
                if size > self._max_size_bytes:
                    raise BlobTooLarge(self._max_size_bytes)
                digest.update(chunk)
                buffered.write(chunk)
            sha256 = digest.hexdigest()
            try:
                return await self._repository.get_by_sha256(sha256), False
            except BlobNotFound:
                pass
            buffered.seek(0)
            data = buffered.read()
            return await self._repository.create(
                Blob(
                    owner_id=actor.account.id,
                    sha256=sha256,
                    size=size,
                    media_type=media_type,
                    data=data,
                )
            )

    async def get_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> Blob:
        """Get blob metadata and content."""
        _ = actor
        return await self._repository.get(blob_id)

    async def download_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> Blob:
        """Get a blob for content download."""
        _ = actor
        return await self._repository.get_content(blob_id)

    async def delete_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete an unreferenced blob."""
        _ = actor
        await self._repository.get(blob_id)
        await self._repository.delete(blob_id)
