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
"""Blob data store backed by S3."""

from typing import Any

import obstore
from obstore.store import S3Store

from kitaru.server.blob_storage_settings import S3BlobStorageSettings
from kitaru.server.domain.blob import BlobContentNotFound


class S3BlobDataStore:
    """Blob content store backed by an S3 bucket."""

    def __init__(self, settings: S3BlobStorageSettings) -> None:
        """Initialize the data store.

        Args:
            settings: S3 blob storage settings.
        """
        self._prefix = settings.prefix
        config: dict[str, Any] = {"bucket": settings.bucket}
        if settings.region is not None:
            config["region"] = settings.region
        if settings.endpoint_url is not None:
            config["endpoint"] = settings.endpoint_url
        if settings.access_key_id is not None:
            config["access_key_id"] = settings.access_key_id
        if settings.secret_access_key is not None:
            config["secret_access_key"] = settings.secret_access_key.get_secret_value()
        self._store = S3Store(**config)

    def _key(self, sha256: str) -> str:
        """Build the object key for a content hash.

        Args:
            sha256: Content hash.

        Returns:
            Object key.
        """
        if self._prefix:
            return f"{self._prefix}/{sha256}"
        return sha256

    async def put(self, sha256: str, data: bytes) -> None:
        """Store content under its hash, idempotent on a repeat hash.

        Args:
            sha256: Content hash.
            data: Content bytes.
        """
        await obstore.put_async(self._store, self._key(sha256), data)

    async def get(self, sha256: str) -> bytes:
        """Load content by its hash.

        Args:
            sha256: Content hash.

        Raises:
            BlobContentNotFound: No content is stored under this hash.

        Returns:
            Content bytes.
        """
        try:
            result = await obstore.get_async(self._store, self._key(sha256))
        except FileNotFoundError as exc:
            raise BlobContentNotFound(sha256) from exc
        return bytes(await result.bytes_async())

    async def delete(self, sha256: str) -> None:
        """Delete content by its hash, idempotent on a missing hash.

        Args:
            sha256: Content hash.
        """
        await obstore.delete_async(self._store, self._key(sha256))
