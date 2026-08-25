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
"""Blob data store interface."""

from typing import Protocol

from kitaru.server.domain.blob import BlobStorageBackend


class BlobDataStore(Protocol):
    """Blob content storage operations."""

    async def put(self, sha256: str, data: bytes) -> None:
        """Store content under its hash, idempotent on a repeat hash.

        Args:
            sha256: Content hash.
            data: Content bytes.
        """
        ...

    async def get(self, sha256: str) -> bytes:
        """Load content by its hash.

        Args:
            sha256: Content hash.

        Raises:
            BlobContentNotFound: No content is stored under this hash.

        Returns:
            Content bytes.
        """
        ...

    async def delete(self, sha256: str) -> None:
        """Delete content by its hash, idempotent on a missing hash.

        Args:
            sha256: Content hash.
        """
        ...


class BlobDataStores:
    """Blob content stores keyed by backend, with one active for writes."""

    def __init__(
        self,
        stores: dict[BlobStorageBackend, BlobDataStore],
        backend: BlobStorageBackend,
    ) -> None:
        """Initialize the stores.

        Args:
            stores: Content stores keyed by the backend they serve.
            backend: Backend newly offloaded payloads are written to.
        """
        self._stores = stores
        self.backend = backend

    def get_write_store(self) -> BlobDataStore:
        """Return the store new content is written to.

        Returns:
            Store configured for the active backend.
        """
        return self.get_store(self.backend)

    def get_store(self, backend: BlobStorageBackend) -> BlobDataStore:
        """Look up the store configured for a backend.

        Args:
            backend: Backend to resolve a store for.

        Raises:
            RuntimeError: No data store is configured for the backend.

        Returns:
            Store configured for the backend.
        """
        store = self._stores.get(backend)
        if store is None:
            raise RuntimeError(f"No data store configured for backend {backend}")
        return store
