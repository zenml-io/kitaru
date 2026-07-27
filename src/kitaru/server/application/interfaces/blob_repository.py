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
"""Blob repository interface."""

import uuid
from typing import Protocol

from kitaru.server.domain.blob import Blob


class BlobRepository(Protocol):
    """Blob persistence operations."""

    async def create(self, blob: Blob) -> Blob:
        """Persist a new blob.

        Args:
            blob: Blob to store.

        Raises:
            DuplicateBlobContent: The content hash is already stored.

        Returns:
            Stored blob with timestamps set.
        """
        ...

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        ...

    async def get_hashes(self, blob_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
        """Load blob content hashes by id, without reading the content.

        Args:
            blob_ids: Ids of the blobs.

        Returns:
            Content hashes keyed by blob id, missing ids omitted.
        """
        ...

    async def get_by_sha256(self, sha256: str) -> Blob | None:
        """Load a blob by content hash.

        Args:
            sha256: Hash of the content.

        Returns:
            Stored blob, ``None`` when the content is not stored.
        """
        ...
