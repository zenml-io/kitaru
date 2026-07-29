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

    async def create(self, blob: Blob) -> tuple[Blob, bool]:
        """Persist a new blob, deduping a concurrent identical upload.

        Args:
            blob: Blob to store.

        Returns:
            Stored blob and whether this call created it. A dedup hit
            returns the existing row with its content left unloaded.
        """
        ...

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id, content included.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        ...

    async def delete(self, blob_id: uuid.UUID) -> None:
        """Delete a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version.
        """
        ...
