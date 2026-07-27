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
"""Blob content storage in the application database."""

from kitaru.server.domain.blob import Blob, BlobLocation, InvalidBlob


class DatabaseBlobStorage:
    """Blob content storage keeping the bytes in the blob row."""

    async def store(self, sha256: str, content: bytes) -> BlobLocation:
        """Store content addressed by its hash.

        Args:
            sha256: Hash of the content.
            content: Content to store.

        Returns:
            Location the content was stored at.
        """
        _ = sha256
        return BlobLocation(data=content)

    async def load(self, blob: Blob) -> bytes:
        """Load the content of a blob.

        Args:
            blob: Blob to read.

        Raises:
            InvalidBlob: The blob holds no inline content.

        Returns:
            Blob content.
        """
        if blob.data is None:
            raise InvalidBlob(f"Blob {blob.id} holds no inline content")
        return blob.data
