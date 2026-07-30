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
"""Blobs resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.blob import BlobResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class BlobsResource:
    """Blob API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def upload(
        self,
        content: bytes,
        media_type: str = "application/octet-stream",
        filename: str | None = None,
    ) -> BlobResponse:
        """Upload a blob, deduping identical content by sha256.

        Args:
            content: Blob content.
            media_type: Content media type.
            filename: Filename sent with the upload.

        Raises:
            APIError: The request failed, including 413 when the content
                exceeds the server's size cap.

        Returns:
            Stored blob metadata, whether newly created or deduped.
        """
        # A filename is what makes httpx encode this as a file field rather
        # than a plain form field, so the server sees an UploadFile.
        response = await self._client.request(
            "POST",
            "/v1/blobs",
            files={"file": (filename or "blob", content, media_type)},
        )
        return BlobResponse.model_validate(response.json())

    async def get(self, blob_id: uuid.UUID) -> BlobResponse:
        """Get a blob's metadata by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            APIError: The request failed, including 404 for a missing blob.

        Returns:
            Stored blob metadata.
        """
        response = await self._client.request("GET", f"/v1/blobs/{blob_id}")
        return BlobResponse.model_validate(response.json())

    async def download(self, blob_id: uuid.UUID) -> bytes:
        """Download a blob's content by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            APIError: The request failed, including 404 for a missing blob.

        Returns:
            Blob content.
        """
        response = await self._client.request("GET", f"/v1/blobs/{blob_id}/content")
        return response.content

    async def delete(self, blob_id: uuid.UUID) -> None:
        """Delete a blob.

        Args:
            blob_id: Id of the blob.

        Raises:
            APIError: The request failed, including 404 for a missing blob
                and 409 when the blob is referenced by a plugin version.
        """
        await self._client.request("DELETE", f"/v1/blobs/{blob_id}")
