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

from kitaru.api_models.v1.blobs import BlobResponse

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

    async def upload(self, data: bytes, media_type: str) -> BlobResponse:
        """Upload a blob.

        Content already stored returns the stored blob instead of a new
        one.

        Args:
            data: Content to upload.
            media_type: Media type of the content.

        Raises:
            APIError: The request failed, including 422 for content over
                the size limit.

        Returns:
            Stored blob.
        """
        response = await self._client.request(
            "POST",
            "/v1/blobs",
            files={"file": ("blob", data, media_type)},
        )
        return BlobResponse.model_validate(response.json())

    async def download(self, blob_id: uuid.UUID) -> bytes:
        """Download the content of a blob.

        Args:
            blob_id: Id of the blob.

        Raises:
            APIError: The request failed, including 404 for a missing
                blob.

        Returns:
            Blob content.
        """
        response = await self._client.request("GET", f"/v1/blobs/{blob_id}/content")
        return response.content
