"""Blobs resource."""

import uuid
from typing import TYPE_CHECKING, BinaryIO

from kitaru.api_models.v1.blob import BlobResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class BlobsResource:
    """Blob API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def upload(
        self,
        content: bytes | BinaryIO,
        filename: str = "blob",
        media_type: str = "application/octet-stream",
    ) -> BlobResponse:
        files = {"file": (filename, content, media_type)}
        response = await self._client.request("POST", "/v1/blobs", files=files)
        return BlobResponse.model_validate(response.json())

    async def get(self, blob_id: uuid.UUID) -> BlobResponse:
        response = await self._client.request("GET", f"/v1/blobs/{blob_id}")
        return BlobResponse.model_validate(response.json())

    async def download(self, blob_id: uuid.UUID) -> bytes:
        response = await self._client.request("GET", f"/v1/blobs/{blob_id}/content")
        return response.content

    async def delete(self, blob_id: uuid.UUID) -> None:
        await self._client.request("DELETE", f"/v1/blobs/{blob_id}")
