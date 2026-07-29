"""Blob DTO conversions."""

from kitaru.api_models.v1.blob import BlobResponse
from kitaru.server.domain.blob import Blob


def blob_to_response(blob: Blob) -> BlobResponse:
    """Convert a blob entity to metadata response."""
    assert blob.created is not None
    return BlobResponse(
        id=blob.id,
        sha256=blob.sha256,
        size=blob.size,
        media_type=blob.media_type,
        created=blob.created,
    )
