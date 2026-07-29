"""Blob API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel


class BlobResponse(ResponseModel):
    """Blob metadata response."""

    id: uuid.UUID = Field(description="Blob id.")
    sha256: str = Field(description="SHA-256 digest.")
    size: int = Field(description="Content size in bytes.")
    media_type: str = Field(description="Content media type.")
    created: datetime = Field(description="Creation time.")
