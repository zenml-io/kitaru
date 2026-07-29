"""Content-addressed blob entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    PayloadTooLargeError,
)
from kitaru.server.domain.ids import uuid7


class BlobNotFound(NotFoundError):
    """Raised when a blob lookup does not resolve."""

    def __init__(self, identifier: uuid.UUID | str) -> None:
        super().__init__(f"Blob {identifier} was not found")


class BlobInUse(ConflictError):
    """Raised when a blob still has dependent resources."""

    def __init__(self, blob_id: uuid.UUID) -> None:
        super().__init__(f"Blob {blob_id} is in use")


class BlobTooLarge(PayloadTooLargeError):
    """Raised when an uploaded blob exceeds the server limit."""

    def __init__(self, max_bytes: int) -> None:
        super().__init__(f"Blob exceeds the {max_bytes} byte limit")


class Blob(DomainModel):
    """Stored content-addressed bytes."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    sha256: str
    size: int
    media_type: str
    data: bytes | None = None
    created: datetime | None = None
