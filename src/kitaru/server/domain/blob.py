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
"""Blob entity and errors."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    ForbiddenError,
    NotFoundError,
    PayloadTooLargeError,
)
from kitaru.server.domain.ids import uuid7


class BlobStorageBackend(StrEnum):
    """Blob storage backend."""

    DATABASE = "database"
    S3 = "s3"


class BlobAccessDenied(ForbiddenError):
    """Raised when the caller's credential does not authorize this blob."""

    def __init__(self, blob_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            blob_id: Id of the blob.
        """
        super().__init__(f"Blob {blob_id} is not accessible to this caller")


class BlobNotFound(NotFoundError):
    """Raised when a blob lookup does not resolve."""

    def __init__(self, blob_id: uuid.UUID | str) -> None:
        """Initialize the error.

        Args:
            blob_id: Id or content hash of the missing blob.
        """
        super().__init__(f"Blob {blob_id} was not found")


class BlobInUse(ConflictError):
    """Raised when a blob is referenced by a plugin version."""

    def __init__(self, blob_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            blob_id: Id of the blob in use.
        """
        super().__init__(f"Blob {blob_id} is in use by a plugin version")


class BlobTooLarge(PayloadTooLargeError):
    """Raised when an upload exceeds the configured blob size cap."""

    def __init__(self, max_bytes: int) -> None:
        """Initialize the error.

        Args:
            max_bytes: Maximum allowed blob size in bytes.
        """
        super().__init__(f"Blob exceeds {max_bytes} bytes")


class Blob(DomainModel):
    """Blob."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID | None
    sha256: str
    size: int
    media_type: str
    stored_in: BlobStorageBackend
    created: datetime | None = None
