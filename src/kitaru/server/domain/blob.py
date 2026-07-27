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
from typing import Annotated, Self

from pydantic import AfterValidator, Field, model_validator

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7

SHA256_LENGTH = 64

MAX_MEDIA_TYPE_LENGTH = 255

MAX_BLOB_URI_LENGTH = 2048

DEFAULT_MEDIA_TYPE = "application/octet-stream"


class BlobNotFound(NotFoundError):
    """Raised when a blob lookup does not resolve."""

    def __init__(self, blob_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            blob_id: Id of the missing blob.
        """
        super().__init__(f"Blob {blob_id} was not found")


class DuplicateBlobContent(ConflictError):
    """Raised when blob content is already stored."""

    def __init__(self, sha256: str) -> None:
        """Initialize the error.

        Args:
            sha256: Hash of the content that is already stored.
        """
        super().__init__(f"Blob content '{sha256}' is already stored")


class InvalidBlob(ValidationError):
    """Raised when a blob violates its shape rules."""


class InvalidMediaType(ValidationError):
    """Raised when a media type exceeds the length limit."""


class BlobTooLarge(ValidationError):
    """Raised when blob content exceeds the size limit."""

    def __init__(self, max_size_bytes: int) -> None:
        """Initialize the error.

        Args:
            max_size_bytes: Size limit in bytes.
        """
        super().__init__(f"Blob exceeds {max_size_bytes} bytes")


def validate_media_type(value: str) -> str:
    """Validate a media type against the length limit.

    Args:
        value: Media type to validate.

    Raises:
        InvalidMediaType: ``value`` is empty or exceeds the length limit.

    Returns:
        Validated media type.
    """
    if not value:
        raise InvalidMediaType("Media type must not be empty")
    if len(value) > MAX_MEDIA_TYPE_LENGTH:
        raise InvalidMediaType(f"Media type exceeds {MAX_MEDIA_TYPE_LENGTH} characters")
    return value


MediaType = Annotated[str, AfterValidator(validate_media_type)]


class BlobLocation(FrozenModel):
    """Blob content location."""

    data: bytes | None = None
    uri: str | None = None


class Blob(DomainModel):
    """Blob."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    sha256: str
    size: int
    media_type: MediaType
    data: bytes | None = None
    uri: str | None = None
    created: datetime | None = None

    @model_validator(mode="after")
    def validate_location(self) -> Self:
        """Validate that the content sits in exactly one location.

        Raises:
            InvalidBlob: Neither or both of the data and the uri are set.

        Returns:
            Validated blob.
        """
        if (self.data is None) == (self.uri is None):
            raise InvalidBlob("Exactly one of the blob data and uri is set")
        return self
