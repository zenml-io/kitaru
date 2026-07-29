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
"""Tests for blob use cases."""

import hashlib
import uuid
from collections.abc import AsyncIterator

import pytest

from conftest import FakeBlobRepository, create_blob
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobInUse, BlobNotFound, BlobTooLarge

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


@pytest.fixture
def repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def service(repository: FakeBlobRepository) -> BlobService:
    """Provide a blob service backed by the fake repository, capped at 16 bytes."""
    return BlobService(repository=repository, max_size_bytes=16)


async def test_upload_blob(service: BlobService) -> None:
    """Upload new content and mark it as created."""
    blob, created = await service.upload_blob(
        _chunks(b"hello "), media_type="text/plain", actor=ACTOR
    )
    assert created is True
    assert blob.owner_id == ACTOR.account.id
    assert blob.media_type == "text/plain"
    assert blob.size == len(b"hello ")
    assert blob.sha256 == hashlib.sha256(b"hello ").hexdigest()
    assert blob.data == b"hello "
    assert blob.created is not None


async def test_upload_blob_default_media_type(service: BlobService) -> None:
    """Default the media type when the caller omits it."""
    blob, _ = await service.upload_blob(
        _chunks(b"x"), media_type="application/octet-stream", actor=ACTOR
    )
    assert blob.media_type == "application/octet-stream"


async def test_upload_blob_dedup(service: BlobService) -> None:
    """Return the stored blob unmarked as created on a dedup hit."""
    first, created_first = await service.upload_blob(
        _chunks(b"same"), media_type="text/plain", actor=ACTOR
    )
    assert created_first is True
    second, created_second = await service.upload_blob(
        _chunks(b"same"), media_type="text/plain", actor=ACTOR
    )
    assert created_second is False
    assert second.id == first.id
    assert second.sha256 == first.sha256


async def test_upload_blob_streams_in_chunks(service: BlobService) -> None:
    """Assemble content spread across multiple chunks."""
    blob, _ = await service.upload_blob(
        _chunks(b"ab", b"cd", b"ef"), media_type="text/plain", actor=ACTOR
    )
    assert blob.data == b"abcdef"
    assert blob.sha256 == hashlib.sha256(b"abcdef").hexdigest()


async def test_upload_blob_too_large(service: BlobService) -> None:
    """Reject an upload exceeding the size cap."""
    with pytest.raises(BlobTooLarge, match="Blob exceeds 16 bytes"):
        await service.upload_blob(
            _chunks(b"x" * 17), media_type="text/plain", actor=ACTOR
        )


async def test_upload_blob_too_large_across_chunks(service: BlobService) -> None:
    """Reject an upload whose cumulative chunks exceed the size cap."""
    with pytest.raises(BlobTooLarge):
        await service.upload_blob(
            _chunks(b"x" * 10, b"y" * 10), media_type="text/plain", actor=ACTOR
        )


async def test_get_blob(service: BlobService) -> None:
    """Get a blob's metadata by id."""
    created, _ = await service.upload_blob(
        _chunks(b"content"), media_type="text/plain", actor=ACTOR
    )
    loaded = await service.get_blob(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_blob_not_found(service: BlobService) -> None:
    """Raise for an unknown blob id."""
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await service.get_blob(missing_id, actor=ACTOR)


async def test_download_blob(service: BlobService) -> None:
    """Get a blob's content by id."""
    created, _ = await service.upload_blob(
        _chunks(b"content"), media_type="text/plain", actor=ACTOR
    )
    downloaded = await service.download_blob(created.id, actor=ACTOR)
    assert downloaded.data == b"content"


async def test_download_blob_not_found(service: BlobService) -> None:
    """Raise for an unknown blob id."""
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await service.download_blob(missing_id, actor=ACTOR)


async def test_delete_blob(service: BlobService) -> None:
    """Delete a stored blob."""
    created, _ = await service.upload_blob(
        _chunks(b"content"), media_type="text/plain", actor=ACTOR
    )
    await service.delete_blob(created.id, actor=ACTOR)
    with pytest.raises(BlobNotFound):
        await service.get_blob(created.id, actor=ACTOR)


async def test_delete_blob_not_found(service: BlobService) -> None:
    """Raise for an unknown blob id."""
    with pytest.raises(BlobNotFound):
        await service.delete_blob(uuid.uuid4(), actor=ACTOR)


async def test_delete_blob_in_use(
    service: BlobService, repository: FakeBlobRepository
) -> None:
    """Raise when a blob is referenced by a plugin version."""
    blob = await create_blob(repository, ACTOR.account.id)
    repository.mark_referenced(blob.id)
    with pytest.raises(BlobInUse, match=f"Blob {blob.id} is in use"):
        await service.delete_blob(blob.id, actor=ACTOR)
