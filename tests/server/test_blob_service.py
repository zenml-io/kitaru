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

import pytest

from conftest import FakeBlobRepository
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobNotFound, BlobTooLarge

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))

CONTENT = b"def score(session):\n    return 1.0\n"


@pytest.fixture
def repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def service(repository: FakeBlobRepository) -> BlobService:
    """Provide a blob service backed by the fake repository."""
    return BlobService(
        repository=repository,
        storage=DatabaseBlobStorage(),
        max_size_bytes=1024,
    )


async def test_upload_blob(service: BlobService) -> None:
    """Upload content owned by the caller."""
    blob, created = await service.upload_blob(
        content=CONTENT, media_type="text/x-python", actor=ACTOR
    )
    assert created is True
    assert blob.owner_id == ACTOR.account.id
    assert blob.sha256 == hashlib.sha256(CONTENT).hexdigest()
    assert blob.size == len(CONTENT)
    assert blob.media_type == "text/x-python"
    assert blob.data == CONTENT
    assert blob.uri is None
    assert blob.created is not None


async def test_upload_blob_deduplicates(service: BlobService) -> None:
    """Return the stored blob for content already uploaded."""
    first, created = await service.upload_blob(
        content=CONTENT, media_type="text/x-python", actor=ACTOR
    )
    assert created is True
    second, created = await service.upload_blob(
        content=CONTENT, media_type="text/plain", actor=FOREIGN_ACTOR
    )
    assert created is False
    assert second.id == first.id
    assert second.media_type == "text/x-python"
    assert second.owner_id == ACTOR.account.id


async def test_upload_blob_too_large(service: BlobService) -> None:
    """Reject content over the size limit."""
    with pytest.raises(BlobTooLarge, match="Blob exceeds 1024 bytes"):
        await service.upload_blob(
            content=b"x" * 1025, media_type="text/plain", actor=ACTOR
        )


async def test_upload_blob_at_size_limit(service: BlobService) -> None:
    """Accept content exactly at the size limit."""
    blob, created = await service.upload_blob(
        content=b"x" * 1024, media_type="text/plain", actor=ACTOR
    )
    assert created is True
    assert blob.size == 1024


async def test_download_blob(service: BlobService) -> None:
    """Download the content of a stored blob."""
    created, _ = await service.upload_blob(
        content=CONTENT, media_type="text/x-python", actor=ACTOR
    )
    blob, content = await service.download_blob(created.id, actor=ACTOR)
    assert blob == created
    assert content == CONTENT


async def test_download_blob_foreign_owner(service: BlobService) -> None:
    """Download a blob owned by another account."""
    created, _ = await service.upload_blob(
        content=CONTENT, media_type="text/x-python", actor=ACTOR
    )
    _, content = await service.download_blob(created.id, actor=FOREIGN_ACTOR)
    assert content == CONTENT


async def test_download_blob_not_found(service: BlobService) -> None:
    """Raise for an unknown blob id."""
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await service.download_blob(missing_id, actor=ACTOR)
