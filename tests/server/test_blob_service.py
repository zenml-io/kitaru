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

from conftest import (
    DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
    FakeBlobDataStore,
    FakeBlobRepository,
    create_blob,
)
from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.services.blob_service import (
    JSON_MEDIA_TYPE,
    TEXT_MEDIA_TYPE,
    BlobService,
    Candidate,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import (
    BlobAccessDenied,
    BlobInUse,
    BlobNotFound,
    BlobStorageBackend,
    BlobTooLarge,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


@pytest.fixture
def repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def data_store() -> FakeBlobDataStore:
    """Provide a fake blob data store."""
    return FakeBlobDataStore()


@pytest.fixture
def service(
    repository: FakeBlobRepository, data_store: FakeBlobDataStore
) -> BlobService:
    """Provide a blob service backed by the fake repository, capped at 16 bytes."""
    return BlobService(
        repository=repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: data_store}, BlobStorageBackend.DATABASE
        ),
        max_size_bytes=16,
        offload_threshold_bytes=DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
    )


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
    assert blob.stored_in == BlobStorageBackend.DATABASE
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


async def test_upload_blob_streams_in_chunks(
    service: BlobService, data_store: FakeBlobDataStore
) -> None:
    """Assemble content spread across multiple chunks."""
    blob, _ = await service.upload_blob(
        _chunks(b"ab", b"cd", b"ef"), media_type="text/plain", actor=ACTOR
    )
    assert await data_store.get(blob.sha256) == b"abcdef"
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
    downloaded, data = await service.download_blob(created.id, actor=ACTOR)
    assert downloaded.id == created.id
    assert data == b"content"


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


def _task_actor(granted_blob_id: uuid.UUID | None = None) -> AuthContext:
    """Build an auth context for a task principal holding the given blob grant."""
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_blob_id is not None:
        grants[GrantKind.BLOB] = frozenset({granted_blob_id})
    return AuthContext(
        account=ACTOR.account,
        principal=TaskPrincipal(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants=grants,
        ),
    )


async def test_get_blob_denies_a_task_principal_without_a_grant(
    service: BlobService, repository: FakeBlobRepository
) -> None:
    """Raise when a task principal reads metadata of a blob it was not granted."""
    blob = await create_blob(repository, ACTOR.account.id)
    with pytest.raises(BlobAccessDenied, match=f"Blob {blob.id} is not accessible"):
        await service.get_blob(blob.id, actor=_task_actor())


async def test_download_blob_denies_a_task_principal_without_a_grant(
    service: BlobService, repository: FakeBlobRepository
) -> None:
    """Raise when a task principal downloads a blob it was not granted."""
    blob = await create_blob(repository, ACTOR.account.id)
    with pytest.raises(BlobAccessDenied):
        await service.download_blob(blob.id, actor=_task_actor())


async def test_download_blob_allows_a_task_principal_holding_the_grant(
    service: BlobService,
    repository: FakeBlobRepository,
    data_store: FakeBlobDataStore,
) -> None:
    """Download a blob the task principal's spec granted it."""
    blob = await create_blob(repository, ACTOR.account.id, data_store=data_store)
    stored, _ = await service.download_blob(blob.id, actor=_task_actor(blob.id))
    assert stored.id == blob.id


async def test_offload_values_under_threshold_stay_inline(
    service: BlobService,
) -> None:
    """Keep a value at or under the offload threshold inline, with no blob id."""
    (result,) = await service.offload_values(
        [Candidate("short", TEXT_MEDIA_TYPE)], ACTOR.account.id
    )
    assert result.value == "short"
    assert result.blob_id is None


async def test_offload_values_keeps_byte_identical_values_apart_by_media_type(
    repository: FakeBlobRepository, data_store: FakeBlobDataStore
) -> None:
    """Store a text value and a byte-identical JSON value as separate blobs."""
    service = BlobService(
        repository=repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: data_store}, BlobStorageBackend.DATABASE
        ),
        max_size_bytes=16,
        offload_threshold_bytes=10,
    )

    padded = "x" * 50
    # The JSON encoding of the plain string equals the raw text of the quoted
    # string, so the two candidates hash to the same sha256 despite carrying
    # different media types.
    text_value = f'"{padded}"'
    json_value = padded

    text_result, json_result = await service.offload_values(
        [
            Candidate(text_value, TEXT_MEDIA_TYPE),
            Candidate(json_value, JSON_MEDIA_TYPE),
        ],
        ACTOR.account.id,
    )
    assert text_result.blob_id is not None
    assert json_result.blob_id is not None
    assert text_result.blob_id != json_result.blob_id
    assert text_result.value is None
    assert json_result.value is None

    text_blob = await repository.get(text_result.blob_id)
    json_blob = await repository.get(json_result.blob_id)
    assert text_blob.sha256 == json_blob.sha256
    assert text_blob.media_type == TEXT_MEDIA_TYPE
    assert json_blob.media_type == JSON_MEDIA_TYPE

    values = await service.hydrate_values([text_result.blob_id, json_result.blob_id])
    assert values[text_result.blob_id] == text_value
    assert values[json_result.blob_id] == json_value


async def test_offload_values_dedupes_identical_values(
    repository: FakeBlobRepository, data_store: FakeBlobDataStore
) -> None:
    """Share one blob between two candidates offloading the same value."""
    service = BlobService(
        repository=repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: data_store}, BlobStorageBackend.DATABASE
        ),
        max_size_bytes=16,
        offload_threshold_bytes=10,
    )
    shared_value = {"a": "i" * 50}
    first, second = await service.offload_values(
        [
            Candidate(shared_value, JSON_MEDIA_TYPE),
            Candidate(shared_value, JSON_MEDIA_TYPE),
        ],
        ACTOR.account.id,
    )
    assert first.blob_id is not None
    assert first.blob_id == second.blob_id


async def test_offload_values_threshold_zero_offloads_every_non_null_value(
    repository: FakeBlobRepository, data_store: FakeBlobDataStore
) -> None:
    """Offload every non-null value when the threshold is zero, leave None inline."""
    service = BlobService(
        repository=repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: data_store}, BlobStorageBackend.DATABASE
        ),
        max_size_bytes=16,
        offload_threshold_bytes=0,
    )
    value_result, null_result = await service.offload_values(
        [Candidate({"a": 1}, JSON_MEDIA_TYPE), Candidate(None, JSON_MEDIA_TYPE)],
        ACTOR.account.id,
    )
    assert value_result.blob_id is not None
    assert value_result.value is None
    assert null_result.blob_id is None
    assert null_result.value is None


async def test_hydrate_values_empty_refs_is_a_no_op(service: BlobService) -> None:
    """Return an empty mapping for an empty batch of refs."""
    assert await service.hydrate_values([]) == {}
