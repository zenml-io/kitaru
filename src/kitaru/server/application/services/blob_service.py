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
"""Blob use cases."""

import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any, NamedTuple

from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.resource_access import check_task_blob_read
from kitaru.server.domain.blob import Blob, BlobStorageBackend, BlobTooLarge

JSON_MEDIA_TYPE = "application/json"
TEXT_MEDIA_TYPE = "text/plain"


class Candidate(NamedTuple):
    """Payload value considered for offload."""

    value: Any
    media_type: str


class Offloaded(NamedTuple):
    """Offload outcome for one candidate."""

    value: Any
    blob_id: uuid.UUID | None


class _Serialized(NamedTuple):
    """Serialized outcome for one candidate."""

    data: bytes | None
    # None when the candidate's value stays inline.
    sha256: str | None


def _serialize(candidate: Candidate) -> bytes | None:
    """Serialize a candidate's value to the bytes it would be stored as.

    Args:
        candidate: Value and media type to serialize.

    Returns:
        Serialized bytes, or ``None`` when the value is ``None``.
    """
    if candidate.value is None:
        return None
    if candidate.media_type == TEXT_MEDIA_TYPE:
        return candidate.value.encode("utf-8")
    return json.dumps(candidate.value, separators=(",", ":")).encode("utf-8")


def _deserialize(blob: Blob, data: bytes) -> Any:
    """Deserialize stored bytes back into a payload value by media type.

    Args:
        blob: Registry row the bytes were stored under.
        data: Stored bytes.

    Returns:
        Deserialized payload value.
    """
    if blob.media_type == TEXT_MEDIA_TYPE:
        return data.decode("utf-8")
    return json.loads(data)


class BlobService:
    """Content-addressed blob storage."""

    def __init__(
        self,
        repository: BlobRepository,
        data_stores: BlobDataStores,
        max_size_bytes: int,
        offload_threshold_bytes: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Blob repository.
            data_stores: Content stores keyed by the backend they serve.
            max_size_bytes: Upload size cap in bytes.
            offload_threshold_bytes: Serialized size above which a payload
                is offloaded, 0 offloads every non-null payload.
        """
        self._repository = repository
        self._data_stores = data_stores
        self._max_size_bytes = max_size_bytes
        self._offload_threshold_bytes = offload_threshold_bytes

    async def upload_blob(
        self,
        content: AsyncIterator[bytes],
        media_type: str,
        actor: AuthContext,
    ) -> tuple[Blob, bool]:
        """Upload a blob, deduping identical content and media type.

        The content is hashed and buffered chunk by chunk, so the cap holds
        the whole way through and the buffer is used directly for the store
        put without a second read. The put runs before the registry write,
        so a failed request orphans content instead of dangling a ref.

        Args:
            content: Upload content, read in chunks.
            media_type: Content media type.
            actor: Caller context.

        Raises:
            BlobTooLarge: The upload exceeds the configured size cap.

        Returns:
            Stored blob and whether this call created it.
        """
        hasher = hashlib.sha256()
        buffer = bytearray()
        size = 0
        async for chunk in content:
            size += len(chunk)
            if size > self._max_size_bytes:
                raise BlobTooLarge(self._max_size_bytes)
            hasher.update(chunk)
            buffer.extend(chunk)
        sha256 = hasher.hexdigest()
        await self._data_stores.get_write_store().put(sha256, bytes(buffer))
        blob = Blob(
            owner_id=actor.account.id,
            sha256=sha256,
            size=size,
            media_type=media_type,
            stored_in=self._data_stores.backend,
        )
        return await self._repository.create(blob)

    async def get_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> Blob:
        """Get a blob's metadata by id.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobAccessDenied: A task principal holds no grant for the blob.
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        check_task_blob_read(blob_id, actor)
        return await self._repository.get(blob_id)

    async def download_blob(
        self, blob_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Blob, bytes]:
        """Get a blob's metadata and content by id.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobAccessDenied: A task principal holds no grant for the blob.
            BlobNotFound: No blob has this id.
            RuntimeError: No data store is configured for the blob's backend.

        Returns:
            Stored blob and its content.
        """
        check_task_blob_read(blob_id, actor)
        blob = await self._repository.get(blob_id)
        store = self._data_stores.get_store(blob.stored_in)
        return blob, await store.get(blob.sha256)

    async def delete_blob(self, blob_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a blob.

        Args:
            blob_id: Id of the blob.
            actor: Caller context.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version.
        """
        _ = actor
        await self._repository.delete(blob_id)

    async def _put_missing(self, data_by_hash: dict[str, bytes]) -> None:
        """Store the content of every hash not already in the registry.

        Args:
            data_by_hash: Serialized bytes keyed by their sha256.
        """
        if not data_by_hash:
            return
        store = self._data_stores.get_write_store()
        if self._data_stores.backend is BlobStorageBackend.DATABASE:
            # The database-backed store shares the request's AsyncSession,
            # which rejects concurrent statement execution, so these puts
            # run one at a time. Every other backend runs concurrently.
            for sha256, data in data_by_hash.items():
                await store.put(sha256, data)
        else:
            await asyncio.gather(
                *(store.put(sha256, data) for sha256, data in data_by_hash.items())
            )

    async def offload_values(
        self, values: Sequence[Candidate], owner_id: uuid.UUID
    ) -> list[Offloaded]:
        """Offload the over-threshold values of a batch in one round trip.

        Args:
            values: Payload values to consider for offload.
            owner_id: Owner stamped on newly created blob registry rows.

        Returns:
            Per-value offload outcome, in input order.
        """
        serialized: list[_Serialized] = []
        for candidate in values:
            data = _serialize(candidate)
            offloaded = data is not None and (
                self._offload_threshold_bytes == 0
                or len(data) > self._offload_threshold_bytes
            )
            sha256 = hashlib.sha256(data).hexdigest() if offloaded else None
            serialized.append(_Serialized(data, sha256))

        # Keyed by (sha256, media_type), since two candidates can hash to the
        # same content while carrying different media types.
        first_index_by_key: dict[tuple[str, str], int] = {}
        for index, (candidate, item) in enumerate(zip(values, serialized, strict=True)):
            if item.sha256 is not None:
                first_index_by_key.setdefault(
                    (item.sha256, candidate.media_type), index
                )

        hashes = {sha256 for sha256, _ in first_index_by_key}
        registry = await self._repository.get_many_by_sha256s(list(hashes))
        missing_keys = [key for key in first_index_by_key if key not in registry]
        data_by_hash: dict[str, bytes] = {}
        for key in missing_keys:
            sha256, _ = key
            if sha256 in data_by_hash:
                continue
            data = serialized[first_index_by_key[key]].data
            # Every key in missing_keys came from a candidate that
            # serialized to non-None bytes.
            assert data is not None
            data_by_hash[sha256] = data
        await self._put_missing(data_by_hash)
        for key in missing_keys:
            sha256, media_type = key
            blob, _ = await self._repository.create(
                Blob(
                    owner_id=owner_id,
                    sha256=sha256,
                    size=len(data_by_hash[sha256]),
                    media_type=media_type,
                    stored_in=self._data_stores.backend,
                )
            )
            registry[key] = blob

        results: list[Offloaded] = []
        for candidate, item in zip(values, serialized, strict=True):
            if item.sha256 is not None:
                blob_id = registry[(item.sha256, candidate.media_type)].id
                results.append(Offloaded(value=None, blob_id=blob_id))
            else:
                results.append(Offloaded(value=candidate.value, blob_id=None))
        return results

    async def hydrate_values(self, refs: Sequence[uuid.UUID]) -> dict[uuid.UUID, Any]:
        """Resolve a batch of blob refs to their deserialized values.

        Args:
            refs: Ids of the referenced blobs.

        Raises:
            RuntimeError: No data store is configured for a blob's backend.

        Returns:
            Deserialized values keyed by blob id.
        """
        if not refs:
            return {}
        registry = await self._repository.get_many(refs)
        blobs = [registry[blob_id] for blob_id in refs]
        for blob in blobs:
            self._data_stores.get_store(blob.stored_in)

        database_blobs = [
            b for b in blobs if b.stored_in is BlobStorageBackend.DATABASE
        ]
        other_blobs = [
            b for b in blobs if b.stored_in is not BlobStorageBackend.DATABASE
        ]

        data_by_blob_id: dict[uuid.UUID, bytes] = {}
        # The database-backed store shares the request's AsyncSession, which
        # rejects concurrent statement execution, so these gets run one at a
        # time. Every other backend runs concurrently.
        for blob in database_blobs:
            data_by_blob_id[blob.id] = await self._data_stores.get_store(
                blob.stored_in
            ).get(blob.sha256)
        other_data = await asyncio.gather(
            *(
                self._data_stores.get_store(blob.stored_in).get(blob.sha256)
                for blob in other_blobs
            )
        )
        for blob, data in zip(other_blobs, other_data, strict=True):
            data_by_blob_id[blob.id] = data

        return {blob.id: _deserialize(blob, data_by_blob_id[blob.id]) for blob in blobs}
