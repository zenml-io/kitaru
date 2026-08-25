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
"""Field-blind payload offload and resolve capability."""

import hashlib
import json
import uuid
from collections.abc import Sequence
from typing import Any

from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.payload import TEXT_MEDIA_TYPE, Payload


def _serialize(payload: Payload) -> bytes:
    """Serialize a payload's value to the bytes it would be stored as.

    Args:
        payload: Payload to serialize.

    Returns:
        Serialized bytes.
    """
    if payload.media_type == TEXT_MEDIA_TYPE:
        return payload.value.encode("utf-8")
    return json.dumps(payload.value, separators=(",", ":")).encode("utf-8")


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


class PayloadStore:
    """Content-addressed offload and resolve for payload values."""

    def __init__(
        self,
        repository: BlobRepository,
        data_stores: BlobDataStores,
        threshold_bytes: int,
    ) -> None:
        """Initialize the store.

        Args:
            repository: Blob repository.
            data_stores: Content stores keyed by the backend they serve.
            threshold_bytes: Serialized size above which a payload is
                offloaded, 0 offloads every payload.
        """
        self._repository = repository
        self._data_stores = data_stores
        self._threshold_bytes = threshold_bytes

    async def offload(self, payloads: Sequence[Payload], owner_id: uuid.UUID) -> None:
        """Offload the over-threshold payloads of a batch in one round trip.

        Payloads that already carry a blob ref are left untouched. An
        offloaded payload keeps its value, with the blob ref set alongside it.

        Args:
            payloads: Payloads to consider for offload.
            owner_id: Owner stamped on newly created blob registry rows.
        """
        candidates = [payload for payload in payloads if payload.blob_id is None]
        if not candidates:
            return

        serialized = [_serialize(payload) for payload in candidates]
        sha256_by_index: dict[int, str] = {
            index: hashlib.sha256(data).hexdigest()
            for index, data in enumerate(serialized)
            if self._threshold_bytes == 0 or len(data) > self._threshold_bytes
        }
        if not sha256_by_index:
            return

        # Keyed by (sha256, media_type), since two payloads can hash to the
        # same content while carrying different media types.
        first_index_by_key: dict[tuple[str, str], int] = {}
        for index, sha256 in sha256_by_index.items():
            media_type = candidates[index].media_type
            # A payload with no blob id was built via json() or text(),
            # which always set the media type.
            assert media_type is not None
            first_index_by_key.setdefault((sha256, media_type), index)

        registry = await self._repository.get_many_by_sha256s(
            list({sha256 for sha256, _ in first_index_by_key})
        )
        missing_keys = [key for key in first_index_by_key if key not in registry]
        data_by_hash: dict[str, bytes] = {}
        for key in missing_keys:
            sha256, _ = key
            data_by_hash.setdefault(sha256, serialized[first_index_by_key[key]])
        await self._data_stores.get_write_store().put_many(data_by_hash)

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

        for index, sha256 in sha256_by_index.items():
            payload = candidates[index]
            media_type = payload.media_type
            assert media_type is not None
            payload.blob_id = registry[(sha256, media_type)].id

    async def resolve(self, payloads: Sequence[Payload]) -> None:
        """Resolve every unresolved ref of a batch in one round trip per backend.

        Already-resolved payloads are left untouched, making this idempotent.

        Args:
            payloads: Payloads to resolve.

        Raises:
            RuntimeError: A blob's backend has no configured data store.
        """
        candidates: list[tuple[Payload, uuid.UUID]] = []
        for payload in payloads:
            if payload.blob_id is not None and not payload.resolved:
                candidates.append((payload, payload.blob_id))
        if not candidates:
            return

        registry = await self._repository.get_many(
            [blob_id for _, blob_id in candidates]
        )

        blobs_by_backend: dict[BlobStorageBackend, list[Blob]] = {}
        for _, blob_id in candidates:
            blob = registry[blob_id]
            blobs_by_backend.setdefault(blob.stored_in, []).append(blob)

        data_by_sha256: dict[str, bytes] = {}
        for backend, blobs in blobs_by_backend.items():
            store = self._data_stores.get_store(backend)
            data_by_sha256.update(await store.get_many([blob.sha256 for blob in blobs]))

        for payload, blob_id in candidates:
            blob = registry[blob_id]
            payload.value = _deserialize(blob, data_by_sha256[blob.sha256])
            payload.media_type = blob.media_type
