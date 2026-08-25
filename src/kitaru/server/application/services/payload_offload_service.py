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
"""Session and node payload offload to blob storage, and hydration back."""

import asyncio
import hashlib
import json
import uuid
from collections.abc import Sequence
from typing import Any, NamedTuple

from kitaru.server.application.interfaces.blob_data_store import BlobDataStore
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import SessionNode

JSON_MEDIA_TYPE = "application/json"
TEXT_MEDIA_TYPE = "text/plain"


class _Candidate(NamedTuple):
    """Payload value considered for offload."""

    value: Any
    media_type: str


class _Offloaded(NamedTuple):
    """Offload outcome for one candidate."""

    value: Any
    blob_id: uuid.UUID | None


def _serialize(candidate: _Candidate) -> bytes | None:
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


class PayloadOffloadService:
    """Session and node payload offload and hydration."""

    def __init__(
        self,
        repository: BlobRepository,
        data_stores: dict[BlobStorageBackend, BlobDataStore],
        backend: BlobStorageBackend,
        threshold_bytes: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Blob repository.
            data_stores: Content stores keyed by the backend they serve.
            backend: Backend newly offloaded payloads are written to.
            threshold_bytes: Serialized size above which a payload is
                offloaded, 0 offloads every non-null payload.
        """
        self._repository = repository
        self._data_stores = data_stores
        self._backend = backend
        self._threshold_bytes = threshold_bytes

    async def _put_missing(self, data_by_hash: dict[str, bytes]) -> None:
        """Store the content of every hash not already in the registry.

        Args:
            data_by_hash: Serialized bytes keyed by their sha256.
        """
        if not data_by_hash:
            return
        store = self._data_stores[self._backend]
        if self._backend is BlobStorageBackend.DATABASE:
            # The database-backed store shares the request's AsyncSession,
            # which rejects concurrent statement execution, so these puts
            # run one at a time. Every other backend runs concurrently.
            for sha256, data in data_by_hash.items():
                await store.put(sha256, data)
        else:
            await asyncio.gather(
                *(store.put(sha256, data) for sha256, data in data_by_hash.items())
            )

    async def _offload(
        self, candidates: Sequence[_Candidate], owner_id: uuid.UUID
    ) -> list[_Offloaded]:
        """Offload the over-threshold candidates of a batch in one round trip.

        Args:
            candidates: Payload values to consider for offload.
            owner_id: Owner stamped on newly created blob registry rows.

        Returns:
            Per-candidate inline value or blob reference, in input order.
        """
        serialized = [_serialize(candidate) for candidate in candidates]
        offloaded_flags = [
            data is not None
            and (self._threshold_bytes == 0 or len(data) > self._threshold_bytes)
            for data in serialized
        ]
        hashes = [
            hashlib.sha256(data).hexdigest() if data is not None and offloaded else None
            for data, offloaded in zip(serialized, offloaded_flags, strict=True)
        ]

        first_index_by_hash: dict[str, int] = {}
        for index, sha256 in enumerate(hashes):
            if sha256 is not None:
                first_index_by_hash.setdefault(sha256, index)

        registry = await self._repository.get_many_by_sha256s(list(first_index_by_hash))
        missing_hashes = [
            sha256 for sha256 in first_index_by_hash if sha256 not in registry
        ]
        data_by_hash: dict[str, bytes] = {}
        for sha256 in missing_hashes:
            data = serialized[first_index_by_hash[sha256]]
            # Every hash in missing_hashes came from a candidate that
            # serialized to non-None bytes.
            assert data is not None
            data_by_hash[sha256] = data
        await self._put_missing(data_by_hash)
        for sha256 in missing_hashes:
            index = first_index_by_hash[sha256]
            blob, _ = await self._repository.create(
                Blob(
                    owner_id=owner_id,
                    sha256=sha256,
                    size=len(data_by_hash[sha256]),
                    media_type=candidates[index].media_type,
                    stored_in=self._backend,
                )
            )
            registry[sha256] = blob

        results: list[_Offloaded] = []
        for candidate, offloaded, sha256 in zip(
            candidates, offloaded_flags, hashes, strict=True
        ):
            if offloaded:
                assert sha256 is not None
                results.append(_Offloaded(value=None, blob_id=registry[sha256].id))
            else:
                results.append(_Offloaded(value=candidate.value, blob_id=None))
        return results

    async def offload_session(self, session: Session) -> Session:
        """Offload a session's inputs and outputs above the configured threshold.

        Returns:
            Session with inputs and outputs offloaded above threshold.
        """
        inputs, outputs = await self._offload(
            [
                _Candidate(session.inputs, JSON_MEDIA_TYPE),
                _Candidate(session.outputs, JSON_MEDIA_TYPE),
            ],
            session.owner_id,
        )
        return session.model_copy(
            update={
                "inputs": inputs.value,
                "inputs_blob_id": inputs.blob_id,
                "outputs": outputs.value,
                "outputs_blob_id": outputs.blob_id,
            }
        )

    async def offload_session_outputs(self, session: Session) -> Session:
        """Offload a session's outputs above the configured threshold.

        Inputs are immutable after creation and stay untouched.

        Returns:
            Session with outputs offloaded above threshold.
        """
        (outputs,) = await self._offload(
            [_Candidate(session.outputs, JSON_MEDIA_TYPE)], session.owner_id
        )
        return session.model_copy(
            update={"outputs": outputs.value, "outputs_blob_id": outputs.blob_id}
        )

    async def offload_nodes(
        self, nodes: list[SessionNode], owner_id: uuid.UUID
    ) -> list[SessionNode]:
        """Offload reasoning, inputs, outputs, and attributes above threshold.

        Args:
            nodes: Nodes to offload, in batch order.
            owner_id: Owner stamped on newly created blob registry rows.

        Returns:
            Nodes with payloads offloaded above threshold, in input order.
        """
        if not nodes:
            return []
        candidates: list[_Candidate] = []
        for node in nodes:
            candidates.append(_Candidate(node.reasoning, TEXT_MEDIA_TYPE))
            candidates.append(_Candidate(node.inputs, JSON_MEDIA_TYPE))
            candidates.append(_Candidate(node.outputs, JSON_MEDIA_TYPE))
            candidates.append(_Candidate(node.attributes, JSON_MEDIA_TYPE))
        offloaded = await self._offload(candidates, owner_id)
        result: list[SessionNode] = []
        for index, node in enumerate(nodes):
            reasoning, inputs, outputs, attributes = offloaded[
                4 * index : 4 * index + 4
            ]
            result.append(
                node.model_copy(
                    update={
                        "reasoning": reasoning.value,
                        "reasoning_blob_id": reasoning.blob_id,
                        "inputs": inputs.value,
                        "inputs_blob_id": inputs.blob_id,
                        "outputs": outputs.value,
                        "outputs_blob_id": outputs.blob_id,
                        "attributes": attributes.value,
                        "attributes_blob_id": attributes.blob_id,
                    }
                )
            )
        return result

    async def _hydrate(self, blob_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Any]:
        """Resolve a batch of blob refs to their deserialized values.

        Args:
            blob_ids: Ids of the referenced blobs.

        Raises:
            RuntimeError: No data store is configured for a blob's backend.

        Returns:
            Deserialized values keyed by blob id.
        """
        if not blob_ids:
            return {}
        registry = await self._repository.get_many(blob_ids)
        blobs = [registry[blob_id] for blob_id in blob_ids]
        for blob in blobs:
            if blob.stored_in not in self._data_stores:
                raise RuntimeError(
                    f"No data store configured for backend {blob.stored_in}"
                )

        database_blobs = [
            b for b in blobs if b.stored_in is BlobStorageBackend.DATABASE
        ]
        other_blobs = [
            b for b in blobs if b.stored_in is not BlobStorageBackend.DATABASE
        ]

        async def _get_database_blobs_sequentially() -> list[bytes]:
            # The database-backed store shares the request's AsyncSession,
            # which rejects concurrent statement execution, so these gets
            # run one at a time. Every other backend runs concurrently.
            return [
                await self._data_stores[blob.stored_in].get(blob.sha256)
                for blob in database_blobs
            ]

        database_data, other_data = await asyncio.gather(
            _get_database_blobs_sequentially(),
            asyncio.gather(
                *(
                    self._data_stores[blob.stored_in].get(blob.sha256)
                    for blob in other_blobs
                )
            ),
        )
        data_by_blob_id = {
            blob.id: data
            for blob, data in zip(database_blobs, database_data, strict=True)
        } | {blob.id: data for blob, data in zip(other_blobs, other_data, strict=True)}
        return {blob.id: _deserialize(blob, data_by_blob_id[blob.id]) for blob in blobs}

    async def hydrate_sessions(self, sessions: list[Session]) -> list[Session]:
        """Resolve inputs and outputs refs across a batch of sessions.

        Returns:
            Sessions with inputs and outputs filled and refs cleared.
        """
        blob_ids = {
            blob_id
            for session in sessions
            for blob_id in (session.inputs_blob_id, session.outputs_blob_id)
            if blob_id is not None
        }
        values = await self._hydrate(list(blob_ids))
        result: list[Session] = []
        for session in sessions:
            update: dict[str, Any] = {}
            if session.inputs_blob_id is not None:
                update["inputs"] = values[session.inputs_blob_id]
                update["inputs_blob_id"] = None
            if session.outputs_blob_id is not None:
                update["outputs"] = values[session.outputs_blob_id]
                update["outputs_blob_id"] = None
            result.append(session.model_copy(update=update) if update else session)
        return result

    async def hydrate_nodes(self, nodes: list[SessionNode]) -> list[SessionNode]:
        """Resolve reasoning, inputs, outputs, and attributes refs across nodes.

        Returns:
            Nodes with payloads filled and refs cleared.
        """
        blob_ids = {
            blob_id
            for node in nodes
            for blob_id in (
                node.reasoning_blob_id,
                node.inputs_blob_id,
                node.outputs_blob_id,
                node.attributes_blob_id,
            )
            if blob_id is not None
        }
        values = await self._hydrate(list(blob_ids))
        result: list[SessionNode] = []
        for node in nodes:
            update: dict[str, Any] = {}
            if node.reasoning_blob_id is not None:
                update["reasoning"] = values[node.reasoning_blob_id]
                update["reasoning_blob_id"] = None
            if node.inputs_blob_id is not None:
                update["inputs"] = values[node.inputs_blob_id]
                update["inputs_blob_id"] = None
            if node.outputs_blob_id is not None:
                update["outputs"] = values[node.outputs_blob_id]
                update["outputs_blob_id"] = None
            if node.attributes_blob_id is not None:
                update["attributes"] = values[node.attributes_blob_id]
                update["attributes_blob_id"] = None
            result.append(node.model_copy(update=update) if update else node)
        return result
