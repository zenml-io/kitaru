"""Dapr execution ledger store and artifact persistence.

Provides the ``ExecutionLedgerStore`` protocol and its concrete
``DaprExecutionLedgerStore`` implementation backed by Dapr state
management APIs. Artifact values are serialized with a JSON-first,
cloudpickle-fallback strategy and wrapped in a versioned envelope.

The internal ``_StateStoreAPI`` protocol isolates the Dapr SDK surface,
allowing unit tests to run against an in-memory fake without a sidecar.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol, TypeVar

from kitaru.engines.dapr.models import (
    ArtifactRecord,
    CheckpointAttemptRecord,
    CheckpointCallRecord,
    ExecutionLedgerRecord,
    WaitRecord,
)
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruStateError
from kitaru.inspection import _qualified_type_name

# ---------------------------------------------------------------------------
# Envelope constants
# ---------------------------------------------------------------------------

_ENVELOPE_VERSION = 1
_INLINE_THRESHOLD_DEFAULT = 262_144  # 256 KiB
_MAX_WRITE_RETRIES_DEFAULT = 5

# ---------------------------------------------------------------------------
# State store adapter protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StateItem:
    """Value returned from a state store get operation."""

    data: bytes | None
    etag: str | None = None


class ETagConflict(Exception):
    """Raised when an optimistic concurrency write fails due to etag mismatch."""


class _StateStoreAPI(Protocol):
    """Minimal adapter over a key-value state store.

    Isolates Dapr SDK specifics so tests can substitute an in-memory fake.
    """

    def get(self, *, store_name: str, key: str) -> _StateItem: ...

    def put(
        self,
        *,
        store_name: str,
        key: str,
        data: bytes,
        etag: str | None = None,
    ) -> str | None:
        """Write data, optionally with etag for CAS. Returns new etag.

        Raises ETagConflict when the etag does not match the current value.
        """
        ...


# ---------------------------------------------------------------------------
# Artifact serialization
# ---------------------------------------------------------------------------


def _serialize_value(value: Any) -> tuple[bytes, str]:
    """Serialize a value using JSON-first, cloudpickle-fallback strategy."""
    # Pydantic v2 models
    if hasattr(value, "model_dump") and callable(value.model_dump):
        value = value.model_dump()

    # JSON path
    try:
        blob = json.dumps(value).encode()
        return blob, "json"
    except (TypeError, ValueError):
        pass

    # Cloudpickle fallback
    import cloudpickle

    return cloudpickle.dumps(value), "cloudpickle"


def _deserialize_value(blob: bytes, serializer: str) -> Any:
    """Deserialize a value using the serializer name from the envelope."""
    if serializer == "json":
        return json.loads(blob)
    if serializer == "cloudpickle":
        import cloudpickle

        return cloudpickle.loads(blob)
    raise KitaruRuntimeError(
        f"Unknown artifact serializer {serializer!r}. Expected 'json' or 'cloudpickle'."
    )


def _encode_envelope(
    value: Any,
    *,
    artifact_id: str,
    inline_threshold: int,
) -> tuple[dict[str, Any], bytes | None]:
    """Build the artifact envelope dict and optional separate blob.

    Returns (envelope_dict, large_blob_or_None).
    """
    blob, serializer = _serialize_value(value)
    envelope: dict[str, Any] = {
        "__kitaru_version": _ENVELOPE_VERSION,
        "__kitaru_serializer": serializer,
        "__kitaru_type": _qualified_type_name(value),
    }

    if len(blob) <= inline_threshold:
        envelope["data_b64"] = base64.b64encode(blob).decode()
        return envelope, None

    envelope["blob_ref"] = _blob_key(artifact_id)
    return envelope, blob


def _decode_envelope(
    envelope: dict[str, Any],
    large_blob: bytes | None,
) -> Any:
    """Decode a value from its serialized envelope."""
    version = envelope.get("__kitaru_version")
    if version != _ENVELOPE_VERSION:
        raise KitaruRuntimeError(
            f"Unsupported artifact envelope version {version!r}. "
            f"Expected {_ENVELOPE_VERSION}."
        )

    serializer = envelope["__kitaru_serializer"]
    data_b64 = envelope.get("data_b64")

    if data_b64 is not None:
        blob = base64.b64decode(data_b64)
    elif large_blob is not None:
        blob = large_blob
    else:
        raise KitaruRuntimeError(
            "Artifact envelope has neither inline data nor a blob reference."
        )

    return _deserialize_value(blob, serializer)


# ---------------------------------------------------------------------------
# Key scheme
# ---------------------------------------------------------------------------


def _exec_key(project: str, exec_id: str) -> str:
    return f"kitaru.exec.{project}.{exec_id}"


def _index_key(project: str) -> str:
    return f"kitaru.exec_index.{project}"


def _artifact_key(project: str, artifact_id: str) -> str:
    return f"kitaru.artifact.{project}.{artifact_id}"


def _blob_key(artifact_id: str) -> str:
    return f"kitaru.artifact_blob.{artifact_id}"


# ---------------------------------------------------------------------------
# Store protocol
# ---------------------------------------------------------------------------


class ExecutionLedgerStore(Protocol):
    """Contract for Dapr execution ledger persistence."""

    def create_execution(self, record: ExecutionLedgerRecord) -> None: ...

    def get_execution(self, exec_id: str) -> ExecutionLedgerRecord: ...

    def list_execution_ids(self) -> tuple[str, ...]: ...

    def upsert_checkpoint_call(
        self, exec_id: str, call: CheckpointCallRecord
    ) -> None: ...

    def append_checkpoint_attempt(
        self, exec_id: str, call_id: str, attempt: CheckpointAttemptRecord
    ) -> None: ...

    def upsert_wait(self, exec_id: str, wait: WaitRecord) -> None: ...

    def store_artifact(
        self, exec_id: str, artifact: ArtifactRecord, value: Any
    ) -> None: ...

    def load_artifact(self, artifact_id: str) -> tuple[ArtifactRecord, Any]: ...

    def merge_execution_metadata(
        self, exec_id: str, metadata: dict[str, Any]
    ) -> None: ...

    def merge_checkpoint_metadata(
        self, exec_id: str, call_id: str, metadata: dict[str, Any]
    ) -> None: ...

    def replace_execution(
        self, exec_id: str, record: ExecutionLedgerRecord
    ) -> None: ...


# ---------------------------------------------------------------------------
# Concrete implementation
# ---------------------------------------------------------------------------


class DaprExecutionLedgerStore:
    """Execution ledger backed by a key-value state store.

    Uses optimistic concurrency (CAS via etags) for read-modify-write
    operations on execution records. Artifact payloads are serialized
    with JSON-first, cloudpickle-fallback and split into inline/blob
    storage based on a configurable size threshold.
    """

    def __init__(
        self,
        *,
        project: str,
        ledger_store_name: str,
        state_api: _StateStoreAPI,
        artifact_store_name: str | None = None,
        artifact_inline_threshold_bytes: int = _INLINE_THRESHOLD_DEFAULT,
        max_write_retries: int = _MAX_WRITE_RETRIES_DEFAULT,
    ) -> None:
        self._project = project
        self._ledger_store = ledger_store_name
        self._artifact_store = artifact_store_name or ledger_store_name
        self._state_api = state_api
        self._inline_threshold = artifact_inline_threshold_bytes
        self._max_retries = max_write_retries

    @classmethod
    def from_dapr_client(
        cls,
        *,
        project: str,
        ledger_store_name: str,
        artifact_store_name: str | None = None,
        dapr_client: Any | None = None,
        artifact_inline_threshold_bytes: int = _INLINE_THRESHOLD_DEFAULT,
        max_write_retries: int = _MAX_WRITE_RETRIES_DEFAULT,
    ) -> DaprExecutionLedgerStore:
        """Create a store backed by a real Dapr state client."""
        from kitaru.engines.dapr._dependencies import require_dapr_sdk

        require_dapr_sdk()

        if dapr_client is None:
            from dapr.clients import DaprClient

            dapr_client = DaprClient()

        adapter = _DaprStateStoreAdapter(dapr_client)
        return cls(
            project=project,
            ledger_store_name=ledger_store_name,
            state_api=adapter,
            artifact_store_name=artifact_store_name,
            artifact_inline_threshold_bytes=artifact_inline_threshold_bytes,
            max_write_retries=max_write_retries,
        )

    # -- Execution CRUD -----------------------------------------------------

    def create_execution(self, record: ExecutionLedgerRecord) -> None:
        """Create a new execution record. Idempotent for identical records."""
        key = _exec_key(self._project, record.exec_id)
        item = self._state_api.get(store_name=self._ledger_store, key=key)

        if item.data is not None:
            existing = ExecutionLedgerRecord.from_dict(json.loads(item.data))
            if existing == record:
                return
            raise KitaruStateError(
                f"Execution {record.exec_id!r} already exists with conflicting data."
            )

        data = json.dumps(record.to_dict()).encode()
        self._state_api.put(store_name=self._ledger_store, key=key, data=data)

        self._add_to_index(record.exec_id)

    def get_execution(self, exec_id: str) -> ExecutionLedgerRecord:
        """Retrieve an execution record by ID."""
        key = _exec_key(self._project, exec_id)
        item = self._state_api.get(store_name=self._ledger_store, key=key)
        if item.data is None:
            raise KitaruRuntimeError(f"Execution {exec_id!r} not found in ledger.")
        try:
            return ExecutionLedgerRecord.from_dict(json.loads(item.data))
        except (json.JSONDecodeError, KeyError) as exc:
            raise KitaruRuntimeError(
                f"Corrupt execution record for {exec_id!r}: {exc}"
            ) from exc

    def list_execution_ids(self) -> tuple[str, ...]:
        """Return execution IDs in insertion order."""
        key = _index_key(self._project)
        item = self._state_api.get(store_name=self._ledger_store, key=key)
        if item.data is None:
            return ()
        try:
            ids = json.loads(item.data)
        except json.JSONDecodeError:
            return ()
        return tuple(ids)

    def replace_execution(self, exec_id: str, record: ExecutionLedgerRecord) -> None:
        """Overwrite an execution record wholesale (for finalization)."""
        self._cas_update_execution(exec_id, lambda _: record)

    # -- Nested mutations ---------------------------------------------------

    def upsert_checkpoint_call(self, exec_id: str, call: CheckpointCallRecord) -> None:
        """Insert or replace a checkpoint call record by call_id."""
        self._cas_update_execution(exec_id, lambda rec: _upsert_checkpoint(rec, call))

    def append_checkpoint_attempt(
        self, exec_id: str, call_id: str, attempt: CheckpointAttemptRecord
    ) -> None:
        """Append or replace a checkpoint attempt by attempt_id."""
        self._cas_update_execution(
            exec_id,
            lambda rec: _append_attempt(rec, call_id, attempt),
        )

    def upsert_wait(self, exec_id: str, wait: WaitRecord) -> None:
        """Insert or replace a wait record by wait_id."""
        self._cas_update_execution(exec_id, lambda rec: _upsert_wait_record(rec, wait))

    # -- Metadata merge -----------------------------------------------------

    def merge_execution_metadata(self, exec_id: str, metadata: dict[str, Any]) -> None:
        """Recursively merge metadata into the execution record."""
        self._cas_update_execution(
            exec_id,
            lambda rec: replace(rec, metadata=_deep_merge(rec.metadata, metadata)),
        )

    def merge_checkpoint_metadata(
        self, exec_id: str, call_id: str, metadata: dict[str, Any]
    ) -> None:
        """Recursively merge metadata into a checkpoint call record."""
        self._cas_update_execution(
            exec_id,
            lambda rec: _merge_checkpoint_meta(rec, call_id, metadata),
        )

    # -- Artifact persistence -----------------------------------------------

    def store_artifact(
        self, exec_id: str, artifact: ArtifactRecord, value: Any
    ) -> None:
        """Persist an artifact value and register it in the execution record."""
        envelope, large_blob = _encode_envelope(
            value,
            artifact_id=artifact.artifact_id,
            inline_threshold=self._inline_threshold,
        )
        artifact_state = {
            "record": artifact.to_dict(),
            "envelope": envelope,
        }
        artifact_key = _artifact_key(self._project, artifact.artifact_id)
        self._state_api.put(
            store_name=self._artifact_store,
            key=artifact_key,
            data=json.dumps(artifact_state).encode(),
        )

        if large_blob is not None:
            blob_key = _blob_key(artifact.artifact_id)
            self._state_api.put(
                store_name=self._artifact_store,
                key=blob_key,
                data=large_blob,
            )

        self._cas_update_execution(
            exec_id,
            lambda rec: _register_artifact(rec, artifact),
        )

    def load_artifact(self, artifact_id: str) -> tuple[ArtifactRecord, Any]:
        """Load an artifact record and its deserialized value."""
        artifact_key = _artifact_key(self._project, artifact_id)
        item = self._state_api.get(store_name=self._artifact_store, key=artifact_key)
        if item.data is None:
            raise KitaruRuntimeError(f"Artifact {artifact_id!r} not found in store.")

        try:
            artifact_state = json.loads(item.data)
        except json.JSONDecodeError as exc:
            raise KitaruRuntimeError(
                f"Corrupt artifact state for {artifact_id!r}: {exc}"
            ) from exc

        record = ArtifactRecord.from_dict(artifact_state["record"])
        envelope = artifact_state["envelope"]

        large_blob: bytes | None = None
        blob_ref = envelope.get("blob_ref")
        if blob_ref is not None:
            blob_item = self._state_api.get(
                store_name=self._artifact_store, key=blob_ref
            )
            if blob_item.data is None:
                raise KitaruRuntimeError(
                    f"Artifact blob for {artifact_id!r} not found at key {blob_ref!r}."
                )
            large_blob = blob_item.data

        value = _decode_envelope(envelope, large_blob)
        return record, value

    # -- CAS helpers --------------------------------------------------------

    def _cas_update_execution(
        self,
        exec_id: str,
        mutate: Callable[[ExecutionLedgerRecord], ExecutionLedgerRecord],
    ) -> None:
        """Read-modify-write an execution record with CAS retry."""
        key = _exec_key(self._project, exec_id)

        for attempt in range(self._max_retries):
            item = self._state_api.get(store_name=self._ledger_store, key=key)
            if item.data is None:
                raise KitaruRuntimeError(f"Execution {exec_id!r} not found in ledger.")

            record = ExecutionLedgerRecord.from_dict(json.loads(item.data))
            updated = mutate(record)
            data = json.dumps(updated.to_dict()).encode()

            try:
                self._state_api.put(
                    store_name=self._ledger_store,
                    key=key,
                    data=data,
                    etag=item.etag,
                )
                return
            except ETagConflict:
                if attempt == self._max_retries - 1:
                    raise KitaruBackendError(
                        f"Failed to update execution {exec_id!r} after "
                        f"{self._max_retries} retries due to concurrent "
                        f"writes (etag conflict on key {key!r})."
                    ) from None

    def _add_to_index(self, exec_id: str) -> None:
        """CAS-update the execution index to include exec_id."""
        key = _index_key(self._project)

        for attempt in range(self._max_retries):
            item = self._state_api.get(store_name=self._ledger_store, key=key)
            if item.data is None:
                ids: list[str] = []
                etag = None
            else:
                ids = json.loads(item.data)
                etag = item.etag

            if exec_id in ids:
                return

            ids.append(exec_id)
            data = json.dumps(ids).encode()

            try:
                self._state_api.put(
                    store_name=self._ledger_store,
                    key=key,
                    data=data,
                    etag=etag,
                )
                return
            except ETagConflict:
                if attempt == self._max_retries - 1:
                    raise KitaruBackendError(
                        f"Failed to update execution index after "
                        f"{self._max_retries} retries (etag conflict)."
                    ) from None


# ---------------------------------------------------------------------------
# Mutation helpers (pure functions on records)
# ---------------------------------------------------------------------------


_T = TypeVar("_T")


def _upsert_by_id(items: tuple[_T, ...], new: _T, *, id_attr: str) -> tuple[_T, ...]:
    """Replace or append an item by a unique ID field, preserving insertion order."""
    new_id = getattr(new, id_attr)
    found = False
    updated: list[_T] = []
    for item in items:
        if getattr(item, id_attr) == new_id:
            updated.append(new)
            found = True
        else:
            updated.append(item)
    if not found:
        updated.append(new)
    return tuple(updated)


def _upsert_checkpoint(
    record: ExecutionLedgerRecord, call: CheckpointCallRecord
) -> ExecutionLedgerRecord:
    """Replace or append a checkpoint call, preserving insertion order."""
    return replace(
        record,
        checkpoints=_upsert_by_id(record.checkpoints, call, id_attr="call_id"),
    )


def _sort_attempts(
    attempts: list[CheckpointAttemptRecord],
) -> list[CheckpointAttemptRecord]:
    """Deterministically sort attempts by number, start time, then ID."""

    def sort_key(a: CheckpointAttemptRecord) -> tuple[int, str, str]:
        # None datetimes sort last via a sentinel
        started = a.started_at.isoformat() if a.started_at else "\xff"
        return (a.attempt_number, started, a.attempt_id)

    return sorted(attempts, key=sort_key)


def _append_attempt(
    record: ExecutionLedgerRecord,
    call_id: str,
    attempt: CheckpointAttemptRecord,
) -> ExecutionLedgerRecord:
    """Append or replace an attempt on a checkpoint call."""
    updated_checkpoints: list[CheckpointCallRecord] = []
    found = False

    for cp in record.checkpoints:
        if cp.call_id == call_id:
            found = True
            attempts = [a for a in cp.attempts if a.attempt_id != attempt.attempt_id]
            attempts.append(attempt)
            attempts = _sort_attempts(attempts)
            updated_checkpoints.append(replace(cp, attempts=tuple(attempts)))
        else:
            updated_checkpoints.append(cp)

    if not found:
        raise KitaruStateError(
            f"Cannot append attempt to unknown checkpoint call {call_id!r}."
        )

    return replace(record, checkpoints=tuple(updated_checkpoints))


def _upsert_wait_record(
    record: ExecutionLedgerRecord, wait: WaitRecord
) -> ExecutionLedgerRecord:
    """Replace or append a wait record, preserving insertion order."""
    return replace(
        record,
        waits=_upsert_by_id(record.waits, wait, id_attr="wait_id"),
    )


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge overlay into base. Non-dict values are replaced."""
    result = dict(base)
    for key, value in overlay.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = _deep_merge(existing, value)
        else:
            result[key] = value
    return result


def _merge_checkpoint_meta(
    record: ExecutionLedgerRecord, call_id: str, metadata: dict[str, Any]
) -> ExecutionLedgerRecord:
    """Merge metadata into a checkpoint call within an execution record."""
    updated_checkpoints: list[CheckpointCallRecord] = []
    found = False

    for cp in record.checkpoints:
        if cp.call_id == call_id:
            found = True
            merged = _deep_merge(cp.metadata, metadata)
            updated_checkpoints.append(replace(cp, metadata=merged))
        else:
            updated_checkpoints.append(cp)

    if not found:
        raise KitaruStateError(
            f"Cannot merge metadata on unknown checkpoint call {call_id!r}."
        )

    return replace(record, checkpoints=tuple(updated_checkpoints))


def _register_artifact(
    record: ExecutionLedgerRecord, artifact: ArtifactRecord
) -> ExecutionLedgerRecord:
    """Register an artifact ref in the execution and its producing checkpoint."""
    updated_artifacts = _upsert_by_id(record.artifacts, artifact, id_attr="artifact_id")

    updated_checkpoints = list(record.checkpoints)
    if artifact.producing_call_id is not None:
        found = False
        for i, cp in enumerate(updated_checkpoints):
            if cp.call_id == artifact.producing_call_id:
                found = True
                cp_artifacts = _upsert_by_id(
                    cp.artifacts, artifact, id_attr="artifact_id"
                )
                updated_checkpoints[i] = replace(cp, artifacts=cp_artifacts)
                break
        if not found:
            raise KitaruStateError(
                f"Cannot register artifact on unknown checkpoint call "
                f"{artifact.producing_call_id!r}."
            )

    return replace(
        record,
        checkpoints=tuple(updated_checkpoints),
        artifacts=updated_artifacts,
    )


# ---------------------------------------------------------------------------
# Dapr SDK adapter (only instantiated via from_dapr_client)
# ---------------------------------------------------------------------------


class _DaprStateStoreAdapter:
    """Adapts the real Dapr Python client to _StateStoreAPI."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(self, *, store_name: str, key: str) -> _StateItem:
        resp = self._client.get_state(store_name=store_name, key=key)
        data = resp.data if resp.data else None
        etag = resp.etag if hasattr(resp, "etag") else None
        return _StateItem(data=data, etag=etag)

    def put(
        self,
        *,
        store_name: str,
        key: str,
        data: bytes,
        etag: str | None = None,
    ) -> str | None:
        try:
            self._client.save_state(
                store_name=store_name,
                key=key,
                value=data,
                etag=etag,
            )
        except Exception as exc:
            # Map Dapr concurrency conflict to our internal signal
            exc_str = str(exc).lower()
            if "etag" in exc_str or "conflict" in exc_str:
                raise ETagConflict(str(exc)) from exc
            raise KitaruBackendError(
                f"State store write failed for key {key!r} "
                f"in store {store_name!r}: {exc}"
            ) from exc
        return None
