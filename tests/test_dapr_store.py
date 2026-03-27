"""Tests for the Dapr execution ledger store and models.

Uses an in-memory fake _StateStoreAPI to test store logic without
a Dapr sidecar. Covers import boundaries, CRUD, idempotency,
artifact serialization, metadata merge, and CAS retry behavior.
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import UTC, datetime
from typing import Any

import pytest

from _dapr_fakes import FakeStateStore
from _dapr_fakes import make_store as _make_store
from _dapr_fakes import sample_artifact as _sample_artifact
from _dapr_fakes import sample_attempt as _sample_attempt
from _dapr_fakes import sample_checkpoint as _sample_checkpoint
from _dapr_fakes import sample_record as _sample_record
from _dapr_fakes import sample_wait as _sample_wait
from kitaru.engines.dapr.models import (
    ArtifactRecord,
    CheckpointAttemptRecord,
    CheckpointCallRecord,
    ExecutionLedgerRecord,
    FailureRecord,
    WaitRecord,
)
from kitaru.engines.dapr.store import (
    DaprExecutionLedgerStore,
    ETagConflict,
    _decode_envelope,
    _serialize_value,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruRuntimeError,
    KitaruStateError,
)

# ═══════════════════════════════════════════════════════════════════════════
# Import boundary tests
# ═══════════════════════════════════════════════════════════════════════════


class TestImportBoundary:
    def test_models_import_without_dapr_sdk(self) -> None:
        """models.py should import without the Dapr SDK installed."""
        mod = importlib.import_module("kitaru.engines.dapr.models")
        assert hasattr(mod, "ExecutionLedgerRecord")

    def test_store_import_without_dapr_sdk(self) -> None:
        """store.py should import without the Dapr SDK installed."""
        mod = importlib.import_module("kitaru.engines.dapr.store")
        assert hasattr(mod, "DaprExecutionLedgerStore")

    def test_from_dapr_client_without_sdk_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """from_dapr_client() without Dapr SDK should raise ImportError."""
        monkeypatch.setitem(sys.modules, "dapr", None)
        monkeypatch.setitem(sys.modules, "dapr.clients", None)

        with pytest.raises(ImportError, match=r"kitaru\[dapr\]"):
            DaprExecutionLedgerStore.from_dapr_client(
                project="test",
                ledger_store_name="store",
            )


# ═══════════════════════════════════════════════════════════════════════════
# Model round-trip tests
# ═══════════════════════════════════════════════════════════════════════════


class TestModelRoundTrip:
    def test_failure_record_round_trip(self) -> None:
        original = FailureRecord(
            message="boom",
            exception_type="RuntimeError",
            traceback="line 42",
            origin=FailureOrigin.USER_CODE,
        )
        assert FailureRecord.from_dict(original.to_dict()) == original

    def test_artifact_record_round_trip(self) -> None:
        original = ArtifactRecord(
            artifact_id="art-1",
            name="output",
            kind="response",
            save_type="step_output",
            producing_call_id="call-1",
            metadata={"tokens": 42},
        )
        assert ArtifactRecord.from_dict(original.to_dict()) == original

    def test_checkpoint_attempt_record_round_trip(self) -> None:
        now = datetime.now(UTC)
        original = CheckpointAttemptRecord(
            attempt_id="att-1",
            attempt_number=2,
            status="completed",
            started_at=now,
            ended_at=now,
            metadata={"retry_reason": "timeout"},
            failure=FailureRecord(message="fail"),
        )
        assert CheckpointAttemptRecord.from_dict(original.to_dict()) == original

    def test_checkpoint_call_record_round_trip(self) -> None:
        now = datetime.now(UTC)
        attempt = CheckpointAttemptRecord(
            attempt_id="att-1",
            attempt_number=1,
            status="completed",
            started_at=now,
        )
        artifact = ArtifactRecord(
            artifact_id="art-1", name="output", save_type="step_output"
        )
        original = CheckpointCallRecord(
            call_id="call-1",
            invocation_id="inv-1",
            name="my_step",
            checkpoint_type="llm",
            status="completed",
            started_at=now,
            ended_at=now,
            metadata={"model": "gpt-4"},
            original_call_id="orig-1",
            upstream_call_ids=("up-1", "up-2"),
            attempts=(attempt,),
            artifacts=(artifact,),
        )
        assert CheckpointCallRecord.from_dict(original.to_dict()) == original

    def test_wait_record_round_trip(self) -> None:
        now = datetime.now(UTC)
        original = WaitRecord(
            wait_id="w-1",
            name="approval",
            status="resolved",
            question="Do you approve?",
            schema={"type": "boolean"},
            metadata={"source": "slack"},
            entered_at=now,
            resolved_at=now,
        )
        assert WaitRecord.from_dict(original.to_dict()) == original

    def test_execution_ledger_record_round_trip(self) -> None:
        now = datetime.now(UTC)
        original = ExecutionLedgerRecord(
            exec_id="exec-1",
            project="my-project",
            backend="dapr",
            flow_name="my_flow",
            status="completed",
            created_at=now,
            updated_at=now,
            ended_at=now,
            original_exec_id="orig-exec",
            metadata={"cost": 0.05},
            status_reason="success",
            frozen_execution_spec={"model": "gpt-4"},
            checkpoints=(
                CheckpointCallRecord(
                    call_id="c1",
                    invocation_id="i1",
                    name="step1",
                ),
            ),
            artifacts=(ArtifactRecord(artifact_id="a1", name="out"),),
            waits=(WaitRecord(wait_id="w1", name="approval"),),
            failure=FailureRecord(message="final failure"),
        )
        assert ExecutionLedgerRecord.from_dict(original.to_dict()) == original

    def test_from_dict_provides_defaults_for_missing_fields(self) -> None:
        """from_dict() should handle sparse data gracefully."""
        minimal = {"exec_id": "x", "project": "p"}
        record = ExecutionLedgerRecord.from_dict(minimal)
        assert record.exec_id == "x"
        assert record.checkpoints == ()
        assert record.artifacts == ()
        assert record.waits == ()
        assert record.metadata == {}
        assert record.failure is None


# ═══════════════════════════════════════════════════════════════════════════
# Execution CRUD tests
# ═══════════════════════════════════════════════════════════════════════════


class TestExecutionCRUD:
    def test_create_and_get_round_trips(self) -> None:
        store, _ = _make_store()
        record = _sample_record(
            flow_name="my_flow",
            status="running",
            created_at=datetime.now(UTC),
        )
        store.create_execution(record)
        retrieved = store.get_execution(record.exec_id)
        assert retrieved == record

    def test_duplicate_identical_create_is_noop(self) -> None:
        store, _ = _make_store()
        record = _sample_record()
        store.create_execution(record)
        store.create_execution(record)  # Should not raise
        assert store.get_execution(record.exec_id) == record

    def test_conflicting_duplicate_create_raises(self) -> None:
        store, _ = _make_store()
        record1 = _sample_record(exec_id="same-id")
        record2 = _sample_record(exec_id="same-id", status="running")
        store.create_execution(record1)

        with pytest.raises(KitaruStateError, match="conflicting"):
            store.create_execution(record2)

    def test_get_missing_execution_raises(self) -> None:
        store, _ = _make_store()
        with pytest.raises(KitaruRuntimeError, match="not found"):
            store.get_execution("nonexistent")

    def test_list_execution_ids_returns_insertion_order(self) -> None:
        store, _ = _make_store()
        ids = ["exec-a", "exec-b", "exec-c"]
        for eid in ids:
            store.create_execution(_sample_record(exec_id=eid))
        assert store.list_execution_ids() == tuple(ids)

    def test_list_execution_ids_empty(self) -> None:
        store, _ = _make_store()
        assert store.list_execution_ids() == ()

    def test_list_execution_ids_no_duplicates(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="dup")
        store.create_execution(record)
        # Duplicate create is idempotent; index should still have one entry
        store.create_execution(record)
        assert store.list_execution_ids() == ("dup",)


# ═══════════════════════════════════════════════════════════════════════════
# Checkpoint and attempt tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCheckpointOperations:
    def test_upsert_checkpoint_appends(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        cp = _sample_checkpoint(call_id="c1")
        store.upsert_checkpoint_call("e1", cp)

        updated = store.get_execution("e1")
        assert len(updated.checkpoints) == 1
        assert updated.checkpoints[0].call_id == "c1"

    def test_upsert_checkpoint_replaces_by_call_id(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        cp1 = _sample_checkpoint(call_id="c1", status="running")
        store.upsert_checkpoint_call("e1", cp1)

        cp2 = _sample_checkpoint(
            call_id="c1",
            name=cp1.name,
            invocation_id=cp1.invocation_id,
            status="completed",
        )
        store.upsert_checkpoint_call("e1", cp2)

        updated = store.get_execution("e1")
        assert len(updated.checkpoints) == 1
        assert updated.checkpoints[0].status == "completed"

    def test_upsert_checkpoint_preserves_insertion_order(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c1"))
        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c2"))
        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c3"))

        updated = store.get_execution("e1")
        ids = [cp.call_id for cp in updated.checkpoints]
        assert ids == ["c1", "c2", "c3"]

    def test_append_attempt_dedupes_by_attempt_id(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)
        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c1"))

        att = _sample_attempt(attempt_id="a1", status="running")
        store.append_checkpoint_attempt("e1", "c1", att)

        att_done = _sample_attempt(attempt_id="a1", status="completed")
        store.append_checkpoint_attempt("e1", "c1", att_done)

        updated = store.get_execution("e1")
        cp = updated.checkpoints[0]
        assert len(cp.attempts) == 1
        assert cp.attempts[0].status == "completed"

    def test_append_attempt_sorts_deterministically(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)
        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c1"))

        now = datetime.now(UTC)
        att2 = _sample_attempt(attempt_id="a2", attempt_number=2, started_at=now)
        att1 = _sample_attempt(attempt_id="a1", attempt_number=1, started_at=now)

        # Insert out of order
        store.append_checkpoint_attempt("e1", "c1", att2)
        store.append_checkpoint_attempt("e1", "c1", att1)

        updated = store.get_execution("e1")
        cp = updated.checkpoints[0]
        assert [a.attempt_number for a in cp.attempts] == [1, 2]

    def test_append_attempt_on_missing_call_raises(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        att = _sample_attempt()
        with pytest.raises(KitaruStateError, match="unknown checkpoint"):
            store.append_checkpoint_attempt("e1", "missing-call", att)


# ═══════════════════════════════════════════════════════════════════════════
# Wait tests
# ═══════════════════════════════════════════════════════════════════════════


class TestWaitOperations:
    def test_upsert_wait_appends(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        wait = _sample_wait(wait_id="w1")
        store.upsert_wait("e1", wait)

        updated = store.get_execution("e1")
        assert len(updated.waits) == 1
        assert updated.waits[0].wait_id == "w1"

    def test_upsert_wait_replaces_by_wait_id(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        wait_pending = _sample_wait(wait_id="w1", status="pending")
        store.upsert_wait("e1", wait_pending)

        wait_resolved = _sample_wait(
            wait_id="w1",
            status="resolved",
            resolved_at=datetime.now(UTC),
        )
        store.upsert_wait("e1", wait_resolved)

        updated = store.get_execution("e1")
        assert len(updated.waits) == 1
        assert updated.waits[0].status == "resolved"


# ═══════════════════════════════════════════════════════════════════════════
# Artifact persistence tests
# ═══════════════════════════════════════════════════════════════════════════


class TestArtifactPersistence:
    def test_json_inline_round_trip(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        value = {"key": "value", "count": 42}
        store.store_artifact("e1", artifact, value)

        loaded_record, loaded_value = store.load_artifact("a1")
        assert loaded_record == artifact
        assert loaded_value == value

    def test_cloudpickle_fallback_round_trip(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        value = frozenset([1, 2, 3])  # Not JSON-serializable
        store.store_artifact("e1", artifact, value)

        _, loaded_value = store.load_artifact("a1")
        assert loaded_value == value

    def test_pydantic_model_serializes_via_json(self) -> None:
        from pydantic import BaseModel

        class MyModel(BaseModel):
            name: str
            score: float

        blob, serializer = _serialize_value(MyModel(name="test", score=0.9))
        assert serializer == "json"
        assert json.loads(blob) == {"name": "test", "score": 0.9}

    def test_large_blob_ref_path(self) -> None:
        """Artifacts exceeding threshold should use blob_ref storage."""
        store, _fake = _make_store(inline_threshold=10)
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        value = "x" * 100  # Bigger than 10 bytes
        store.store_artifact("e1", artifact, value)

        _, loaded_value = store.load_artifact("a1")
        assert loaded_value == value

    def test_artifact_registered_in_execution(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        store.store_artifact("e1", artifact, "data")

        updated = store.get_execution("e1")
        assert len(updated.artifacts) == 1
        assert updated.artifacts[0].artifact_id == "a1"

    def test_artifact_registered_on_checkpoint(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)
        store.upsert_checkpoint_call("e1", _sample_checkpoint(call_id="c1"))

        artifact = _sample_artifact(artifact_id="a1", producing_call_id="c1")
        store.store_artifact("e1", artifact, "data")

        updated = store.get_execution("e1")
        assert len(updated.checkpoints[0].artifacts) == 1
        assert updated.checkpoints[0].artifacts[0].artifact_id == "a1"

    def test_duplicate_artifact_does_not_create_duplicate_refs(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        store.store_artifact("e1", artifact, "data1")
        store.store_artifact("e1", artifact, "data2")

        updated = store.get_execution("e1")
        assert len(updated.artifacts) == 1

    def test_artifact_with_unknown_producing_call_raises(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1", producing_call_id="missing-call")
        with pytest.raises(KitaruStateError, match="unknown checkpoint"):
            store.store_artifact("e1", artifact, "data")

    def test_load_missing_artifact_raises(self) -> None:
        store, _ = _make_store()
        with pytest.raises(KitaruRuntimeError, match="not found"):
            store.load_artifact("nonexistent")

    def test_missing_blob_ref_raises(self) -> None:
        """If the envelope has a blob_ref but the blob is missing, error."""
        store, fake = _make_store(inline_threshold=10)
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        artifact = _sample_artifact(artifact_id="a1")
        store.store_artifact("e1", artifact, "x" * 100)

        # Remove the blob manually
        blob_key = "kitaru.artifact_blob.a1"
        del fake._data[("test-ledger", blob_key)]

        with pytest.raises(KitaruRuntimeError, match=r"blob.*not found"):
            store.load_artifact("a1")


# ═══════════════════════════════════════════════════════════════════════════
# Envelope edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestEnvelopeEdgeCases:
    def test_unknown_serializer_raises(self) -> None:
        envelope = {
            "__kitaru_version": 1,
            "__kitaru_serializer": "msgpack",
            "__kitaru_type": "builtins.str",
            "data_b64": "dGVzdA==",
        }
        with pytest.raises(KitaruRuntimeError, match="Unknown artifact serializer"):
            _decode_envelope(envelope, None)

    def test_unsupported_version_raises(self) -> None:
        envelope = {
            "__kitaru_version": 999,
            "__kitaru_serializer": "json",
            "__kitaru_type": "builtins.str",
            "data_b64": "dGVzdA==",
        }
        with pytest.raises(KitaruRuntimeError, match=r"Unsupported.*version"):
            _decode_envelope(envelope, None)

    def test_envelope_missing_data_and_blob_raises(self) -> None:
        envelope = {
            "__kitaru_version": 1,
            "__kitaru_serializer": "json",
            "__kitaru_type": "builtins.str",
        }
        with pytest.raises(KitaruRuntimeError, match="neither inline data"):
            _decode_envelope(envelope, None)

    def test_none_value_serializes_via_json(self) -> None:
        blob, serializer = _serialize_value(None)
        assert serializer == "json"
        assert json.loads(blob) is None

    def test_list_value_serializes_via_json(self) -> None:
        blob, serializer = _serialize_value([1, 2, 3])
        assert serializer == "json"
        assert json.loads(blob) == [1, 2, 3]


# ═══════════════════════════════════════════════════════════════════════════
# CAS retry tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCASRetry:
    def test_single_conflict_is_retried_successfully(self) -> None:
        """A single etag conflict should be retried and succeed."""
        store, _ = _make_store(conflict_keys={"kitaru.exec.test-project.e1"})
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        # This triggers a CAS update which will hit the one-time conflict
        cp = _sample_checkpoint(call_id="c1")
        store.upsert_checkpoint_call("e1", cp)

        updated = store.get_execution("e1")
        assert len(updated.checkpoints) == 1

    def test_repeated_conflict_raises_backend_error(self) -> None:
        """Exhausting retries should raise KitaruBackendError."""

        class AlwaysConflictStore(FakeStateStore):
            def put(self, **kwargs: Any) -> str | None:
                if kwargs.get("etag") is not None:
                    raise ETagConflict("always conflict")
                return super().put(**kwargs)

        fake = AlwaysConflictStore()
        store = DaprExecutionLedgerStore(
            project="test-project",
            ledger_store_name="test-ledger",
            state_api=fake,
            max_write_retries=3,
        )

        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        cp = _sample_checkpoint(call_id="c1")
        with pytest.raises(KitaruBackendError, match="retries"):
            store.upsert_checkpoint_call("e1", cp)


# ═══════════════════════════════════════════════════════════════════════════
# Metadata merge tests
# ═══════════════════════════════════════════════════════════════════════════


class TestMetadataMerge:
    def test_merge_execution_metadata_adds_new_keys(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1", metadata={"a": 1})
        store.create_execution(record)

        store.merge_execution_metadata("e1", {"b": 2})

        updated = store.get_execution("e1")
        assert updated.metadata == {"a": 1, "b": 2}

    def test_merge_execution_metadata_overwrites_scalar(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1", metadata={"a": 1})
        store.create_execution(record)

        store.merge_execution_metadata("e1", {"a": 99})

        updated = store.get_execution("e1")
        assert updated.metadata == {"a": 99}

    def test_merge_execution_metadata_deep_merges_dicts(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1", metadata={"nested": {"x": 1, "y": 2}})
        store.create_execution(record)

        store.merge_execution_metadata("e1", {"nested": {"y": 99, "z": 3}})

        updated = store.get_execution("e1")
        assert updated.metadata == {"nested": {"x": 1, "y": 99, "z": 3}}

    def test_merge_checkpoint_metadata_adds_new_keys(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)
        cp = _sample_checkpoint(call_id="c1", metadata={"model": "gpt-4"})
        store.upsert_checkpoint_call("e1", cp)

        store.merge_checkpoint_metadata("e1", "c1", {"tokens": 100})

        updated = store.get_execution("e1")
        assert updated.checkpoints[0].metadata == {
            "model": "gpt-4",
            "tokens": 100,
        }

    def test_merge_checkpoint_metadata_deep_merges(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)
        cp = _sample_checkpoint(call_id="c1", metadata={"usage": {"input": 10}})
        store.upsert_checkpoint_call("e1", cp)

        store.merge_checkpoint_metadata("e1", "c1", {"usage": {"output": 20}})

        updated = store.get_execution("e1")
        assert updated.checkpoints[0].metadata == {"usage": {"input": 10, "output": 20}}

    def test_merge_checkpoint_metadata_missing_call_raises(self) -> None:
        store, _ = _make_store()
        record = _sample_record(exec_id="e1")
        store.create_execution(record)

        with pytest.raises(KitaruStateError, match="unknown checkpoint"):
            store.merge_checkpoint_metadata("e1", "missing", {"a": 1})
