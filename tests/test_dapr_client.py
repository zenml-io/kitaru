"""Tests for the Dapr client adapter (Phase 9).

All tests run against in-memory fakes — no Dapr sidecar required.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from _dapr_fakes import (
    FakeWorkflowClient,
    FakeWorkflowState,
    make_store,
    sample_artifact,
    sample_checkpoint,
    sample_log,
    sample_record,
    sample_wait,
)
from kitaru._client._models import ExecutionStatus
from kitaru.engines.dapr.client import (
    DaprClientAdapter,
    _pending_waits_from_ledger,
    _select_dapr_wait,
    _to_dapr_public_status,
)
from kitaru.engines.dapr.models import (
    DAPR_METADATA_NAMESPACE,
    FLOW_RESULT_ARTIFACT_ID_KEY,
    INTERNAL_ARTIFACT_FLAG,
    FailureRecord,
    LogRecord,
    decode_transport_value,
)
from kitaru.errors import (
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)

# ---------------------------------------------------------------------------
# Fake KitaruClient for testing (avoids ZenML dependency)
# ---------------------------------------------------------------------------


class _FakeKitaruClient:
    """Minimal stand-in for KitaruClient in adapter tests."""

    def __init__(self) -> None:
        self._project = "test-project"

    def _load_artifact_value(self, artifact_id: str) -> Any:
        return None

    def _uses_dapr(self) -> bool:
        return True


def _make_adapter(
    *,
    project: str = "test-project",
    workflow_client: FakeWorkflowClient | None = None,
) -> tuple[DaprClientAdapter, Any, FakeWorkflowClient | None]:
    """Create an adapter with a fake-backed store and optional workflow client."""
    store, _fake_state = make_store(project=project)
    adapter = DaprClientAdapter(store=store, workflow_client=workflow_client)
    return adapter, store, workflow_client


# ---------------------------------------------------------------------------
# Status mapping tests
# ---------------------------------------------------------------------------


class TestStatusMapping:
    def test_pending_maps_to_running(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status="pending",
                workflow_status=None,
                has_pending_waits=False,
            )
            == ExecutionStatus.RUNNING
        )

    def test_running_maps_to_running(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="running",
                has_pending_waits=False,
            )
            == ExecutionStatus.RUNNING
        )

    def test_running_with_pending_waits_maps_to_waiting(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="running",
                has_pending_waits=True,
            )
            == ExecutionStatus.WAITING
        )

    def test_completed_maps_to_completed(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="completed",
                has_pending_waits=False,
            )
            == ExecutionStatus.COMPLETED
        )

    def test_failed_maps_to_failed(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status="failed",
                workflow_status=None,
                has_pending_waits=False,
            )
            == ExecutionStatus.FAILED
        )

    def test_terminated_maps_to_cancelled(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="terminated",
                has_pending_waits=False,
            )
            == ExecutionStatus.CANCELLED
        )

    def test_suspended_without_waits_maps_to_running(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="suspended",
                has_pending_waits=False,
            )
            == ExecutionStatus.RUNNING
        )

    def test_suspended_with_waits_maps_to_waiting(self) -> None:
        assert (
            _to_dapr_public_status(
                ledger_status=None,
                workflow_status="suspended",
                has_pending_waits=True,
            )
            == ExecutionStatus.WAITING
        )


# ---------------------------------------------------------------------------
# Wait helper tests
# ---------------------------------------------------------------------------


class TestWaitHelpers:
    def test_pending_waits_filters_resolved(self) -> None:
        record = sample_record(
            waits=(
                sample_wait(wait_id="w1", name="approval", status="pending"),
                sample_wait(wait_id="w2", name="review", status="resolved"),
            ),
        )
        pending = _pending_waits_from_ledger(record)
        assert len(pending) == 1
        assert pending[0].wait_id == "w1"

    def test_select_wait_by_name(self) -> None:
        record = sample_record(
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        pending = _pending_waits_from_ledger(record)
        selected = _select_dapr_wait(exec_id="exec-1", wait="approval", pending=pending)
        assert selected.wait_id == "w1"

    def test_select_wait_by_id(self) -> None:
        record = sample_record(
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        pending = _pending_waits_from_ledger(record)
        selected = _select_dapr_wait(exec_id="exec-1", wait="w1", pending=pending)
        assert selected.wait_id == "w1"

    def test_select_wait_ambiguous(self) -> None:
        record = sample_record(
            waits=(
                sample_wait(wait_id="w1", name="approval", status="pending"),
                sample_wait(wait_id="w2", name="approval", status="pending"),
            ),
        )
        pending = _pending_waits_from_ledger(record)
        with pytest.raises(KitaruStateError, match="Multiple"):
            _select_dapr_wait(exec_id="exec-1", wait="approval", pending=pending)

    def test_select_wait_not_found(self) -> None:
        record = sample_record(
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        pending = _pending_waits_from_ledger(record)
        with pytest.raises(KitaruStateError, match="no pending wait"):
            _select_dapr_wait(exec_id="exec-1", wait="unknown", pending=pending)


# ---------------------------------------------------------------------------
# Adapter execution tests
# ---------------------------------------------------------------------------


class TestGetExecution:
    def test_running_execution(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            flow_name="my_flow",
            status="running",
            created_at=datetime(2025, 1, 1, tzinfo=UTC),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert execution.exec_id == "exec-1"
        assert execution.flow_name == "my_flow"
        assert execution.status == ExecutionStatus.RUNNING
        assert execution.stack_name is None

    def test_execution_with_pending_wait_shows_waiting(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            status="running",
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert execution.status == ExecutionStatus.WAITING
        assert execution.pending_wait is not None
        assert execution.pending_wait.name == "approval"

    def test_execution_with_workflow_status_override(self) -> None:
        wf_client = FakeWorkflowClient()
        wf_client.states["exec-1"] = FakeWorkflowState(
            instance_id="exec-1",
            runtime_status="completed",
        )
        adapter, store, _ = _make_adapter(workflow_client=wf_client)
        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert execution.status == ExecutionStatus.COMPLETED

    def test_execution_falls_back_to_ledger_when_workflow_missing(self) -> None:
        wf_client = FakeWorkflowClient()
        adapter, store, _ = _make_adapter(workflow_client=wf_client)
        record = sample_record(exec_id="exec-1", status="completed")
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert execution.status == ExecutionStatus.COMPLETED

    def test_execution_with_checkpoints_and_artifacts(self) -> None:
        adapter, store, _ = _make_adapter()
        art = sample_artifact(
            artifact_id="art-1",
            name="output",
            producing_call_id="cp-1",
        )
        cp = sample_checkpoint(
            call_id="cp-1",
            name="step_a",
            status="completed",
            artifacts=(art,),
        )
        record = sample_record(
            exec_id="exec-1",
            checkpoints=(cp,),
            artifacts=(art,),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert len(execution.checkpoints) == 1
        assert execution.checkpoints[0].name == "step_a"
        assert len(execution.artifacts) == 1
        assert execution.artifacts[0].name == "output"

    def test_failed_execution_has_failure_info(self) -> None:
        adapter, store, _ = _make_adapter()
        failure = FailureRecord(message="boom", exception_type="ValueError")
        record = sample_record(exec_id="exec-1", status="failed", failure=failure)
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("exec-1", client)
        assert execution.status == ExecutionStatus.FAILED
        assert execution.failure is not None
        assert execution.failure.message == "boom"


# ---------------------------------------------------------------------------
# List / latest tests
# ---------------------------------------------------------------------------


class TestListExecutions:
    def test_list_returns_reverse_insertion_order(self) -> None:
        adapter, store, _ = _make_adapter()
        for i in range(3):
            store.create_execution(sample_record(exec_id=f"exec-{i}"))

        client = _FakeKitaruClient()
        executions = adapter.list_executions(client)
        assert [e.exec_id for e in executions] == ["exec-2", "exec-1", "exec-0"]

    def test_list_filters_by_flow(self) -> None:
        adapter, store, _ = _make_adapter()
        store.create_execution(sample_record(exec_id="e1", flow_name="flow_a"))
        store.create_execution(sample_record(exec_id="e2", flow_name="flow_b"))

        client = _FakeKitaruClient()
        results = adapter.list_executions(client, flow="flow_a")
        assert len(results) == 1
        assert results[0].flow_name == "flow_a"

    def test_list_filters_by_status(self) -> None:
        adapter, store, _ = _make_adapter()
        store.create_execution(sample_record(exec_id="e1", status="completed"))
        store.create_execution(sample_record(exec_id="e2", status="running"))

        client = _FakeKitaruClient()
        results = adapter.list_executions(client, status=ExecutionStatus.COMPLETED)
        assert len(results) == 1
        assert results[0].exec_id == "e1"

    def test_list_applies_limit(self) -> None:
        adapter, store, _ = _make_adapter()
        for i in range(5):
            store.create_execution(sample_record(exec_id=f"exec-{i}"))

        client = _FakeKitaruClient()
        results = adapter.list_executions(client, limit=2)
        assert len(results) == 2


# ---------------------------------------------------------------------------
# Wait resolution tests
# ---------------------------------------------------------------------------


class TestWaitResolution:
    def test_input_sends_event_with_wait_id(self) -> None:
        wf_client = FakeWorkflowClient()
        wf_client.states["exec-1"] = FakeWorkflowState(instance_id="exec-1")
        adapter, store, _ = _make_adapter(workflow_client=wf_client)

        record = sample_record(
            exec_id="exec-1",
            status="running",
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        adapter.resolve_wait("exec-1", client, wait="approval", value="yes")

        assert len(wf_client.events) == 1
        instance_id, event_name, payload = wf_client.events[0]
        assert (instance_id, event_name) == ("exec-1", "w1")
        assert decode_transport_value(payload["__kitaru_transport"]) == "yes"

    def test_abort_sends_abort_envelope(self) -> None:
        wf_client = FakeWorkflowClient()
        wf_client.states["exec-1"] = FakeWorkflowState(instance_id="exec-1")
        adapter, store, _ = _make_adapter(workflow_client=wf_client)

        record = sample_record(
            exec_id="exec-1",
            status="running",
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        adapter.abort_wait("exec-1", client, wait="approval")

        assert len(wf_client.events) == 1
        _, _, data = wf_client.events[0]
        assert data == {"__kitaru_resolution": "abort"}

    def test_input_fails_with_no_pending_waits(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruStateError, match="no pending waits"):
            adapter.resolve_wait("exec-1", client, wait="whatever", value="x")


# ---------------------------------------------------------------------------
# Cancel tests
# ---------------------------------------------------------------------------


class TestCancelExecution:
    def test_cancel_terminates_workflow(self) -> None:
        wf_client = FakeWorkflowClient()
        wf_client.states["exec-1"] = FakeWorkflowState(instance_id="exec-1")
        adapter, store, _ = _make_adapter(workflow_client=wf_client)

        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        result = adapter.cancel_execution("exec-1", client)
        assert result.status == ExecutionStatus.CANCELLED
        assert "exec-1" in wf_client.terminated


# ---------------------------------------------------------------------------
# Resume tests
# ---------------------------------------------------------------------------


class TestResumeExecution:
    def test_resume_fails_with_pending_waits(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            status="suspended",
            waits=(sample_wait(wait_id="w1", name="approval", status="pending"),),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruStateError, match="Resolve pending wait"):
            adapter.resume_execution("exec-1", client)

    def test_resume_fails_when_not_suspended(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruStateError, match="Only suspended"):
            adapter.resume_execution("exec-1", client)

    def test_resume_succeeds_for_suspended(self) -> None:
        wf_client = FakeWorkflowClient()
        wf_client.states["exec-1"] = FakeWorkflowState(
            instance_id="exec-1", runtime_status="suspended"
        )
        adapter, store, _ = _make_adapter(workflow_client=wf_client)
        record = sample_record(exec_id="exec-1", status="suspended")
        store.create_execution(record)

        client = _FakeKitaruClient()
        adapter.resume_execution("exec-1", client)
        assert "exec-1" in wf_client.resumed


# ---------------------------------------------------------------------------
# Retry tests
# ---------------------------------------------------------------------------


class TestRetryExecution:
    def test_retry_requires_failed_status(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruStateError, match="Only failed"):
            adapter.retry_execution("exec-1", client)

    def test_retry_requires_workflow_name(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="failed")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="workflow name"):
            adapter.retry_execution("exec-1", client)

    def test_retry_requires_persisted_input(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            status="failed",
            workflow_name="test_flow",
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="inputs"):
            adapter.retry_execution("exec-1", client)

    def test_retry_schedules_new_workflow(self) -> None:
        wf_client = FakeWorkflowClient()
        adapter, store, _ = _make_adapter(workflow_client=wf_client)
        record = sample_record(
            exec_id="exec-1",
            flow_name="my_flow",
            workflow_name="test_flow",
            status="failed",
        )
        store.create_execution(record)
        store.store_execution_input("exec-1", {"args": (), "kwargs": {"x": 1}})

        client = _FakeKitaruClient()
        result = adapter.retry_execution("exec-1", client)

        assert len(wf_client.scheduled) == 1
        scheduled = wf_client.scheduled[0]
        assert scheduled["workflow_name"] == "test_flow"
        assert scheduled["input"]["exec_id"] == result.exec_id
        stored_input = store.load_execution_input(result.exec_id)
        assert stored_input["original_exec_id"] == "exec-1"
        assert result.original_exec_id == "exec-1"


# ---------------------------------------------------------------------------
# Replay tests
# ---------------------------------------------------------------------------


class TestReplayExecution:
    def test_replay_requires_non_running(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="running")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruStateError, match="non-running"):
            adapter.replay_execution("exec-1", client, from_="step_a")

    def test_replay_requires_workflow_name(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1", status="completed")
        store.create_execution(record)

        client = _FakeKitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="workflow name"):
            adapter.replay_execution("exec-1", client, from_="step_a")

    def test_replay_schedules_new_seeded_workflow(self) -> None:
        wf_client = FakeWorkflowClient()
        adapter, store, _ = _make_adapter(workflow_client=wf_client)

        art = sample_artifact(
            artifact_id="art-1",
            name="output",
            producing_call_id="step_a:0",
        )
        cp_a = sample_checkpoint(
            call_id="step_a:0",
            name="step_a",
            status="completed",
            started_at=datetime(2025, 1, 1, tzinfo=UTC),
            artifacts=(art,),
        )
        cp_b = sample_checkpoint(
            call_id="step_b:0",
            name="step_b",
            status="completed",
            started_at=datetime(2025, 1, 2, tzinfo=UTC),
        )
        record = sample_record(
            exec_id="exec-1",
            flow_name="my_flow",
            workflow_name="test_flow",
            status="completed",
            checkpoints=(cp_a, cp_b),
            artifacts=(art,),
        )
        store.create_execution(record)
        store.store_execution_input("exec-1", {"args": (), "kwargs": {"x": 1}})
        store.store_artifact("exec-1", art, 42)

        client = _FakeKitaruClient()
        adapter.replay_execution("exec-1", client, from_="step_b")

        assert len(wf_client.scheduled) == 1
        new_exec_id = wf_client.scheduled[0]["instance_id"]
        payload = store.load_execution_input(new_exec_id)
        assert payload["original_exec_id"] == "exec-1"
        assert payload["replay_seed"]["source_exec_id"] == "exec-1"
        assert "step_a:0" in payload["replay_seed"]["seeded_results"]
        assert payload["replay_seed"]["seeded_results"]["step_a:0"] == 42


# ---------------------------------------------------------------------------
# Logs tests
# ---------------------------------------------------------------------------


class TestLogs:
    def test_logs_returns_sorted_entries(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            logs=(
                sample_log(
                    message="second",
                    source="step",
                    timestamp=datetime(2025, 1, 2, tzinfo=UTC),
                ),
                sample_log(
                    message="first",
                    source="step",
                    timestamp=datetime(2025, 1, 1, tzinfo=UTC),
                ),
            ),
        )
        store.create_execution(record)

        entries = adapter.get_logs("exec-1")
        assert len(entries) == 2
        assert entries[0].message == "first"
        assert entries[1].message == "second"

    def test_logs_filters_by_source(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            logs=(
                sample_log(message="step log", source="step"),
                sample_log(message="runner log", source="runner"),
            ),
        )
        store.create_execution(record)

        entries = adapter.get_logs("exec-1", source="runner")
        assert len(entries) == 1
        assert entries[0].message == "runner log"

    def test_logs_filters_by_checkpoint(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            logs=(
                sample_log(message="from A", source="step", checkpoint_name="step_a"),
                sample_log(message="from B", source="step", checkpoint_name="step_b"),
            ),
        )
        store.create_execution(record)

        entries = adapter.get_logs("exec-1", checkpoint="step_a")
        assert len(entries) == 1
        assert entries[0].message == "from A"

    def test_logs_applies_limit(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="exec-1",
            logs=tuple(sample_log(message=f"msg-{i}", source="step") for i in range(5)),
        )
        store.create_execution(record)

        entries = adapter.get_logs("exec-1", limit=2)
        assert len(entries) == 2

    def test_logs_rejects_checkpoint_with_runner_source(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="exec-1")
        store.create_execution(record)

        with pytest.raises(KitaruUsageError, match="cannot be combined"):
            adapter.get_logs("exec-1", source="runner", checkpoint="step_a")


# ---------------------------------------------------------------------------
# Artifact tests
# ---------------------------------------------------------------------------


class TestArtifacts:
    def test_get_artifact_ref_resolves_producing_call(self) -> None:
        adapter, store, _ = _make_adapter()
        art = sample_artifact(
            artifact_id="art-1",
            name="output",
            producing_call_id="cp-1",
            exec_id="exec-1",
        )
        cp = sample_checkpoint(
            call_id="cp-1",
            name="my_step",
            artifacts=(art,),
        )
        record = sample_record(
            exec_id="exec-1",
            checkpoints=(cp,),
            artifacts=(art,),
        )
        store.create_execution(record)
        store.store_artifact("exec-1", art, "hello world")

        client = _FakeKitaruClient()
        ref = adapter.get_artifact_ref("art-1", client)
        assert ref.producing_call == "my_step"
        assert ref.name == "output"

    def test_load_artifact_value(self) -> None:
        adapter, store, _ = _make_adapter()
        art = sample_artifact(artifact_id="art-1", name="output")
        record = sample_record(exec_id="exec-1", artifacts=(art,))
        store.create_execution(record)
        store.store_artifact("exec-1", art, {"key": "value"})

        value = adapter.load_artifact_value("art-1")
        assert value == {"key": "value"}


# ---------------------------------------------------------------------------
# Store extension tests (execution input, logs, new model fields)
# ---------------------------------------------------------------------------


class TestStoreExtensions:
    def test_execution_input_round_trip(self) -> None:
        store, _ = make_store()
        record = sample_record(exec_id="exec-1")
        store.create_execution(record)

        payload = {"args": (1, 2), "kwargs": {"x": "hello"}}
        store.store_execution_input("exec-1", payload)
        loaded = store.load_execution_input("exec-1")
        assert loaded["args"] == [1, 2]
        assert loaded["kwargs"] == {"x": "hello"}

    def test_execution_input_not_found(self) -> None:
        store, _ = make_store()
        with pytest.raises(Exception, match="not found"):
            store.load_execution_input("nonexistent")

    def test_append_log_entry(self) -> None:
        store, _ = make_store()
        record = sample_record(exec_id="exec-1")
        store.create_execution(record)

        log = sample_log(message="hello world", source="step")
        store.append_log_entry("exec-1", log)

        loaded = store.get_execution("exec-1")
        assert len(loaded.logs) == 1
        assert loaded.logs[0].message == "hello world"

    def test_workflow_name_round_trip(self) -> None:
        store, _ = make_store()
        record = sample_record(
            exec_id="exec-1",
            workflow_name="test_flow",
        )
        store.create_execution(record)
        loaded = store.get_execution("exec-1")
        assert loaded.workflow_name == "test_flow"

    def test_artifact_exec_id_round_trip(self) -> None:
        store, _ = make_store()
        record = sample_record(exec_id="exec-1")
        store.create_execution(record)

        art = sample_artifact(
            artifact_id="art-1",
            name="output",
            exec_id="exec-1",
        )
        store.store_artifact("exec-1", art, "value")

        loaded_record, loaded_value = store.load_artifact("art-1")
        assert loaded_record.exec_id == "exec-1"
        assert loaded_value == "value"

    def test_log_record_serialization(self) -> None:
        log = LogRecord(
            message="test",
            source="step",
            checkpoint_name="cp1",
            level="INFO",
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            module="mod",
            filename="file.py",
            lineno=42,
        )
        roundtripped = LogRecord.from_dict(log.to_dict())
        assert roundtripped == log


# ═══════════════════════════════════════════════════════════════════════════
# Execution result loading
# ═══════════════════════════════════════════════════════════════════════════


class TestExecutionResultLoading:
    def test_load_execution_result_returns_value(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(
            exec_id="e1",
            metadata={
                DAPR_METADATA_NAMESPACE: {
                    FLOW_RESULT_ARTIFACT_ID_KEY: "e1:flow_result",
                },
            },
        )
        store.create_execution(record)

        result_art = sample_artifact(
            artifact_id="e1:flow_result",
            name="__flow_result__",
            save_type="flow_output",
            metadata={INTERNAL_ARTIFACT_FLAG: True},
        )
        store.store_artifact("e1", result_art, {"answer": 42})

        result = adapter.load_execution_result("e1")
        assert result == {"answer": 42}

    def test_load_execution_result_raises_when_metadata_missing(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="e1", metadata={})
        store.create_execution(record)

        with pytest.raises(KitaruRuntimeError, match="flow result metadata"):
            adapter.load_execution_result("e1")

    def test_load_execution_result_raises_when_no_kitaru_namespace(self) -> None:
        adapter, store, _ = _make_adapter()
        record = sample_record(exec_id="e1", metadata={"other": "data"})
        store.create_execution(record)

        with pytest.raises(KitaruRuntimeError, match="flow result metadata"):
            adapter.load_execution_result("e1")


# ═══════════════════════════════════════════════════════════════════════════
# Internal artifact filtering
# ═══════════════════════════════════════════════════════════════════════════


class TestInternalArtifactFiltering:
    def test_internal_artifacts_hidden_from_execution(self) -> None:
        adapter, store, _ = _make_adapter()

        public_art = sample_artifact(
            artifact_id="a1",
            name="user_data",
            save_type="manual",
            producing_call_id="cp1",
        )
        internal_art = sample_artifact(
            artifact_id="e1:flow_result",
            name="__flow_result__",
            save_type="flow_output",
            metadata={INTERNAL_ARTIFACT_FLAG: True},
        )

        record = sample_record(
            exec_id="e1",
            status="completed",
            checkpoints=(
                sample_checkpoint(
                    call_id="cp1",
                    name="my_step",
                    status="completed",
                    artifacts=(public_art,),
                ),
            ),
            artifacts=(public_art, internal_art),
        )
        store.create_execution(record)
        store.store_artifact("e1", public_art, "user_value")
        store.store_artifact("e1", internal_art, "flow_result_value")

        client = _FakeKitaruClient()
        execution = adapter.get_execution("e1", client, include_details=True)

        # Internal artifact should be filtered out
        artifact_ids = [a.artifact_id for a in execution.artifacts]
        assert "a1" in artifact_ids
        assert "e1:flow_result" not in artifact_ids

    def test_internal_artifacts_hidden_from_checkpoint(self) -> None:
        adapter, store, _ = _make_adapter()

        internal_art = sample_artifact(
            artifact_id="int-art",
            name="__flow_result__",
            metadata={INTERNAL_ARTIFACT_FLAG: True},
        )
        public_art = sample_artifact(
            artifact_id="pub-art",
            name="output",
        )

        record = sample_record(
            exec_id="e1",
            checkpoints=(
                sample_checkpoint(
                    call_id="cp1",
                    name="my_step",
                    status="completed",
                    artifacts=(internal_art, public_art),
                ),
            ),
        )
        store.create_execution(record)

        client = _FakeKitaruClient()
        execution = adapter.get_execution("e1", client, include_details=True)

        cp_artifacts = execution.checkpoints[0].artifacts
        artifact_ids = [a.artifact_id for a in cp_artifacts]
        assert "pub-art" in artifact_ids
        assert "int-art" not in artifact_ids
