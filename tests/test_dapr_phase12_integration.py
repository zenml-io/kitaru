"""Phase 12: Dapr backend end-to-end integration tests.

Exercises the full flow → checkpoint → interpreter → store → client
pipeline using fake infrastructure (no real Dapr sidecar needed).

Test matrix from the implementation plan:
1.  Single checkpoint flow
2.  Multi-checkpoint linear flow
3.  Synthetic LLM checkpoint
4.  submit() + result() concurrency
5.  map() fan-out + result()
6.  wait() + external input resolution
7.  wait() + abort
8.  Crash recovery (restart without re-executing completed checkpoints)
9.  Cross-execution replay
10. Client execution hydration (get, list, logs)
11. Client input resolution (public KitaruClient surface)
12. Divergence detection
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import pytest

from kitaru._client._models import ExecutionStatus
from kitaru.engines.dapr.client import DaprClientAdapter
from kitaru.engines.dapr.interpreter import _CURRENT_ORCHESTRATOR_SESSION
from kitaru.engines.dapr.models import LogRecord
from kitaru.errors import FailureOrigin, KitaruDivergenceError
from tests._dapr_harness import DaprPhase12Harness

# Suppress the experimental-engine warning — gating is tested in
# test_engine_registry.py.
pytestmark = pytest.mark.filterwarnings("ignore:.*experimental.*:UserWarning")


# ---------------------------------------------------------------------------
# Test-only flow wrapper (mimics the interface DaprPhase12Harness expects)
# ---------------------------------------------------------------------------


class _FlowWrapper:
    """Minimal wrapper that mimics the interface DaprPhase12Harness expects."""

    def __init__(self, func: Any, name: str, definition: Any) -> None:
        self._func = func
        self._name = name
        self._definition = definition

    def run(self, *args: Any, **kwargs: Any) -> Any:
        return self._definition.run(args=args, kwargs=kwargs)


class _FakeClient:
    """Minimal stand-in for KitaruClient in adapter calls.

    Duck-types the subset of KitaruClient that DaprClientAdapter uses.
    """

    def __init__(self, harness: DaprPhase12Harness) -> None:
        self._harness = harness
        self._dapr_adapter = harness.adapter

    def _get_dapr_adapter(self) -> DaprClientAdapter:
        return self._harness.adapter

    def _uses_dapr(self) -> bool:
        return True

    def _load_artifact_value(self, artifact_id: str) -> Any:
        return self._harness.adapter.load_artifact_value(artifact_id)

    def _load_execution_result(self, exec_id: str) -> Any:
        return self._harness.adapter.load_execution_result(exec_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_harness() -> DaprPhase12Harness:
    return DaprPhase12Harness()


def _wait_for_pending_waits(
    harness: DaprPhase12Harness,
    exec_id: str,
    running: Any = None,
    *,
    timeout: float = 5.0,
    poll: float = 0.05,
) -> None:
    """Poll until at least one pending wait appears in the ledger."""
    from tests._dapr_harness import RunningDaprExecution

    is_running = isinstance(running, RunningDaprExecution)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_running:
            if running.error_box:
                raise running.error_box["error"]
            if running.thread is not None and not running.thread.is_alive():
                raise RuntimeError(
                    f"Background thread died without error for {exec_id}"
                )
        pending = harness.adapter.get_pending_waits(exec_id)
        if pending:
            return
        time.sleep(poll)
    raise TimeoutError(f"No pending waits appeared for {exec_id} within {timeout}s")


def _build_flow(
    harness: DaprPhase12Harness,
    func: Any,
    name: str,
) -> _FlowWrapper:
    """Register a flow and return a harness-compatible wrapper."""
    flow_def = harness.backend.create_flow_definition(
        entrypoint=func, registration_name=name
    )
    return _FlowWrapper(func=func, name=name, definition=flow_def)


# ---------------------------------------------------------------------------
# Scenario 1: Single checkpoint flow
# ---------------------------------------------------------------------------


class TestSingleCheckpointFlow:
    def test_single_checkpoint_completes(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 2,
            registration_name="double",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        cp_def = harness.backend.get_checkpoint_definitions()["double"]

        def my_flow(x: int) -> int:
            return cp_def.call(x)

        wrapper = _build_flow(harness, my_flow, "my_flow")
        exec_id, result = harness.run_flow_sync(wrapper, 21)

        assert result == 42

        record = harness.store.get_execution(exec_id)
        assert record.status == "completed"
        assert len(record.checkpoints) == 1
        assert record.checkpoints[0].name == "double"
        assert record.checkpoints[0].status == "completed"
        # Store deduplicates by attempt_id, so running is replaced by completed
        assert len(record.checkpoints[0].attempts) == 1
        assert record.checkpoints[0].attempts[0].status == "completed"

        execution = harness.adapter.get_execution(exec_id, _FakeClient(harness))
        assert execution.status == ExecutionStatus.COMPLETED
        assert execution.failure is None

        loaded_result = harness.adapter.load_execution_result(exec_id)
        assert loaded_result == 42


# ---------------------------------------------------------------------------
# Scenario 2: Multi-checkpoint linear flow
# ---------------------------------------------------------------------------


class TestMultiCheckpointLinearFlow:
    def test_sequential_checkpoints_complete_in_order(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x, y: x + y,
            registration_name="add",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 3,
            registration_name="multiply",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: f"result={x}",
            registration_name="format",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        defs = harness.backend.get_checkpoint_definitions()

        def pipeline_flow(a: int, b: int) -> str:
            s = defs["add"].call(a, b)
            m = defs["multiply"].call(s)
            return defs["format"].call(m)

        wrapper = _build_flow(harness, pipeline_flow, "pipeline_flow")
        exec_id, result = harness.run_flow_sync(wrapper, 3, 7)

        assert result == "result=30"

        record = harness.store.get_execution(exec_id)
        assert len(record.checkpoints) == 3
        names = [cp.name for cp in record.checkpoints]
        assert names == ["add", "multiply", "format"]
        assert all(cp.status == "completed" for cp in record.checkpoints)


# ---------------------------------------------------------------------------
# Scenario 3: Synthetic LLM checkpoint
# ---------------------------------------------------------------------------


class TestSyntheticLLMCheckpoint:
    def test_llm_style_metadata_and_artifacts(self) -> None:
        harness = _make_harness()

        def llm_checkpoint(prompt: str) -> str:
            from kitaru.engines.dapr.backend import (
                _CURRENT_ACTIVITY_BINDING,
                _PendingManualArtifact,
            )

            binding = _CURRENT_ACTIVITY_BINDING.get()
            if binding is not None:
                binding.store.merge_checkpoint_metadata(
                    binding.request.exec_id,
                    binding.request.call_id,
                    {"model": "gpt-4", "tokens": 42},
                )
                binding.pending_manual_artifacts["context"] = _PendingManualArtifact(
                    name="context",
                    value={"prompt": prompt},
                    kind="json",
                    tags=("llm",),
                )

            return f"Response to: {prompt}"

        harness.backend.create_checkpoint_definition(
            entrypoint=llm_checkpoint,
            registration_name="llm_call",
            retries=0,
            checkpoint_type="llm",
            runtime=None,
        )

        llm_def = harness.backend.get_checkpoint_definitions()["llm_call"]

        def llm_flow(prompt: str) -> str:
            return llm_def.call(prompt)

        wrapper = _build_flow(harness, llm_flow, "llm_flow")
        exec_id, result = harness.run_flow_sync(wrapper, "Hello AI")

        assert result == "Response to: Hello AI"

        record = harness.store.get_execution(exec_id)
        cp = record.checkpoints[0]
        assert cp.checkpoint_type == "llm"
        assert cp.metadata.get("model") == "gpt-4"
        assert cp.metadata.get("tokens") == 42

        manual_arts = [a for a in cp.artifacts if a.save_type == "manual"]
        assert len(manual_arts) == 1
        assert manual_arts[0].name == "context"

        _, loaded = harness.store.load_artifact(manual_arts[0].artifact_id)
        assert loaded == {"prompt": "Hello AI"}


# ---------------------------------------------------------------------------
# Scenario 4: submit() + result() concurrency
# ---------------------------------------------------------------------------


class TestSubmitResultConcurrency:
    def test_submitted_checkpoints_resolve_on_result(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 10,
            registration_name="times_ten",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x + 1,
            registration_name="plus_one",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        defs = harness.backend.get_checkpoint_definitions()

        def concurrent_flow(a: int, b: int) -> tuple[int, int]:
            f1 = defs["times_ten"].submit(a)
            f2 = defs["plus_one"].submit(b)
            return f1.result(), f2.result()

        wrapper = _build_flow(harness, concurrent_flow, "concurrent_flow")
        exec_id, result = harness.run_flow_sync(wrapper, 5, 99)

        assert result == (50, 100)

        record = harness.store.get_execution(exec_id)
        assert len(record.checkpoints) == 2
        assert all(cp.status == "completed" for cp in record.checkpoints)


# ---------------------------------------------------------------------------
# Scenario 5: map() fan-out + result()
# ---------------------------------------------------------------------------


class TestMapFanOut:
    def test_map_expands_and_collects_results(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x**2,
            registration_name="square",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        sq_def = harness.backend.get_checkpoint_definitions()["square"]

        def map_flow(items: list[int]) -> list[int]:
            futures = sq_def.map(items)
            return futures.result()

        wrapper = _build_flow(harness, map_flow, "map_flow")
        exec_id, result = harness.run_flow_sync(wrapper, [2, 3, 4])

        assert result == [4, 9, 16]

        record = harness.store.get_execution(exec_id)
        assert len(record.checkpoints) == 3
        assert all(cp.name == "square" for cp in record.checkpoints)


# ---------------------------------------------------------------------------
# Scenario 6: wait() + external input resolution
# ---------------------------------------------------------------------------


class TestWaitExternalInputResolution:
    def test_wait_resolves_via_client(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x.upper(),
            registration_name="upper",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        upper_def = harness.backend.get_checkpoint_definitions()["upper"]

        def wait_flow() -> str:
            session = _CURRENT_ORCHESTRATOR_SESSION.get()
            assert session is not None
            user_input = session.wait_for_input(
                name="approval", question="Provide input", timeout=30
            )
            return upper_def.call(user_input)

        wrapper = _build_flow(harness, wait_flow, "wait_flow")

        running = harness.run_flow_in_background(
            wrapper,
            wait_resolver_factory=lambda eid: harness.make_wait_controller(eid),
        )

        _wait_for_pending_waits(harness, running.exec_id, running)

        pending = harness.adapter.get_pending_waits(running.exec_id)
        assert len(pending) == 1
        assert pending[0].name == "approval"

        harness.adapter.resolve_wait(
            running.exec_id,
            _FakeClient(harness),
            wait="approval",
            value="hello",
        )

        running.join(timeout=5)

        assert "result" in running.result_box
        assert running.result_box["result"] == "HELLO"


# ---------------------------------------------------------------------------
# Scenario 7: wait() + abort
# ---------------------------------------------------------------------------


class TestWaitAbort:
    def test_abort_sets_wait_status_and_flow_receives_envelope(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: f"processed: {x}",
            registration_name="process",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        proc_def = harness.backend.get_checkpoint_definitions()["process"]

        def abort_flow() -> str:
            session = _CURRENT_ORCHESTRATOR_SESSION.get()
            assert session is not None
            data = session.wait_for_input(
                name="confirm", question="Confirm?", timeout=30
            )
            if isinstance(data, dict) and data.get("__kitaru_resolution") == "abort":
                return proc_def.call("aborted")
            return proc_def.call(str(data))

        wrapper = _build_flow(harness, abort_flow, "abort_flow")

        running = harness.run_flow_in_background(
            wrapper,
            wait_resolver_factory=lambda eid: harness.make_wait_controller(eid),
        )

        _wait_for_pending_waits(harness, running.exec_id, running)

        harness.adapter.abort_wait(
            running.exec_id, _FakeClient(harness), wait="confirm"
        )

        running.join(timeout=5)

        assert "result" in running.result_box
        assert running.result_box["result"] == "processed: aborted"

        record = harness.store.get_execution(running.exec_id)
        wait_records = [w for w in record.waits if w.name == "confirm"]
        assert len(wait_records) == 1
        assert wait_records[0].status == "aborted"


# ---------------------------------------------------------------------------
# Scenario 8: Crash recovery
# ---------------------------------------------------------------------------


class TestCrashRecovery:
    def test_restart_skips_completed_checkpoints(self) -> None:
        """Simulate crash recovery by restarting with seeded results."""
        harness = _make_harness()

        call_counts: dict[str, int] = {"step_a": 0, "step_b": 0}

        def step_a(x: int) -> int:
            call_counts["step_a"] += 1
            return x + 1

        def step_b(x: int) -> int:
            call_counts["step_b"] += 1
            return x * 10

        harness.backend.create_checkpoint_definition(
            entrypoint=step_a,
            registration_name="step_a",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=step_b,
            registration_name="step_b",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        defs = harness.backend.get_checkpoint_definitions()

        def recovery_flow(x: int) -> int:
            val = defs["step_a"].call(x)
            return defs["step_b"].call(val)

        wrapper = _build_flow(harness, recovery_flow, "recovery_flow")

        # Run 1: populate the ledger
        exec_id, result = harness.run_flow_sync(wrapper, 5)
        assert result == 60
        assert call_counts == {"step_a": 1, "step_b": 1}

        # Build replay seed (simulates crash recovery)
        seed = harness.build_replay_seed_from_execution(exec_id)
        assert "step_a:0" in seed.seeded_results

        # Run 2: seeded restart — completed checkpoints should be skipped
        call_counts["step_a"] = 0
        call_counts["step_b"] = 0

        _exec_id2, result2 = harness.run_flow_sync(wrapper, 5, replay_seed=seed)

        assert call_counts["step_a"] == 0  # seeded
        assert result2 == 60


# ---------------------------------------------------------------------------
# Scenario 9: Cross-execution replay
# ---------------------------------------------------------------------------


class TestCrossExecutionReplay:
    def test_replay_creates_new_execution_with_seeded_results(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x + 10,
            registration_name="add_ten",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 2,
            registration_name="double",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        defs = harness.backend.get_checkpoint_definitions()

        def replay_flow(x: int) -> int:
            a = defs["add_ten"].call(x)
            return defs["double"].call(a)

        wrapper = _build_flow(harness, replay_flow, "replay_flow")

        src_exec_id, original_result = harness.run_flow_sync(wrapper, 5)
        assert original_result == 30

        fake_client = _FakeClient(harness)
        replayed = harness.adapter.replay_execution(
            src_exec_id,
            fake_client,
            from_="double",
        )

        assert replayed.exec_id != src_exec_id
        assert replayed.original_exec_id == src_exec_id

        assert len(harness.workflow_client.scheduled) > 0
        payload = harness.store.load_execution_input(replayed.exec_id)
        assert "replay_seed" in payload
        assert payload["replay_seed"]["source_exec_id"] == src_exec_id
        assert payload["replay_seed"]["seeded_results"]["add_ten:0"] == 15


# ---------------------------------------------------------------------------
# Scenario 10: Client execution hydration (get, list, logs)
# ---------------------------------------------------------------------------


class TestClientExecutionHydration:
    def test_get_list_logs_hydration(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda: "done",
            registration_name="noop",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        noop_def = harness.backend.get_checkpoint_definitions()["noop"]

        def hydration_flow() -> str:
            return noop_def.call()

        wrapper = _build_flow(harness, hydration_flow, "hydration_flow")
        exec_id, _ = harness.run_flow_sync(wrapper)

        now = datetime.now(UTC)
        harness.add_log_entries(
            exec_id,
            [
                LogRecord(
                    message="Step started",
                    source="step",
                    checkpoint_name="noop",
                    level="INFO",
                    timestamp=now,
                ),
                LogRecord(
                    message="Step done",
                    source="step",
                    checkpoint_name="noop",
                    level="INFO",
                    timestamp=now,
                ),
            ],
        )

        fake_client = _FakeClient(harness)

        # get
        execution = harness.adapter.get_execution(exec_id, fake_client)
        assert execution.status == ExecutionStatus.COMPLETED
        assert len(execution.checkpoints) == 1
        assert execution.failure is None

        # list
        all_execs = harness.adapter.list_executions(fake_client)
        assert exec_id in [e.exec_id for e in all_execs]

        # list with flow filter
        filtered = harness.adapter.list_executions(fake_client, flow="hydration_flow")
        assert all(e.flow_name == "hydration_flow" for e in filtered)

        # logs
        logs = harness.adapter.get_logs(exec_id, source="step")
        assert len(logs) == 2

        # logs with checkpoint filter
        cp_logs = harness.adapter.get_logs(exec_id, source="step", checkpoint="noop")
        assert len(cp_logs) == 2


# ---------------------------------------------------------------------------
# Scenario 11: Client input resolution (public facade)
# ---------------------------------------------------------------------------


class TestClientInputResolution:
    def test_input_resolves_through_adapter(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda x: x.lower(),
            registration_name="lower",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        lower_def = harness.backend.get_checkpoint_definitions()["lower"]

        def input_flow() -> str:
            session = _CURRENT_ORCHESTRATOR_SESSION.get()
            assert session is not None
            user_val = session.wait_for_input(
                name="user_prompt", question="Enter value", timeout=30
            )
            return lower_def.call(user_val)

        wrapper = _build_flow(harness, input_flow, "input_flow")

        running = harness.run_flow_in_background(
            wrapper,
            wait_resolver_factory=lambda eid: harness.make_wait_controller(eid),
        )

        _wait_for_pending_waits(harness, running.exec_id, running)

        fake_client = _FakeClient(harness)

        # Verify pending waits through adapter (same path KitaruClient uses)
        pending = harness.adapter.get_pending_waits(running.exec_id)
        assert len(pending) == 1
        assert pending[0].name == "user_prompt"

        # Resolve through adapter
        harness.adapter.resolve_wait(
            running.exec_id,
            fake_client,
            wait="user_prompt",
            value="SHOUTING",
        )

        running.join(timeout=5)

        assert "result" in running.result_box
        assert running.result_box["result"] == "shouting"


# ---------------------------------------------------------------------------
# Scenario 12: Divergence detection
# ---------------------------------------------------------------------------


class TestDivergenceDetection:
    def test_divergence_raises_and_finalizes_as_failed(self) -> None:
        harness = _make_harness()

        harness.backend.create_checkpoint_definition(
            entrypoint=lambda: "a",
            registration_name="checkpoint_a",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        harness.backend.create_checkpoint_definition(
            entrypoint=lambda: "b",
            registration_name="checkpoint_b",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        defs = harness.backend.get_checkpoint_definitions()

        # Mutable counter: first interpreter iteration calls checkpoint_a,
        # but after suspend/re-run the counter flips to checkpoint_b at
        # the same op index → divergence.
        call_count = {"n": 0}

        def divergent_flow() -> str:
            call_count["n"] += 1
            if call_count["n"] <= 1:
                return defs["checkpoint_a"].call()
            else:
                return defs["checkpoint_b"].call()

        wrapper = _build_flow(harness, divergent_flow, "divergent_flow")

        with pytest.raises(KitaruDivergenceError, match="divergence"):
            harness.run_flow_sync(wrapper)

        exec_ids = harness.store.list_execution_ids()
        failed_exec_id = None
        for eid in reversed(exec_ids):
            rec = harness.store.get_execution(eid)
            if rec.flow_name == "divergent_flow":
                failed_exec_id = eid
                break

        assert failed_exec_id is not None
        record = harness.store.get_execution(failed_exec_id)
        assert record.status == "failed"
        assert record.failure is not None
        assert record.failure.origin == FailureOrigin.DIVERGENCE

        execution = harness.adapter.get_execution(failed_exec_id, _FakeClient(harness))
        assert execution.status == ExecutionStatus.FAILED
        assert execution.failure is not None
        assert execution.failure.origin == FailureOrigin.DIVERGENCE
