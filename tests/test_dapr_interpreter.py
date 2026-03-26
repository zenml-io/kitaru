"""Tests for the Dapr wrapper workflow interpreter (Phase 8).

Covers: multi-checkpoint flows, fan-out (submit/map/product), wait
resolution, crash recovery via replay seed, divergence detection,
metadata buffering, and future resolution.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from _dapr_fakes import make_store, sample_record
from kitaru.engines.dapr.backend import (
    DaprCheckpointActivityRequest,
    DaprExecutionEngineBackend,
    _run_checkpoint_activity,
)
from kitaru.engines.dapr.interpreter import (
    DaprNativeMapFuture,
    DaprNativeStepFuture,
    DaprOrchestratorSession,
    FlowSuspendRequested,
    ReplaySeed,
    _dapr_flow_dispatch_scope,
    _fingerprint_key,
    _is_dapr_flow_dispatch_allowed,
    _OpFingerprint,
    _WaitSuspendPayload,
    interpret_flow,
)
from kitaru.engines.dapr.store import ExecutionLedgerStore
from kitaru.errors import KitaruDivergenceError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(
    exec_id: str | None = None,
    flow_name: str = "test_flow",
    replay_seed: ReplaySeed | None = None,
) -> tuple[DaprOrchestratorSession, ExecutionLedgerStore]:
    """Create a session backed by a fake store."""
    store, _fake = make_store()
    eid = exec_id or str(uuid4())
    store.create_execution(sample_record(exec_id=eid, flow_name=flow_name))
    session = DaprOrchestratorSession(
        exec_id=eid,
        flow_name=flow_name,
        store=store,
        replay_seed=replay_seed,
    )
    return session, store


def _make_activity_executor(
    store: ExecutionLedgerStore,
    entrypoints: dict[str, Any] | None = None,
) -> Any:
    """Build an activity executor that calls _run_checkpoint_activity."""
    ep_map = entrypoints or {}

    def executor(request: DaprCheckpointActivityRequest) -> Any:
        entrypoint = ep_map.get(request.checkpoint_name)
        if entrypoint is None:
            raise RuntimeError(
                f"No entrypoint registered for {request.checkpoint_name!r}"
            )
        return _run_checkpoint_activity(request, entrypoint=entrypoint, store=store)

    return executor


# ---------------------------------------------------------------------------
# FlowSuspendRequested
# ---------------------------------------------------------------------------


class TestFlowSuspendRequested:
    def test_attributes(self) -> None:
        exc = FlowSuspendRequested(op_index=3, kind="checkpoint", payload="x")
        assert exc.op_index == 3
        assert exc.kind == "checkpoint"
        assert exc.payload == "x"

    def test_is_base_exception(self) -> None:
        assert issubclass(FlowSuspendRequested, BaseException)
        assert not issubclass(FlowSuspendRequested, Exception)

    def test_str_contains_op_and_kind(self) -> None:
        exc = FlowSuspendRequested(op_index=7, kind="wait")
        assert "op=7" in str(exc)
        assert "wait" in str(exc)


# ---------------------------------------------------------------------------
# Fingerprint / divergence
# ---------------------------------------------------------------------------


class TestFingerprint:
    def test_same_fingerprint_same_key(self) -> None:
        fp1 = _OpFingerprint(kind="checkpoint", name="a", call_id="a:0")
        fp2 = _OpFingerprint(kind="checkpoint", name="a", call_id="a:0")
        assert _fingerprint_key(fp1) == _fingerprint_key(fp2)

    def test_different_name_different_key(self) -> None:
        fp1 = _OpFingerprint(kind="checkpoint", name="a", call_id="a:0")
        fp2 = _OpFingerprint(kind="checkpoint", name="b", call_id="a:0")
        assert _fingerprint_key(fp1) != _fingerprint_key(fp2)


# ---------------------------------------------------------------------------
# DaprOrchestratorSession — unit tests
# ---------------------------------------------------------------------------


class TestSessionCallCheckpoint:
    def test_first_call_suspends(self) -> None:
        session, _store = _make_session()
        with pytest.raises(FlowSuspendRequested) as exc_info:
            session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(42,),
                kwargs={},
            )
        assert exc_info.value.kind == "checkpoint"

    def test_resolved_call_returns_cached(self) -> None:
        session, _store = _make_session()
        session.record_resolution("step_a:0", 99)
        result = session.call_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(42,),
            kwargs={},
        )
        assert result == 99

    def test_user_provided_call_id(self) -> None:
        session, _store = _make_session()
        session.record_resolution("my_custom_id", "hello")
        result = session.call_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(),
            kwargs={},
            call_id="my_custom_id",
        )
        assert result == "hello"

    def test_deterministic_call_ids(self) -> None:
        """Same checkpoint called twice gets deterministic IDs."""
        session, _store = _make_session()
        with pytest.raises(FlowSuspendRequested) as exc_info:
            session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(),
                kwargs={},
            )
        payload = exc_info.value.payload
        assert payload.call_id == "step_a:0"

        session.record_resolution("step_a:0", 1)
        with pytest.raises(FlowSuspendRequested) as exc_info:
            session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(),
                kwargs={},
            )
        payload = exc_info.value.payload
        assert payload.call_id == "step_a:1"


class TestSessionSubmitCheckpoint:
    def test_submit_returns_future(self) -> None:
        session, _store = _make_session()
        future = session.submit_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(1,),
            kwargs={},
        )
        assert isinstance(future, DaprNativeStepFuture)
        assert future.running()

    def test_resolved_submit_returns_resolved_future(self) -> None:
        session, _store = _make_session()
        session.record_resolution("step_a:0", 42)
        future = session.submit_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(1,),
            kwargs={},
        )
        assert not future.running()
        assert future.result() == 42


class TestSessionMapCheckpoint:
    def test_map_returns_map_future(self) -> None:
        session, _store = _make_session()
        map_future = session.map_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            mapped_args=([1, 2, 3],),
            kwargs={},
        )
        assert isinstance(map_future, DaprNativeMapFuture)
        assert len(map_future) == 3

    def test_empty_map(self) -> None:
        session, _store = _make_session()
        map_future = session.map_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            mapped_args=([],),
            kwargs={},
        )
        assert len(map_future) == 0

    def test_no_args_returns_empty(self) -> None:
        session, _store = _make_session()
        map_future = session.map_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            mapped_args=(),
            kwargs={},
        )
        assert len(map_future) == 0


class TestSessionProductCheckpoint:
    def test_product_cartesian(self) -> None:
        session, _store = _make_session()
        map_future = session.product_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            product_args=([1, 2], ["a", "b"]),
            kwargs={},
        )
        # 2x2 = 4 combinations
        assert len(map_future) == 4


class TestSessionWait:
    def test_first_wait_suspends(self) -> None:
        session, _store = _make_session()
        with pytest.raises(FlowSuspendRequested) as exc_info:
            session.wait_for_input(timeout=60)
        assert exc_info.value.kind == "wait"

    def test_resolved_wait_returns_value(self) -> None:
        session, _store = _make_session()
        session.record_wait_resolution("approval", "yes")
        result = session.wait_for_input(name="approval", timeout=60)
        assert result == "yes"


class TestSessionDivergence:
    def test_divergence_on_name_change(self) -> None:
        session, _store = _make_session()
        session.record_resolution("step_a:0", 1)
        # First iteration: call step_a
        session.call_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(),
            kwargs={},
        )
        # Reset for second iteration
        session.reset_for_replay()
        # Second iteration: call step_b at same op_index → divergence
        session.record_resolution("step_b:0", 2)
        with pytest.raises(KitaruDivergenceError, match="divergence"):
            session.call_checkpoint(
                checkpoint_name="step_b",
                checkpoint_type=None,
                retry_policy=None,
                args=(),
                kwargs={},
            )


class TestSessionMetadata:
    def test_buffer_and_drain(self) -> None:
        session, _store = _make_session()
        assert not session.has_buffered_metadata()
        session.buffer_metadata({"key": "value"})
        assert session.has_buffered_metadata()
        drained = session.drain_metadata()
        assert drained == [{"key": "value"}]
        assert not session.has_buffered_metadata()


class TestSessionReplaySeed:
    def test_seeded_call_returns_immediately(self) -> None:
        seed = ReplaySeed(
            source_exec_id="old-exec",
            seeded_results={"step_a:0": 42},
        )
        session, _store = _make_session(replay_seed=seed)
        result = session.call_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(),
            kwargs={},
        )
        assert result == 42

    def test_non_seeded_call_suspends(self) -> None:
        seed = ReplaySeed(
            source_exec_id="old-exec",
            seeded_results={"step_a:0": 42},
        )
        session, _store = _make_session(replay_seed=seed)
        # step_a:0 is seeded, but step_b:0 is not
        session.call_checkpoint(
            checkpoint_name="step_a",
            checkpoint_type=None,
            retry_policy=None,
            args=(),
            kwargs={},
        )
        with pytest.raises(FlowSuspendRequested):
            session.call_checkpoint(
                checkpoint_name="step_b",
                checkpoint_type=None,
                retry_policy=None,
                args=(),
                kwargs={},
            )


# ---------------------------------------------------------------------------
# Native future types
# ---------------------------------------------------------------------------


class TestDaprNativeStepFuture:
    def test_running_when_unresolved(self) -> None:
        session, _store = _make_session()
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        assert future.running()

    def test_not_running_when_resolved(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c1", 99)
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        assert not future.running()

    def test_result_returns_value(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c1", 42)
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        assert future.result() == 42

    def test_result_suspends_when_unresolved(self) -> None:
        session, _store = _make_session()
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        with pytest.raises(FlowSuspendRequested) as exc_info:
            future.result()
        assert exc_info.value.kind == "future_join"

    def test_load_delegates_to_result(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c1", "data")
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        assert future.load() == "data"

    def test_wait_passes_when_resolved(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c1", 1)
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        future.wait()  # Should not raise

    def test_wait_suspends_when_unresolved(self) -> None:
        session, _store = _make_session()
        future = DaprNativeStepFuture(
            invocation_id="inv1", call_id="c1", session=session
        )
        with pytest.raises(FlowSuspendRequested):
            future.wait()


class TestDaprNativeMapFuture:
    def test_len(self) -> None:
        session, _store = _make_session()
        futures = [
            DaprNativeStepFuture(
                invocation_id=f"inv{i}", call_id=f"c{i}", session=session
            )
            for i in range(3)
        ]
        mf = DaprNativeMapFuture(futures=futures)
        assert len(mf) == 3

    def test_running(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c0", 10)
        futures = [
            DaprNativeStepFuture(invocation_id="inv0", call_id="c0", session=session),
            DaprNativeStepFuture(invocation_id="inv1", call_id="c1", session=session),
        ]
        mf = DaprNativeMapFuture(futures=futures)
        assert mf.running()  # c1 still unresolved

        session.record_resolution("c1", 20)
        assert not mf.running()

    def test_result(self) -> None:
        session, _store = _make_session()
        session.record_resolution("c0", 10)
        session.record_resolution("c1", 20)
        futures = [
            DaprNativeStepFuture(invocation_id="inv0", call_id="c0", session=session),
            DaprNativeStepFuture(invocation_id="inv1", call_id="c1", session=session),
        ]
        mf = DaprNativeMapFuture(futures=futures)
        assert mf.result() == [10, 20]

    def test_iter(self) -> None:
        session, _store = _make_session()
        futures = [
            DaprNativeStepFuture(invocation_id="inv0", call_id="c0", session=session),
        ]
        mf = DaprNativeMapFuture(futures=futures)
        collected = list(mf)
        assert len(collected) == 1
        assert collected[0] is futures[0]


# ---------------------------------------------------------------------------
# Dapr flow dispatch scope
# ---------------------------------------------------------------------------


class TestDaprFlowDispatchScope:
    def test_default_not_allowed(self) -> None:
        assert not _is_dapr_flow_dispatch_allowed()

    def test_allowed_inside_scope(self) -> None:
        with _dapr_flow_dispatch_scope():
            assert _is_dapr_flow_dispatch_allowed()
        assert not _is_dapr_flow_dispatch_allowed()


# ---------------------------------------------------------------------------
# interpret_flow — end-to-end tests
# ---------------------------------------------------------------------------


class TestInterpretFlowSingleCheckpoint:
    def test_basic_flow(self) -> None:
        """A flow with a single checkpoint returns the checkpoint result."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"step_a": lambda x: x + 10}
        )

        def my_flow(x: int) -> int:
            return session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(x,),
                kwargs={},
            )

        result = interpret_flow(
            flow_func=my_flow,
            args=(5,),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == 15


class TestInterpretFlowMultiCheckpoint:
    def test_sequential_checkpoints(self) -> None:
        """Two sequential checkpoints: step_a then step_b."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store,
            entrypoints={
                "step_a": lambda x: x + 1,
                "step_b": lambda x: x * 2,
            },
        )

        def my_flow(x: int) -> int:
            a = session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(x,),
                kwargs={},
            )
            b = session.call_checkpoint(
                checkpoint_name="step_b",
                checkpoint_type=None,
                retry_policy=None,
                args=(a,),
                kwargs={},
            )
            return b

        result = interpret_flow(
            flow_func=my_flow,
            args=(5,),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == 12  # (5 + 1) * 2

    def test_three_checkpoints(self) -> None:
        """Three sequential checkpoints accumulate results."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store,
            entrypoints={
                "add": lambda x: x + 1,
                "mul": lambda x: x * 3,
                "sub": lambda x: x - 5,
            },
        )

        def my_flow(x: int) -> int:
            a = session.call_checkpoint(
                checkpoint_name="add",
                checkpoint_type=None,
                retry_policy=None,
                args=(x,),
                kwargs={},
            )
            b = session.call_checkpoint(
                checkpoint_name="mul",
                checkpoint_type=None,
                retry_policy=None,
                args=(a,),
                kwargs={},
            )
            c = session.call_checkpoint(
                checkpoint_name="sub",
                checkpoint_type=None,
                retry_policy=None,
                args=(b,),
                kwargs={},
            )
            return c

        result = interpret_flow(
            flow_func=my_flow,
            args=(2,),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == 4  # (2 + 1) * 3 - 5 = 4


class TestInterpretFlowFanOut:
    def test_submit_and_result(self) -> None:
        """submit() creates a future, result() resolves it."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"double": lambda x: x * 2}
        )

        def my_flow() -> list[int]:
            f1 = session.submit_checkpoint(
                checkpoint_name="double",
                checkpoint_type=None,
                retry_policy=None,
                args=(5,),
                kwargs={},
            )
            f2 = session.submit_checkpoint(
                checkpoint_name="double",
                checkpoint_type=None,
                retry_policy=None,
                args=(10,),
                kwargs={},
            )
            return [f1.result(), f2.result()]

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == [10, 20]

    def test_map_checkpoint(self) -> None:
        """map() fans out across an iterable."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"square": lambda x: x * x}
        )

        def my_flow() -> list[int]:
            mf = session.map_checkpoint(
                checkpoint_name="square",
                checkpoint_type=None,
                retry_policy=None,
                mapped_args=([1, 2, 3],),
                kwargs={},
            )
            return mf.result()

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == [1, 4, 9]

    def test_product_checkpoint(self) -> None:
        """product() fans out across cartesian product."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"add": lambda a, b: a + b}
        )

        def my_flow() -> list[int]:
            mf = session.product_checkpoint(
                checkpoint_name="add",
                checkpoint_type=None,
                retry_policy=None,
                product_args=([1, 2], [10, 20]),
                kwargs={},
            )
            return mf.result()

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        # Cartesian: (1,10), (1,20), (2,10), (2,20) → 11, 21, 12, 22
        assert result == [11, 21, 12, 22]

    def test_unresolved_futures_flushed_on_completion(self) -> None:
        """Futures not explicitly resolved are flushed after flow returns."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"side_effect": lambda: "done"}
        )

        def my_flow() -> str:
            # Submit but never call .result()
            session.submit_checkpoint(
                checkpoint_name="side_effect",
                checkpoint_type=None,
                retry_policy=None,
                args=(),
                kwargs={},
            )
            return "flow_done"

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == "flow_done"
        # The side_effect future should have been resolved
        assert "side_effect:0" in session.resolved_values


class TestInterpretFlowWait:
    def test_wait_resolves_via_resolver(self) -> None:
        """wait() suspends and is resolved by the wait_resolver callback."""
        session, store = _make_session()
        executor = _make_activity_executor(store, entrypoints={})

        def wait_resolver(payload: _WaitSuspendPayload) -> Any:
            return "approved"

        def my_flow() -> str:
            answer = session.wait_for_input(
                name="approval", question="Proceed?", timeout=60
            )
            return f"user said {answer}"

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
            wait_resolver=wait_resolver,
        )
        assert result == "user said approved"

    def test_wait_before_checkpoint(self) -> None:
        """wait() then checkpoint, both resolve correctly."""
        session, store = _make_session()
        executor = _make_activity_executor(
            store, entrypoints={"greet": lambda name: f"Hello {name}"}
        )

        def wait_resolver(payload: _WaitSuspendPayload) -> Any:
            return "Alice"

        def my_flow() -> str:
            name = session.wait_for_input(name="get_name", timeout=60)
            return session.call_checkpoint(
                checkpoint_name="greet",
                checkpoint_type=None,
                retry_policy=None,
                args=(name,),
                kwargs={},
            )

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
            wait_resolver=wait_resolver,
        )
        assert result == "Hello Alice"


class TestInterpretFlowReplaySeed:
    def test_seeded_checkpoint_skipped(self) -> None:
        """Seeded checkpoints return immediately without activity execution."""
        store, _fake = make_store()
        exec_id = str(uuid4())
        store.create_execution(sample_record(exec_id=exec_id))

        seed = ReplaySeed(
            source_exec_id="prior-exec",
            seeded_results={"step_a:0": 42},
        )
        session = DaprOrchestratorSession(
            exec_id=exec_id, flow_name="my_flow", store=store, replay_seed=seed
        )

        calls: list[str] = []

        def step_a_fn(x: int) -> int:
            calls.append("step_a")
            return x + 1

        def step_b_fn(x: int) -> int:
            calls.append("step_b")
            return x * 10

        executor = _make_activity_executor(
            store,
            entrypoints={"step_a": step_a_fn, "step_b": step_b_fn},
        )

        def my_flow() -> int:
            a = session.call_checkpoint(
                checkpoint_name="step_a",
                checkpoint_type=None,
                retry_policy=None,
                args=(5,),
                kwargs={},
            )
            b = session.call_checkpoint(
                checkpoint_name="step_b",
                checkpoint_type=None,
                retry_policy=None,
                args=(a,),
                kwargs={},
            )
            return b

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        # step_a returns seeded value 42, step_b executes with 42
        assert result == 420
        # step_a was NOT actually executed (seeded)
        assert "step_a" not in calls
        assert "step_b" in calls


class TestInterpretFlowDivergence:
    def test_flow_code_change_detected(self) -> None:
        """Changing flow structure between iterations raises divergence."""
        store, _fake = make_store()
        exec_id = str(uuid4())
        store.create_execution(sample_record(exec_id=exec_id))

        session = DaprOrchestratorSession(
            exec_id=exec_id, flow_name="my_flow", store=store
        )

        call_count = 0

        def my_flow() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First iteration: call step_a
                return session.call_checkpoint(
                    checkpoint_name="step_a",
                    checkpoint_type=None,
                    retry_policy=None,
                    args=(),
                    kwargs={},
                )
            else:
                # Second iteration: call step_b at same position → divergence
                return session.call_checkpoint(
                    checkpoint_name="step_b",
                    checkpoint_type=None,
                    retry_policy=None,
                    args=(),
                    kwargs={},
                )

        executor = _make_activity_executor(
            store,
            entrypoints={
                "step_a": lambda: 1,
                "step_b": lambda: 2,
            },
        )

        with pytest.raises(KitaruDivergenceError, match="divergence"):
            interpret_flow(
                flow_func=my_flow,
                args=(),
                kwargs={},
                session=session,
                activity_executor=executor,
            )


class TestInterpretFlowMetadata:
    def test_metadata_flushed_on_completion(self) -> None:
        """Buffered metadata is flushed to the store on flow completion."""
        session, store = _make_session()
        executor = _make_activity_executor(store, entrypoints={})

        def my_flow() -> str:
            session.buffer_metadata({"key1": "value1"})
            session.buffer_metadata({"key2": "value2"})
            return "done"

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == "done"

        record = store.get_execution(session.exec_id)
        assert record.metadata["key1"] == "value1"
        assert record.metadata["key2"] == "value2"


class TestInterpretFlowCheckpointViaBackend:
    """Test that DaprCheckpointDefinition routes through the orchestrator."""

    def test_checkpoint_definition_call(self) -> None:
        """DaprCheckpointDefinition.call() routes to orchestrator session."""
        store, _fake = make_store()
        exec_id = str(uuid4())
        store.create_execution(sample_record(exec_id=exec_id))

        backend = DaprExecutionEngineBackend()
        backend.bind_ledger_store_provider(lambda: store)

        step_defn = backend.create_checkpoint_definition(
            entrypoint=lambda x: x + 100,
            registration_name="add_hundred",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        session = DaprOrchestratorSession(
            exec_id=exec_id, flow_name="my_flow", store=store
        )
        executor = _make_activity_executor(
            store, entrypoints={"add_hundred": lambda x: x + 100}
        )

        def my_flow(x: int) -> int:
            return step_defn.call(x)

        result = interpret_flow(
            flow_func=my_flow,
            args=(5,),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == 105

    def test_checkpoint_definition_submit(self) -> None:
        """DaprCheckpointDefinition.submit() returns a future."""
        store, _fake = make_store()
        exec_id = str(uuid4())
        store.create_execution(sample_record(exec_id=exec_id))

        backend = DaprExecutionEngineBackend()
        backend.bind_ledger_store_provider(lambda: store)

        step_defn = backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 3,
            registration_name="triple",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        session = DaprOrchestratorSession(
            exec_id=exec_id, flow_name="my_flow", store=store
        )
        executor = _make_activity_executor(
            store, entrypoints={"triple": lambda x: x * 3}
        )

        def my_flow() -> int:
            f = step_defn.submit(7)
            return f.result()

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == 21

    def test_checkpoint_definition_map(self) -> None:
        """DaprCheckpointDefinition.map() fans out."""
        store, _fake = make_store()
        exec_id = str(uuid4())
        store.create_execution(sample_record(exec_id=exec_id))

        backend = DaprExecutionEngineBackend()
        backend.bind_ledger_store_provider(lambda: store)

        step_defn = backend.create_checkpoint_definition(
            entrypoint=lambda x: x * x,
            registration_name="square",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        session = DaprOrchestratorSession(
            exec_id=exec_id, flow_name="my_flow", store=store
        )
        executor = _make_activity_executor(
            store, entrypoints={"square": lambda x: x * x}
        )

        def my_flow() -> list[int]:
            mf = step_defn.map([2, 3, 4])
            return mf.result()

        result = interpret_flow(
            flow_func=my_flow,
            args=(),
            kwargs={},
            session=session,
            activity_executor=executor,
        )
        assert result == [4, 9, 16]


# ---------------------------------------------------------------------------
# Store: replace_execution
# ---------------------------------------------------------------------------


class TestReplaceExecution:
    def test_replace_updates_status(self) -> None:
        store, _fake = make_store()
        exec_id = str(uuid4())
        original = sample_record(exec_id=exec_id, status="running")
        store.create_execution(original)

        from dataclasses import replace

        updated = replace(original, status="completed")
        store.replace_execution(exec_id, updated)

        loaded = store.get_execution(exec_id)
        assert loaded.status == "completed"

    def test_replace_preserves_checkpoints(self) -> None:
        from _dapr_fakes import sample_checkpoint

        store, _fake = make_store()
        exec_id = str(uuid4())
        original = sample_record(exec_id=exec_id)
        store.create_execution(original)

        cp = sample_checkpoint(call_id="cp1", name="step_a")
        store.upsert_checkpoint_call(exec_id, cp)

        from dataclasses import replace

        record = store.get_execution(exec_id)
        updated = replace(record, status="completed")
        store.replace_execution(exec_id, updated)

        loaded = store.get_execution(exec_id)
        assert loaded.status == "completed"
        assert len(loaded.checkpoints) == 1
        assert loaded.checkpoints[0].call_id == "cp1"
