"""Tests for the Dapr execution engine backend.

Covers import boundaries, checkpoint definition registration, retry
policy mapping, activity wrapper success/failure paths, runtime session
dispatch, and activity registration via fake registrar.
"""

from __future__ import annotations

import importlib
import warnings
from typing import Any
from uuid import uuid4

import pytest
from zenml.enums import StepRuntime

from _dapr_fakes import (
    FakeActivityRegistrar,
    make_store,
    sample_record,
)
from kitaru._config._core import ExplicitOverrides
from kitaru.engines.dapr.backend import (
    DaprCheckpointActivityRequest,
    DaprCheckpointDefinition,
    DaprExecutionEngineBackend,
    DaprFlowDefinition,
    DaprFlowRunHandle,
    DaprRuntimeSession,
    _manual_artifact_id,
    _run_checkpoint_activity,
    _step_output_artifact_id,
    _to_retry_policy,
)
from kitaru.engines.dapr.store import ExecutionLedgerStore
from kitaru.errors import (
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    exec_id: str = "e1",
    call_id: str | None = None,
    checkpoint_name: str = "my_checkpoint",
    **kwargs: Any,
) -> DaprCheckpointActivityRequest:
    kwargs.setdefault("flow_name", "test_flow")
    kwargs.setdefault("invocation_id", str(uuid4()))
    return DaprCheckpointActivityRequest(
        exec_id=exec_id,
        call_id=call_id or str(uuid4()),
        checkpoint_name=checkpoint_name,
        **kwargs,
    )


def _setup_store_with_execution(
    exec_id: str = "e1",
) -> tuple[ExecutionLedgerStore, Any]:
    """Create a store with a pre-created execution record."""
    store, fake = make_store()
    record = sample_record(exec_id=exec_id)
    store.create_execution(record)
    return store, fake


# ═══════════════════════════════════════════════════════════════════════════
# Import boundary tests
# ═══════════════════════════════════════════════════════════════════════════


class TestImportBoundary:
    def test_backend_imports_without_dapr_sdk(self) -> None:
        """backend.py should import without the Dapr SDK installed."""
        mod = importlib.import_module("kitaru.engines.dapr.backend")
        assert hasattr(mod, "DaprExecutionEngineBackend")

    def test_get_engine_backend_dapr_works_without_sdk(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get_engine_backend('dapr') should not require Dapr SDK."""
        from kitaru.engines._registry import (
            _reset_engine_backend_cache,
            get_engine_backend,
        )

        monkeypatch.setenv("KITARU_ENABLE_EXPERIMENTAL_DAPR", "1")
        _reset_engine_backend_cache()
        backend = get_engine_backend("dapr")
        assert backend.name == "dapr"
        _reset_engine_backend_cache()


# ═══════════════════════════════════════════════════════════════════════════
# Retry policy mapping
# ═══════════════════════════════════════════════════════════════════════════


class TestRetryPolicyMapping:
    def test_zero_retries_returns_none(self) -> None:
        assert _to_retry_policy(0) is None

    def test_negative_retries_returns_none(self) -> None:
        assert _to_retry_policy(-1) is None

    def test_positive_retries_returns_spec(self) -> None:
        spec = _to_retry_policy(3)
        assert spec is not None
        assert spec.max_attempts == 4  # 1 original + 3 retries

    def test_single_retry(self) -> None:
        spec = _to_retry_policy(1)
        assert spec is not None
        assert spec.max_attempts == 2


# ═══════════════════════════════════════════════════════════════════════════
# Backend class tests
# ═══════════════════════════════════════════════════════════════════════════


class TestDaprBackend:
    def test_backend_name(self) -> None:
        backend = DaprExecutionEngineBackend()
        assert backend.name == "dapr"

    def test_create_flow_definition_returns_placeholder(self) -> None:
        backend = DaprExecutionEngineBackend()
        defn = backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="my_flow",
        )
        assert isinstance(defn, DaprFlowDefinition)

    def test_flow_definition_run_creates_record_and_returns_handle(self) -> None:
        backend = DaprExecutionEngineBackend()
        store, _ = make_store()
        backend.bind_ledger_store_provider(lambda: store)

        defn = backend.create_flow_definition(
            entrypoint=lambda x: x,
            registration_name="my_flow",
        )
        handle = defn.run(args=(42,), kwargs={"key": "val"})

        assert isinstance(handle, DaprFlowRunHandle)
        assert handle.exec_id

        # Verify ledger record was created
        record = store.get_execution(handle.exec_id)
        assert record.flow_name == "my_flow"
        assert record.workflow_name == "my_flow"
        assert record.status == "pending"
        assert record.created_at is not None

        # Verify execution input was persisted (tuples become lists via JSON)
        input_data = store.load_execution_input(handle.exec_id)
        assert input_data["args"] == [42]
        assert input_data["kwargs"] == {"key": "val"}

    def test_flow_definition_run_persists_frozen_spec(self) -> None:
        backend = DaprExecutionEngineBackend()
        store, _ = make_store()
        backend.bind_ledger_store_provider(lambda: store)

        defn = backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="my_flow",
        )

        class FakeSpec:
            def model_dump(self, mode: str = "python") -> dict[str, Any]:
                return {"stack": "local", "retries": 0}

        handle = defn.run(frozen_execution_spec=FakeSpec())
        record = store.get_execution(handle.exec_id)
        assert record.frozen_execution_spec == {"stack": "local", "retries": 0}

    def test_flow_definition_replay_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        defn = backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="my_flow",
        )
        with pytest.raises(KitaruFeatureNotAvailableError, match="not yet"):
            defn.replay()

    def test_flow_definition_stores_in_registry(self) -> None:
        backend = DaprExecutionEngineBackend()
        backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="flow_a",
        )
        backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="flow_b",
        )
        defns = backend.get_flow_definitions()
        assert set(defns.keys()) == {"flow_a", "flow_b"}

    def test_create_checkpoint_definition(self) -> None:
        backend = DaprExecutionEngineBackend()
        defn = backend.create_checkpoint_definition(
            entrypoint=lambda x: x,
            registration_name="my_step",
            retries=2,
            checkpoint_type="llm_call",
            runtime=None,
        )
        assert isinstance(defn, DaprCheckpointDefinition)
        assert defn.registration_name == "my_step"
        assert defn.retry_policy is not None
        assert defn.retry_policy.max_attempts == 3

    def test_checkpoint_definition_call_requires_session(self) -> None:
        backend = DaprExecutionEngineBackend()
        defn = backend.create_checkpoint_definition(
            entrypoint=lambda: None,
            registration_name="step",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        with pytest.raises(KitaruRuntimeError, match="orchestrator"):
            defn.call()

    def test_checkpoint_definition_submit_requires_session(self) -> None:
        backend = DaprExecutionEngineBackend()
        defn = backend.create_checkpoint_definition(
            entrypoint=lambda: None,
            registration_name="step",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        with pytest.raises(KitaruRuntimeError, match="orchestrator"):
            defn.submit()

    def test_checkpoint_stores_in_registry(self) -> None:
        backend = DaprExecutionEngineBackend()
        backend.create_checkpoint_definition(
            entrypoint=lambda: None,
            registration_name="step_a",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        backend.create_checkpoint_definition(
            entrypoint=lambda: None,
            registration_name="step_b",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )
        defns = backend.get_checkpoint_definitions()
        assert set(defns.keys()) == {"step_a", "step_b"}

    def test_runtime_session_is_dapr_session(self) -> None:
        backend = DaprExecutionEngineBackend()
        session = backend.create_runtime_session()
        assert isinstance(session, DaprRuntimeSession)

    def test_execution_graph_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.raises(KitaruFeatureNotAvailableError):
            backend.execution_graph_from_run(None)


# ═══════════════════════════════════════════════════════════════════════════
# Activity registration tests
# ═══════════════════════════════════════════════════════════════════════════


class TestActivityRegistration:
    def test_register_activities_with_fake_registrar(self) -> None:
        backend = DaprExecutionEngineBackend()
        store, _ = make_store()
        backend.bind_ledger_store_provider(lambda: store)

        backend.create_checkpoint_definition(
            entrypoint=lambda x: x * 2,
            registration_name="double",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        registrar = FakeActivityRegistrar()
        backend.register_checkpoint_activities(registrar)

        assert "double" in registrar.registered
        assert callable(registrar.registered["double"])

    def test_register_without_store_provider_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        backend.create_checkpoint_definition(
            entrypoint=lambda: None,
            registration_name="step",
            retries=0,
            checkpoint_type=None,
            runtime=None,
        )

        registrar = FakeActivityRegistrar()
        with pytest.raises(KitaruRuntimeError, match="store provider"):
            backend.register_checkpoint_activities(registrar)

    def test_registered_activities_sorted_by_name(self) -> None:
        backend = DaprExecutionEngineBackend()
        store, _ = make_store()
        backend.bind_ledger_store_provider(lambda: store)

        for name in ["charlie", "alpha", "bravo"]:
            backend.create_checkpoint_definition(
                entrypoint=lambda: None,
                registration_name=name,
                retries=0,
                checkpoint_type=None,
                runtime=None,
            )

        registrar = FakeActivityRegistrar()
        backend.register_checkpoint_activities(registrar)

        assert list(registrar.registered.keys()) == ["alpha", "bravo", "charlie"]


# ═══════════════════════════════════════════════════════════════════════════
# Activity wrapper — success path
# ═══════════════════════════════════════════════════════════════════════════


class TestActivitySuccess:
    def test_basic_activity_returns_result(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        def double(x: int) -> int:
            return x * 2

        request = _make_request(exec_id="e1", args=(21,))
        result = _run_checkpoint_activity(request, entrypoint=double, store=store)
        assert result == 42

    def test_call_record_created_and_completed(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(exec_id="e1", call_id="c1")
        _run_checkpoint_activity(request, entrypoint=lambda: "ok", store=store)

        record = store.get_execution("e1")
        assert len(record.checkpoints) == 1
        cp = record.checkpoints[0]
        assert cp.call_id == "c1"
        assert cp.status == "completed"
        assert cp.ended_at is not None

    def test_attempt_record_created_and_completed(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(exec_id="e1", call_id="c1")
        _run_checkpoint_activity(request, entrypoint=lambda: "ok", store=store)

        record = store.get_execution("e1")
        cp = record.checkpoints[0]
        assert len(cp.attempts) == 1
        att = cp.attempts[0]
        assert att.status == "completed"
        assert att.attempt_number == 1
        assert att.started_at is not None
        assert att.ended_at is not None

    def test_step_output_artifact_persisted(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(exec_id="e1", call_id="c1")
        _run_checkpoint_activity(
            request, entrypoint=lambda: {"key": "value"}, store=store
        )

        art_id = _step_output_artifact_id("c1")
        art_record, value = store.load_artifact(art_id)
        assert art_record.save_type == "step_output"
        assert art_record.name == "output"
        assert value == {"key": "value"}

    def test_save_inside_checkpoint_flushes_on_success(self) -> None:
        """kitaru.save() artifacts should be flushed on activity success."""
        # save() calls _require_checkpoint_scope which parses UUIDs
        eid = str(uuid4())
        iid = str(uuid4())
        store, _ = _setup_store_with_execution(eid)

        def checkpoint_with_save() -> str:
            import kitaru

            kitaru.save("my_data", {"saved": True}, type="context")
            return "done"

        request = _make_request(exec_id=eid, call_id="c1", invocation_id=iid)
        _run_checkpoint_activity(request, entrypoint=checkpoint_with_save, store=store)

        # Manual artifact should be registered
        record = store.get_execution(eid)
        manual_arts = [a for a in record.artifacts if a.save_type == "manual"]
        assert len(manual_arts) == 1
        assert manual_arts[0].name == "my_data"

        # Load the actual value
        _, value = store.load_artifact(manual_arts[0].artifact_id)
        assert value == {"saved": True}

    def test_log_inside_checkpoint_merges_metadata(self) -> None:
        """kitaru.log() should merge into checkpoint metadata."""
        eid = str(uuid4())
        store, _ = _setup_store_with_execution(eid)

        def checkpoint_with_log() -> str:
            import kitaru

            kitaru.log(tokens=100, model="gpt-4")
            return "done"

        request = _make_request(exec_id=eid, call_id="c1")
        _run_checkpoint_activity(request, entrypoint=checkpoint_with_log, store=store)

        record = store.get_execution(eid)
        cp = record.checkpoints[0]
        assert cp.metadata.get("tokens") == 100
        assert cp.metadata.get("model") == "gpt-4"

    def test_load_from_prior_execution(self) -> None:
        """kitaru.load() should read artifacts from a prior execution."""
        # load() calls _require_checkpoint_scope which parses UUIDs
        eid1 = str(uuid4())
        eid2 = str(uuid4())
        iid = str(uuid4())
        store, _ = _setup_store_with_execution(eid1)

        # Store an artifact in eid1
        from kitaru.engines.dapr.models import ArtifactRecord

        artifact = ArtifactRecord(
            artifact_id="prior-art",
            name="context",
            save_type="manual",
        )
        store.store_artifact(eid1, artifact, {"prior": "data"})

        # Create eid2 and run activity that loads from eid1
        store.create_execution(sample_record(exec_id=eid2))

        def checkpoint_with_load() -> Any:
            import kitaru

            return kitaru.load(eid1, "context")

        request = _make_request(exec_id=eid2, call_id="c1", invocation_id=iid)
        result = _run_checkpoint_activity(
            request, entrypoint=checkpoint_with_load, store=store
        )
        assert result == {"prior": "data"}

    def test_request_metadata_merged_into_call(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(
            exec_id="e1",
            call_id="c1",
            metadata={"custom": "info"},
        )
        _run_checkpoint_activity(request, entrypoint=lambda: "ok", store=store)

        record = store.get_execution("e1")
        assert record.checkpoints[0].metadata.get("custom") == "info"


# ═══════════════════════════════════════════════════════════════════════════
# Activity wrapper — failure path
# ═══════════════════════════════════════════════════════════════════════════


class TestActivityFailure:
    def test_exception_is_reraised(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        def failing() -> None:
            raise ValueError("boom")

        request = _make_request(exec_id="e1")
        with pytest.raises(ValueError, match="boom"):
            _run_checkpoint_activity(request, entrypoint=failing, store=store)

    def test_attempt_marked_failed(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(exec_id="e1", call_id="c1")
        with pytest.raises(ValueError):
            _run_checkpoint_activity(
                request,
                entrypoint=lambda: (_ for _ in ()).throw(ValueError("fail")),
                store=store,
            )

        record = store.get_execution("e1")
        cp = record.checkpoints[0]
        assert cp.status == "failed"
        assert len(cp.attempts) == 1
        assert cp.attempts[0].status == "failed"
        assert cp.attempts[0].failure is not None
        assert "fail" in cp.attempts[0].failure.message

    def test_call_record_marked_failed(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        request = _make_request(exec_id="e1", call_id="c1")
        with pytest.raises(RuntimeError):
            _run_checkpoint_activity(
                request,
                entrypoint=lambda: (_ for _ in ()).throw(RuntimeError("crash")),
                store=store,
            )

        record = store.get_execution("e1")
        cp = record.checkpoints[0]
        assert cp.status == "failed"
        assert cp.failure is not None
        assert "crash" in cp.failure.message

    def test_failure_records_traceback(self) -> None:
        store, _ = _setup_store_with_execution("e1")

        def deep_failure() -> None:
            raise TypeError("type mismatch")

        request = _make_request(exec_id="e1", call_id="c1")
        with pytest.raises(TypeError):
            _run_checkpoint_activity(request, entrypoint=deep_failure, store=store)

        record = store.get_execution("e1")
        failure = record.checkpoints[0].failure
        assert failure is not None
        assert failure.traceback is not None
        assert "TypeError" in failure.traceback

    def test_buffered_artifacts_not_flushed_on_failure(self) -> None:
        """Manual save() artifacts should NOT be persisted if activity fails."""
        # save() calls _require_checkpoint_scope which parses UUIDs
        eid = str(uuid4())
        iid = str(uuid4())
        store, _ = _setup_store_with_execution(eid)

        def save_then_fail() -> None:
            import kitaru

            kitaru.save("partial", {"leaked": True}, type="context")
            raise RuntimeError("boom after save")

        request = _make_request(exec_id=eid, call_id="c1", invocation_id=iid)
        with pytest.raises(RuntimeError, match="boom after save"):
            _run_checkpoint_activity(request, entrypoint=save_then_fail, store=store)

        # No manual artifacts should be in the execution record
        record = store.get_execution(eid)
        manual_arts = [a for a in record.artifacts if a.save_type == "manual"]
        assert len(manual_arts) == 0


# ═══════════════════════════════════════════════════════════════════════════
# Retry idempotency
# ═══════════════════════════════════════════════════════════════════════════


class TestRetryIdempotency:
    def test_same_call_id_different_attempt_numbers(self) -> None:
        """Two invocations with same call_id but explicit attempt numbers."""
        store, _ = _setup_store_with_execution("e1")

        # Attempt 1 fails
        request1 = _make_request(exec_id="e1", call_id="c1", attempt_number=1)
        with pytest.raises(ValueError):
            _run_checkpoint_activity(
                request1,
                entrypoint=lambda: (_ for _ in ()).throw(ValueError("fail1")),
                store=store,
            )

        # Attempt 2 succeeds
        request2 = _make_request(exec_id="e1", call_id="c1", attempt_number=2)
        result = _run_checkpoint_activity(
            request2, entrypoint=lambda: "success", store=store
        )
        assert result == "success"

        record = store.get_execution("e1")
        cp = record.checkpoints[0]
        assert len(cp.attempts) == 2
        assert cp.attempts[0].status == "failed"
        assert cp.attempts[1].status == "completed"
        # Call status should reflect the latest update
        assert cp.status == "completed"

    def test_step_output_artifact_id_is_deterministic(self) -> None:
        """Same call_id always produces same step_output artifact ID."""
        id1 = _step_output_artifact_id("c1")
        id2 = _step_output_artifact_id("c1")
        assert id1 == id2 == "c1:step_output"

    def test_manual_artifact_id_is_deterministic(self) -> None:
        """Same (call_id, name) always produces same manual artifact ID."""
        id1 = _manual_artifact_id("c1", "my_artifact")
        id2 = _manual_artifact_id("c1", "my_artifact")
        assert id1 == id2


# ═══════════════════════════════════════════════════════════════════════════
# Runtime session tests
# ═══════════════════════════════════════════════════════════════════════════


class TestDaprRuntimeSession:
    def test_wait_raises_feature_not_available(self) -> None:
        session = DaprRuntimeSession()
        with pytest.raises(KitaruFeatureNotAvailableError, match="wait"):
            session.wait(timeout=60)

    def test_save_outside_binding_raises(self) -> None:
        session = DaprRuntimeSession()
        with pytest.raises(KitaruRuntimeError, match="outside"):
            session.save_artifact("x", "val", type="output")

    def test_load_outside_binding_raises(self) -> None:
        session = DaprRuntimeSession()
        with pytest.raises(KitaruRuntimeError, match="outside"):
            session.load_artifact("e1", "x")

    def test_log_outside_binding_raises(self) -> None:
        session = DaprRuntimeSession()
        with pytest.raises(KitaruRuntimeError, match="outside"):
            session.log_metadata({"a": 1})


# ═══════════════════════════════════════════════════════════════════════════
# Capability gating tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCapabilityGating:
    def test_no_overrides_passes_validation(self) -> None:
        backend = DaprExecutionEngineBackend()
        backend.validate_flow_run_options(ExplicitOverrides())

    def test_explicit_stack_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.raises(KitaruFeatureNotAvailableError, match="stack"):
            backend.validate_flow_run_options(ExplicitOverrides(stack=True))

    def test_explicit_image_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.raises(KitaruFeatureNotAvailableError, match="image"):
            backend.validate_flow_run_options(ExplicitOverrides(image=True))

    def test_explicit_stack_and_image_both_in_error(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.raises(
            KitaruFeatureNotAvailableError, match=r"stack.*image|image.*stack"
        ):
            backend.validate_flow_run_options(ExplicitOverrides(stack=True, image=True))

    def test_explicit_cache_logs_debug_warning(self, caplog: Any) -> None:
        backend = DaprExecutionEngineBackend()
        with caplog.at_level("DEBUG", logger="kitaru.engines.dapr.backend"):
            backend.validate_flow_run_options(ExplicitOverrides(cache=True))
        assert "cache" in caplog.text.lower()

    def test_cache_without_stack_or_image_does_not_raise(self) -> None:
        backend = DaprExecutionEngineBackend()
        # Should not raise — cache is just a debug warning
        backend.validate_flow_run_options(ExplicitOverrides(cache=True))

    def test_replay_support_raises(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.raises(KitaruFeatureNotAvailableError, match="replay"):
            backend.validate_flow_replay_support()

    def test_isolated_runtime_warning(self) -> None:
        backend = DaprExecutionEngineBackend()
        with pytest.warns(UserWarning, match="isolated.*ignored"):
            backend.create_checkpoint_definition(
                entrypoint=lambda: None,
                registration_name="step",
                retries=0,
                checkpoint_type=None,
                runtime=StepRuntime.ISOLATED,
            )

    def test_inline_runtime_no_warning(self) -> None:
        backend = DaprExecutionEngineBackend()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            backend.create_checkpoint_definition(
                entrypoint=lambda: None,
                registration_name="step",
                retries=0,
                checkpoint_type=None,
                runtime=StepRuntime.INLINE,
            )

    def test_none_runtime_no_warning(self) -> None:
        backend = DaprExecutionEngineBackend()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            backend.create_checkpoint_definition(
                entrypoint=lambda: None,
                registration_name="step",
                retries=0,
                checkpoint_type=None,
                runtime=None,
            )
