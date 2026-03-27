"""Fake-driven Dapr end-to-end integration harness.

Wires together the Dapr backend, interpreter, store, and client adapter
without a real Dapr sidecar. Provides helpers for running flows, finalizing
executions, resolving waits, and building replay seeds.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from kitaru.engines.dapr.backend import (
    DaprCheckpointActivityRequest,
    DaprExecutionEngineBackend,
    _build_failure_record,
)
from kitaru.engines.dapr.client import DaprClientAdapter
from kitaru.engines.dapr.interpreter import (
    DaprOrchestratorSession,
    ReplaySeed,
    WaitResolver,
    _WaitSuspendPayload,
    interpret_flow,
)
from kitaru.engines.dapr.models import (
    DAPR_METADATA_NAMESPACE,
    FLOW_RESULT_ARTIFACT_ID_KEY,
    FLOW_RESULT_ARTIFACT_NAME,
    FLOW_RESULT_SAVE_TYPE,
    INTERNAL_ARTIFACT_FLAG,
    ArtifactRecord,
    LogRecord,
    WaitRecord,
    decode_transport_value,
)
from kitaru.engines.dapr.store import DaprExecutionLedgerStore
from tests._dapr_fakes import (
    FakeActivityRegistrar,
    FakeRuntimeHost,
    FakeWorkflowClient,
    FakeWorkflowState,
    make_store,
)

# ---------------------------------------------------------------------------
# Running execution handle
# ---------------------------------------------------------------------------


@dataclass
class RunningDaprExecution:
    """Handle to a Dapr execution that may be running in a background thread."""

    exec_id: str
    thread: threading.Thread | None = None
    result_box: dict[str, Any] = field(default_factory=dict)
    error_box: dict[str, BaseException] = field(default_factory=dict)

    def join(self, timeout: float = 10.0) -> None:
        if self.thread is not None:
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                raise TimeoutError(
                    f"Execution {self.exec_id} did not complete within {timeout}s"
                )


# ---------------------------------------------------------------------------
# Wait controller
# ---------------------------------------------------------------------------


class LedgerBackedWaitController:
    """Bridges interpreter wait suspensions to the fake workflow event queue.

    When the interpreter suspends on a ``wait()``, this controller:
    1. Writes a pending WaitRecord to the ledger
    2. Sets workflow status to "suspended"
    3. Blocks until a matching event arrives via ``FakeWorkflowClient``
    4. Marks the wait as resolved/aborted
    5. Sets workflow status back to "running"
    6. Returns the event payload to the interpreter
    """

    def __init__(
        self,
        *,
        exec_id: str,
        store: DaprExecutionLedgerStore,
        workflow_client: FakeWorkflowClient,
        default_timeout: float = 5.0,
    ) -> None:
        self._exec_id = exec_id
        self._store = store
        self._workflow_client = workflow_client
        self._default_timeout = default_timeout

    def __call__(self, payload: _WaitSuspendPayload) -> Any:
        now = datetime.now(UTC)

        wait_record = WaitRecord(
            wait_id=payload.wait_id,
            name=payload.name or payload.wait_id,
            question=payload.question,
            schema=None,
            metadata=dict(payload.metadata) if payload.metadata else {},
            status="pending",
            entered_at=now,
        )
        self._store.upsert_wait(self._exec_id, wait_record)
        record = self._store.get_execution(self._exec_id)
        self._store.replace_execution(
            self._exec_id,
            replace(record, status="suspended", updated_at=now),
        )

        if self._exec_id in self._workflow_client.states:
            self._workflow_client.states[self._exec_id].runtime_status = "suspended"

        data = self._workflow_client.wait_for_event(
            self._exec_id,
            payload.wait_id,
            timeout=self._default_timeout,
        )
        if isinstance(data, dict) and "__kitaru_transport" in data:
            data = decode_transport_value(data["__kitaru_transport"])

        resolved_at = datetime.now(UTC)

        is_abort = isinstance(data, dict) and data.get("__kitaru_resolution") == "abort"
        updated = replace(
            wait_record,
            status="aborted" if is_abort else "resolved",
            resolved_at=resolved_at,
        )
        self._store.upsert_wait(self._exec_id, updated)
        record = self._store.get_execution(self._exec_id)
        self._store.replace_execution(
            self._exec_id,
            replace(record, status="running", updated_at=resolved_at),
        )

        if self._exec_id in self._workflow_client.states:
            self._workflow_client.states[self._exec_id].runtime_status = "running"

        return data


# ---------------------------------------------------------------------------
# Main harness
# ---------------------------------------------------------------------------


class DaprPhase12Harness:
    """End-to-end Dapr test harness. One per test."""

    def __init__(self) -> None:
        self.store, _fake_state = make_store()
        self.workflow_client = FakeWorkflowClient()
        self.backend = DaprExecutionEngineBackend()
        self.backend.bind_ledger_store_provider(lambda: self.store)
        self.runtime_host = FakeRuntimeHost(workflow_client=self.workflow_client)
        self.backend.bind_runtime_host(self.runtime_host)
        self.adapter = DaprClientAdapter(
            store=self.store,
            workflow_client=self.workflow_client,
        )
        self._activity_dispatch: dict[str, Any] = {}

    def register_checkpoints(self) -> None:
        """Register all known checkpoint definitions as activity callables."""
        registrar = FakeActivityRegistrar()
        self.backend.register_checkpoint_activities(registrar)
        self._activity_dispatch = dict(registrar.registered)

    def _activity_executor(self, request: DaprCheckpointActivityRequest) -> Any:
        """Dispatch an activity request to the registered callable."""
        fn = self._activity_dispatch.get(request.checkpoint_name)
        if fn is None:
            raise RuntimeError(
                f"No activity registered for checkpoint {request.checkpoint_name!r}. "
                f"Available: {sorted(self._activity_dispatch)}"
            )
        return fn(request)

    def run_flow_sync(
        self,
        flow_wrapper: Any,
        *args: Any,
        replay_seed: ReplaySeed | None = None,
        wait_resolver: WaitResolver | None = None,
        **kwargs: Any,
    ) -> tuple[str, Any]:
        """Run a flow synchronously and return (exec_id, result).

        Handles the full lifecycle: run → interpret → finalize.
        """
        self.register_checkpoints()
        handle = flow_wrapper.run(*args, **kwargs)
        exec_id = handle.exec_id

        self.workflow_client.states[exec_id] = FakeWorkflowState(
            instance_id=exec_id, runtime_status="running"
        )

        flow_func = flow_wrapper._func
        flow_name = flow_wrapper._name

        session = DaprOrchestratorSession(
            exec_id=exec_id,
            flow_name=flow_name,
            store=self.store,
            replay_seed=replay_seed,
        )

        try:
            result = interpret_flow(
                flow_func=flow_func,
                args=args,
                kwargs=kwargs,
                session=session,
                activity_executor=self._activity_executor,
                wait_resolver=wait_resolver,
            )
            self.finalize_success(exec_id, result)
            return exec_id, result
        except BaseException as exc:
            self.finalize_failure(exec_id, exc)
            raise

    def run_flow_in_background(
        self,
        flow_wrapper: Any,
        *args: Any,
        replay_seed: ReplaySeed | None = None,
        wait_resolver: WaitResolver | None = None,
        wait_resolver_factory: Callable[[str], WaitResolver] | None = None,
        **kwargs: Any,
    ) -> RunningDaprExecution:
        """Run a flow in a background thread.

        Use ``wait_resolver_factory`` instead of ``wait_resolver`` when the
        resolver needs the exec_id (e.g. ``LedgerBackedWaitController``).
        The factory is called with the exec_id after the execution record
        is created but before the thread starts.
        """
        self.register_checkpoints()
        handle = flow_wrapper.run(*args, **kwargs)
        exec_id = handle.exec_id

        self.workflow_client.states[exec_id] = FakeWorkflowState(
            instance_id=exec_id, runtime_status="running"
        )

        if wait_resolver_factory is not None:
            wait_resolver = wait_resolver_factory(exec_id)

        running = RunningDaprExecution(exec_id=exec_id)

        flow_func = flow_wrapper._func
        flow_name = flow_wrapper._name

        def _run() -> None:
            session = DaprOrchestratorSession(
                exec_id=exec_id,
                flow_name=flow_name,
                store=self.store,
                replay_seed=replay_seed,
            )
            try:
                result = interpret_flow(
                    flow_func=flow_func,
                    args=args,
                    kwargs=kwargs,
                    session=session,
                    activity_executor=self._activity_executor,
                    wait_resolver=wait_resolver,
                )
                self.finalize_success(exec_id, result)
                running.result_box["result"] = result
            except BaseException as exc:
                self.finalize_failure(exec_id, exc)
                running.error_box["error"] = exc

        thread = threading.Thread(target=_run, daemon=True)
        running.thread = thread
        thread.start()
        return running

    def finalize_success(self, exec_id: str, result: Any) -> None:
        """Persist the flow result and mark execution as completed."""
        result_artifact_id = f"{exec_id}:flow_result"
        artifact = ArtifactRecord(
            artifact_id=result_artifact_id,
            name=FLOW_RESULT_ARTIFACT_NAME,
            save_type=FLOW_RESULT_SAVE_TYPE,
            metadata={INTERNAL_ARTIFACT_FLAG: True},
            exec_id=exec_id,
        )
        self.store.store_artifact(exec_id, artifact, result)

        self.store.merge_execution_metadata(
            exec_id,
            {
                DAPR_METADATA_NAMESPACE: {
                    FLOW_RESULT_ARTIFACT_ID_KEY: result_artifact_id
                }
            },
        )

        record = self.store.get_execution(exec_id)
        updated = replace(
            record,
            status="completed",
            ended_at=datetime.now(UTC),
        )
        self.store.replace_execution(exec_id, updated)

        if exec_id in self.workflow_client.states:
            self.workflow_client.states[exec_id].runtime_status = "completed"

    def finalize_failure(self, exec_id: str, exc: BaseException) -> None:
        """Mark execution as failed with failure details."""
        failure = _build_failure_record(exc)
        record = self.store.get_execution(exec_id)
        updated = replace(
            record,
            status="failed",
            ended_at=datetime.now(UTC),
            failure=failure,
        )
        self.store.replace_execution(exec_id, updated)

        if exec_id in self.workflow_client.states:
            self.workflow_client.states[exec_id].runtime_status = "failed"

    def build_replay_seed_from_execution(self, exec_id: str) -> ReplaySeed:
        """Build a ReplaySeed from completed checkpoint outputs."""
        record = self.store.get_execution(exec_id)
        seeded_results: dict[str, Any] = {}
        for cp in record.checkpoints:
            if cp.status != "completed":
                continue
            for art in cp.artifacts:
                if art.save_type == "step_output" and art.name == "output":
                    try:
                        _, value = self.store.load_artifact(art.artifact_id)
                        seeded_results[cp.call_id] = value
                    except Exception:
                        pass
                    break
        return ReplaySeed(
            source_exec_id=exec_id,
            seeded_results=seeded_results,
        )

    def make_wait_controller(self, exec_id: str) -> LedgerBackedWaitController:
        """Create a wait controller for a specific execution."""
        return LedgerBackedWaitController(
            exec_id=exec_id,
            store=self.store,
            workflow_client=self.workflow_client,
        )

    def add_log_entries(self, exec_id: str, entries: list[LogRecord]) -> None:
        """Append log entries to an execution's ledger record."""
        for entry in entries:
            self.store.append_log_entry(exec_id, entry)
