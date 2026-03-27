"""Shared in-memory fakes for Dapr engine tests.

Provides ``FakeStateStore`` (an in-memory ``_StateStoreAPI``),
``FakeActivityRegistrar``, and ``FakeWorkflowClient`` for testing
Dapr store and backend logic without a Dapr sidecar.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from kitaru.engines.dapr.models import (
    ArtifactRecord,
    CheckpointAttemptRecord,
    CheckpointCallRecord,
    ExecutionLedgerRecord,
    LogRecord,
    WaitRecord,
)
from kitaru.engines.dapr.store import (
    DaprExecutionLedgerStore,
    ETagConflict,
    _StateItem,
)

# ---------------------------------------------------------------------------
# In-memory fake state store
# ---------------------------------------------------------------------------


class FakeStateStore:
    """In-memory _StateStoreAPI for testing without a Dapr sidecar.

    Tracks etags and can inject a configurable number of conflicts.
    """

    def __init__(self, *, conflict_keys: set[str] | None = None) -> None:
        self._data: dict[tuple[str, str], tuple[bytes, str]] = {}
        self._etag_counter = 0
        self._conflict_keys = conflict_keys or set()
        self._conflict_count: dict[str, int] = {}

    def _next_etag(self) -> str:
        self._etag_counter += 1
        return str(self._etag_counter)

    def get(self, *, store_name: str, key: str) -> _StateItem:
        entry = self._data.get((store_name, key))
        if entry is None:
            return _StateItem(data=None, etag=None)
        return _StateItem(data=entry[0], etag=entry[1])

    def put(
        self,
        *,
        store_name: str,
        key: str,
        data: bytes,
        etag: str | None = None,
    ) -> str | None:
        existing = self._data.get((store_name, key))

        # Simulate conflict for configured keys on CAS writes (once per key)
        if key in self._conflict_keys and etag is not None:
            already_conflicted = self._conflict_count.get(key, 0)
            if already_conflicted == 0:
                self._conflict_count[key] = 1
                raise ETagConflict(f"Simulated conflict on {key!r}")

        # Etag check
        if etag is not None:
            if existing is None:
                raise ETagConflict(f"Key {key!r} does not exist for etag write")
            if existing[1] != etag:
                raise ETagConflict(
                    f"Etag mismatch for {key!r}: expected {existing[1]!r}, got {etag!r}"
                )

        new_etag = self._next_etag()
        self._data[(store_name, key)] = (data, new_etag)
        return new_etag


# ---------------------------------------------------------------------------
# Fake activity registrar
# ---------------------------------------------------------------------------


class FakeActivityRegistrar:
    """In-memory activity registrar for testing backend registration."""

    def __init__(self) -> None:
        self.registered: dict[str, Callable[..., Any]] = {}

    def register_activity(
        self,
        *,
        name: str,
        fn: Callable[..., Any],
    ) -> None:
        self.registered[name] = fn


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------


def make_store(
    *,
    project: str = "test-project",
    conflict_keys: set[str] | None = None,
    inline_threshold: int = 262_144,
    max_retries: int = 5,
) -> tuple[DaprExecutionLedgerStore, FakeStateStore]:
    """Create a DaprExecutionLedgerStore backed by a FakeStateStore."""
    fake = FakeStateStore(conflict_keys=conflict_keys)
    store = DaprExecutionLedgerStore(
        project=project,
        ledger_store_name="test-ledger",
        state_api=fake,
        artifact_inline_threshold_bytes=inline_threshold,
        max_write_retries=max_retries,
    )
    return store, fake


def sample_record(
    exec_id: str | None = None,
    project: str = "test-project",
    **kwargs: Any,
) -> ExecutionLedgerRecord:
    return ExecutionLedgerRecord(
        exec_id=exec_id or str(uuid4()),
        project=project,
        **kwargs,
    )


def sample_checkpoint(
    call_id: str | None = None,
    name: str = "my_checkpoint",
    **kwargs: Any,
) -> CheckpointCallRecord:
    kwargs.setdefault("invocation_id", str(uuid4()))
    return CheckpointCallRecord(
        call_id=call_id or str(uuid4()),
        name=name,
        **kwargs,
    )


def sample_attempt(
    attempt_id: str | None = None,
    attempt_number: int = 1,
    **kwargs: Any,
) -> CheckpointAttemptRecord:
    kwargs.setdefault("status", "running")
    return CheckpointAttemptRecord(
        attempt_id=attempt_id or str(uuid4()),
        attempt_number=attempt_number,
        **kwargs,
    )


def sample_wait(
    wait_id: str | None = None,
    name: str = "approval",
    **kwargs: Any,
) -> WaitRecord:
    return WaitRecord(
        wait_id=wait_id or str(uuid4()),
        name=name,
        **kwargs,
    )


def sample_artifact(
    artifact_id: str | None = None,
    name: str = "output",
    **kwargs: Any,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id or str(uuid4()),
        name=name,
        **kwargs,
    )


def sample_log(
    message: str = "hello",
    **kwargs: Any,
) -> LogRecord:
    return LogRecord(message=message, **kwargs)


# ---------------------------------------------------------------------------
# Fake workflow client
# ---------------------------------------------------------------------------


@dataclass
class FakeWorkflowState:
    """In-memory workflow state for testing."""

    instance_id: str
    runtime_status: str = "running"
    created_at: datetime | None = None
    last_updated_at: datetime | None = None


class FakeWorkflowClient:
    """In-memory workflow client for testing without a Dapr sidecar.

    Thread-safe: ``raise_workflow_event`` notifies any thread blocked
    in ``wait_for_event``, used by wait-resolution integration tests.
    """

    def __init__(self) -> None:
        self.states: dict[str, FakeWorkflowState] = {}
        self.events: list[tuple[str, str, Any]] = []
        self.terminated: list[str] = []
        self.resumed: list[str] = []
        self.scheduled: list[dict[str, Any]] = []
        self._condition = threading.Condition()

    def get_workflow_state(self, instance_id: str) -> FakeWorkflowState:
        if instance_id not in self.states:
            raise LookupError(f"Workflow {instance_id!r} not found")
        return self.states[instance_id]

    def raise_workflow_event(
        self, instance_id: str, event_name: str, data: Any
    ) -> None:
        with self._condition:
            self.events.append((instance_id, event_name, data))
            self._condition.notify_all()

    def wait_for_event(
        self,
        instance_id: str,
        event_name: str,
        *,
        timeout: float = 5.0,
    ) -> Any:
        """Block until an event matching (instance_id, event_name) arrives.

        Consumes the matched event from the queue and returns its data.
        Raises ``TimeoutError`` if the event does not arrive in time.
        """

        def _has_match() -> bool:
            return any(
                iid == instance_id and ename == event_name
                for iid, ename, _ in self.events
            )

        def _consume_match() -> Any:
            for i, (iid, ename, data) in enumerate(self.events):
                if iid == instance_id and ename == event_name:
                    self.events.pop(i)
                    return data
            raise LookupError("No matching event found")

        with self._condition:
            if _has_match():
                return _consume_match()

            if not self._condition.wait_for(_has_match, timeout=timeout):
                raise TimeoutError(
                    f"Timed out waiting for event {event_name!r} "
                    f"on workflow {instance_id!r}"
                )
            return _consume_match()

    def terminate_workflow(self, instance_id: str) -> None:
        self.terminated.append(instance_id)
        if instance_id in self.states:
            self.states[instance_id].runtime_status = "terminated"

    def resume_workflow(self, instance_id: str) -> None:
        self.resumed.append(instance_id)

    def schedule_new_workflow(
        self,
        workflow_name: str,
        *,
        input: Any,
        instance_id: str | None = None,
    ) -> str:
        exec_id = instance_id or str(uuid4())
        self.scheduled.append(
            {
                "workflow_name": workflow_name,
                "input": input,
                "instance_id": exec_id,
            }
        )
        self.states[exec_id] = FakeWorkflowState(
            instance_id=exec_id,
            runtime_status="pending",
        )
        return exec_id
