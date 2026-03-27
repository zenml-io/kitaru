"""Focused unit tests for the Dapr runtime host."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest

from _dapr_fakes import FakeWorkflowClient, make_store, sample_record
from kitaru.engines.dapr.backend import DaprExecutionEngineBackend
from kitaru.engines.dapr.models import (
    FLOW_RESULT_ARTIFACT_ID_KEY,
    FinalizeExecutionPayload,
    WorkflowStartPayload,
    decode_transport_value,
    encode_transport_value,
)
from kitaru.engines.dapr.runtime import DaprRuntimeHost
from kitaru.wait import wait


class _RecordingWorkflowRuntime:
    instances: ClassVar[list[_RecordingWorkflowRuntime]] = []

    def __init__(self) -> None:
        self.registered_workflows: dict[str, Any] = {}
        self.registered_activities: dict[str, Any] = {}
        self.start_calls = 0
        self.shutdown_calls = 0
        self.__class__.instances.append(self)

    def register_workflow(self, fn: Any, *, name: str | None = None) -> None:
        self.registered_workflows[name or fn.__name__] = fn

    def register_activity(self, fn: Any, *, name: str | None = None) -> None:
        self.registered_activities[name or fn.__name__] = fn

    def start(self) -> None:
        self.start_calls += 1

    def shutdown(self) -> None:
        self.shutdown_calls += 1


@dataclass
class _FakeRetryPolicy:
    first_retry_interval: Any
    max_number_of_attempts: int
    backoff_coefficient: float = 1.0


class _FakeWorkflowContext:
    def __init__(self) -> None:
        self.activity_calls: list[dict[str, Any]] = []
        self.wait_calls: list[str] = []

    def call_activity(
        self,
        activity: str,
        *,
        input: Any = None,
        retry_policy: Any = None,
        app_id: str | None = None,
    ) -> dict[str, Any]:
        marker = {
            "kind": "activity",
            "name": activity,
            "input": input,
            "retry_policy": retry_policy,
        }
        self.activity_calls.append(marker)
        return marker

    def wait_for_external_event(self, name: str) -> dict[str, Any]:
        self.wait_calls.append(name)
        return {"kind": "wait", "name": name}


@pytest.fixture
def workflow_sdk_fakes(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    from dapr.ext import workflow

    _RecordingWorkflowRuntime.instances.clear()
    clients: list[FakeWorkflowClient] = []

    def _client_factory() -> FakeWorkflowClient:
        client = FakeWorkflowClient()
        clients.append(client)
        return client

    monkeypatch.setattr(workflow, "WorkflowRuntime", _RecordingWorkflowRuntime)
    monkeypatch.setattr(workflow, "DaprWorkflowClient", _client_factory)
    monkeypatch.setattr(workflow, "RetryPolicy", _FakeRetryPolicy)
    monkeypatch.setattr(workflow, "when_all", lambda tasks: tasks)
    return {"clients": clients}


def _make_backend_with_store() -> tuple[DaprExecutionEngineBackend, Any]:
    backend = DaprExecutionEngineBackend()
    store, _ = make_store()
    backend.bind_ledger_store_provider(lambda: store)
    return backend, store


class TestRuntimeHostLifecycle:
    def test_ensure_started_is_idempotent(
        self, workflow_sdk_fakes: dict[str, Any]
    ) -> None:
        backend, _store = _make_backend_with_store()
        backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="my_flow",
        )
        backend.create_checkpoint_definition(
            entrypoint=lambda x: x,
            registration_name="step_a",
            retries=1,
            checkpoint_type=None,
            runtime=None,
        )

        host = DaprRuntimeHost(backend=backend)
        host.ensure_started()
        host.ensure_started()

        runtime = _RecordingWorkflowRuntime.instances[0]
        assert runtime.start_calls == 1
        assert set(runtime.registered_workflows) == {"my_flow"}
        assert set(runtime.registered_activities) == {"step_a", "__kitaru_finalize"}
        assert host.workflow_client_if_started() is not None

    def test_schedule_execution_uses_exec_id_as_instance_id(
        self, workflow_sdk_fakes: dict[str, Any]
    ) -> None:
        backend, _store = _make_backend_with_store()
        backend.create_flow_definition(
            entrypoint=lambda: None,
            registration_name="my_flow",
        )

        host = DaprRuntimeHost(backend=backend)
        host.schedule_execution(workflow_name="my_flow", exec_id="exec-123")

        client = workflow_sdk_fakes["clients"][0]
        assert client.scheduled == [
            {
                "workflow_name": "my_flow",
                "input": {"exec_id": "exec-123"},
                "instance_id": "exec-123",
            }
        ]


class TestRuntimeHostWorkflowBridge:
    def test_workflow_body_suspends_on_checkpoint_then_finalizes(self) -> None:
        backend, store = _make_backend_with_store()
        checkpoint = backend.create_checkpoint_definition(
            entrypoint=lambda x: x + 1,
            registration_name="step_a",
            retries=2,
            checkpoint_type=None,
            runtime=None,
        )
        flow = backend.create_flow_definition(
            entrypoint=lambda x: checkpoint.call(x),
            registration_name="my_flow",
        )
        host = DaprRuntimeHost(backend=backend)

        exec_id = "exec-1"
        store.create_execution(
            sample_record(
                exec_id=exec_id,
                flow_name="my_flow",
                workflow_name="my_flow",
                status="pending",
            )
        )
        store.store_execution_input(exec_id, {"args": (5,), "kwargs": {}})

        ctx = _FakeWorkflowContext()
        body = host._make_workflow_body(flow)
        generator = body(ctx, WorkflowStartPayload(exec_id=exec_id).to_dict())

        first = next(generator)
        assert first["name"] == "step_a"
        assert decode_transport_value(first["input"]["args"]) == [5]
        assert first["retry_policy"].max_number_of_attempts == 3

        activity_shim = host._make_activity_shim(checkpoint)
        result_payload = activity_shim(None, first["input"])

        finalize_call = generator.send(result_payload)
        assert finalize_call["name"] == "__kitaru_finalize"
        host._finalize_activity(None, finalize_call["input"])

        with pytest.raises(StopIteration) as stop:
            generator.send(None)
        assert stop.value.value == {"exec_id": exec_id}
        assert store.get_execution(exec_id).status == "completed"
        assert (
            store.get_execution(exec_id).metadata["kitaru"][FLOW_RESULT_ARTIFACT_ID_KEY]
            == f"{exec_id}:flow_result"
        )

    def test_workflow_body_waits_for_event_and_marks_wait_resolved(self) -> None:
        backend, store = _make_backend_with_store()
        flow = backend.create_flow_definition(
            entrypoint=lambda: wait(name="approval", question="Proceed?", timeout=60),
            registration_name="wait_flow",
        )
        host = DaprRuntimeHost(backend=backend)

        exec_id = "exec-wait"
        store.create_execution(
            sample_record(
                exec_id=exec_id,
                flow_name="wait_flow",
                workflow_name="wait_flow",
                status="pending",
            )
        )
        store.store_execution_input(exec_id, {"args": (), "kwargs": {}})

        ctx = _FakeWorkflowContext()
        body = host._make_workflow_body(flow)
        generator = body(ctx, WorkflowStartPayload(exec_id=exec_id).to_dict())

        first = next(generator)
        assert first == {"kind": "wait", "name": "approval"}
        record = store.get_execution(exec_id)
        assert record.status == "suspended"
        assert record.waits[0].status == "pending"

        finalize_call = generator.send(
            {
                "__kitaru_transport": encode_transport_value(
                    "approved",
                    label="approval",
                ).to_dict()
            }
        )
        assert finalize_call["name"] == "__kitaru_finalize"
        post_wait = store.get_execution(exec_id)
        assert post_wait.status == "running"
        assert post_wait.waits[0].status == "resolved"
        host._finalize_activity(None, finalize_call["input"])
        with pytest.raises(StopIteration):
            generator.send(None)

    def test_finalize_failure_keeps_terminated_status(self) -> None:
        backend, store = _make_backend_with_store()
        host = DaprRuntimeHost(backend=backend)
        exec_id = "exec-term"
        store.create_execution(
            sample_record(
                exec_id=exec_id,
                status="terminated",
                ended_at=None,
            )
        )

        host._finalize_activity(
            None,
            FinalizeExecutionPayload(
                exec_id=exec_id,
                status="failed",
                ended_at=datetime.now(UTC).isoformat(),
                failure={"message": "boom"},
            ).to_dict(),
        )

        record = store.get_execution(exec_id)
        assert record.status == "terminated"
        assert record.ended_at is not None
