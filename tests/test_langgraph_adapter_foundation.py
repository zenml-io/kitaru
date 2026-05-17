"""Foundation tests for the LangGraph adapter scaffold."""

from __future__ import annotations

import importlib
import inspect
import json
import sys
import types
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruRuntimeError, KitaruUsageError


@pytest.fixture
def langgraph_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Import the adapter with a fake optional SDK module installed."""
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.langgraph"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.setitem(sys.modules, "langgraph", types.ModuleType("langgraph"))
    return importlib.import_module("kitaru.adapters.langgraph")


def test_public_import_surface(langgraph_adapter: types.ModuleType) -> None:
    assert langgraph_adapter.KitaruGraphRunner
    assert langgraph_adapter.LangGraphRunRequest
    assert langgraph_adapter.LangGraphRunResult
    assert langgraph_adapter.LangGraphCapturePolicy
    assert langgraph_adapter.LangGraphDurabilityPolicy
    assert langgraph_adapter.build_resume_request
    assert langgraph_adapter.wait_for_interrupt

    public_names = set(langgraph_adapter.__all__)
    assert "graph_call" not in public_names

    signature = inspect.signature(langgraph_adapter.KitaruGraphRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "durability_mode" not in signature.parameters
    assert not hasattr(langgraph_adapter.KitaruGraphRunner, "stream")
    assert not hasattr(langgraph_adapter.KitaruGraphRunner, "astream")


def test_langgraph_analytics_events_exist() -> None:
    values = [
        event.value for event in AnalyticsEvent if event.name.startswith("LANGGRAPH_")
    ]

    assert values == [
        "Kitaru LangGraph wrapped",
        "Kitaru LangGraph run completed",
        "Kitaru LangGraph interrupted",
    ]


def test_runner_requires_stable_name(langgraph_adapter: types.ModuleType) -> None:
    with pytest.raises(KitaruUsageError, match="stable `name`"):
        langgraph_adapter.KitaruGraphRunner(SimpleNamespace(invoke=lambda *_: None))


def test_runner_only_accepts_graph_call_strategy(
    langgraph_adapter: types.ModuleType,
) -> None:
    runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        checkpoint_strategy="graph_call",
    )

    assert runner.checkpoint_strategy == "graph_call"
    with pytest.raises(KitaruUsageError, match="graph_call"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            checkpoint_strategy="nodes",
        )


def test_run_request_start_resume_and_config_merge(
    langgraph_adapter: types.ModuleType,
) -> None:
    start = langgraph_adapter.LangGraphRunRequest.start(
        {"count": 1},
        thread_id="thread-1",
        configurable={"tenant": "acme", "thread_id": "wrong"},
        config={"configurable": {"region": "eu", "thread_id": "older"}},
        checkpoint_id="checkpoint-1",
        checkpoint_ns="ns",
    )

    merged = langgraph_adapter.merge_config(start)

    assert start.kind == "start"
    assert merged["configurable"] == {
        "region": "eu",
        "tenant": "acme",
        "thread_id": "thread-1",
        "checkpoint_id": "checkpoint-1",
        "checkpoint_ns": "ns",
    }

    command = SimpleNamespace(resume=True)
    resume = langgraph_adapter.LangGraphRunRequest.resume(
        command,
        thread_id="thread-1",
    )
    assert resume.kind == "resume"
    assert resume.command is command

    none_input_start = langgraph_adapter.LangGraphRunRequest.start(
        None,
        thread_id="thread-none",
    )
    assert none_input_start.kind == "start"
    assert none_input_start.input is None

    direct_none_input_start = langgraph_adapter.LangGraphRunRequest(
        kind="start",
        input=None,
        thread_id="thread-none-direct",
    )
    assert direct_none_input_start.input is None

    with pytest.raises(ValidationError, match="thread_id"):
        langgraph_adapter.LangGraphRunRequest.start({"count": 1}, thread_id="")
    with pytest.raises(ValidationError, match="requires input"):
        langgraph_adapter.LangGraphRunRequest(kind="start", thread_id="thread-1")
    with pytest.raises(ValidationError, match="requires command"):
        langgraph_adapter.LangGraphRunRequest(kind="resume", thread_id="thread-1")


def test_capture_policy_and_checkpoint_config_validation(
    langgraph_adapter: types.ModuleType,
) -> None:
    with pytest.raises(ValidationError):
        langgraph_adapter.LangGraphCapturePolicy(capture_mode="raw")
    with pytest.raises(ValidationError):
        langgraph_adapter.LangGraphCapturePolicy(save_everything=True)

    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            run_checkpoint_config={"runtime": "isolated"},
        )
    with pytest.raises(KitaruUsageError, match="cache must be a boolean"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            run_checkpoint_config={"cache": "nope"},
        )


def test_outer_checkpoint_defaults_disable_cache_and_retries(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    seen: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        seen.update(kwargs)
        body = cast(Callable[[], object], kwargs["body"])
        return body()

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"count": 1},
            thread_id="thread-1",
        )
    )

    assert result.output == {"count": 1}
    assert seen["config"] == {
        "retries": 0,
        "cache": False,
        "type": "graph_call",
        "runtime": "inline",
    }


def test_outer_checkpoint_overrides_are_honored(
    langgraph_adapter: types.ModuleType,
) -> None:
    runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        run_checkpoint_config={"cache": True, "retries": 2, "type": "custom"},
    )

    assert runner._graph_call_checkpoint_config() == {
        "cache": True,
        "retries": 2,
        "type": "custom",
        "runtime": "inline",
    }


def test_event_tracker_uses_role_first_artifact_names(
    langgraph_adapter: types.ModuleType,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")

    tracker = tracking.EventTracker(graph_name="Fake Graph", run_label="ab12cd34")

    assert tracker.event_log_artifact_name == "event_log__Fake_Graph_ab12cd34"
    assert tracker.run_summary_artifact_name == "run_summary__Fake_Graph_ab12cd34"


def test_successful_graph_call_saves_event_artifacts_in_checkpoint_scope(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    saved: list[tuple[str, object, str]] = []
    logged: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return {"echo": input}

    def fake_save(name: str, value: object, *, type: str) -> None:
        saved.append((name, value, type))

    def fake_log(**kwargs: object) -> None:
        logged.update(kwargs)

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking.kitaru, "save", fake_save)
    monkeypatch.setattr(tracking.kitaru, "log", fake_log)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())

    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.output == {"echo": {"input": "value"}}
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None
    assert result.event_log_artifact_name.startswith("event_log__fake_")
    assert result.run_summary_artifact_name.startswith("run_summary__fake_")
    assert [(name, type_) for name, _value, type_ in saved] == [
        (result.event_log_artifact_name, "context"),
        (result.run_summary_artifact_name, "context"),
    ]
    assert logged

    event_log = cast(list[dict[str, object]], saved[0][1])
    run_summary = cast(dict[str, object], saved[1][1])
    assert [event["kind"] for event in event_log] == [
        "graph_call_started",
        "graph_call_completed",
    ]
    assert run_summary["status"] == "completed"
    assert run_summary["thread_id"] == "thread-1"

    logged_events = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_EVENTS_METADATA_KEY],
    )
    logged_summaries = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_RUN_SUMMARIES_METADATA_KEY],
    )
    event_metadata = next(iter(logged_events.values()))
    summary_metadata = next(iter(logged_summaries.values()))
    assert event_metadata["artifact_name"] == result.event_log_artifact_name
    assert event_metadata["event_count"] == 2
    assert "kind" not in event_metadata
    assert summary_metadata["artifact_name"] == result.run_summary_artifact_name
    assert summary_metadata["status"] == "completed"
    assert summary_metadata["thread_id"] == "thread-1"


def test_failed_graph_call_saves_event_artifacts_in_checkpoint_scope(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    saved: list[tuple[str, object, str]] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, _input: object, **_kwargs: object) -> object:
            raise RuntimeError("boom")

    def fake_save(name: str, value: object, *, type: str) -> None:
        saved.append((name, value, type))

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking.kitaru, "save", fake_save)
    monkeypatch.setattr(tracking.kitaru, "log", lambda **_kwargs: None)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())

    with pytest.raises(RuntimeError, match="boom"):
        runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"input": "value"},
                thread_id="thread-1",
            )
        )

    assert [(name.split("__", 1)[0], type_) for name, _value, type_ in saved] == [
        ("event_log", "context"),
        ("run_summary", "context"),
    ]
    event_log = cast(list[dict[str, object]], saved[0][1])
    run_summary = cast(dict[str, object], saved[1][1])
    assert [event["kind"] for event in event_log] == [
        "graph_call_started",
        "graph_call_failed",
    ]
    assert run_summary["status"] == "failed"
    assert run_summary["thread_id"] == "thread-1"
    assert run_summary["error_type"] == "RuntimeError"
    assert run_summary["error_message"] == "boom"


def test_event_persistence_save_failure_is_best_effort_by_default(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    logged: dict[str, object] = {}
    saved_names: list[str] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def fake_save(name: str, _value: object, *, type: str) -> None:
        assert type == "context"
        saved_names.append(name)
        if name.startswith("event_log__"):
            raise RuntimeError("artifact store unavailable")

    def fake_log(**kwargs: object) -> None:
        logged.update(kwargs)

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking.kitaru, "save", fake_save)
    monkeypatch.setattr(tracking.kitaru, "log", fake_log)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())

    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.output == {"input": "value"}
    assert any(name.startswith("event_log__fake_") for name in saved_names)
    assert any(name.startswith("run_summary__fake_") for name in saved_names)

    summaries = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_RUN_SUMMARIES_METADATA_KEY],
    )
    summary_metadata = next(iter(summaries.values()))
    failures = cast(list[dict[str, object]], summary_metadata["persistence_failures"])
    assert failures[0]["operation"] == "save_event_log"
    assert failures[0]["artifact_name"] == result.event_log_artifact_name


def test_event_persistence_can_fail_strictly(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def fake_save(name: str, _value: object, *, type: str) -> None:
        assert type == "context"
        if name.startswith("event_log__"):
            raise RuntimeError("artifact store unavailable")

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking.kitaru, "save", fake_save)
    monkeypatch.setattr(tracking.kitaru, "log", lambda **_kwargs: None)

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        capture=langgraph_adapter.LangGraphCapturePolicy(
            fail_on_event_persistence_error=True
        ),
    )

    with pytest.raises(KitaruRuntimeError, match="LangGraph event/log persistence"):
        runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"input": "value"},
                thread_id="thread-1",
            )
        )


def test_durability_policy_supplies_default_graph_durability(
    langgraph_adapter: types.ModuleType,
) -> None:
    seen: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **kwargs: object) -> object:
            seen.update(kwargs)
            return input

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        durability=langgraph_adapter.LangGraphDurabilityPolicy(mode="exit"),
    )
    runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"count": 1},
            thread_id="thread-1",
        )
    )

    assert seen["durability"] == "exit"


def test_capture_policy_can_disable_state_snapshot_inspection(
    langgraph_adapter: types.ModuleType,
) -> None:
    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

        def get_state(self, _config: object) -> object:
            raise AssertionError("get_state should not be called")

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        capture=langgraph_adapter.LangGraphCapturePolicy(save_state_snapshot=False),
    )

    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(None, thread_id="thread-none")
    )

    assert result.status == "completed"
    assert result.output is None
    assert result.state_summary is None


def test_default_task_capture_saves_metadata_not_raw_task_internals(
    langgraph_adapter: types.ModuleType,
) -> None:
    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

        def get_state(self, _config: object) -> object:
            return SimpleNamespace(
                config={"configurable": {"checkpoint_id": "checkpoint-1"}},
                next=(),
                values={"answer": 42},
                tasks=(
                    SimpleNamespace(
                        id="task-1",
                        name="review_node",
                        path=("review_node",),
                        interrupts=(),
                        result={"sensitive": "do not capture by default"},
                        error=None,
                    ),
                ),
            )

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())

    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"answer": 41},
            thread_id="thread-1",
        )
    )

    assert result.state_summary is not None
    assert result.state_summary.values is None
    assert result.state_summary.tasks == [
        {
            "index": 0,
            "id": "task-1",
            "name": "review_node",
            "path": ["review_node"],
            "interrupt_count": 0,
            "has_result": True,
            "result_has_interrupt": False,
            "has_error": False,
            "error_type": None,
        }
    ]


def test_save_context_captures_resolved_context_factory_value(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    logged: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def fake_log(**kwargs: object) -> None:
        logged.update(kwargs)

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking.kitaru, "log", fake_log)

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        capture=langgraph_adapter.LangGraphCapturePolicy(save_context=True),
        context_factory=lambda _request: {"tenant": "acme"},
    )

    runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
            context={"tenant": "stale"},
        )
    )

    summaries = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_RUN_SUMMARIES_METADATA_KEY],
    )
    summary = next(iter(summaries.values()))
    assert summary["context"] == {"tenant": "acme"}


def test_failed_graph_call_persists_failure_summary(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    logged: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, _input: object, **_kwargs: object) -> object:
            raise RuntimeError("boom")

    def fake_log(**kwargs: object) -> None:
        logged.update(kwargs)

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking.kitaru, "log", fake_log)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())

    with pytest.raises(RuntimeError, match="boom"):
        runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"input": "value"},
                thread_id="thread-1",
            )
        )

    summaries = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_RUN_SUMMARIES_METADATA_KEY],
    )
    summary = next(iter(summaries.values()))
    assert summary["status"] == "failed"
    assert summary["thread_id"] == "thread-1"
    assert summary["error_type"] == "RuntimeError"
    assert summary["error_message"] == "boom"
    assert summary["input"] == {"input": "value"}

    event_logs = cast(
        dict[str, list[dict[str, object]]],
        logged[constants.LANGGRAPH_EVENTS_METADATA_KEY],
    )
    events = next(iter(event_logs.values()))
    assert [event["kind"] for event in events] == [
        "graph_call_started",
        "graph_call_failed",
    ]


def test_wait_for_interrupt_forwards_metadata_to_wait_and_resume_request(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hitl = importlib.import_module("kitaru.adapters.langgraph._hitl")
    wait_metadata: dict[str, object] = {}
    resume_metadata: dict[str, object] | None = None

    result = langgraph_adapter.LangGraphRunResult(
        status="interrupted",
        thread_id="thread-1",
        interrupts=[
            langgraph_adapter.LangGraphInterruptSummary(
                index=0,
                value={"question": "approve?"},
                node_name="approval_node",
            )
        ],
        pending_state=langgraph_adapter.LangGraphPendingState(thread_id="thread-1"),
    )

    def fake_wait(**kwargs: object) -> object:
        wait_metadata.update(cast(dict[str, object], kwargs["metadata"]))
        return {"approved": True}

    def fake_build_resume_request(
        _result: object,
        _payload: object,
        **kwargs: object,
    ) -> object:
        nonlocal resume_metadata
        resume_metadata = cast(dict[str, object] | None, kwargs["metadata"])
        return SimpleNamespace(metadata=resume_metadata)

    monkeypatch.setattr(hitl, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(hitl.kitaru, "wait", fake_wait)
    monkeypatch.setattr(hitl, "build_resume_request", fake_build_resume_request)

    resume_request = langgraph_adapter.wait_for_interrupt(
        result,
        schema=dict,
        metadata={"review_id": "review-1"},
    )

    assert wait_metadata["adapter"] == "langgraph"
    assert wait_metadata["source"] == "interrupt_bridge"
    assert wait_metadata["node_name"] == "approval_node"
    assert wait_metadata["user_metadata"] == {"review_id": "review-1"}
    assert resume_metadata == {"review_id": "review-1"}
    assert resume_request.metadata == {"review_id": "review-1"}


def test_build_resume_request_uses_selected_interrupt_id(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCommand:
        def __init__(self, *, resume: object) -> None:
            self.resume = resume

    fake_langgraph_types = types.ModuleType("langgraph.types")
    cast(Any, fake_langgraph_types).Command = FakeCommand
    monkeypatch.setitem(sys.modules, "langgraph.types", fake_langgraph_types)

    result = langgraph_adapter.LangGraphRunResult(
        status="interrupted",
        thread_id="thread-1",
        interrupts=[
            langgraph_adapter.LangGraphInterruptSummary(
                index=0,
                interrupt_id="interrupt-0",
                value={"question": "first?"},
                node_name="first_node",
            ),
            langgraph_adapter.LangGraphInterruptSummary(
                index=1,
                interrupt_id="interrupt-1",
                value={"question": "second?"},
                node_name="second_node",
            ),
        ],
        pending_state=langgraph_adapter.LangGraphPendingState(thread_id="thread-1"),
    )

    request = langgraph_adapter.build_resume_request(
        result,
        {"approved": True},
        interrupt_index=1,
    )

    assert request.command.resume == {"interrupt-1": {"approved": True}}


def test_redaction_handles_non_serializable_values_and_odd_keys(
    langgraph_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.langgraph._serialization")

    class BadRepr:
        def __repr__(self) -> str:
            raise RuntimeError("repr exploded")

    class OddKey:
        def __str__(self) -> str:
            raise RuntimeError("key exploded")

    cyclic: dict[str, object] = {"api_key": "SECRET-IN-CYCLE"}
    cyclic["self"] = cyclic

    redacted = serialization.redact_config(
        {
            "api_key": BadRepr(),
            "payload": BadRepr(),
            OddKey(): "value",
            "items": {BadRepr()},
            "cycle": cyclic,
        }
    )

    assert redacted["api_key"] == "[REDACTED]"
    payload = cast(dict[str, str], redacted["payload"])
    assert payload["python_type"].endswith("BadRepr")
    assert "repr" not in payload
    assert any(key.startswith("<unprintable key") for key in redacted)
    cycle = cast(dict[str, object], redacted["cycle"])
    assert cycle["api_key"] == "[REDACTED]"
    cycle_self = cast(dict[str, str], cycle["self"])
    assert cycle_self["serialization_error"] == "cycle_detected"
    assert "SECRET-IN-CYCLE" not in json.dumps(redacted)


def test_redaction_handles_common_secret_key_forms(
    langgraph_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.langgraph._serialization")

    redacted = serialization.redact_config(
        {
            "x-api-key": "secret",
            "Authorization": "Bearer secret",
            "safe": "value",
        }
    )

    assert redacted == {
        "x-api-key": "[REDACTED]",
        "Authorization": "[REDACTED]",
        "safe": "value",
    }
