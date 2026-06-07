"""Foundation tests for the LangGraph adapter scaffold."""

from __future__ import annotations

import asyncio
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
    assert langgraph_adapter.LangGraphCallCheckpointPolicy
    assert langgraph_adapter.LangGraphCapturePolicy
    assert langgraph_adapter.LangGraphDurabilityPolicy
    assert langgraph_adapter.LangGraphStreamPolicy
    assert langgraph_adapter.build_resume_request
    assert langgraph_adapter.wait_for_interrupt

    public_names = set(langgraph_adapter.__all__)
    assert "graph_call" not in public_names

    signature = inspect.signature(langgraph_adapter.KitaruGraphRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "call_checkpoint_policy" in signature.parameters
    assert "durability_mode" not in signature.parameters
    assert hasattr(langgraph_adapter.KitaruGraphRunner, "stream")
    assert hasattr(langgraph_adapter.KitaruGraphRunner, "astream")
    assert "stream_policy" in signature.parameters


def test_synthetic_checkpoint_marks_flow_result_non_candidate(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = importlib.import_module("kitaru.adapters.langgraph._utils")
    captured: dict[str, Any] = {}

    class FakeCheckpoint:
        _step = object()

    def fake_checkpoint(**kwargs: Any) -> Any:
        captured.update(kwargs)

        def decorate(func: Any) -> FakeCheckpoint:
            captured["decorated_name"] = func.__name__
            return FakeCheckpoint()

        return decorate

    monkeypatch.setattr(utils, "_synthetic_checkpoint", fake_checkpoint)

    utils._build_checkpoint_step(
        config={"type": "llm_call", "cache": False, "retries": 2},
        step_name="graph call",
        body=lambda: "ok",
    )

    assert langgraph_adapter.KitaruGraphRunner
    assert captured["flow_result_candidate"] is False
    assert captured["type"] == "llm_call"
    assert captured["cache"] is False
    assert captured["retries"] == 2
    assert captured["decorated_name"] == "graph_call"


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


def test_runner_accepts_only_graph_call_and_calls_strategies(
    langgraph_adapter: types.ModuleType,
) -> None:
    graph_runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        checkpoint_strategy="graph_call",
    )
    calls_runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        checkpoint_strategy="calls",
    )

    assert graph_runner.checkpoint_strategy == "graph_call"
    assert calls_runner.checkpoint_strategy == "calls"
    with pytest.raises(KitaruUsageError, match=r"graph_call.*calls"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            checkpoint_strategy="nodes",
        )


def test_runner_rejects_incompatible_graph_call_and_calls_configs(
    langgraph_adapter: types.ModuleType,
) -> None:
    policy = langgraph_adapter.LangGraphCallCheckpointPolicy()

    with pytest.raises(KitaruUsageError, match="run_checkpoint_config"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            checkpoint_strategy="calls",
            run_checkpoint_config={"type": "custom"},
        )
    with pytest.raises(KitaruUsageError, match="call_checkpoint_policy"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            checkpoint_strategy="graph_call",
            call_checkpoint_policy=policy,
        )


def test_call_checkpoint_policy_and_capture_flags_are_dependency_safe(
    langgraph_adapter: types.ModuleType,
) -> None:
    policy_module = importlib.import_module("kitaru.adapters.langgraph._policy")

    capture = langgraph_adapter.LangGraphCapturePolicy()
    assert capture.emit_call_events is True
    assert capture.save_model_input is True
    assert capture.save_model_response is True
    assert capture.save_model_usage is True
    assert capture.save_tool_args is True
    assert capture.save_tool_result is True
    capture_fields = langgraph_adapter.LangGraphCapturePolicy.model_fields
    assert "raw message/system text is omitted" in cast(
        str, capture_fields["save_model_input"].description
    )
    assert "event artifact reference" in cast(
        str, capture_fields["save_model_response"].description
    )
    assert "event artifact reference" in cast(
        str, capture_fields["save_tool_result"].description
    )

    policy = langgraph_adapter.LangGraphCallCheckpointPolicy(
        model_checkpoint_config=False,
        tool_checkpoint_config={"type": "toolish"},
        tool_checkpoint_config_by_name={"skip_me": False, "custom": {"cache": False}},
        summary_checkpoint_config={"retries": 1},
    )
    assert policy_module.resolve_model_checkpoint_config(policy) is None
    assert (
        policy_module.resolve_tool_call_checkpoint_config(policy, tool_name="skip_me")
        is None
    )
    assert policy_module.resolve_tool_call_checkpoint_config(
        policy, tool_name="plain"
    ) == {"type": "toolish"}
    assert policy_module.resolve_tool_call_checkpoint_config(
        policy, tool_name="custom"
    ) == {"cache": False, "type": "tool_call"}
    assert policy_module.resolve_summary_checkpoint_config(policy) == {
        "retries": 1,
        "type": "langgraph_summary",
    }
    assert policy.persist_run_artifacts is True
    assert "skip both summary checkpoint persistence" in cast(
        str,
        langgraph_adapter.LangGraphCallCheckpointPolicy.model_fields[
            "persist_run_artifacts"
        ].description,
    )

    with pytest.raises(ValidationError, match="runtime='isolated'"):
        langgraph_adapter.LangGraphCallCheckpointPolicy(
            model_checkpoint_config={"runtime": "isolated"}
        )
    with pytest.raises(ValidationError, match="runtime='isolated'"):
        langgraph_adapter.LangGraphCallCheckpointPolicy(
            summary_checkpoint_config={"runtime": "isolated"}
        )


def test_structural_checkpoint_artifact_refs_are_adapter_local(
    langgraph_adapter: types.ModuleType,
) -> None:
    utils = importlib.import_module("kitaru.adapters.langgraph._utils")

    assert utils.get_adapter_checkpoint_artifact_refs() is None
    with utils.adapter_checkpoint_artifact_refs(
        input_artifacts={"model_input": "model_input"},
        output_artifacts={"output": "output"},
    ) as refs:
        assert utils.get_adapter_checkpoint_artifact_refs() is refs
        assert refs.input_artifacts == {"model_input": "model_input"}
        assert refs.output_artifacts == {"output": "output"}
    assert utils.get_adapter_checkpoint_artifact_refs() is None


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


class ForeignLangGraphRunResult:
    """Same-shaped LangGraph result with intentionally different identity."""

    def __init__(self, *, thread_id: str = "thread-1") -> None:
        interrupt = {"index": 0, "value": {"question": "continue?"}}
        self.schema_version = 1
        self.status = "interrupted"
        self.output = None
        self.thread_id = thread_id
        self.latest_checkpoint_id = "checkpoint-1"
        self.next_nodes = ["approval"]
        self.interrupts = [interrupt]
        self.pending_state = {
            "thread_id": thread_id,
            "checkpoint_id": "checkpoint-1",
            "next_nodes": ["approval"],
            "interrupts": [interrupt],
        }
        self.state_summary = {
            "latest_checkpoint_id": "checkpoint-1",
            "next_nodes": ["approval"],
            "interrupts": [interrupt],
        }
        self.warnings: list[str] = []

    def model_dump(self, *, mode: str) -> dict[str, object]:
        assert mode == "python"
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "output": self.output,
            "thread_id": self.thread_id,
            "latest_checkpoint_id": self.latest_checkpoint_id,
            "next_nodes": self.next_nodes,
            "interrupts": self.interrupts,
            "pending_state": self.pending_state,
            "state_summary": self.state_summary,
            "warnings": self.warnings,
        }


class InvalidForeignLangGraphRunResult(ForeignLangGraphRunResult):
    """Foreign LangGraph result whose dumped payload violates the schema."""

    def model_dump(self, *, mode: str) -> dict[str, object]:
        payload = super().model_dump(mode=mode)
        payload["unexpected_field"] = "bad"
        return payload


class CheckpointableGraph:
    name = "fake"
    checkpointer = object()

    def invoke(self, input: object, **_kwargs: object) -> object:
        return input

    async def ainvoke(self, input: object, **_kwargs: object) -> object:
        return input


def test_invoke_canonicalizes_foreign_graph_call_checkpoint_result(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module,
        "run_sync_in_checkpoint",
        lambda **_kwargs: ForeignLangGraphRunResult(),
    )

    runner = langgraph_adapter.KitaruGraphRunner(CheckpointableGraph())
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start({"count": 1}, thread_id="thread-1")
    )

    assert isinstance(result, langgraph_adapter.LangGraphRunResult)
    assert not isinstance(result, ForeignLangGraphRunResult)
    assert isinstance(result.pending_state, langgraph_adapter.LangGraphPendingState)
    assert isinstance(
        result.pending_state.interrupts[0],
        langgraph_adapter.LangGraphInterruptSummary,
    )
    assert isinstance(result.interrupts[0], langgraph_adapter.LangGraphInterruptSummary)


def test_ainvoke_canonicalizes_foreign_graph_call_checkpoint_result(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)

    async def fake_run_async_in_checkpoint(**_kwargs: object) -> object:
        return ForeignLangGraphRunResult()

    monkeypatch.setattr(
        agent_module, "run_async_in_checkpoint", fake_run_async_in_checkpoint
    )

    runner = langgraph_adapter.KitaruGraphRunner(CheckpointableGraph())

    async def call_ainvoke() -> object:
        return await runner.ainvoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"count": 1}, thread_id="thread-1"
            )
        )

    result = asyncio.run(call_ainvoke())

    assert isinstance(result, langgraph_adapter.LangGraphRunResult)
    assert not isinstance(result, ForeignLangGraphRunResult)
    assert isinstance(result.pending_state, langgraph_adapter.LangGraphPendingState)
    assert isinstance(result.state_summary, langgraph_adapter.LangGraphStateSummary)


def test_invalid_graph_call_checkpoint_result_fails_before_success_tracking(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module,
        "run_sync_in_checkpoint",
        lambda **_kwargs: InvalidForeignLangGraphRunResult(),
    )
    runner = langgraph_adapter.KitaruGraphRunner(CheckpointableGraph())
    track_calls: list[tuple[object, dict[str, object]]] = []
    monkeypatch.setattr(
        agent_module,
        "track",
        lambda event, metadata: track_calls.append((event, metadata)),
    )
    with pytest.raises(ValidationError):
        runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"count": 1}, thread_id="thread-1"
            )
        )

    assert track_calls == []


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


def test_event_tracker_records_model_and_tool_calls_in_reserved_order(
    langgraph_adapter: types.ModuleType,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")

    tracker = tracking.EventTracker(graph_name="Fake Graph", run_label="ab12cd34")
    model_id, model_context = tracker.start_model_event()
    tracker.reserve_tool_call_order(
        parent_model_event_id=model_id,
        tool_call_ids=["call-a", "call-b"],
    )
    tool_b_id, tool_b_context = tracker.start_tool_event(tool_call_id="call-b")
    tracker.record_tool_event(
        tool_b_id,
        tool_b_context,
        status="completed",
        duration_ms=2.0,
        tool_name="second_tool",
        tool_call_id="call-b",
        checkpoint_mode="metadata_only",
    )
    tool_a_id, tool_a_context = tracker.start_tool_event(tool_call_id="call-a")
    tracker.record_tool_event(
        tool_a_id,
        tool_a_context,
        status="completed",
        duration_ms=1.0,
        tool_name="first_tool",
        tool_call_id="call-a",
        checkpoint_mode="true",
    )
    tracker.record_model_event(
        model_id,
        model_context,
        status="completed",
        duration_ms=3.0,
        model_name="fake-model",
        source="unit-test",
    )

    events = [event.model_dump(mode="json") for event in tracker.events]
    assert [event["kind"] for event in events] == [
        "model_call",
        "tool_call",
        "tool_call",
    ]
    assert [event["tool_call_id"] for event in events[1:]] == ["call-a", "call-b"]
    assert events[1]["parent_event_ids"] == [model_id]
    assert events[2]["parent_event_ids"] == [model_id]
    assert events[1]["checkpoint_mode"] == "true"
    assert events[2]["checkpoint_mode"] == "metadata_only"

    summary = tracker.build_run_summary()
    assert summary["model_call_count"] == 1
    assert summary["tool_call_count"] == 2
    assert summary["event_ids_in_order"] == [
        model_id,
        tool_a_id,
        tool_b_id,
    ]


def test_calls_mode_sets_active_context_and_skips_outer_graph_checkpoint(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    checkpoint_names: list[str] = []
    seen_context: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            seen_context["tracker"] = tracking.get_current_tracker()
            seen_context["policy"] = tracking.get_active_call_checkpoint_policy()
            seen_context["capture"] = tracking.get_active_capture_policy()
            return {"echo": input}

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        checkpoint_names.append(cast(str, kwargs["step_name"]))
        body = cast(Callable[[], object], kwargs["body"])
        return body()

    def fake_log(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )
    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking.kitaru, "log", fake_log)

    policy = langgraph_adapter.LangGraphCallCheckpointPolicy()
    capture = langgraph_adapter.LangGraphCapturePolicy(save_context=True)
    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        checkpoint_strategy="calls",
        call_checkpoint_policy=policy,
        capture=capture,
    )

    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.output == {"echo": {"input": "value"}}
    assert seen_context["tracker"] is not None
    assert seen_context["policy"] is policy
    assert seen_context["capture"] is capture
    assert tracking.get_current_tracker() is None
    assert tracking.get_active_call_checkpoint_policy() is None
    assert tracking.get_active_capture_policy() is None
    assert not any(name.endswith("_langgraph_call") for name in checkpoint_names)
    assert any(name.startswith("langgraph_summary__fake_") for name in checkpoint_names)


def test_calls_mode_nested_checkpoint_policy_controls_validation(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    called = False
    active_policy: object | None = None

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            nonlocal active_policy, called
            called = True
            active_policy = tracking.get_active_call_checkpoint_policy()
            return input

    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: True)

    error_runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        checkpoint_strategy="calls",
    )
    with pytest.raises(KitaruUsageError, match="nested_checkpoint_policy"):
        error_runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"input": "value"},
                thread_id="thread-1",
            )
        )
    assert called is False

    metadata_runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        checkpoint_strategy="calls",
        call_checkpoint_policy=langgraph_adapter.LangGraphCallCheckpointPolicy(
            nested_checkpoint_policy="metadata_only"
        ),
    )
    result = metadata_runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )
    assert result.output == {"input": "value"}
    assert called is True
    assert active_policy is not None
    assert active_policy.model_checkpoint_config is False
    assert active_policy.tool_checkpoint_config is False


def test_calls_mode_strict_summary_failure_preserves_original_graph_exception(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    checkpoint_names: list[str] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, _input: object, **_kwargs: object) -> object:
            raise ValueError("graph boom")

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        checkpoint_names.append(cast(str, kwargs["step_name"]))
        raise RuntimeError("summary unavailable")

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        checkpoint_strategy="calls",
        capture=langgraph_adapter.LangGraphCapturePolicy(
            fail_on_event_persistence_error=True
        ),
    )

    with pytest.raises(ValueError, match="graph boom") as exc_info:
        runner.invoke(
            langgraph_adapter.LangGraphRunRequest.start(
                {"input": "value"},
                thread_id="thread-1",
            )
        )

    assert any(name.startswith("langgraph_summary__fake_") for name in checkpoint_names)
    assert any("summary unavailable" in note for note in exc_info.value.__notes__)


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


def test_find_usage_ignores_application_usage_dict_without_token_fields(
    langgraph_adapter: types.ModuleType,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")

    assert (
        agent_module._find_usage(
            {"usage": {"feature": "beta"}},
            max_depth=3,
        )
        is None
    )


def test_find_usage_accepts_zero_token_usage(
    langgraph_adapter: types.ModuleType,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")

    usage = agent_module._find_usage(
        {"message": {"usage_metadata": {"input_tokens": 0, "output_tokens": 1}}},
        max_depth=3,
    )

    assert usage == {"input_tokens": 0, "output_tokens": 1}


def test_graph_call_counts_request_response_token_aliases(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    logged: list[dict[str, Any]] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return {
                "usage": {
                    "request_tokens": 4,
                    "response_tokens": 6,
                    "tokens_total": 10,
                },
                "echo": input,
            }

    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.usage is not None
    assert result.usage.input_tokens == 4
    assert result.usage.output_tokens == 6
    assert result.usage.total_tokens == 10
    assert logged[0]["usage"]["input_tokens"] == 4
    assert logged[0]["usage"]["output_tokens"] == 6
    assert logged[0]["usage"]["total_tokens"] == 10


def test_graph_call_keeps_successful_run_when_cost_calculator_fails(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    logged: list[dict[str, Any]] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return {"usage": {"input_tokens": 1, "output_tokens": 2}, "echo": input}

    def fail_cost(_usage: object) -> float:
        raise RuntimeError("pricing service down")

    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        cost_calculator=fail_cost,
    )
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.estimated_cost_usd is None
    assert any("cost calculator failed" in warning for warning in result.warnings)
    assert len(logged) == 1
    assert logged[0]["cost"]["estimated_cost_usd"] is None
    assert logged[0]["warnings"] == result.warnings


def test_graph_call_logs_one_record_without_usage(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    logged: list[dict[str, Any]] = []

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return {"usage": {"feature": "beta"}, "echo": input}

    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"input": "value"},
            thread_id="thread-1",
        )
    )

    assert result.usage is None
    assert len(logged) == 1
    assert logged[0]["usage"]["total_tokens"] is None


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
