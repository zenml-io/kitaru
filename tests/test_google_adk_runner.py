"""Runner wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    agent_module = importlib.import_module("kitaru.adapters.google_adk._agent")
    tracking_module = importlib.import_module("kitaru.adapters.google_adk._tracking")
    return adapter, agent_module, tracking_module


class FakeRunner:
    name = "fake_runner"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def run(self, *, user_id: str, session_id: str, new_message: Any, **kwargs: Any):
        self.calls.append(
            {
                "user_id": user_id,
                "session_id": session_id,
                "new_message": new_message,
                "kwargs": kwargs,
            }
        )
        return [{"final_output": f"echo:{new_message}"}]


class AsyncOnlyRunner:
    name = "async_runner"

    async def run_async(self, *, user_id: str, session_id: str, new_message: Any):
        yield {"final_output": new_message}


class ContextAwareRunner:
    name = "context_runner"

    def __init__(self, *, model: Any, tool: Any, tracking_module: Any) -> None:
        self.model = model
        self.tool = tool
        self.tracking_module = tracking_module
        self.seen_tracker: Any | None = None
        self.seen_policy: Any | None = None

    def run(self, *, user_id: str, session_id: str, new_message: Any, **kwargs: Any):
        self.seen_tracker = self.tracking_module.current_tracker()
        self.seen_policy = self.tracking_module.current_call_policy()
        model_events = asyncio.run(
            _collect_model_events(self.model, {"prompt": new_message})
        )
        tool_context = ToolContext()
        tool_result = asyncio.run(
            self.tool.run_async(args={"query": new_message}, tool_context=tool_context)
        )
        return [
            {
                "final_output": {
                    "model_events": model_events,
                    "tool_result": tool_result,
                    "tool_state": tool_context.state,
                }
            }
        ]


class FailingRunner:
    name = "failing_runner"

    def __init__(self, tracking_module: Any) -> None:
        self.tracking_module = tracking_module
        self.seen_tracker: Any | None = None

    def run(self, *, user_id: str, session_id: str, new_message: Any, **kwargs: Any):
        self.seen_tracker = self.tracking_module.current_tracker()
        raise RuntimeError("sync boom")


class AsyncFailingRunner:
    name = "async_failing_runner"

    def __init__(self, tracking_module: Any) -> None:
        self.tracking_module = tracking_module
        self.seen_tracker: Any | None = None

    async def run_async(self, *, user_id: str, session_id: str, new_message: Any):
        self.seen_tracker = self.tracking_module.current_tracker()
        raise RuntimeError("async boom")
        yield {"final_output": new_message}


class RawModel:
    model = "gemini-fake-runner"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        self.calls.append({"request": dict(llm_request), "stream": stream})
        yield {"text": f"model:{llm_request['prompt']}"}


class RawTool:
    name = "runner_lookup"
    description = "Local runner lookup."

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["lookup_marker"] = f"tool:{args['query']}"
        return {"answer": f"tool:{args['query']}"}


class ToolContext:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {"seed": "same"}


async def _collect_model_events(model: Any, request: dict[str, Any]) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def test_runner_call_wraps_sync_runner_in_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fake_checkpoint)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="runner_call")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="hi")
    )

    assert result.status == "completed"
    assert result.final_output == "echo:hi"
    assert result.event_log_artifact_name is None
    assert result.run_summary_artifact_name is None
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["adk_input"]["adapter"] == "google_adk"
    )
    assert runner.calls[0]["new_message"] == "hi"


def test_runner_call_outside_flow_calls_runner_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="runner_call")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="direct")
    )

    assert result.final_output == "echo:direct"
    assert runner.calls[0]["kwargs"] == {}


def test_calls_mode_runs_directly_and_warns_when_no_wrappers_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="calls")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="calls")
    )

    assert result.status == "completed"
    assert result.final_output == "echo:calls"
    assert result.events == [{"final_output": "echo:calls"}]
    assert not any("kind" in event for event in result.events)
    assert runner.calls[0]["new_message"] == "calls"
    assert result.warnings == [
        "Google ADK calls mode observed no KitaruADKModel or KitaruADKTool "
        "calls, so this run has no per-call ADK checkpoints. Arbitrary "
        "unmodified ADK internals are not checkpointed."
    ]


def test_calls_mode_does_not_open_outer_runner_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()
    policy = adapter.ADKCallCheckpointPolicy(persist_run_artifacts=False)

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    def fail_if_checkpointed(**kwargs: Any) -> Any:
        raise AssertionError("calls mode must not wrap the ADK runner in a checkpoint")

    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fail_if_checkpointed)

    wrapped = adapter.KitaruADKRunner(
        runner,
        checkpoint_strategy="calls",
        call_checkpoint_policy=policy,
    )
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="direct")
    )

    assert result.final_output == "echo:direct"
    assert runner.calls[0]["new_message"] == "direct"


def test_calls_mode_persists_summary_after_runner_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    order: list[str] = []
    summary_checkpoints: list[dict[str, Any]] = []

    class OrderedRunner(FakeRunner):
        def run(
            self,
            *,
            user_id: str,
            session_id: str,
            new_message: Any,
            **kwargs: Any,
        ) -> Any:
            order.append("runner")
            return super().run(
                user_id=user_id,
                session_id=session_id,
                new_message=new_message,
                **kwargs,
            )

    runner = OrderedRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    def fake_summary_checkpoint(**kwargs: Any) -> Any:
        order.append("summary")
        summary_checkpoints.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fake_summary_checkpoint)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="calls")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="summary")
    )

    assert result.final_output == "echo:summary"
    assert order == ["runner", "summary"]
    assert len(summary_checkpoints) == 1
    assert summary_checkpoints[0]["step_name"] == "fake_runner_google_adk_calls_summary"


def test_calls_mode_keeps_user_run_kwargs_but_does_not_inject_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()
    existing_plugins = ["existing"]
    request = adapter.ADKRunRequest(
        user_id="u",
        session_id="s",
        message="calls",
        run_kwargs={"plugins": existing_plugins, "temperature": 0.1},
    )

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="calls")
    result = wrapped.run_sync(request)

    assert result.final_output == "echo:calls"
    assert runner.calls[0]["kwargs"] == {
        "plugins": existing_plugins,
        "temperature": 0.1,
    }
    assert request.run_kwargs == {"plugins": ["existing"], "temperature": 0.1}


def test_calls_mode_tracker_context_is_active_for_explicit_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, tracking_module = _modules(monkeypatch)
    raw_model = RawModel()
    raw_tool = RawTool()
    wrapped_model = adapter.KitaruADKModel(raw_model, name="runner_model")
    wrapped_tool = adapter.KitaruADKTool(raw_tool, name="runner_tool")
    runner = ContextAwareRunner(
        model=wrapped_model,
        tool=wrapped_tool,
        tracking_module=tracking_module,
    )
    policy = adapter.ADKCallCheckpointPolicy(persist_run_artifacts=False)

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(
        runner,
        checkpoint_strategy="calls",
        call_checkpoint_policy=policy,
    )
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="cats")
    )

    assert runner.seen_tracker is not None
    assert runner.seen_policy is policy
    assert raw_model.calls == [{"request": {"prompt": "cats"}, "stream": False}]
    assert raw_tool.calls == [{"query": "cats"}]
    assert result.final_output == {
        "model_events": [{"text": "model:cats"}],
        "tool_result": {"answer": "tool:cats"},
        "tool_state": {"seed": "same", "lookup_marker": "tool:cats"},
    }
    tracker_events = [event for event in result.events if "kind" in event]
    assert [event["kind"] for event in tracker_events] == ["model_call", "tool_call"]
    assert [event["status"] for event in tracker_events] == [
        "metadata_only",
        "metadata_only",
    ]
    assert tracker_events[0]["model_name"] == "gemini-fake-runner"
    assert tracker_events[1]["tool_name"] == "runner_tool"
    assert result.warnings == [
        "Google ADK calls mode used explicit KitaruADKModel / KitaruADKTool "
        "wrapper calls. Only those wrapper calls can create per-call "
        "checkpoints; arbitrary unmodified ADK internals are not checkpointed."
    ]


def test_calls_mode_rejects_nested_checkpoint_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, _tracking_module = _modules(monkeypatch)
    runner = FakeRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: True)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="calls")

    with pytest.raises(KitaruUsageError, match="nested_checkpoint_policy"):
        wrapped.run_sync(
            adapter.ADKRunRequest(user_id="u", session_id="s", message="nested")
        )

    assert runner.calls == []


def test_run_sync_rejects_async_only_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, _agent_module, _tracking_module = _modules(monkeypatch)
    wrapped = adapter.KitaruADKRunner(AsyncOnlyRunner())

    with pytest.raises(KitaruUsageError, match="only exposes `run_async"):
        wrapped.run_sync(
            adapter.ADKRunRequest(user_id="u", session_id="s", message="x")
        )


def test_calls_mode_sync_runner_exception_propagates_and_marks_tracker_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, tracking_module = _modules(monkeypatch)
    runner = FailingRunner(tracking_module)
    wrapped = adapter.KitaruADKRunner(
        runner,
        checkpoint_strategy="calls",
        call_checkpoint_policy=adapter.ADKCallCheckpointPolicy(
            persist_run_artifacts=False
        ),
    )
    returned: list[Any] = []

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    with pytest.raises(RuntimeError, match="sync boom"):
        returned.append(
            wrapped.run_sync(
                adapter.ADKRunRequest(user_id="u", session_id="s", message="x")
            )
        )

    assert returned == []
    assert runner.seen_tracker is not None
    assert runner.seen_tracker.summary()["status"] == "failed"
    assert runner.seen_tracker.summary()["error"] == "sync boom"


def test_calls_mode_async_runner_exception_propagates_and_marks_tracker_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module, tracking_module = _modules(monkeypatch)
    runner = AsyncFailingRunner(tracking_module)
    wrapped = adapter.KitaruADKRunner(
        runner,
        checkpoint_strategy="calls",
        call_checkpoint_policy=adapter.ADKCallCheckpointPolicy(
            persist_run_artifacts=False
        ),
    )
    returned: list[Any] = []

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    with pytest.raises(RuntimeError, match="async boom"):
        returned.append(
            asyncio.run(
                wrapped.run(
                    adapter.ADKRunRequest(user_id="u", session_id="s", message="x")
                )
            )
        )

    assert returned == []
    assert runner.seen_tracker is not None
    assert runner.seen_tracker.summary()["status"] == "failed"
    assert runner.seen_tracker.summary()["error"] == "async boom"
