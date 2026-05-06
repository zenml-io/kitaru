"""Focused tests for OpenAI Agents SDK call-level checkpointing."""

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("agents")

from agents import Agent, RunConfig, function_tool
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)
from zenml.client import Client

from kitaru import flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest
from kitaru.errors import KitaruUsageError


class StaticTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(self.text, response_id=f"resp_text_{self.call_count}")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class RepeatedToolCallingModel(Model):
    def __init__(self) -> None:
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        if self.call_count in {1, 2}:
            return ModelResponse(
                output=[
                    ResponseFunctionToolCall(
                        arguments='{"value": 4}',
                        call_id=f"call_repeat_{self.call_count}",
                        id=f"fc_repeat_{self.call_count}",
                        name="double_value",
                        status="completed",
                        type="function_call",
                    )
                ],
                usage=Usage(
                    requests=1, input_tokens=3, output_tokens=2, total_tokens=5
                ),
                response_id=f"resp_repeat_{self.call_count}",
            )
        return _text_response("repeated tool complete", response_id="resp_repeat_final")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class ToolCallingModel(Model):
    def __init__(self) -> None:
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        if self.call_count % 2 == 1:
            return ModelResponse(
                output=[
                    ResponseFunctionToolCall(
                        arguments='{"value": 4}',
                        call_id="call_cached_tool",
                        id="fc_1",
                        name="double_value",
                        status="completed",
                        type="function_call",
                    )
                ],
                usage=Usage(
                    requests=1, input_tokens=3, output_tokens=2, total_tokens=5
                ),
                response_id="resp_tool_call",
            )
        return _text_response("tool complete", response_id="resp_final")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


def _text_response(text: str, *, response_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseOutputMessage(
                id=f"msg_{response_id}",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text=text,
                        type="output_text",
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            )
        ],
        usage=Usage(requests=1, input_tokens=2, output_tokens=3, total_tokens=5),
        response_id=response_id,
    )


def _wait_for_hydrated_run(exec_id: str) -> Any:
    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    if not run.status.is_finished:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    assert run.status.is_successful
    return run.get_hydrated_version()


def _step_names(hydrated_run: Any) -> set[str]:
    return set(hydrated_run.steps)


class TestOpenAIEventTrackerToolCallOrdering:
    def _record_completed_model(self, tracker: Any) -> tuple[str, Any]:
        event_id, event_context = tracker.start_llm_event()
        tracker.record_event(
            event_id,
            event_context,
            kind="llm_call",
            status="completed",
            duration_ms=1.0,
            artifacts={},
            metadata={"response_id": "resp_ordering"},
        )
        return event_id, event_context

    def _record_completed_tool(
        self,
        tracker: Any,
        event_id: str,
        event_context: Any,
        *,
        name: str,
    ) -> None:
        tracker.record_event(
            event_id,
            event_context,
            kind="tool_call",
            status="completed",
            duration_ms=1.0,
            artifacts={},
            metadata={"tool_name": name},
        )

    def test_reserved_tool_ids_follow_model_order_when_start_order_reverses(
        self,
    ) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_id != beta_id
        assert model_context.sequence_index < alpha_context.sequence_index
        assert alpha_context.sequence_index < beta_context.sequence_index

    def test_reverse_completion_order_sorts_events_persisted_log_and_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from kitaru.adapters.openai_agents import _tracking

        tracker = _tracking.EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])
        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(tracker, beta_id, beta_context, name="beta")
        self._record_completed_tool(tracker, alpha_id, alpha_context, name="alpha")

        assert [event.event_id for event in tracker.events] == [
            model_id,
            alpha_id,
            beta_id,
        ]

        logged: dict[str, Any] = {}
        monkeypatch.setattr(_tracking, "is_inside_flow", lambda: True)
        monkeypatch.setattr(
            _tracking.kitaru,
            "log",
            lambda **kwargs: logged.update(kwargs),
        )

        tracker.persist()

        events_dump = logged["openai_agents_events"][tracker.run_label]
        summary_dump = logged["openai_agents_run_summaries"][tracker.run_label]
        assert [event["event_id"] for event in events_dump] == [
            model_id,
            alpha_id,
            beta_id,
        ]
        assert summary_dump["event_ids_in_order"] == [model_id, alpha_id, beta_id]
        assert summary_dump["total_events"] == 3

    def test_missing_or_unreserved_tool_call_id_keeps_counter_fallback(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, model_context = self._record_completed_model(tracker)

        event_id, event_context = tracker.start_tool_event(tool_call_id=None)

        assert event_id.startswith(
            f"{tracker.agent_name}_{tracker.run_label}_tool_call_"
        )
        assert event_context.sequence_index > model_context.sequence_index

    def test_abandoned_reserved_tool_slot_does_not_count_or_leak(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(
            tracker,
            alpha_id,
            alpha_context,
            name="alpha",
        )

        summary = tracker.build_run_summary()

        assert summary["total_events"] == 2
        assert summary["event_ids_in_order"] == [model_id, alpha_id]

        next_model_id, next_model_context = tracker.start_llm_event()
        fallback_id, fallback_context = tracker.start_tool_event(
            tool_call_id="call_beta"
        )
        assert fallback_id != next_model_id
        assert fallback_context.sequence_index > next_model_context.sequence_index

    def test_nested_llm_start_does_not_clear_sibling_tool_reservation(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        nested_model_id, nested_context = tracker.start_llm_event()
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_id != beta_id
        assert nested_model_id not in {alpha_id, beta_id}
        assert alpha_context.sequence_index < beta_context.sequence_index
        assert beta_context.sequence_index < nested_context.sequence_index


class TestOpenAIModelToolCallReservations:
    def test_trackable_tool_call_ids_follow_model_response_order(self) -> None:
        from kitaru.adapters.openai_agents._model import _trackable_tool_call_ids

        @function_tool
        def alpha() -> str:
            return "alpha"

        @function_tool
        def beta() -> str:
            return "beta"

        response = SimpleNamespace(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_alpha",
                    id="fc_alpha",
                    name="alpha",
                    status="completed",
                    type="function_call",
                ),
                {"name": "hosted_lookup", "call_id": "call_hosted"},
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_beta",
                    id="fc_beta",
                    name="beta",
                    status="completed",
                    type="function_call",
                ),
            ]
        )

        assert _trackable_tool_call_ids(response, [alpha, beta]) == [
            "call_alpha",
            "call_beta",
        ]

    @pytest.mark.anyio
    async def test_get_response_reserves_tool_order_when_checkpoint_is_cached(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import kitaru.adapters.openai_agents._model as openai_model
        from kitaru.adapters.openai_agents._model import KitaruOpenAIModel
        from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

        @function_tool
        def alpha() -> str:
            return "alpha"

        @function_tool
        def beta() -> str:
            return "beta"

        cached_response = SimpleNamespace(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_alpha",
                    id="fc_alpha",
                    name="alpha",
                    status="completed",
                    type="function_call",
                ),
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_beta",
                    id="fc_beta",
                    name="beta",
                    status="completed",
                    type="function_call",
                ),
            ],
            usage=None,
            response_id="resp_cached",
        )
        seen_tool_call_ids: list[list[str]] = []

        class FakeTracker:
            def reserve_tool_call_order(self, tool_call_ids: list[str]) -> None:
                seen_tool_call_ids.append(tool_call_ids)

        async def fake_run_async_in_checkpoint(**_kwargs: Any) -> Any:
            return cached_response

        monkeypatch.setattr(openai_model, "is_inside_flow", lambda: True)
        monkeypatch.setattr(openai_model, "is_inside_checkpoint", lambda: False)
        monkeypatch.setattr(openai_model, "get_current_tracker", lambda: FakeTracker())
        monkeypatch.setattr(
            openai_model,
            "run_async_in_checkpoint",
            fake_run_async_in_checkpoint,
        )
        model = KitaruOpenAIModel(
            SimpleNamespace(),
            capture=OpenAICapturePolicy(),
            agent_name="cached_agent",
            checkpoint_config={},
        )

        response = await model.get_response(
            None,
            "prompt",
            None,
            [alpha, beta],
            None,
            [],
            None,
            previous_response_id=None,
            conversation_id=None,
            prompt=None,
        )

        assert response is cached_response
        assert seen_tool_call_ids == [["call_alpha", "call_beta"]]


@pytest.mark.anyio
async def test_openai_tool_call_passes_tool_call_id_to_event_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    seen_tool_call_ids: list[str | None] = []

    @function_tool
    def publish() -> str:
        return "unused"

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            seen_tool_call_ids.append(tool_call_id)
            return "event-1", SimpleNamespace(sequence_index=1)

        def record_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    async def callback(_context: Any, _input_json: str) -> str:
        return "published"

    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: True)

    result = await openai_tools._tracked_tool_call(
        callback,
        SimpleNamespace(),
        "{}",
        tool=publish,
        capture=OpenAICapturePolicy(save_input=False, save_final_output=False),
        tool_call_id="call_publish",
    )

    assert result == "published"
    assert seen_tool_call_ids == ["call_publish"]


@pytest.mark.anyio
async def test_openai_tracked_tool_execution_remains_concurrent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import anyio

    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    started: list[str] = []
    both_started = anyio.Event()

    async def _mark_started(name: str) -> None:
        started.append(name)
        if len(started) == 2:
            both_started.set()
        await both_started.wait()

    @function_tool
    def alpha() -> str:
        return "unused"

    @function_tool
    def beta() -> str:
        return "unused"

    class FakeTracker:
        def __init__(self) -> None:
            self._counter = 0

        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            del tool_call_id
            self._counter += 1
            return f"event-{self._counter}", SimpleNamespace(
                sequence_index=self._counter,
            )

        def record_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    fake_tracker = FakeTracker()
    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: fake_tracker)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: True)
    capture = OpenAICapturePolicy(save_input=False, save_final_output=False)
    results: dict[str, str] = {}

    async def _run_tool(name: str, tool: Any, tool_call_id: str) -> None:
        async def callback(_context: Any, _input_json: str) -> str:
            await _mark_started(name)
            return name

        results[name] = await openai_tools._tracked_tool_call(
            callback,
            SimpleNamespace(),
            "{}",
            tool=tool,
            capture=capture,
            tool_call_id=tool_call_id,
        )

    with anyio.fail_after(1):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(_run_tool, "alpha", alpha, "call_alpha")
            task_group.start_soon(_run_tool, "beta", beta, "call_beta")

    assert set(started) == {"alpha", "beta"}
    assert results == {"alpha": "alpha", "beta": "beta"}


def test_calls_strategy_model_call_runs_inside_checkpoint(primed_zenml) -> None:
    model = StaticTextModel("model checkpointed")
    agent_name = f"openai_model_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def model_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    handle = model_flow.run("same prompt", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)
    assert any("openai_model_call" in name for name in _step_names(hydrated))
    assert model.call_count == 1


def test_calls_strategy_model_checkpoint_cache_skips_inner_model(
    primed_zenml,
) -> None:
    model = StaticTextModel("cached model")
    agent_name = f"openai_cached_model_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def cached_model_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = cached_model_flow.run("stable prompt", "first")
    _wait_for_hydrated_run(first.exec_id)
    assert model.call_count == 1

    second = cached_model_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert model.call_count == 1


def test_calls_strategy_function_tool_runs_inside_checkpoint_and_caches(
    primed_zenml,
) -> None:
    side_effects: list[int] = []

    @function_tool
    def double_value(value: int) -> str:
        side_effects.append(value)
        return f"doubled={value * 2}"

    model = ToolCallingModel()
    agent_name = f"openai_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[double_value]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = tool_flow.run("please use the tool", "first")
    _wait_for_hydrated_run(first.exec_id)
    assert side_effects == [4]
    assert model.call_count == 2
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert any("double_value_tool_call" in name for name in _step_names(first_hydrated))

    second = tool_flow.run("please use the tool", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert side_effects == [4]
    assert model.call_count == 2


def test_same_args_tool_calls_without_visible_call_id_do_not_collide(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    side_effects: list[int] = []

    @function_tool
    def double_value(value: int) -> str:
        side_effects.append(value)
        return f"doubled={value * 2}"

    import kitaru.adapters.openai_agents._tools as openai_tools

    monkeypatch.setattr(openai_tools, "_tool_call_id", lambda _context: None)
    model = RepeatedToolCallingModel()
    agent_name = f"openai_repeat_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[double_value]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def repeated_tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = repeated_tool_flow.run("use the repeated tool", "first")
    _wait_for_hydrated_run(first.exec_id)
    assert side_effects == [4, 4]
    assert model.call_count == 3

    second = repeated_tool_flow.run("use the repeated tool", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert side_effects == [4, 4]
    assert model.call_count == 3


def test_calls_strategy_rejects_inside_existing_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = StaticTextModel("nested")
    runner = KitaruRunner(Agent(name=f"nested_{uuid4().hex[:8]}", model=model))
    monkeypatch.setitem(
        runner._require_calls_scope.__globals__,
        "is_inside_checkpoint",
        lambda: True,
    )

    with pytest.raises(KitaruUsageError, match="must run from a flow body"):
        runner.run_sync(OpenAIRunRequest.start("hello"))
