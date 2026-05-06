"""Focused tests for OpenAI Agents SDK call-level checkpointing."""

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
