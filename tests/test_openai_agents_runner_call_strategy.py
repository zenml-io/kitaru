"""Focused tests for OpenAI runner-call checkpointing and RunState bridging."""

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("agents")

from agents import Agent, RunConfig, function_tool
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import ResponseOutputMessage, ResponseOutputText
from zenml.client import Client

from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIApprovalDecision,
    OpenAIRunRequest,
    OpenAIRunStateEnvelope,
)


class StaticTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(self.text, response_id=f"resp_static_{self.call_count}")

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


def test_runner_call_strategy_checkpoints_outer_runner_and_caches(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = monkeypatch
    model = StaticTextModel("outer call result")
    agent_name = f"openai_runner_call_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def runner_call_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    first = runner_call_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    names = _step_names(first_hydrated)
    assert any("openai_runner_call" in name for name in names)
    assert not any("openai_model_call" in name for name in names)
    assert model.call_count == 1

    second = runner_call_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert model.call_count == 1


def test_runner_call_strategy_does_not_invoke_calls_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as openai_agent_module
    import kitaru.adapters.openai_agents._model as openai_model_module
    import kitaru.adapters.openai_agents._tools as openai_tools_module

    @function_tool
    def double_value(value: int) -> str:
        return f"doubled={value * 2}"

    def fail_model_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap model calls")

    def fail_tool_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap function tools")

    monkeypatch.setattr(
        openai_model_module,
        "kitaruify_openai_model",
        fail_model_wrapper,
    )
    monkeypatch.setattr(
        openai_tools_module,
        "kitaruify_openai_tools",
        fail_tool_wrapper,
    )
    monkeypatch.setattr(
        openai_agent_module,
        "run_openai_agent_sync",
        lambda **_kwargs: SimpleNamespace(final_output="ok"),
    )

    runner = KitaruRunner(
        Agent(name="coarse", model=StaticTextModel("ok"), tools=[double_value]),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    result = runner.run_sync(OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "ok"


def test_run_state_envelope_serialization_and_approval_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._runner as openai_runner

    class FakeState:
        def __init__(self) -> None:
            self._current_step = SimpleNamespace(interruptions=["first", "second"])
            self.approved: list[object] = []
            self.rejected: list[tuple[object, str | None]] = []

        def to_json(self, **kwargs: object) -> dict[str, object]:
            return {
                "current_turn": 1,
                "strict_context": kwargs["strict_context"],
                "has_context_serializer": callable(kwargs["context_serializer"]),
            }

        def approve(self, item: object) -> None:
            self.approved.append(item)

        def reject(
            self,
            item: object,
            *,
            rejection_message: str | None = None,
        ) -> None:
            self.rejected.append((item, rejection_message))

    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")
    state = FakeState()

    envelope = openai_runner.serialize_run_state(
        state,
        strict_sdk_version=True,
        context_serializer=lambda value: value,
        strict_context=True,
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(interruption_index=1, approve=True),
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(
            interruption_index=0,
            approve=False,
            rejection_message="nope",
        ),
    )

    assert envelope.agents_sdk_version == "0.15.0"
    assert envelope.state_json["current_turn"] == 1
    assert state.approved == ["second"]
    assert state.rejected == [("first", "nope")]


def test_runner_resume_applies_decision_before_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_state = SimpleNamespace(
        _current_step=SimpleNamespace(interruptions=["approval-item"]),
        approved=[],
    )

    def approve(item: object) -> None:
        fake_state.approved.append(item)

    fake_state.approve = approve
    seen_input: list[object] = []

    runner = KitaruRunner(
        SimpleNamespace(name="resume-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._sdk_input_sync.__globals__,
        "deserialize_run_state_sync",
        lambda *_args, **_kwargs: fake_state,
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        lambda **kwargs: (
            seen_input.append(kwargs["input"])
            or SimpleNamespace(final_output="resumed")
        ),
    )
    request = OpenAIRunRequest.resume(
        OpenAIRunStateEnvelope(
            agents_sdk_version="0.15.0",
            state_json={"current_turn": 1},
        ),
        OpenAIApprovalDecision(approve=True),
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert result.final_output == "resumed"
    assert fake_state.approved == ["approval-item"]
    assert seen_input == [fake_state]
