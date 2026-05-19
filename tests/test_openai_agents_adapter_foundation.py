"""Foundation tests for the OpenAI Agents SDK adapter scaffold."""

from __future__ import annotations

import importlib
import inspect
import sys
import types
from types import SimpleNamespace
from typing import cast

import pytest
from pydantic import ValidationError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruUsageError
from tests._checkpoint_handle_helpers import (
    assert_checkpoint_handle_error,
    checkpoint_output_handle,
)


@pytest.fixture
def openai_agents_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Import the adapter with a fake optional SDK module installed."""
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.openai_agents"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.setitem(sys.modules, "agents", types.ModuleType("agents"))
    return importlib.import_module("kitaru.adapters.openai_agents")


def test_public_import_surface_uses_calls_vocabulary(
    openai_agents_adapter: types.ModuleType,
) -> None:
    assert openai_agents_adapter.KitaruRunner
    assert openai_agents_adapter.OpenAIRunRequest
    assert openai_agents_adapter.OpenAIRunResult
    assert openai_agents_adapter.OpenAIRunStateEnvelope
    assert openai_agents_adapter.OpenAIApprovalDecision
    assert openai_agents_adapter.OpenAIInterruptionSummary
    assert openai_agents_adapter.wait_for_approval
    assert openai_agents_adapter.build_resume_request

    public_names = set(openai_agents_adapter.__all__)
    assert "durability_mode" not in public_names
    assert "step" not in public_names
    assert "run" not in public_names

    signature = inspect.signature(openai_agents_adapter.KitaruRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "durability_mode" not in signature.parameters


def test_openai_analytics_events_do_not_use_old_mode_vocabulary() -> None:
    values = [
        event.value
        for event in AnalyticsEvent
        if event.name.startswith("OPENAI_AGENTS_")
    ]

    assert values
    assert all(" step " not in f" {value.lower()} " for value in values)


def test_openai_event_kind_uses_runner_call_not_run(
    openai_agents_adapter: types.ModuleType,
) -> None:
    event = openai_agents_adapter.OpenAIRunEvent(
        event_id="evt_1",
        kind="runner_call",
        status="completed",
        sequence_index=0,
        run_label="agent-runner-call",
        agent_name="agent",
    )

    assert event.kind == "runner_call"
    with pytest.raises(ValidationError):
        openai_agents_adapter.OpenAIRunEvent(
            event_id="evt_2",
            kind="run",
            status="completed",
            sequence_index=1,
            run_label="agent-runner-call",
            agent_name="agent",
        )


def test_kitaru_runner_accepts_public_checkpoint_strategies(
    openai_agents_adapter: types.ModuleType,
) -> None:
    calls_runner = openai_agents_adapter.KitaruRunner(
        SimpleNamespace(name="agent"), checkpoint_strategy="calls"
    )
    runner_call_runner = openai_agents_adapter.KitaruRunner(
        SimpleNamespace(name="agent"), checkpoint_strategy="runner_call"
    )

    assert calls_runner.checkpoint_strategy == "calls"
    assert runner_call_runner.checkpoint_strategy == "runner_call"


@pytest.mark.parametrize("legacy_name", ["step", "run"])
def test_kitaru_runner_rejects_old_mode_names(
    openai_agents_adapter: types.ModuleType,
    legacy_name: str,
) -> None:
    with pytest.raises(
        KitaruUsageError, match=r"calls.*runner_call|runner_call.*calls"
    ):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"), checkpoint_strategy=legacy_name
        )


def test_kitaru_runner_requires_stable_name(
    openai_agents_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="stable `name`"):
        openai_agents_adapter.KitaruRunner(SimpleNamespace())


def test_kitaru_runner_requires_context_codec_pair(
    openai_agents_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="provided together"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            context_serializer=lambda value: value,
        )


def test_run_sync_rejects_running_event_loop(
    openai_agents_adapter: types.ModuleType,
) -> None:
    runner = openai_agents_adapter.KitaruRunner(SimpleNamespace(name="agent"))
    request = openai_agents_adapter.OpenAIRunRequest.start("hello")

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="already running event loop"):
            runner.run_sync(request)

    import asyncio

    asyncio.run(call_sync())


def test_runner_call_strategy_runs_without_calls_wrappers(
    openai_agents_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_agents = sys.modules["agents"]
    monkeypatch.setattr(
        fake_agents,
        "RunConfig",
        lambda: SimpleNamespace(),
        raising=False,
    )
    agent = SimpleNamespace(name="agent")
    calls: list[object] = []

    def fake_run_openai_agent_sync(**kwargs: object) -> SimpleNamespace:
        calls.append(kwargs["agent"])
        return SimpleNamespace(final_output="hello", last_response_id="resp_1")

    agent_module = importlib.import_module("kitaru.adapters.openai_agents._agent")
    monkeypatch.setattr(
        agent_module,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )
    runner = openai_agents_adapter.KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
    )

    result = runner.run_sync(openai_agents_adapter.OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "hello"
    assert calls == [agent]


def test_openai_run_request_start_and_resume_validation(
    openai_agents_adapter: types.ModuleType,
) -> None:
    envelope = openai_agents_adapter.OpenAIRunStateEnvelope(
        agents_sdk_version="0.15.0",
        state_json={"current_turn": 1},
    )
    decision = openai_agents_adapter.OpenAIApprovalDecision(approve=True)

    start = openai_agents_adapter.OpenAIRunRequest.start("hello")
    resume = openai_agents_adapter.OpenAIRunRequest.resume(envelope, decision)

    assert start.kind == "start"
    assert start.input == "hello"
    assert resume.kind == "resume"
    assert resume.pending_state == envelope
    assert resume.decision == decision

    with pytest.raises(ValidationError, match="requires input"):
        openai_agents_adapter.OpenAIRunRequest(kind="start")
    with pytest.raises(ValidationError, match="forbids input"):
        openai_agents_adapter.OpenAIRunRequest(
            kind="resume",
            input="not allowed",
            pending_state=envelope,
            decision=decision,
        )


def test_openai_run_request_rejects_arbitrary_input_object(
    openai_agents_adapter: types.ModuleType,
) -> None:
    with pytest.raises(ValidationError):
        openai_agents_adapter.OpenAIRunRequest(kind="start", input={"role": "user"})


def test_openai_run_request_rejects_checkpoint_handles(
    openai_agents_adapter: types.ModuleType,
) -> None:
    handle = checkpoint_output_handle()

    with pytest.raises(ValidationError) as direct_exc:
        openai_agents_adapter.OpenAIRunRequest.start(cast(str, handle))
    assert_checkpoint_handle_error(
        direct_exc,
        field_name="OpenAIRunRequest.input",
    )

    with pytest.raises(ValidationError) as nested_exc:
        openai_agents_adapter.OpenAIRunRequest.start(
            [{"role": "user", "content": handle}]
        )
    assert_checkpoint_handle_error(
        nested_exc,
        field_name="OpenAIRunRequest.input[0].content",
    )

    valid = openai_agents_adapter.OpenAIRunRequest.start(
        [{"role": "user", "content": "hello"}]
    )
    assert valid.input == [{"role": "user", "content": "hello"}]


def test_interrupted_result_requires_state_and_interruption(
    openai_agents_adapter: types.ModuleType,
) -> None:
    envelope = openai_agents_adapter.OpenAIRunStateEnvelope(
        agents_sdk_version="0.15.0",
        state_json={"current_turn": 1},
    )
    interruption = openai_agents_adapter.OpenAIInterruptionSummary(
        index=0,
        kind="tool_approval",
        tool_name="publish",
        call_id="call_123",
    )

    result = openai_agents_adapter.OpenAIRunResult(
        status="interrupted",
        pending_state=envelope,
        interruptions=[interruption],
    )

    assert result.status == "interrupted"
    assert result.pending_state == envelope

    with pytest.raises(ValidationError, match="require pending_state"):
        openai_agents_adapter.OpenAIRunResult(status="interrupted")


def test_build_resume_request_uses_first_interruption_by_default(
    openai_agents_adapter: types.ModuleType,
) -> None:
    envelope = openai_agents_adapter.OpenAIRunStateEnvelope(
        agents_sdk_version="0.15.0",
        state_json={"current_turn": 1},
    )
    result = openai_agents_adapter.OpenAIRunResult(
        status="interrupted",
        pending_state=envelope,
        interruptions=[
            openai_agents_adapter.OpenAIInterruptionSummary(
                index=3,
                kind="tool_approval",
                tool_name="publish",
            )
        ],
    )

    request = openai_agents_adapter.build_resume_request(result, approve=True)

    assert request.kind == "resume"
    assert request.decision is not None
    assert request.decision.interruption_index == 3
    assert request.decision.approve is True

    with pytest.raises(KitaruUsageError, match="does not contain interruption index"):
        openai_agents_adapter.build_resume_request(
            result,
            approve=True,
            interruption_index=99,
        )


def test_wait_for_approval_bridges_to_kitaru_wait(
    openai_agents_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hitl_module = importlib.import_module("kitaru.adapters.openai_agents._hitl")
    envelope = openai_agents_adapter.OpenAIRunStateEnvelope(
        agents_sdk_version="0.15.0",
        state_json={"current_turn": 1},
    )
    result = openai_agents_adapter.OpenAIRunResult(
        status="interrupted",
        pending_state=envelope,
        interruptions=[
            openai_agents_adapter.OpenAIInterruptionSummary(
                index=3,
                kind="tool_approval",
                tool_name="publish",
                call_id="call_123",
            )
        ],
    )
    wait_calls: list[dict[str, object]] = []

    def fake_wait(**kwargs: object) -> bool:
        wait_calls.append(kwargs)
        return False

    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(hitl_module.kitaru, "wait", fake_wait)

    request = openai_agents_adapter.wait_for_approval(
        result,
        name="approve publish",
        denial_message="not today",
    )

    assert request.decision is not None
    assert request.decision.approve is False
    assert request.decision.rejection_message == "not today"
    assert wait_calls[0]["schema"] is bool
    assert wait_calls[0]["name"] == "approve publish"
    assert wait_calls[0]["metadata"] == {
        "adapter": "openai_agents",
        "source": "approval_bridge",
        "interruption_index": 3,
        "tool_name": "publish",
        "call_id": "call_123",
    }


def test_capture_policy_rejects_unknown_fields(
    openai_agents_adapter: types.ModuleType,
) -> None:
    with pytest.raises(ValidationError):
        openai_agents_adapter.OpenAICapturePolicy(durability_mode="step")


def test_durability_policy_not_on_public_surface(
    openai_agents_adapter: types.ModuleType,
) -> None:
    assert not hasattr(openai_agents_adapter, "OpenAIDurabilityPolicy")


def test_checkpoint_config_validation_rejects_invalid_values(
    openai_agents_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            model_checkpoint_config={"runtime": "isolated"},
        )
    with pytest.raises(KitaruUsageError, match="Expected 'inline'"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            model_checkpoint_config={"runtime": "banana"},
        )
    with pytest.raises(KitaruUsageError, match="non-negative integer"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            model_checkpoint_config={"retries": -1},
        )
    with pytest.raises(KitaruUsageError, match="non-empty string"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            model_checkpoint_config={"type": ""},
        )
    with pytest.raises(KitaruUsageError, match="Unsupported keys"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            model_checkpoint_config={"cache": False},
        )


def test_tool_checkpoint_override_validation(
    openai_agents_adapter: types.ModuleType,
) -> None:
    runner = openai_agents_adapter.KitaruRunner(
        SimpleNamespace(name="agent"),
        tool_checkpoint_config_by_name={"expensive_tool": False},
    )

    assert runner.name == "agent"

    with pytest.raises(KitaruUsageError, match="non-empty tool name"):
        openai_agents_adapter.KitaruRunner(
            SimpleNamespace(name="agent"),
            tool_checkpoint_config_by_name={"": False},
        )
