"""Fake-SDK tests for Gemini interaction checkpoint semantics."""

from __future__ import annotations

import asyncio
import importlib
import types
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.errors import (
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from tests._gemini_fake_sdk import (
    install_fake_google_genai,
    purge_gemini_adapter_modules,
)


@pytest.fixture
def gemini_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    purge_gemini_adapter_modules(monkeypatch)
    install_fake_google_genai(monkeypatch)
    return importlib.import_module("kitaru.adapters.gemini")


class FakeInteractions:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.create_calls: list[dict[str, Any]] = []
        self.get_calls: list[tuple[str, dict[str, Any]]] = []

    def create(self, **kwargs: Any) -> Any:
        self.create_calls.append(kwargs)
        return self.responses.pop(0)

    def get(self, id: str, **kwargs: Any) -> Any:
        self.get_calls.append((id, kwargs))
        return self.responses.pop(0)


class FakeClient:
    def __init__(self, responses: list[Any]) -> None:
        self.interactions = FakeInteractions(responses)


class ClientWithPublicState:
    def __init__(self) -> None:
        self.interactions = object()
        self.api_key = "secret"


def _completed_interaction(**updates: Any) -> SimpleNamespace:
    defaults = {
        "id": "interaction-1",
        "status": "completed",
        "previous_interaction_id": None,
        "model": "gemini-test",
        "agent": None,
        "outputs": [SimpleNamespace(type="text", text="hello from gemini")],
        "usage": SimpleNamespace(total_tokens=5),
    }
    defaults.update(updates)
    return SimpleNamespace(**defaults)


def _assert_get_timeout(
    call: tuple[str, dict[str, Any]],
    *,
    interaction_id: str,
    max_timeout_s: float,
) -> float:
    assert call[0] == interaction_id
    timeout = call[1].get("timeout")
    assert isinstance(timeout, float)
    assert 0 < timeout <= max_timeout_s
    return timeout


def _install_fake_runner_clock(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sleep_overrun_s: float = 0.0,
) -> list[float]:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    clock = {"now": 100.0}
    sleep_durations: list[float] = []

    def fake_perf_counter() -> float:
        return clock["now"]

    async def fake_sleep(duration_s: float) -> None:
        sleep_durations.append(duration_s)
        clock["now"] += duration_s + sleep_overrun_s

    monkeypatch.setattr(runner_module.time, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(runner_module.asyncio, "sleep", fake_sleep)
    return sleep_durations


def _patch_invocation_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[types.ModuleType, list[dict[str, Any]]]:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(agent, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: False)
    return agent, calls


def _patch_flow_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> list[dict[str, Any]]:
    agent, calls = _patch_invocation_scope(monkeypatch)

    def fake_checkpoint(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent, "run_sync_in_checkpoint", fake_checkpoint)
    return calls


def _patch_async_flow_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> list[dict[str, Any]]:
    agent, calls = _patch_invocation_scope(monkeypatch)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(agent, "run_async_in_checkpoint", fake_checkpoint)
    return calls


def test_run_sync_creates_one_synthetic_interaction_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    checkpoint_calls = _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
        generation_config={"temperature": 0.1},
    )

    result = runner.run_sync(request)

    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["config"]["type"] == "agent_call"
    assert checkpoint_calls[0]["step_name"] == "gemini_gemini_interaction"
    assert result.status == "completed"
    assert result.interaction_id == "interaction-1"
    assert result.output_text == "hello from gemini"
    assert result.steps[0].type == "text"
    assert result.usage == {"total_tokens": 5}
    assert "outputs rather than `steps`" in " ".join(result.warnings)
    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["input"] == "hello"
    assert create_kwargs["model"] == "gemini-test"
    assert create_kwargs["generation_config"] == {"temperature": 0.1}
    assert "agent" not in create_kwargs


def test_structured_response_forwards_format_and_mime_type(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    response_format = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
    }
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
        response_format=response_format,
        response_mime_type="application/json",
    )

    runner.run_sync(request)

    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["response_format"] == response_format
    assert create_kwargs["response_mime_type"] == "application/json"


def test_async_run_creates_one_synthetic_interaction_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    checkpoint_calls = _patch_async_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = asyncio.run(runner.run(request))

    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["step_name"] == "gemini_gemini_interaction"
    assert result.status == "completed"
    assert client.interactions.create_calls[0]["input"] == "hello"


def test_antigravity_environment_uses_top_level_extra_body(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction(agent="antigravity-preview-05-2026")])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.antigravity("summarize repo")

    runner.run_sync(request)

    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["agent"] == "antigravity-preview-05-2026"
    assert create_kwargs["extra_body"] == {"environment": "remote"}
    assert "agent_config" not in create_kwargs


def test_agent_request_forwards_agent_config(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction(agent="deep-research")])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "research this",
        agent="deep-research",
        agent_config={"max_steps": 3},
    )

    runner.run_sync(request)

    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["agent"] == "deep-research"
    assert create_kwargs["agent_config"] == {"max_steps": 3}
    assert "generation_config" not in create_kwargs


def test_requires_action_normalizes_function_call(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                status="requires_action",
                outputs=[
                    {
                        "type": "function_call",
                        "id": "call-1",
                        "name": "lookup",
                        "arguments": {"city": "Delft"},
                    }
                ],
                usage=None,
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "lookup weather",
        model="gemini-test",
        tools=[{"type": "function", "name": "lookup"}],
    )

    result = runner.run_sync(request)

    assert result.status == "requires_action"
    assert result.steps[0].type == "function_call"
    assert result.steps[0].step_id == "call-1"
    assert result.steps[0].call_id == "call-1"
    assert result.steps[0].tool_name == "lookup"


def test_real_steps_are_normalized_before_outputs_fallback(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                steps=[
                    SimpleNamespace(
                        id="step-1",
                        type="message",
                        role="assistant",
                        status="completed",
                        content=[{"type": "text", "text": "from real steps"}],
                    )
                ],
                outputs=[SimpleNamespace(type="text", text="fallback output")],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = runner.run_sync(request)

    assert result.output_text == "from real steps"
    assert result.steps[0].step_id == "step-1"
    assert result.steps[0].call_id is None
    assert "outputs rather than `steps`" not in " ".join(result.warnings)


def test_sdk_output_text_takes_precedence_over_timeline_step_text(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                output_text="final answer",
                steps=[
                    SimpleNamespace(type="user_input", text="original user prompt"),
                    SimpleNamespace(type="tool_result", text="intermediate tool text"),
                    SimpleNamespace(
                        type="message",
                        role="assistant",
                        text="final answer",
                    ),
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text == "final answer"
    assert [step.type for step in result.steps] == [
        "user_input",
        "tool_result",
        "message",
    ]
    assert result.steps[0].text_preview is None
    assert result.steps[1].text_preview is None
    assert result.steps[2].text_preview == "final answer"


def test_fallback_output_uses_only_safe_final_model_step(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                output_text=None,
                steps=[
                    SimpleNamespace(type="user_input", text="secret prompt"),
                    SimpleNamespace(type="tool_result", text="private tool result"),
                    SimpleNamespace(
                        type="message",
                        role="assistant",
                        text="final answer",
                    ),
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text == "final answer"
    assert "secret prompt" not in result.output_text
    assert "private tool result" not in result.output_text
    assert [step.text_preview for step in result.steps] == [None, None, "final answer"]


def test_fallback_output_does_not_use_text_before_later_function_call(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                status="requires_action",
                output_text=None,
                steps=[
                    SimpleNamespace(
                        id="message-1",
                        type="message",
                        role="assistant",
                        text="draft answer before tool call",
                    ),
                    SimpleNamespace(
                        id="call-1",
                        type="function_call",
                        name="lookup",
                        arguments={"city": "Delft"},
                    ),
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text is None
    assert result.steps[0].step_id == "message-1"
    assert result.steps[0].call_id is None
    assert result.steps[1].step_id == "call-1"
    assert result.steps[1].call_id == "call-1"
    assert [step.text_preview for step in result.steps] == [None, None]


def test_fallback_output_is_none_for_unsafe_or_ambiguous_timeline_text(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                output_text=None,
                steps=[
                    SimpleNamespace(type="user_input", text="secret prompt"),
                    SimpleNamespace(type="tool_result", text="private tool result"),
                    SimpleNamespace(type="message", text="ambiguous message text"),
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text is None
    assert [step.text_preview for step in result.steps] == [None, None, None]


@pytest.mark.parametrize("unsafe_type", ["tool_result", "sandbox"])
def test_fallback_output_rejects_top_level_model_step_with_nested_unsafe_content(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    unsafe_type: str,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                output_text=None,
                steps=[
                    SimpleNamespace(
                        type="message",
                        role="assistant",
                        text="final answer plus private nested content",
                        content=[
                            {"type": "text", "text": "final answer"},
                            {"type": unsafe_type, "content": "private nested content"},
                        ],
                    )
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text is None
    assert result.steps[0].text_preview is None


@pytest.mark.parametrize("unsafe_type", ["tool_result", "sandbox"])
def test_fallback_output_rejects_top_level_text_with_mapping_unsafe_content(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    unsafe_type: str,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                output_text=None,
                steps=[
                    SimpleNamespace(
                        type="message",
                        role="assistant",
                        text="final answer plus private nested content",
                        content={
                            "type": unsafe_type,
                            "content": "private nested content",
                        },
                    )
                ],
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-1")

    result = runner.run_sync(request)

    assert result.output_text is None
    assert result.steps[0].text_preview is None


def test_runtime_client_without_interactions_contract_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=SimpleNamespace(interactions=SimpleNamespace(create=object())),
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    with pytest.raises(KitaruFeatureNotAvailableError) as exc_info:
        runner.run_sync(request)

    message = str(exc_info.value)
    assert "Interactions preview API" in message
    assert "interactions.create" in message
    assert "interactions.get" in message


def test_function_result_request_constructs_matching_create_payload(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [_completed_interaction(previous_interaction_id="interaction-1")]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.function_result(
        previous_interaction_id="interaction-1",
        function_call_id="call-1",
        function_name="lookup",
        function_result={"answer": 42},
        model="gemini-test",
    )

    runner.run_sync(request)

    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["previous_interaction_id"] == "interaction-1"
    assert create_kwargs["input"] == [
        {
            "type": "function_result",
            "call_id": "call-1",
            "name": "lookup",
            "result": {"answer": 42},
        }
    ]


def test_poll_fetches_existing_interaction_without_create(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction(id="interaction-existing")])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-existing")

    result = runner.run_sync(request)

    assert result.interaction_id == "interaction-existing"
    assert client.interactions.create_calls == []
    assert client.interactions.get_calls == [("interaction-existing", {})]
    assert result.poll_count == 1


def test_poll_request_waits_until_stable_with_timeout(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    sleep_durations = _install_fake_runner_clock(monkeypatch)
    client = FakeClient(
        [
            _completed_interaction(id="background-1", status="in_progress"),
            _completed_interaction(id="background-1", status="completed"),
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=0.001,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll(
        "background-1",
        timeout_s=100.0,
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert client.interactions.create_calls == []
    assert result.poll_count == 2
    assert sleep_durations == pytest.approx([0.001])
    first_timeout = _assert_get_timeout(
        client.interactions.get_calls[0],
        interaction_id="background-1",
        max_timeout_s=100.0,
    )
    second_timeout = _assert_get_timeout(
        client.interactions.get_calls[1],
        interaction_id="background-1",
        max_timeout_s=100.0,
    )
    assert first_timeout == pytest.approx(100.0)
    assert second_timeout == pytest.approx(99.999)


def test_background_polling_reuses_created_interaction_id(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(id="background-1", status="in_progress"),
            _completed_interaction(id="background-1", status="completed"),
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=0.001,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
        timeout_s=0.01,
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert len(client.interactions.create_calls) == 1
    assert len(client.interactions.get_calls) == 1
    _assert_get_timeout(
        client.interactions.get_calls[0],
        interaction_id="background-1",
        max_timeout_s=0.01,
    )
    assert result.poll_count == 1


def test_unstable_status_raises_instead_of_successful_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction(id="background-1", status="queued")])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        runner.run_sync(request)

    message = str(exc_info.value)
    assert "background-1" in message
    assert "non-stable status 'queued'" in message
    assert "GeminiInteractionRequest.poll" in message
    assert "duplicate job" in message
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == []


def test_background_poll_timeout_raises_without_full_interval_oversleep(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    sleep_durations = _install_fake_runner_clock(
        monkeypatch,
        sleep_overrun_s=0.000001,
    )
    client = FakeClient(
        [
            _completed_interaction(id="background-1", status="in_progress"),
            _completed_interaction(id="background-1", status="in_progress"),
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=10.0,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
        timeout_s=0.001,
    )

    with pytest.raises(KitaruRuntimeError, match="background-1"):
        runner.run_sync(request)

    assert client.interactions.get_calls == []
    assert sleep_durations == pytest.approx([0.001])


@pytest.mark.parametrize(
    "terminal_status",
    ["failed", "cancelled", "canceled", "incomplete", "budget_exceeded"],
)
def test_background_create_terminal_failure_status_does_not_poll(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    terminal_status: str,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [_completed_interaction(id="background-1", status=terminal_status)]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=0.001,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
        timeout_s=100.0,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        runner.run_sync(request)

    message = str(exc_info.value)
    assert "background-1" in message
    assert f"non-stable status '{terminal_status}'" in message
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == []


@pytest.mark.parametrize(
    "terminal_status",
    ["failed", "cancelled", "incomplete", "budget_exceeded"],
)
def test_background_polling_stops_promptly_on_terminal_failure_status(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    terminal_status: str,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    sleep_durations = _install_fake_runner_clock(monkeypatch)
    client = FakeClient(
        [
            _completed_interaction(id="background-1", status="in_progress"),
            _completed_interaction(id="background-1", status=terminal_status),
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=0.001,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
        timeout_s=100.0,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        runner.run_sync(request)

    message = str(exc_info.value)
    assert "background-1" in message
    assert f"non-stable status '{terminal_status}'" in message
    assert len(client.interactions.get_calls) == 1
    _assert_get_timeout(
        client.interactions.get_calls[0],
        interaction_id="background-1",
        max_timeout_s=100.0,
    )
    assert sleep_durations == pytest.approx([0.001])


def test_nested_checkpoint_rejected_before_sdk_invocation(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    with pytest.raises(KitaruUsageError, match="inside an existing Kitaru checkpoint"):
        runner.run_sync(request)

    assert client.interactions.create_calls == []


def test_direct_execution_inside_checkpoint_warns(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        allow_direct_execution_inside_checkpoint=True,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = runner.run_sync(request)

    assert "ran directly inside an existing Kitaru checkpoint" in " ".join(
        result.warnings
    )
    assert len(client.interactions.create_calls) == 1


def test_cache_key_uses_explicit_cache_identity_not_live_client_state(
    gemini_adapter: types.ModuleType,
) -> None:
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")
    runner_a = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=ClientWithPublicState(),
    )
    runner_b = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=object(),
    )
    project_a = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=ClientWithPublicState(),
        cache_identity="project-a/us-central1",
    )
    project_b = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=ClientWithPublicState(),
        cache_identity="project-b/us-central1",
    )

    assert runner_a._interaction_cache_key(request) == runner_b._interaction_cache_key(
        request
    )
    assert project_a._interaction_cache_key(
        request
    ) != project_b._interaction_cache_key(request)


def test_cache_identity_must_be_stable_string(
    gemini_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="stable string"):
        gemini_adapter.KitaruGeminiInteractionsRunner(
            name="gemini",
            client=ClientWithPublicState(),
            cache_identity=object(),
        )


def test_request_manifest_records_json_null_function_result_turn(
    gemini_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.gemini._serialization")
    request = gemini_adapter.GeminiInteractionRequest.function_result(
        previous_interaction_id="interaction-1",
        function_call_id="call-null",
        function_result=None,
        model="m",
    )

    manifest = serialization.redacted_request_manifest(request, client=None)

    assert manifest["request"]["has_function_result"] is True
    assert manifest["request"]["function_result_is_json_null"] is True


def test_request_manifest_redacts_secret_like_fields(
    gemini_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.gemini._serialization")
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="m",
        generation_config={"api_key": "secret", "temperature": 0.2},
        response_format={"authorization": "Bearer secret"},
        response_mime_type="application/json",
    )

    manifest = serialization.redacted_request_manifest(
        request,
        client={
            "token": "secret",
            "headers": [
                ("Authorization", "Bearer secret"),
                ["x-api-key", "secret"],
                ["Content-Type", "application/json"],
            ],
            "nested": {"name": "Authorization", "value": "Bearer nested"},
        },
    )

    assert manifest["request"]["generation_config"]["api_key"] == "[REDACTED]"
    assert manifest["request"]["generation_config"]["temperature"] == 0.2
    assert manifest["request"]["agent_config"] is None
    assert manifest["request"]["response_format"]["authorization"] == "[REDACTED]"
    assert manifest["request"]["response_mime_type"] == "application/json"
    assert manifest["client"]["token"] == "[REDACTED]"
    assert manifest["client"]["headers"] == [
        ["Authorization", "[REDACTED]"],
        ["x-api-key", "[REDACTED]"],
        ["Content-Type", "application/json"],
    ]
    assert manifest["client"]["nested"] == {
        "name": "Authorization",
        "value": "[REDACTED]",
    }

    agent_request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        agent="deep-research",
        agent_config={
            "api_key": "secret",
            "auth": "Bearer secret",
            "bearer": "secret",
            "max_steps": 3,
            "oauth_client": "secret",
        },
    )
    agent_manifest = serialization.redacted_request_manifest(
        agent_request,
        client=None,
    )
    assert agent_manifest["request"]["generation_config"] == {}
    assert agent_manifest["request"]["agent_config"]["api_key"] == "[REDACTED]"
    assert agent_manifest["request"]["agent_config"]["auth"] == "[REDACTED]"
    assert agent_manifest["request"]["agent_config"]["bearer"] == "[REDACTED]"
    assert agent_manifest["request"]["agent_config"]["max_steps"] == 3
    assert agent_manifest["request"]["agent_config"]["oauth_client"] == "[REDACTED]"


def test_capture_failures_are_non_fatal_by_default(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        allow_direct_execution_inside_checkpoint=True,
    )
    monkeypatch.setattr(
        runner,
        "_save_artifact",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = runner.run_sync(request)

    assert "artifact capture failed" in " ".join(result.warnings)
    assert result.metadata["capture_failures"]


def test_strict_capture_failure_raises_after_sdk_success(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        capture=gemini_adapter.GeminiInteractionCapturePolicy(
            fail_on_artifact_capture_error=True
        ),
        allow_direct_execution_inside_checkpoint=True,
    )
    monkeypatch.setattr(
        runner,
        "_save_artifact",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    with pytest.raises(KitaruRuntimeError, match="retrying may duplicate"):
        runner.run_sync(request)


def test_failed_event_error_redacts_exception_message(
    gemini_adapter: types.ModuleType,
) -> None:
    events = importlib.import_module("kitaru.adapters.gemini._events")

    error = events.error_from_exception(RuntimeError("prompt: secret user content"))

    assert error.exception_type == "RuntimeError"
    assert "secret user content" not in error.message
    assert "redacted" in error.message.lower()
