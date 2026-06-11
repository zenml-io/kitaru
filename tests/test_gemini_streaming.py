"""Focused tests for Gemini Interactions streaming."""

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
from tests._gemini_usage_helpers import collect_usage_records


@pytest.fixture
def gemini_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    purge_gemini_adapter_modules(monkeypatch)
    install_fake_google_genai(monkeypatch)
    return importlib.import_module("kitaru.adapters.gemini")


class FakeInteractions:
    def __init__(self, streams: list[Any]) -> None:
        self.streams = list(streams)
        self.create_calls: list[dict[str, Any]] = []
        self.get_calls: list[tuple[str, dict[str, Any]]] = []

    def create(self, **kwargs: Any) -> Any:
        self.create_calls.append(kwargs)
        return self.streams.pop(0)

    def get(self, id: str, **kwargs: Any) -> Any:
        self.get_calls.append((id, kwargs))
        return self.streams.pop(0)


class FakeClient:
    def __init__(self, streams: list[Any]) -> None:
        self.interactions = FakeInteractions(streams)


class FakeAsyncClient:
    def __init__(self, streams: list[Any]) -> None:
        self.interactions = FakeInteractions([])
        self.aio = SimpleNamespace(interactions=FakeInteractions(streams))


class AsyncStream:
    def __init__(self, events: list[Any]) -> None:
        self.events = list(events)

    async def __aiter__(self) -> Any:
        for event in self.events:
            yield event


class FailingAsyncStream:
    def __init__(self, events: list[Any]) -> None:
        self.events = list(events)

    async def __aiter__(self) -> Any:
        for event in self.events:
            yield event
        raise RuntimeError("stream observation dropped")


class CreateWithoutStreamInteractions:
    def __init__(self, streams: list[Any]) -> None:
        self.streams = list(streams)
        self.create_calls: list[dict[str, Any]] = []
        self.get_calls: list[tuple[str, dict[str, Any]]] = []

    def create(
        self,
        *,
        input: Any,
        agent: str,
        background: bool = False,
        store: bool = True,
        timeout: float | None = None,
    ) -> Any:
        call = {
            "input": input,
            "agent": agent,
            "background": background,
            "store": store,
        }
        if timeout is not None:
            call["timeout"] = timeout
        self.create_calls.append(call)
        return self.streams.pop(0)

    def get(self, id: str, **kwargs: Any) -> Any:
        self.get_calls.append((id, kwargs))
        return self.streams.pop(0)


def _event(event_type: str, **fields: Any) -> SimpleNamespace:
    return SimpleNamespace(type=event_type, **fields)


def _background_created_interaction(
    *,
    interaction_id: str = "background-1",
    status: str = "in_progress",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=interaction_id,
        status=status,
        agent="antigravity-preview-05-2026",
        environment_id="environment-1",
        usage=SimpleNamespace(seed_tokens=1),
    )


def _background_observation_stream(
    *,
    interaction_id: str = "background-1",
) -> list[Any]:
    return [
        _event(
            "step.start",
            id="evt-bg-1",
            step_index=0,
            step=SimpleNamespace(
                id="step-bg-1",
                type="message",
                role="assistant",
                status="in_progress",
            ),
        ),
        _event(
            "step.delta",
            id="evt-bg-2",
            step_index=0,
            delta={"type": "text", "text": "background answer"},
        ),
        _event("step.stop", id="evt-bg-3", step_index=0),
        _event(
            "interaction.completed",
            id="evt-bg-4",
            interaction=SimpleNamespace(id=interaction_id, status="completed"),
        ),
        _event("done", id="evt-bg-5"),
    ]


def _completed_stream(*, interaction_id: str = "interaction-1") -> list[Any]:
    return [
        _event(
            "interaction.created",
            id="evt-1",
            interaction=SimpleNamespace(
                id=interaction_id,
                status="in_progress",
                model="gemini-test",
            ),
        ),
        _event(
            "step.start",
            id="evt-2",
            step_index=0,
            step=SimpleNamespace(
                id="step-1",
                type="message",
                role="assistant",
                status="in_progress",
            ),
        ),
        _event(
            "step.delta",
            id="evt-3",
            step_index=0,
            delta={"type": "text", "text": "hello "},
        ),
        _event(
            "step.delta",
            id="evt-4",
            step_index=0,
            delta={"type": "text", "text": "world"},
        ),
        _event("step.stop", id="evt-5", step_index=0),
        _event(
            "interaction.completed",
            id="evt-6",
            interaction=SimpleNamespace(
                id=interaction_id,
                status="completed",
                model="gemini-test",
                usage=SimpleNamespace(total_tokens=7),
            ),
        ),
        _event("done", id="evt-7"),
    ]


def _final_payload_only_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-final", status="in_progress"),
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(
                id="interaction-final",
                status="completed",
                output_text="final answer from completed event",
                steps=[
                    SimpleNamespace(
                        id="step-final",
                        type="message",
                        role="assistant",
                        text="final answer from completed event",
                    )
                ],
                usage=SimpleNamespace(total_tokens=11),
            ),
        ),
        _event("done"),
    ]


def _content_delta_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-content", status="in_progress"),
        ),
        SimpleNamespace(
            event_type="content.start",
            event_id="evt-content-1",
            index=0,
            content=SimpleNamespace(type="text", text=""),
        ),
        SimpleNamespace(
            event_type="content.delta",
            event_id="evt-content-2",
            index=0,
            delta=SimpleNamespace(type="text", text="hello "),
        ),
        SimpleNamespace(
            event_type="content.delta",
            event_id="evt-content-3",
            index=0,
            delta=SimpleNamespace(type="text", text="world"),
        ),
        SimpleNamespace(event_type="content.stop", event_id="evt-content-4", index=0),
        _event(
            "interaction.complete",
            interaction=SimpleNamespace(id="interaction-content", status="completed"),
        ),
    ]


def _unsafe_tool_text_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-tool", status="in_progress"),
        ),
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(id="tool-1", type="tool_result"),
        ),
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "text", "text": "private tool result"},
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(id="interaction-tool", status="completed"),
        ),
        _event("done"),
    ]


def _user_role_text_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-user", status="in_progress"),
        ),
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(id="user-step", type="message", role="user"),
        ),
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "text", "text": "private user text"},
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(id="interaction-user", status="completed"),
        ),
        _event("done"),
    ]


def _completed_without_nested_status_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-status", status="in_progress"),
        ),
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(id="step-status", type="message", role="assistant"),
        ),
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "text", "text": "status answer"},
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(id="interaction-status"),
        ),
        _event("done"),
    ]


def _requires_action_stream(
    *,
    arguments: str = '{"city":"Delft"}',
    function_name_field: str = "name",
    function_name: str = "lookup",
) -> list[Any]:
    function_call_step = {
        "id": "call-1",
        "type": "function_call",
        function_name_field: function_name,
    }
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-2", status="in_progress"),
        ),
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(**function_call_step),
        ),
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "arguments_delta", "delta": arguments},
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(id="interaction-2", status="requires_action"),
        ),
        _event("done"),
    ]


def _sparse_step_index_stream() -> list[Any]:
    return [
        _event(
            "interaction.created",
            interaction=SimpleNamespace(id="interaction-sparse", status="in_progress"),
        ),
        _event(
            "step.start",
            step_index=1_000_000,
            step=SimpleNamespace(id="step-sparse", type="message", role="assistant"),
        ),
        _event(
            "step.delta",
            step_index=1_000_000,
            delta={"type": "text", "text": "sparse answer"},
        ),
        _event(
            "interaction.completed",
            interaction=SimpleNamespace(id="interaction-sparse", status="completed"),
        ),
        _event("done"),
    ]


def _patch_flow_checkpoint(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(agent, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent, "run_sync_in_checkpoint", fake_checkpoint)
    return calls


def _patch_async_flow_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> list[dict[str, Any]]:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(agent, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(agent, "run_async_in_checkpoint", fake_checkpoint)
    return calls


def _stream_public_plan() -> Any:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    return agent._PublicInteractionPlan(surface="run_stream_sync")


def _collect_live_events(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, dict[str, Any], bool]]:
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    events: list[tuple[str, dict[str, Any], bool]] = []

    def publish(kind: str, payload: dict[str, Any], *, flush: bool = False) -> None:
        events.append((kind, payload, flush))

    monkeypatch.setattr(streaming.kitaru_events, "publish", publish)
    return events


def test_public_stream_surface_and_capture_defaults(
    gemini_adapter: types.ModuleType,
) -> None:
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini")
    policy = gemini_adapter.GeminiInteractionCapturePolicy()

    assert hasattr(runner, "run_stream")
    assert hasattr(runner, "run_stream_sync")
    assert policy.include_stream_text_deltas is False
    assert gemini_adapter.GEMINI_STREAM_STARTED == "gemini_interactions.stream.started"
    assert (
        gemini_adapter.GEMINI_STREAM_COMPLETED
        in gemini_adapter.GEMINI_STREAM_EVENT_KINDS
    )
    assert (
        gemini_adapter.GEMINI_STREAM_FAILED
        in gemini_adapter.GEMINI_STREAM_TERMINAL_EVENT_KINDS
    )


def test_run_stream_sync_rejects_running_event_loop(
    gemini_adapter: types.ModuleType,
) -> None:
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini")
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="already running event loop"):
            runner.run_stream_sync(request)

    asyncio.run(call_sync())


def test_publisher_hides_text_deltas_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
    )

    publisher.event(
        _event("step.delta", delta={"type": "text", "text": "secret output"})
    )

    kind, payload, flush = events[-1]
    assert kind == streaming.GEMINI_STREAM_EVENT
    assert flush is False
    assert payload["category"] == "text_delta"
    assert payload["display"] == "Gemini text delta"
    assert "text_delta" not in payload
    assert "secret output" not in repr(payload)


def test_publisher_handles_content_delta_text_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
        include_text_deltas=True,
    )

    publisher.event(
        SimpleNamespace(
            event_type="content.delta",
            event_id="evt-content-1",
            index=0,
            delta=SimpleNamespace(type="text", text="hello world"),
        )
    )

    payload = events[-1][1]
    assert payload["category"] == "text_delta"
    assert payload["event_type"] == "content.delta"
    assert payload["event_id"] == "evt-content-1"
    assert payload["step_index"] == 0
    assert payload["display"] == "hello world"
    assert payload["text_delta"] == "hello world"


def test_publisher_can_opt_into_clipped_text_deltas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
        include_text_deltas=True,
    )

    publisher.event(
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(id="step-1", type="message", role="assistant"),
        )
    )
    publisher.event(
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "text", "text": "hello world"},
        )
    )

    payload = events[-1][1]
    assert payload["display"] == "hello world"
    assert payload["text_delta"] == "hello world"


def test_publisher_hides_user_role_text_even_when_text_deltas_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
        include_text_deltas=True,
    )

    publisher.event(
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(id="user-step", type="message", role="user"),
        )
    )
    publisher.event(
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "text", "text": "private user text"},
        )
    )

    payload = events[-1][1]
    assert payload["category"] == "text_delta"
    assert payload["display"] == "Gemini text delta"
    assert "text_delta" not in payload
    assert "private user text" not in repr(payload)


def test_publisher_uses_function_name_fallback_for_tool_argument_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream_sync",
    )

    publisher.event(
        _event(
            "step.start",
            step_index=0,
            step=SimpleNamespace(
                id="call-1",
                type="function_call",
                function_name="lookup_function",
            ),
        )
    )
    publisher.event(
        _event(
            "step.delta",
            step_index=0,
            delta={"type": "arguments_delta", "delta": '{"city":"Delft"}'},
        )
    )

    start_payload = events[0][1]
    arguments_payload = events[1][1]
    assert start_payload["tool_name"] == "lookup_function"
    assert arguments_payload["category"] == "tool_arguments_delta"
    assert arguments_payload["tool_name"] == "lookup_function"
    assert "Delft" not in repr(arguments_payload)


def test_publisher_hides_tool_arguments_and_flushes_terminal_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream_sync",
    )

    publisher.started()
    publisher.event(
        _event(
            "step.delta",
            step=SimpleNamespace(id="call-1", type="function_call", name="lookup"),
            delta={"type": "arguments_delta", "delta": '{"city":"Delft"}'},
        )
    )
    publisher.completed(status="completed", interaction_id="interaction-1")

    assert events[0][0] == streaming.GEMINI_STREAM_STARTED
    assert events[-1][0] == streaming.GEMINI_STREAM_COMPLETED
    assert events[-1][2] is True
    arguments_payload = events[1][1]
    assert arguments_payload["category"] == "tool_arguments_delta"
    assert arguments_payload["tool_name"] == "lookup"
    assert "Delft" not in repr(arguments_payload)


def test_publisher_unknown_event_does_not_expose_arbitrary_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = _collect_live_events(monkeypatch)
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
    )

    publisher.event(SimpleNamespace(name="secret prompt shaped as a name"))

    payload = events[-1][1]
    assert payload["category"] == "unknown_event"
    assert payload["display"] == "SimpleNamespace"
    assert "secret prompt" not in repr(payload)


def test_publisher_degrades_unknown_events_and_swallows_publish_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streaming = importlib.import_module("kitaru.adapters.gemini._streaming")
    monkeypatch.setattr(
        streaming.kitaru_events,
        "publish",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    publisher = streaming.GeminiStreamPublisher(
        runner_name="gemini",
        surface="run_stream",
    )

    publisher.event(_event("unexpected.provider.event", raw_prompt="do not leak"))
    publisher.failed(RuntimeError("provider details"))


def test_stream_bridge_create_drains_callback_and_accumulates_completed_result(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_completed_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )
    callback_events: list[Any] = []

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            on_event=callback_events.append,
            allow_sync_stream=True,
        )
    )

    assert client.interactions.create_calls[0]["stream"] is True
    assert client.interactions.create_calls[0]["input"] == "hello"
    assert client.interactions.get_calls == []
    assert len(callback_events) == 7
    assert result.status == "completed"
    assert result.interaction_id == "interaction-1"
    assert result.output_text == "hello world"
    assert result.usage == {"total_tokens": 7}
    assert result.stream_metadata is not None
    assert result.stream_metadata["event_count"] == 7
    assert result.stream_metadata["last_event_id"] == "evt-7"


def test_stream_bridge_accumulates_content_delta_events(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_content_delta_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "completed"
    assert result.interaction_id == "interaction-content"
    assert result.output_text == "hello world"
    assert result.raw_steps == [
        {"type": "output_text", "status": "completed", "text": "hello world"}
    ]
    assert result.stream_metadata is not None
    assert result.stream_metadata["counts_by_event_type"]["content.delta"] == 2


def test_stream_bridge_uses_final_completed_payload_when_deltas_are_empty(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_final_payload_only_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "completed"
    assert result.interaction_id == "interaction-final"
    assert result.output_text == "final answer from completed event"
    assert result.steps[0].step_id == "step-final"
    assert result.steps[0].text_preview == "final answer from completed event"
    assert result.usage == {"total_tokens": 11}


def test_stream_bridge_does_not_promote_tool_text_delta_to_output(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_unsafe_tool_text_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "completed"
    assert result.output_text is None
    assert result.steps[0].type == "tool_result"
    assert result.steps[0].text_preview is None


def test_stream_bridge_does_not_promote_user_role_text_delta_to_output(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_user_role_text_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "completed"
    assert result.output_text is None
    assert result.raw_steps[0]["role"] == "user"
    assert result.raw_steps[0]["type"] == "message"
    assert result.raw_steps[0]["text"] == "private user text"
    assert result.steps[0].text_preview is None


def test_stream_bridge_terminal_completed_event_without_status_overrides_in_progress(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_completed_without_nested_status_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "completed"
    assert result.output_text == "status answer"
    assert result.stream_metadata is not None
    assert result.stream_metadata["final_status"] == "completed"


def test_stream_bridge_poll_uses_get_stream_true(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeAsyncClient(
        [AsyncStream(_completed_stream(interaction_id="existing"))]
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("existing")

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
        )
    )

    assert client.interactions.create_calls == []
    assert client.interactions.get_calls == []
    assert client.aio.interactions.create_calls == []
    assert client.aio.interactions.get_calls == [("existing", {"stream": True})]
    assert result.interaction_id == "existing"
    assert result.poll_count == 1


def test_stream_bridge_background_create_then_get_stream_same_id(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(),
            AsyncStream(_background_observation_stream()),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        environment="remote",
        background=True,
        timeout_s=30.0,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert len(client.interactions.create_calls) == 1
    assert client.interactions.create_calls[0]["background"] is True
    assert client.interactions.create_calls[0]["store"] is True
    assert "stream" not in client.interactions.create_calls[0]
    assert len(client.interactions.get_calls) == 1
    get_id, get_kwargs = client.interactions.get_calls[0]
    assert get_id == "background-1"
    assert get_kwargs["stream"] is True
    assert result.status == "completed"
    assert result.interaction_id == "background-1"
    assert result.agent == "antigravity-preview-05-2026"
    assert result.environment_id == "environment-1"
    assert result.output_text == "background answer"
    assert result.stream_metadata is not None
    assert (
        result.stream_metadata["observation_mode"]
        == "background_create_then_get_stream"
    )
    assert result.stream_metadata["fallback_used"] is False


def test_stream_bridge_background_resume_create_then_get_stream_same_new_id(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(interaction_id="background-2"),
            AsyncStream(_background_observation_stream(interaction_id="background-2")),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.resume(
        "continue inspection",
        previous_interaction_id="background-1",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=30.0,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert len(client.interactions.create_calls) == 1
    assert (
        client.interactions.create_calls[0]["previous_interaction_id"] == "background-1"
    )
    assert "stream" not in client.interactions.create_calls[0]
    assert len(client.interactions.get_calls) == 1
    get_id, get_kwargs = client.interactions.get_calls[0]
    assert get_id == "background-2"
    assert get_kwargs["stream"] is True
    assert 0 < get_kwargs["timeout"] <= 30.0
    assert result.interaction_id == "background-2"


@pytest.mark.parametrize("terminal_status", ["failed", "cancelled", "incomplete"])
def test_stream_bridge_background_terminal_create_status_does_not_observe(
    gemini_adapter: types.ModuleType,
    terminal_status: str,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_background_created_interaction(status=terminal_status)])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=30.0,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        asyncio.run(
            runner_module.run_gemini_interaction_streamed(
                request=request,
                client=client,
                client_factory=None,
                allow_sync_stream=True,
            )
        )

    assert f"non-stable status '{terminal_status}'" in str(exc_info.value)
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == []


def test_stream_bridge_background_expired_deadline_does_not_get_timeout_zero(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    monkeypatch.setattr(runner_module, "_remaining_timeout_s", lambda deadline: 0.0)
    client = FakeClient([_background_created_interaction()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=0.001,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        asyncio.run(
            runner_module.run_gemini_interaction_streamed(
                request=request,
                client=client,
                client_factory=None,
                allow_sync_stream=True,
            )
        )

    assert "GeminiInteractionRequest.poll" in str(exc_info.value)
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == []


def test_stream_bridge_background_does_not_require_create_stream_support(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    interactions = CreateWithoutStreamInteractions(
        [
            _background_created_interaction(),
            AsyncStream(_background_observation_stream()),
        ]
    )
    client = SimpleNamespace(interactions=interactions)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=30.0,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.interaction_id == "background-1"
    assert len(interactions.create_calls) == 1
    create_kwargs = interactions.create_calls[0]
    assert create_kwargs["input"] == "inspect repository"
    assert create_kwargs["agent"] == "antigravity-preview-05-2026"
    assert create_kwargs["background"] is True
    assert create_kwargs["store"] is True
    assert 0 < create_kwargs["timeout"] <= 30.0
    assert len(interactions.get_calls) == 1
    get_id, get_kwargs = interactions.get_calls[0]
    assert get_id == "background-1"
    assert get_kwargs["stream"] is True
    assert 0 < get_kwargs["timeout"] <= 30.0


def test_stream_bridge_background_without_timeout_checks_same_id_once(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(),
            SimpleNamespace(
                id="background-1",
                status="completed",
                agent="antigravity-preview-05-2026",
                output_text="one status check final answer",
            ),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == [("background-1", {})]
    assert result.status == "completed"
    assert result.output_text == "one status check final answer"
    assert result.stream_metadata is not None
    assert result.stream_metadata["fallback_used"] is True


def test_stream_bridge_background_stream_failure_polls_same_id_without_second_create(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(),
            FailingAsyncStream([]),
            SimpleNamespace(
                id="background-1",
                status="completed",
                agent="antigravity-preview-05-2026",
                output_text="fallback final answer",
            ),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=30.0,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert len(client.interactions.create_calls) == 1
    assert len(client.interactions.get_calls) == 2
    stream_get_id, stream_get_kwargs = client.interactions.get_calls[0]
    fallback_get_id, fallback_get_kwargs = client.interactions.get_calls[1]
    assert stream_get_id == fallback_get_id == "background-1"
    assert stream_get_kwargs["stream"] is True
    assert 0 < stream_get_kwargs["timeout"] <= 30.0
    assert 0 < fallback_get_kwargs["timeout"] <= 30.0
    assert result.status == "completed"
    assert result.output_text == "fallback final answer"
    assert result.stream_metadata is not None
    assert (
        result.stream_metadata["observation_mode"]
        == "background_create_then_get_stream"
    )
    assert result.stream_metadata["fallback_used"] is True
    assert result.stream_metadata["final_status"] == "completed"
    assert any("same interaction id" in warning for warning in result.warnings)


def test_stream_bridge_background_provider_error_event_polls_same_id(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(),
            AsyncStream([_event("error")]),
            SimpleNamespace(
                id="background-1",
                status="completed",
                agent="antigravity-preview-05-2026",
                output_text="provider recovered answer",
            ),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
        timeout_s=30.0,
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert len(client.interactions.create_calls) == 1
    assert len(client.interactions.get_calls) == 2
    stream_get_id, stream_get_kwargs = client.interactions.get_calls[0]
    fallback_get_id, fallback_get_kwargs = client.interactions.get_calls[1]
    assert stream_get_id == fallback_get_id == "background-1"
    assert stream_get_kwargs["stream"] is True
    assert 0 < stream_get_kwargs["timeout"] <= 30.0
    assert 0 < fallback_get_kwargs["timeout"] <= 30.0
    assert result.status == "completed"
    assert result.output_text == "provider recovered answer"
    assert result.stream_metadata is not None
    assert result.stream_metadata["fallback_used"] is True


def test_stream_bridge_background_fallback_unstable_status_raises_poll_instruction(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _background_created_interaction(),
            SimpleNamespace(id="background-1", status="in_progress"),
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "inspect repository",
        agent="antigravity-preview-05-2026",
        background=True,
    )

    with pytest.raises(KitaruRuntimeError) as exc_info:
        asyncio.run(
            runner_module.run_gemini_interaction_streamed(
                request=request,
                client=client,
                client_factory=None,
                allow_sync_stream=True,
            )
        )

    message = str(exc_info.value)
    assert "background-1" in message
    assert "non-stable status 'in_progress'" in message
    assert "GeminiInteractionRequest.poll" in message
    assert "duplicate job" in message
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == [("background-1", {})]


@pytest.mark.parametrize(
    ("function_name_field", "function_name"),
    [("tool_name", "lookup_tool"), ("function_name", "lookup_function")],
)
def test_stream_bridge_accumulates_requires_action_function_name_fallbacks(
    gemini_adapter: types.ModuleType,
    function_name_field: str,
    function_name: str,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            _requires_action_stream(
                function_name_field=function_name_field,
                function_name=function_name,
            )
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "lookup weather",
        model="gemini-test",
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "requires_action"
    assert result.steps[0].call_id == "call-1"
    assert result.steps[0].tool_name == function_name


def test_stream_bridge_accumulates_requires_action(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_requires_action_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "lookup weather",
        model="gemini-test",
        tools=[{"type": "function", "name": "lookup"}],
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "requires_action"
    assert result.output_text is None
    assert result.steps[0].type == "function_call"
    assert result.steps[0].step_id == "call-1"
    assert result.steps[0].call_id == "call-1"
    assert result.steps[0].tool_name == "lookup"


def test_stream_bridge_provider_error_event_fails(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient(
        [
            [
                _event("interaction.created", interaction=SimpleNamespace(id="i")),
                _event("error"),
            ]
        ]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    with pytest.raises(KitaruRuntimeError, match="provider error event"):
        asyncio.run(
            runner_module.run_gemini_interaction_streamed(
                request=request,
                client=client,
                client_factory=None,
                allow_sync_stream=True,
            )
        )


def test_stream_bridge_sparse_step_index_does_not_allocate_dense_steps(
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    client = FakeClient([_sparse_step_index_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.output_text == "sparse answer"
    assert len(result.raw_steps) == 1
    assert result.raw_steps[0]["id"] == "step-sparse"
    assert result.stream_metadata is not None
    assert result.stream_metadata["accumulated_step_count"] == 1


def test_stream_bridge_bounds_accumulated_text(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    accumulator_module = importlib.import_module(
        "kitaru.adapters.gemini._stream_accumulator"
    )
    monkeypatch.setattr(accumulator_module, "_MAX_ACCUMULATED_STREAM_TEXT_CHARS", 5)
    client = FakeClient([_completed_stream()])
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.output_text == "hello"
    assert result.raw_steps[0]["text_delta_truncated"] is True
    assert result.stream_metadata is not None
    assert result.stream_metadata["text_truncated"] is True


def test_stream_bridge_bounds_accumulated_arguments(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    runner_module = importlib.import_module("kitaru.adapters.gemini._runner")
    accumulator_module = importlib.import_module(
        "kitaru.adapters.gemini._stream_accumulator"
    )
    monkeypatch.setattr(accumulator_module, "_MAX_ACCUMULATED_STREAM_ARGUMENT_CHARS", 5)
    client = FakeClient(
        [_requires_action_stream(arguments='{"city":"Delft","country":"NL"}')]
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "lookup weather",
        model="gemini-test",
        tools=[{"type": "function", "name": "lookup"}],
    )

    result = asyncio.run(
        runner_module.run_gemini_interaction_streamed(
            request=request,
            client=client,
            client_factory=None,
            allow_sync_stream=True,
        )
    )

    assert result.status == "requires_action"
    assert len(result.raw_steps[0]["arguments_delta"]) == 5
    assert result.raw_steps[0]["arguments_delta_truncated"] is True
    assert result.stream_metadata is not None
    assert result.stream_metadata["arguments_truncated"] is True


def test_run_stream_rejects_sync_only_client_before_provider_create(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_async_flow_checkpoint(monkeypatch)
    client = FakeClient([_completed_stream()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini", client=client)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    with pytest.raises(
        KitaruFeatureNotAvailableError, match="async streaming requires"
    ):
        asyncio.run(runner.run_stream(request))

    assert client.interactions.create_calls == []
    assert client.interactions.get_calls == []


def test_run_stream_sync_logs_canonical_gemini_usage_record(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch)
    records = collect_usage_records(monkeypatch)
    client = FakeClient([_completed_stream()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini", client=client)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
    )

    result = runner.run_stream_sync(request)

    assert result.status == "completed"
    assert result.usage == {"total_tokens": 7}
    assert len(records) == 1
    record = records[0]
    assert record["adapter"] == "gemini_interactions"
    assert record["surface"] == "gemini_interaction"
    assert record["model"] == "gemini-test"
    assert record["requested_model"] == "gemini-test"
    assert record["resolved_model"] == "gemini-test"
    assert record["usage"]["total_tokens"] == 7
    assert record["cost"]["source"] == "none"
    assert record["billing_effect"] == "incurred"


def test_runner_stream_lifecycle_publish_failures_do_not_replace_success(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch)
    agent = importlib.import_module("kitaru.adapters.gemini._agent")

    def raise_started(self: Any) -> None:
        raise RuntimeError("started publish failed")

    def raise_completed(self: Any, **kwargs: Any) -> None:
        raise RuntimeError("completed publish failed")

    monkeypatch.setattr(agent.GeminiStreamPublisher, "started", raise_started)
    monkeypatch.setattr(agent.GeminiStreamPublisher, "completed", raise_completed)

    client = FakeClient([_completed_stream()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini", client=client)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = runner.run_stream_sync(request)

    assert result.status == "completed"
    assert result.output_text == "hello world"
    assert client.interactions.create_calls[0]["stream"] is True


def test_runner_stream_failed_publish_failure_preserves_provider_error(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch)
    agent = importlib.import_module("kitaru.adapters.gemini._agent")

    def raise_failed(self: Any, error: BaseException) -> None:
        raise RuntimeError("failed publish replaced original")

    monkeypatch.setattr(agent.GeminiStreamPublisher, "failed", raise_failed)

    client = FakeClient(
        [
            [
                _event("interaction.created", interaction=SimpleNamespace(id="i")),
                _event("error"),
            ]
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini", client=client)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    with pytest.raises(KitaruRuntimeError, match="provider error event"):
        runner.run_stream_sync(request)


def test_runner_stream_lifecycle_and_cache_identity(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    checkpoint_calls = _patch_flow_checkpoint(monkeypatch)
    live_events = _collect_live_events(monkeypatch)
    client = FakeClient([_completed_stream()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini", client=client)
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    result = runner.run_stream_sync(request)
    stream_shapes = importlib.import_module("kitaru.adapters.gemini._stream_shapes")

    assert result.status == "completed"
    assert (
        result.metadata["stream"]["reconstruction"]
        == stream_shapes._STREAM_RECONSTRUCTION_POLICY
    )
    assert checkpoint_calls[0]["step_name"] == "gemini_gemini_interaction"
    assert client.interactions.create_calls[0]["stream"] is True
    assert live_events[0][0] == gemini_adapter.GEMINI_STREAM_STARTED
    assert live_events[-1][0] == gemini_adapter.GEMINI_STREAM_COMPLETED
    assert live_events[-1][2] is True

    non_stream_key = runner._interaction_cache_key(request)
    stream_key = runner._interaction_cache_key(
        request,
        plan=runner._interaction_plan(_stream_public_plan()),
    )
    text_runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        capture=gemini_adapter.GeminiInteractionCapturePolicy(
            include_stream_text_deltas=True
        ),
    )
    stream_text_key = text_runner._interaction_cache_key(
        request,
        plan=text_runner._interaction_plan(_stream_public_plan()),
    )
    assert stream_key != non_stream_key
    assert stream_text_key != stream_key
