"""Focused tests for PydanticAI live streaming events."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("pydantic_ai")


async def _async_events(events: list[Any]) -> AsyncIterator[Any]:
    for event in events:
        yield event


class _FakeTracker:
    def __init__(self) -> None:
        self.stream_records: list[dict[str, Any]] = []
        self.model_records: list[dict[str, Any]] = []
        self.reserved_tool_calls: list[dict[str, Any]] = []

    def record_stream_event(
        self, *, duration_ms: float, error: BaseException | None
    ) -> None:
        self.stream_records.append({"duration_ms": duration_ms, "error": error})

    def start_model_event(self) -> tuple[str, SimpleNamespace]:
        return "model-event-1", SimpleNamespace(sequence_index=0, turn_index=0)

    def artifact_name(self, event_id: str, role: str) -> str:
        return f"{event_id}-{role}"

    def record_model_event(self, event_id: str, context: Any, **kwargs: Any) -> None:
        self.model_records.append({"event_id": event_id, "context": context, **kwargs})

    def reserve_tool_call_order(self, **kwargs: Any) -> None:
        self.reserved_tool_calls.append(kwargs)


def _capture_published(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    published: list[dict[str, Any]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append({"kind": kind, "payload": payload, "flush": flush})

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._streaming.kitaru_events.publish",
        fake_publish,
    )
    return published


def _make_agent(*, capture: Any = None) -> Any:
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import KitaruAgent

    return KitaruAgent(Agent(TestModel(), name="streamer"), capture=capture)


def test_pydantic_ai_stream_constants_are_stable() -> None:
    from kitaru.adapters.pydantic_ai._streaming import (
        PYDANTIC_AI_STREAM_COMPLETED,
        PYDANTIC_AI_STREAM_EVENT,
        PYDANTIC_AI_STREAM_EVENT_KINDS,
        PYDANTIC_AI_STREAM_FAILED,
        PYDANTIC_AI_STREAM_STARTED,
        PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS,
    )

    assert PYDANTIC_AI_STREAM_STARTED == "pydantic_ai.stream.started"
    assert PYDANTIC_AI_STREAM_EVENT == "pydantic_ai.stream.event"
    assert PYDANTIC_AI_STREAM_COMPLETED == "pydantic_ai.stream.completed"
    assert PYDANTIC_AI_STREAM_FAILED == "pydantic_ai.stream.failed"
    assert PYDANTIC_AI_STREAM_EVENT_KINDS == (
        PYDANTIC_AI_STREAM_STARTED,
        PYDANTIC_AI_STREAM_EVENT,
        PYDANTIC_AI_STREAM_COMPLETED,
        PYDANTIC_AI_STREAM_FAILED,
    )
    assert PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS == (
        PYDANTIC_AI_STREAM_COMPLETED,
        PYDANTIC_AI_STREAM_FAILED,
    )


def test_public_adapter_exports_pydantic_ai_stream_constants() -> None:
    from kitaru.adapters import pydantic_ai as kp

    assert kp.PYDANTIC_AI_STREAM_EVENT_KINDS[0] == kp.PYDANTIC_AI_STREAM_STARTED
    assert kp.PYDANTIC_AI_STREAM_EVENT in kp.PYDANTIC_AI_STREAM_EVENT_KINDS
    assert kp.PYDANTIC_AI_STREAM_FAILED in kp.PYDANTIC_AI_STREAM_TERMINAL_EVENT_KINDS


def test_capture_policy_defaults_to_no_stream_transcripts() -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy

    assert CapturePolicy().save_stream_transcripts is False


def test_publisher_normalizes_agent_name() -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    publisher = PydanticAIStreamPublisher(
        agent_name="Support Agent!!",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    payload = publisher.normalize_event(SimpleNamespace(event_kind="part_start"))

    assert payload["agent_name"] == "Support_Agent"


@pytest.mark.anyio
async def test_streamed_response_get_event_iterator_tracks_events_directly() -> None:
    from pydantic_ai.models import ModelRequestParameters

    from kitaru.adapters.pydantic_ai._model import KitaruStreamedResponse

    class _WrappedStream:
        def __init__(self, events: list[Any]) -> None:
            self.model_request_parameters = ModelRequestParameters()
            self.final_result_event = SimpleNamespace(event_kind="final_result")
            self._events = events

        def __aiter__(self) -> AsyncIterator[Any]:
            return _async_events(self._events)

    events = [SimpleNamespace(event_kind="part_start")]
    tracked: list[Any] = []
    wrapped: Any = _WrappedStream(events)
    response = KitaruStreamedResponse(wrapped, on_event=tracked.append)

    received = [event async for event in response._get_event_iterator()]

    assert received == events
    assert tracked == events
    assert response.final_result_event is wrapped.final_result_event


def test_publisher_order_and_terminal_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    published = _capture_published(monkeypatch)
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    publisher.started()
    publisher.event(
        SimpleNamespace(
            event_kind="part_delta",
            index=1,
            delta=SimpleNamespace(
                part_delta_kind="text",
                content_delta="hello there",
            ),
        )
    )
    publisher.completed(event_count=1)
    publisher.failed(RuntimeError("boom"))

    assert [event["kind"] for event in published] == [
        "pydantic_ai.stream.started",
        "pydantic_ai.stream.event",
        "pydantic_ai.stream.completed",
        "pydantic_ai.stream.failed",
    ]
    assert [event["flush"] for event in published] == [False, False, True, True]
    payload = published[1]["payload"]
    assert payload["adapter"] == "pydantic_ai"
    assert payload["agent_name"] == "agent"
    assert payload["surface"] == "run"
    assert payload["source"] == "event_stream_handler"
    assert payload["text_delta"] == "hello there"
    assert published[2]["payload"]["event_count"] == 1


def test_publisher_failed_marks_cancelled_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    published = _capture_published(monkeypatch)
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    publisher.failed(asyncio.CancelledError())

    assert published[0]["kind"] == "pydantic_ai.stream.failed"
    assert published[0]["payload"]["status"] == "cancelled"
    assert published[0]["flush"] is True


def test_publisher_swallow_publish_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    def fail_publish(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("stream backend unavailable")

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._streaming.kitaru_events.publish",
        fail_publish,
    )
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    publisher.started()
    publisher.event(SimpleNamespace(event_kind="part_start"))
    publisher.completed()
    publisher.failed(RuntimeError("boom"))


def test_normalization_fallback_never_dumps_raw_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    class BadEvent:
        @property
        def event_kind(self) -> str:
            raise RuntimeError("cannot inspect")

        def __repr__(self) -> str:
            return "RAW SECRET PROMPT"

    published = _capture_published(monkeypatch)
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    publisher.event(BadEvent())

    payload = published[0]["payload"]
    assert payload["category"] == "stream_event_normalization_failed"
    assert payload["event_type"] == "BadEvent"
    assert "RAW SECRET PROMPT" not in str(payload)


def test_normalizer_omits_private_content_and_clips_text() -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    long_text = "x" * 300
    event = SimpleNamespace(
        event_kind="function_tool_call",
        part=SimpleNamespace(
            part_kind="tool-call",
            tool_name="lookup_secret",
            tool_call_id="call-1",
            args={"password": "do-not-leak"},
        ),
        delta=SimpleNamespace(part_delta_kind="text", content_delta=long_text),
        result=SimpleNamespace(content="tool result should not leak"),
    )
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=True,
    )

    payload = publisher.normalize_event(event)

    assert payload["tool_name"] == "lookup_secret"
    assert payload["tool_call_id"] == "call-1"
    assert payload["text_delta"].endswith("...")
    assert len(payload["text_delta"]) == 240
    assert "password" not in str(payload)
    assert "do-not-leak" not in str(payload)
    assert "tool result should not leak" not in str(payload)


def test_stream_transcripts_serialization_fallback_omits_raw_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _model as model_module

    class SecretEvent:
        def __repr__(self) -> str:
            return "RAW SECRET STREAM EVENT"

    def fail_dump(*_args: object, **_kwargs: object) -> object:
        raise ValueError("cannot serialize")

    monkeypatch.setattr(
        model_module,
        "_MODEL_STREAM_EVENT_ADAPTER",
        SimpleNamespace(dump_python=fail_dump),
    )

    serialized = model_module._serialize_stream_event(SecretEvent())

    assert serialized == {
        "event_type": "SecretEvent",
        "serialization_error": "stream_event_serialization_failed",
    }
    assert "RAW SECRET STREAM EVENT" not in repr(serialized)


def test_save_stream_transcripts_false_omits_text_delta() -> None:
    from kitaru.adapters.pydantic_ai._streaming import PydanticAIStreamPublisher

    event = SimpleNamespace(
        event_kind="part_delta",
        delta=SimpleNamespace(
            part_delta_kind="text", content_delta="visible only if saved"
        ),
    )
    publisher = PydanticAIStreamPublisher(
        agent_name="agent",
        surface="run",
        source="event_stream_handler",
        include_content=False,
    )

    payload = publisher.normalize_event(event)

    assert "text_delta" not in payload
    assert "visible only if saved" not in str(payload)
    assert payload["display"] == "Text delta"


@pytest.mark.anyio
async def test_handler_wrapper_publishes_lazily_and_preserves_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _agent as agent_module
    from kitaru.adapters.pydantic_ai._streaming import stream_surface

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: tracker)
    events = [
        SimpleNamespace(event_kind="part_start"),
        SimpleNamespace(event_kind="part_end"),
    ]
    received: list[Any] = []

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for event in stream:
            received.append(event)

    agent = _make_agent()
    wrapped = agent._prepare_event_stream_handler(handler)
    assert wrapped is not None

    with stream_surface("run"):
        await wrapped(SimpleNamespace(), _async_events(events))

    assert received == events
    assert [event["kind"] for event in published] == [
        "pydantic_ai.stream.started",
        "pydantic_ai.stream.event",
        "pydantic_ai.stream.event",
        "pydantic_ai.stream.completed",
    ]
    assert published[-1]["payload"]["event_count"] == 2
    assert len(tracker.stream_records) == 1
    assert tracker.stream_records[0]["error"] is None


@pytest.mark.anyio
async def test_handler_failure_publishes_failed_and_records_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _agent as agent_module
    from kitaru.adapters.pydantic_ai._streaming import stream_surface

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: tracker)
    failure = RuntimeError("handler crashed")

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for _event in stream:
            pass
        raise failure

    agent = _make_agent()
    wrapped = agent._prepare_event_stream_handler(handler)
    assert wrapped is not None

    with (
        pytest.raises(RuntimeError, match="handler crashed"),
        stream_surface("run_sync"),
    ):
        await wrapped(
            SimpleNamespace(),
            _async_events([SimpleNamespace(event_kind="part_start")]),
        )

    assert [event["kind"] for event in published] == [
        "pydantic_ai.stream.started",
        "pydantic_ai.stream.event",
        "pydantic_ai.stream.failed",
    ]
    assert published[-1]["flush"] is True
    assert tracker.stream_records[0]["error"] is failure


@pytest.mark.anyio
async def test_emit_child_events_false_keeps_handler_and_durable_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _agent as agent_module

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: tracker)
    received: list[Any] = []

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for event in stream:
            received.append(event)

    agent = _make_agent(capture=CapturePolicy(emit_child_events=False))
    wrapped = agent._prepare_event_stream_handler(handler)
    assert wrapped is not None
    event = SimpleNamespace(event_kind="part_start")

    await wrapped(SimpleNamespace(), _async_events([event]))

    assert received == [event]
    assert published == []
    assert len(tracker.stream_records) == 1


@pytest.mark.anyio
async def test_handler_suppresses_nested_model_live_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _agent as agent_module
    from kitaru.adapters.pydantic_ai._streaming import (
        model_stream_live_events_suppressed,
    )

    _capture_published(monkeypatch)
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: None)
    suppression_states: list[bool] = []

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for _event in stream:
            suppression_states.append(model_stream_live_events_suppressed())

    agent = _make_agent()
    wrapped = agent._prepare_event_stream_handler(handler)
    assert wrapped is not None

    await wrapped(
        SimpleNamespace(), _async_events([SimpleNamespace(event_kind="part_start")])
    )

    assert suppression_states == [True]
    assert model_stream_live_events_suppressed() is False


@pytest.mark.anyio
async def test_model_request_stream_publishes_events_and_keeps_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(
        model_module.kitaru,
        "save",
        lambda *args, **kwargs: saved.append({"args": args, "kwargs": kwargs}),
    )
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=True,
        ),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    async with model.request_stream([], None, ModelRequestParameters()) as response:
        events = [event async for event in response]

    assert events
    published_kinds = [event["kind"] for event in published]
    assert published_kinds[0] == "pydantic_ai.stream.started"
    assert "pydantic_ai.stream.event" in published_kinds
    assert published[-1]["kind"] == "pydantic_ai.stream.completed"
    assert published[-1]["payload"]["event_count"] == len(events)
    assert tracker.model_records[-1]["status"] == "completed"
    assert tracker.model_records[-1]["stream_event_count"] == len(events)
    assert saved[0]["args"][1]["event_count"] == len(events)


@pytest.mark.anyio
async def test_model_request_stream_uses_surface_and_duplicate_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel
    from kitaru.adapters.pydantic_ai._streaming import (
        stream_surface,
        suppress_model_stream_live_events,
    )

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module.kitaru, "save", lambda *_args, **_kwargs: None)
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(save_prompts=False, save_responses=False),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    with stream_surface("run_stream"):
        async with model.request_stream([], None, ModelRequestParameters()) as response:
            async for _event in response:
                break

    assert published[0]["payload"]["surface"] == "run_stream"
    unsuppressed_event_count = sum(
        event["kind"] == "pydantic_ai.stream.event" for event in published
    )
    assert unsuppressed_event_count == 1

    published.clear()
    with suppress_model_stream_live_events():
        async with model.request_stream([], None, ModelRequestParameters()) as response:
            async for _event in response:
                break

    assert published == []


@pytest.mark.anyio
async def test_model_request_stream_save_transcripts_false_omits_text_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=False,
        ),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    async with model.request_stream([], None, ModelRequestParameters()) as response:
        async for _event in response:
            pass

    event_payloads = [
        event["payload"]
        for event in published
        if event["kind"] == "pydantic_ai.stream.event"
    ]
    assert event_payloads
    assert all("text_delta" not in payload for payload in event_payloads)
    assert all("success" not in str(payload) for payload in event_payloads)


@pytest.mark.anyio
async def test_run_stream_and_iter_still_require_explicit_checkpoints() -> None:
    from pydantic_ai.exceptions import UserError

    agent = _make_agent()

    with pytest.raises(UserError, match=r"explicit `@kitaru.checkpoint`"):
        async with agent.run_stream("hello"):
            pass
    with pytest.raises(UserError, match=r"explicit `@kitaru.checkpoint`"):
        async with agent.iter("hello"):
            pass


@pytest.mark.anyio
async def test_iter_lifecycle_publishes_without_proxying_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.runtime import _checkpoint_scope

    published = _capture_published(monkeypatch)
    agent = _make_agent()

    with _checkpoint_scope(name="iter-checkpoint", checkpoint_type="custom"):
        async with agent.iter("hello") as run:
            assert run is not None

    assert [event["kind"] for event in published] == [
        "pydantic_ai.stream.started",
        "pydantic_ai.stream.completed",
    ]
    assert all(event["payload"]["surface"] == "iter" for event in published)
