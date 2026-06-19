"""Focused tests for PydanticAI live streaming events."""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from types import SimpleNamespace
from typing import Any, cast

import pytest

pytest.importorskip("pydantic_ai")


async def _async_events(events: list[Any]) -> AsyncIterator[Any]:
    for event in events:
        yield event


def _contains_kitaru_truncation(value: Any) -> bool:
    if isinstance(value, dict):
        return "_kitaru_truncated" in value or any(
            _contains_kitaru_truncation(nested) for nested in value.values()
        )
    if isinstance(value, list):
        return any(_contains_kitaru_truncation(item) for item in value)
    return False


def _nested_wide_payload(*, width: int = 8, depth: int = 4) -> dict[str, Any]:
    payload: Any = "x" * 100
    for level in range(depth):
        payload = {f"level-{level}-key-{index}": payload for index in range(width)}
    return cast(dict[str, Any], payload)


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


class _FailingCompletionTracker(_FakeTracker):
    def record_model_event(self, event_id: str, context: Any, **kwargs: Any) -> None:
        if kwargs["status"] == "completed":
            raise RuntimeError("capture failed")
        super().record_model_event(event_id, context, **kwargs)


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


def _make_agent(*, capture: Any = None, event_stream_handler: Any = None) -> Any:
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import KitaruAgent

    return KitaruAgent(
        Agent(TestModel(), name="streamer"),
        capture=capture,
        event_stream_handler=event_stream_handler,
    )


def _make_tracked_model(
    monkeypatch: pytest.MonkeyPatch,
    wrapped_model: Any,
    *,
    capture: Any,
    tracker: _FakeTracker | None = None,
) -> tuple[Any, list[dict[str, Any]], _FakeTracker]:
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    published = _capture_published(monkeypatch)
    tracker = tracker or _FakeTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module.kitaru, "save", lambda *_args, **_kwargs: None)
    model = KitaruModel(wrapped_model, capture=capture, agent_name="streamer")
    monkeypatch.setattr(model, "_should_track", lambda: True)
    return model, published, tracker


def _stream_event_sources(published: list[dict[str, Any]]) -> list[str]:
    return [
        event["payload"]["source"]
        for event in published
        if event["kind"] == "pydantic_ai.stream.event"
    ]


def _pydantic_ai_stream_sources(published: list[dict[str, Any]]) -> list[str]:
    return [
        event["payload"]["source"]
        for event in published
        if event["kind"].startswith("pydantic_ai.stream.")
    ]


def _quiet_stream_capture_policy() -> Any:
    from kitaru.adapters.pydantic_ai import CapturePolicy

    return CapturePolicy(
        save_prompts=False,
        save_responses=False,
        save_stream_transcripts=False,
    )


def _collecting_handler() -> tuple[list[Any], Any]:
    received: list[Any] = []

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for event in stream:
            received.append(event)

    return received, handler


def _assert_only_handler_stream_events(
    published: list[dict[str, Any]], received: list[Any]
) -> None:
    stream_item_sources = _stream_event_sources(published)
    all_stream_sources = _pydantic_ai_stream_sources(published)
    assert received
    assert stream_item_sources
    assert all_stream_sources
    assert all(source == "event_stream_handler" for source in stream_item_sources), (
        stream_item_sources
    )
    assert "model_request_stream" not in all_stream_sources
    assert len(stream_item_sources) == len(received)


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


def test_capture_policy_defaults_to_save_stream_transcripts() -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy

    assert CapturePolicy().save_stream_transcripts is True


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


def test_stream_transcripts_serialization_bounds_retained_event_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _model as model_module

    def dump_large_event(*_args: object, **_kwargs: object) -> object:
        return {
            "event_kind": "part_delta",
            "content": "x" * 5_000,
            "items": list(range(25)),
        }

    monkeypatch.setattr(
        model_module,
        "_MODEL_STREAM_EVENT_ADAPTER",
        SimpleNamespace(dump_python=dump_large_event),
    )

    serialized = model_module._serialize_stream_event(SimpleNamespace())

    assert serialized["content"].endswith("...")
    assert (
        len(serialized["content"]) <= model_module._MAX_STREAM_TRANSCRIPT_STRING_CHARS
    )
    assert (
        len(serialized["items"]) == model_module._MAX_STREAM_TRANSCRIPT_EVENT_ITEMS + 1
    )
    assert serialized["items"][-1] == {"_kitaru_omitted_items": 5}


def test_stream_transcripts_serialization_bounds_nested_wide_retained_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _model as model_module

    def dump_nested_wide_event(*_args: object, **_kwargs: object) -> object:
        return {"event_kind": "part_delta", "payload": _nested_wide_payload()}

    monkeypatch.setattr(model_module, "_MAX_STREAM_TRANSCRIPT_TOTAL_ITEMS", 24)
    monkeypatch.setattr(
        model_module,
        "_MODEL_STREAM_EVENT_ADAPTER",
        SimpleNamespace(dump_python=dump_nested_wide_event),
    )

    serialized = model_module._serialize_stream_event(SimpleNamespace())

    assert _contains_kitaru_truncation(serialized)
    assert len(repr(serialized)) < 5_000


def test_stream_transcripts_serialization_bounds_huge_mapping_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _model as model_module

    huge_key = "x" * 100_000

    class BadKey:
        def __str__(self) -> str:
            raise RuntimeError("key must not crash serialization")

    def dump_huge_key_event(*_args: object, **_kwargs: object) -> object:
        return {
            "event_kind": "part_delta",
            "payload": {
                huge_key: "small",
                BadKey(): "also small",
            },
        }

    monkeypatch.setattr(model_module, "_MAX_STREAM_TRANSCRIPT_APPROX_CHARS", 80)
    monkeypatch.setattr(
        model_module,
        "_MODEL_STREAM_EVENT_ADAPTER",
        SimpleNamespace(dump_python=dump_huge_key_event),
    )

    serialized = model_module._serialize_stream_event(SimpleNamespace())

    assert len(repr(serialized)) < 500
    assert huge_key not in repr(serialized)
    assert serialized["payload"]["_kitaru_omitted_keys"] >= 1


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
async def test_handler_suppresses_own_model_live_events_without_global_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import _agent as agent_module
    from kitaru.adapters.pydantic_ai._streaming import (
        model_stream_live_events_suppressed,
    )

    _capture_published(monkeypatch)
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: None)
    model_suppression_states: list[bool] = []
    global_suppression_states: list[bool] = []

    async def handler(_ctx: Any, stream: AsyncIterable[Any]) -> None:
        async for _event in stream:
            model_suppression_states.append(
                agent._model._live_stream_events_suppressed()
            )
            global_suppression_states.append(model_stream_live_events_suppressed())

    agent = _make_agent()
    wrapped = agent._prepare_event_stream_handler(handler)
    assert wrapped is not None

    await wrapped(
        SimpleNamespace(), _async_events([SimpleNamespace(event_kind="part_start")])
    )

    assert model_suppression_states == [True]
    assert global_suppression_states == [False]
    assert agent._model._live_stream_events_suppressed() is False
    assert model_stream_live_events_suppressed() is False


@pytest.mark.anyio
async def test_agent_run_with_handler_publishes_stream_events_once_from_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    received, handler = _collecting_handler()
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    await agent.run("hello", event_stream_handler=handler)

    _assert_only_handler_stream_events(published, received)


@pytest.mark.anyio
async def test_agent_run_with_constructor_handler_publishes_once_from_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    received, handler = _collecting_handler()
    agent = _make_agent(
        capture=_quiet_stream_capture_policy(),
        event_stream_handler=handler,
    )

    await agent.run("hello")

    _assert_only_handler_stream_events(published, received)


@pytest.mark.anyio
async def test_agent_run_with_handler_keeps_capture_without_model_live_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _agent as agent_module
    from kitaru.adapters.pydantic_ai import _model as model_module

    published = _capture_published(monkeypatch)
    tracker = _FakeTracker()
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(agent_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(
        model_module.kitaru,
        "save",
        lambda *args, **kwargs: saved.append({"args": args, "kwargs": kwargs}),
    )
    received, handler = _collecting_handler()
    agent = _make_agent(
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=True,
        ),
    )

    await agent.run("hello", event_stream_handler=handler)

    _assert_only_handler_stream_events(published, received)
    assert saved
    transcript = saved[0]["args"][1]
    assert tracker.model_records[-1]["status"] == "completed"
    assert tracker.model_records[-1]["stream_event_count"] == len(received)
    assert transcript["event_count"] == len(received)
    assert len(transcript["events"]) == len(received)
    assert transcript["events_truncated"] is False
    assert transcript["omitted_event_count"] == 0


@pytest.mark.anyio
async def test_model_live_suppression_does_not_leak_to_same_model_child_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._streaming import stream_surface

    model, published, _tracker = _make_tracked_model(
        monkeypatch,
        TestModel(),
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=False,
        ),
    )

    async def child_no_handler_request() -> list[Any]:
        with stream_surface("child-no-handler"):
            async with model.request_stream(
                [], None, ModelRequestParameters()
            ) as response:
                return [event async for event in response]

    with model.suppress_live_stream_events():
        events = await asyncio.create_task(child_no_handler_request())

    child_payloads = [
        event["payload"]
        for event in published
        if event["kind"].startswith("pydantic_ai.stream.")
        and event["payload"]["surface"] == "child-no-handler"
    ]
    assert events
    assert child_payloads
    assert "model_request_stream" in {payload["source"] for payload in child_payloads}


@pytest.mark.anyio
async def test_claim_first_suppression_does_not_leak_to_preexisting_child_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._streaming import stream_surface

    child_release = asyncio.Event()
    model, published, _tracker = _make_tracked_model(
        monkeypatch,
        TestModel(),
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=False,
        ),
    )

    async def child_no_handler_request() -> list[Any]:
        await child_release.wait()
        with stream_surface("child-after-claim"):
            async with model.request_stream(
                [], None, ModelRequestParameters()
            ) as response:
                return [event async for event in response]

    with model.suppress_live_stream_events(claim_first_stream_task=True):
        child_task = asyncio.create_task(child_no_handler_request())
        with stream_surface("mirrored"):
            async with model.request_stream(
                [], None, ModelRequestParameters()
            ) as response:
                mirrored_events = [event async for event in response]
        child_release.set()
        child_events = await asyncio.wait_for(child_task, timeout=5)

    sources_by_surface: dict[str, set[str]] = {}
    for event in published:
        if event["kind"].startswith("pydantic_ai.stream."):
            payload = event["payload"]
            sources_by_surface.setdefault(payload["surface"], set()).add(
                payload["source"]
            )

    assert mirrored_events
    assert child_events
    assert "model_request_stream" not in sources_by_surface.get("mirrored", set())
    assert "model_request_stream" in sources_by_surface["child-after-claim"]


@pytest.mark.anyio
async def test_model_live_suppression_is_context_local_for_overlapping_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    from contextlib import asynccontextmanager

    from pydantic_ai.models import Model, ModelRequestParameters

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._streaming import (
        PydanticAIStreamPublisher,
        current_stream_surface,
        stream_surface,
        suppress_model_stream_live_events,
    )

    class ControlledStreamedResponse:
        def __init__(
            self,
            *,
            release: asyncio.Event,
            event_kind: str,
        ) -> None:
            self.model_request_parameters = ModelRequestParameters()
            self.final_result_event = SimpleNamespace(event_kind="final_result")
            self._release = release
            self._event = SimpleNamespace(event_kind=event_kind)
            self._response = SimpleNamespace(
                model_name="fake-test-model",
                usage=None,
                tool_calls=[],
            )

        async def __aiter__(self) -> AsyncIterator[Any]:
            await self._release.wait()
            yield self._event

        def get(self) -> Any:
            return self._response

    class ControlledWrappedModel(Model):
        def __init__(self, streams: dict[str, ControlledStreamedResponse]) -> None:
            self._streams = streams

        @property
        def model_name(self) -> str:
            return "controlled-model"

        @property
        def system(self) -> str:
            return "test"

        async def request(self, *_args: Any, **_kwargs: Any) -> Any:
            raise NotImplementedError("non-streaming requests are not used")

        @asynccontextmanager
        async def request_stream(
            self,
            _messages: Any,
            _model_settings: Any,
            _model_request_parameters: Any,
            _run_context: Any = None,
        ) -> AsyncIterator[ControlledStreamedResponse]:
            yield self._streams[current_stream_surface(default="missing-surface")]

    handler_release = asyncio.Event()
    no_handler_release = asyncio.Event()
    no_handler_entered = asyncio.Event()
    model, published, _tracker = _make_tracked_model(
        monkeypatch,
        ControlledWrappedModel(
            {
                "handler-run": ControlledStreamedResponse(
                    release=handler_release,
                    event_kind="handler_part",
                ),
                "no-handler-run": ControlledStreamedResponse(
                    release=no_handler_release,
                    event_kind="no_handler_part",
                ),
            }
        ),
        capture=CapturePolicy(
            save_prompts=False,
            save_responses=False,
            save_stream_transcripts=False,
        ),
    )

    async def handler_active_request() -> None:
        with (
            suppress_model_stream_live_events(),
            model.suppress_live_stream_events(),
            stream_surface("handler-run"),
        ):
            handler_publisher = PydanticAIStreamPublisher(
                agent_name="streamer",
                surface="handler-run",
                source="event_stream_handler",
                include_content=False,
            )
            handler_publisher.started()
            async with model.request_stream(
                [], None, ModelRequestParameters()
            ) as response:
                await no_handler_entered.wait()
                handler_release.set()
                event_count = 0
                async for event in response:
                    event_count += 1
                    handler_publisher.event(event)
            handler_publisher.completed(event_count=event_count)

    async def no_handler_request() -> None:
        with stream_surface("no-handler-run"):
            async with model.request_stream(
                [], None, ModelRequestParameters()
            ) as response:
                no_handler_entered.set()
                no_handler_release.set()
                async for _event in response:
                    pass

    await asyncio.wait_for(
        asyncio.gather(handler_active_request(), no_handler_request()), timeout=5
    )

    sources_by_surface: dict[str, list[str]] = {}
    categories_by_surface: dict[str, list[str]] = {}
    for event in published:
        if event["kind"].startswith("pydantic_ai.stream."):
            payload = event["payload"]
            surface = payload["surface"]
            sources_by_surface.setdefault(surface, []).append(payload["source"])
            categories_by_surface.setdefault(surface, []).append(payload["category"])

    assert sources_by_surface["handler-run"]
    assert sources_by_surface["no-handler-run"]
    assert set(sources_by_surface["handler-run"]) == {"event_stream_handler"}
    assert "handler_part" in categories_by_surface["handler-run"]
    assert "model_request_stream" in sources_by_surface["no-handler-run"]
    assert "event_stream_handler" not in sources_by_surface["no-handler-run"]
    assert "no_handler_part" in categories_by_surface["no-handler-run"]


@pytest.mark.anyio
async def test_agent_run_without_handler_does_not_publish_stream_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    await agent.run("hello")

    assert _stream_event_sources(published) == []


def test_agent_run_sync_with_handler_publishes_stream_events_once_from_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    received, handler = _collecting_handler()
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    agent.run_sync("hello", event_stream_handler=handler)

    _assert_only_handler_stream_events(published, received)


def test_agent_run_sync_without_handler_does_not_publish_stream_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    agent.run_sync("hello")

    assert _stream_event_sources(published) == []


@pytest.mark.anyio
async def test_agent_run_stream_with_handler_publishes_stream_events_once_from_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.runtime import _checkpoint_scope

    published = _capture_published(monkeypatch)
    received, handler = _collecting_handler()
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    with _checkpoint_scope(name="run-stream-checkpoint", checkpoint_type="custom"):
        async with agent.run_stream("hello", event_stream_handler=handler) as result:
            await result.get_output()

    _assert_only_handler_stream_events(published, received)


@pytest.mark.anyio
async def test_agent_run_stream_without_handler_keeps_model_stream_live_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.runtime import _checkpoint_scope

    published = _capture_published(monkeypatch)
    agent = _make_agent(capture=_quiet_stream_capture_policy())

    with _checkpoint_scope(name="run-stream-checkpoint", checkpoint_type="custom"):
        async with agent.run_stream("hello") as result:
            await result.get_output()

    sources = _pydantic_ai_stream_sources(published)
    assert sources
    assert "model_request_stream" in sources
    assert "event_stream_handler" not in sources


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
    transcript = saved[0]["args"][1]
    assert tracker.model_records[-1]["status"] == "completed"
    assert tracker.model_records[-1]["stream_event_count"] == len(events)
    assert transcript["event_count"] == len(events)
    assert len(transcript["events"]) == len(events)
    assert transcript["events_truncated"] is False
    assert transcript["omitted_event_count"] == 0


@pytest.mark.anyio
async def test_model_request_stream_transcript_keeps_bounded_event_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    tracker = _FakeTracker()
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module, "_MAX_STREAM_TRANSCRIPT_EVENTS", 1)
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

    transcript = saved[0]["args"][1]
    assert len(events) > 1
    assert transcript["event_count"] == len(events)
    assert len(transcript["events"]) == 1
    assert transcript["events_truncated"] is True
    assert transcript["omitted_event_count"] == len(events) - 1
    assert tracker.model_records[-1]["stream_event_count"] == len(events)


@pytest.mark.anyio
async def test_model_request_stream_transcript_bounds_final_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    tracker = _FakeTracker()
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(
        model_module,
        "_serialize_model_response",
        lambda _response: {"content": "x" * 5_000, "items": list(range(25))},
    )
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
        async for _event in response:
            pass

    transcript = saved[0]["args"][1]
    final_response = transcript["final_response"]
    assert transcript["final_response_truncated"] is True
    assert final_response["content"].endswith("...")
    assert (
        len(final_response["content"])
        <= model_module._MAX_STREAM_TRANSCRIPT_STRING_CHARS
    )
    assert (
        len(final_response["items"])
        == model_module._MAX_STREAM_TRANSCRIPT_EVENT_ITEMS + 1
    )
    assert final_response["items"][-1] == {"_kitaru_omitted_items": 5}


@pytest.mark.anyio
async def test_model_request_stream_transcript_bounds_nested_wide_final_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    tracker = _FakeTracker()
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module, "_MAX_STREAM_TRANSCRIPT_TOTAL_ITEMS", 24)
    monkeypatch.setattr(
        model_module,
        "_serialize_model_response",
        lambda _response: {"payload": _nested_wide_payload()},
    )
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
        async for _event in response:
            pass

    transcript = saved[0]["args"][1]
    final_response = transcript["final_response"]
    assert transcript["final_response_truncated"] is True
    assert _contains_kitaru_truncation(final_response)
    assert len(repr(final_response)) < 5_000


@pytest.mark.anyio
async def test_model_request_stream_publishes_completed_after_capture_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    order: list[str] = []
    published: list[str] = []

    class OrderingTracker(_FakeTracker):
        def record_model_event(
            self, event_id: str, context: Any, **kwargs: Any
        ) -> None:
            order.append(f"record_{kwargs['status']}")
            super().record_model_event(event_id, context, **kwargs)

        def reserve_tool_call_order(self, **kwargs: Any) -> None:
            order.append("tool_order_reserved")
            super().reserve_tool_call_order(**kwargs)

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        _ = payload, flush
        published.append(kind)
        if kind == "pydantic_ai.stream.completed":
            order.append("stream_completed")

    tracker = OrderingTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(
        model_module.kitaru,
        "save",
        lambda *_args, **_kwargs: order.append("artifact_saved"),
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._streaming.kitaru_events.publish",
        fake_publish,
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
        async for _event in response:
            pass

    assert published[-1] == "pydantic_ai.stream.completed"
    assert order[-4:] == [
        "artifact_saved",
        "tool_order_reserved",
        "record_completed",
        "stream_completed",
    ]


@pytest.mark.anyio
async def test_model_request_stream_capture_failure_publishes_failed_not_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    published = _capture_published(monkeypatch)
    tracker = _FailingCompletionTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module.kitaru, "save", lambda *_args, **_kwargs: None)
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(save_prompts=False, save_responses=False),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    with pytest.raises(RuntimeError, match="capture failed"):
        async with model.request_stream([], None, ModelRequestParameters()) as response:
            async for _event in response:
                pass

    published_kinds = [event["kind"] for event in published]
    assert "pydantic_ai.stream.completed" not in published_kinds
    assert published_kinds[-1] == "pydantic_ai.stream.failed"
    assert tracker.model_records[-1]["status"] == "failed"


@pytest.mark.anyio
async def test_model_request_stream_tool_order_failure_publishes_failed_not_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel

    class FailingToolOrderTracker(_FakeTracker):
        def reserve_tool_call_order(self, **kwargs: Any) -> None:
            _ = kwargs
            raise RuntimeError("tool order failed")

    published = _capture_published(monkeypatch)
    tracker = FailingToolOrderTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module.kitaru, "save", lambda *_args, **_kwargs: None)
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(save_prompts=False, save_responses=False),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    with pytest.raises(RuntimeError, match="tool order failed"):
        async with model.request_stream([], None, ModelRequestParameters()) as response:
            async for _event in response:
                pass

    published_kinds = [event["kind"] for event in published]
    assert "pydantic_ai.stream.completed" not in published_kinds
    assert published_kinds[-1] == "pydantic_ai.stream.failed"
    assert [record["status"] for record in tracker.model_records] == ["failed"]


@pytest.mark.anyio
async def test_model_request_stream_suppression_disables_failed_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai import _model as model_module
    from kitaru.adapters.pydantic_ai._model import KitaruModel
    from kitaru.adapters.pydantic_ai._streaming import suppress_model_stream_live_events

    published = _capture_published(monkeypatch)
    tracker = _FailingCompletionTracker()
    monkeypatch.setattr(model_module, "get_current_tracker", lambda: tracker)
    monkeypatch.setattr(model_module.kitaru, "save", lambda *_args, **_kwargs: None)
    model = KitaruModel(
        TestModel(),
        capture=CapturePolicy(save_prompts=False, save_responses=False),
        agent_name="streamer",
    )
    monkeypatch.setattr(model, "_should_track", lambda: True)

    with (
        pytest.raises(RuntimeError, match="capture failed"),
        suppress_model_stream_live_events(),
    ):
        async with model.request_stream([], None, ModelRequestParameters()) as response:
            async for _event in response:
                pass

    assert published == []
    assert tracker.model_records[-1]["status"] == "failed"
    assert tracker.model_records[-1]["stream_event_count"] > 0


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
