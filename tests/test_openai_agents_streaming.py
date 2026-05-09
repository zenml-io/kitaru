"""Focused tests for OpenAI Agents SDK streaming support."""

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("agents")

from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAICapturePolicy,
    OpenAIRunRequest,
    OpenAIRunResult,
)
from kitaru.errors import KitaruUsageError


class FakeZenMLStreams:
    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.flush_count = 0

    def publish(
        self,
        payload: dict[str, Any],
        *,
        kind: str,
        stream_id: str | None,
        index: int | None,
    ) -> None:
        self.published.append(
            {
                "kind": kind,
                "stream_id": stream_id,
                "index": index,
                "payload": payload,
            }
        )

    def flush(self) -> None:
        self.flush_count += 1


class FakeStreamingResult:
    def __init__(self, events: list[Any]) -> None:
        self._events = events
        self.drained = False

    async def stream_events(self) -> Any:
        for event in self._events:
            yield event
        self.drained = True


@pytest.mark.anyio
async def test_run_stream_runner_call_drains_stream_and_finalizes_in_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as openai_agent
    import kitaru.adapters.openai_agents._streaming as openai_streaming
    from kitaru.adapters.openai_agents._tracking import get_current_tracker

    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(
        openai_streaming,
        "_load_zenml_streams",
        lambda: fake_streams,
    )
    monkeypatch.setattr(openai_agent, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_agent, "is_inside_checkpoint", lambda: False)

    checkpoint_calls: list[dict[str, Any]] = []

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> OpenAIRunResult:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(
        openai_agent,
        "run_async_in_checkpoint",
        fake_run_async_in_checkpoint,
    )

    fake_sdk_result = FakeStreamingResult(
        [{"type": "raw_response_event", "data": {"delta": "hello"}}]
    )
    seen_runner_kwargs: dict[str, Any] = {}

    async def fake_run_openai_agent_streamed(**kwargs: Any) -> FakeStreamingResult:
        seen_runner_kwargs.update(kwargs)
        return fake_sdk_result

    monkeypatch.setattr(
        openai_agent,
        "run_openai_agent_streamed",
        fake_run_openai_agent_streamed,
    )

    build_saw_drained: list[bool] = []

    def fake_build_run_result(
        sdk_result: FakeStreamingResult, **_kwargs: Any
    ) -> OpenAIRunResult:
        build_saw_drained.append(sdk_result.drained)
        return OpenAIRunResult(status="completed", final_output="done")

    monkeypatch.setattr(openai_agent, "build_run_result", fake_build_run_result)

    finalize_saw_active_tracker: list[bool] = []

    def fake_finalize(
        self: KitaruRunner,
        result: OpenAIRunResult,
        *,
        tracker: Any,
    ) -> OpenAIRunResult:
        del self
        finalize_saw_active_tracker.append(get_current_tracker() is tracker)
        return result.model_copy(update={"event_log_artifact_name": "events"})

    monkeypatch.setattr(KitaruRunner, "_finalize_run_result", fake_finalize)

    runner = KitaruRunner(
        SimpleNamespace(name="stream-agent"),
        checkpoint_strategy="runner_call",
        capture=OpenAICapturePolicy(save_response_items=True),
    )

    result = await runner.run_stream(OpenAIRunRequest.start("hi", max_turns=4))

    assert result.status == "completed"
    assert result.final_output == "done"
    assert result.event_log_artifact_name == "events"
    assert fake_sdk_result.drained
    assert build_saw_drained == [True]
    assert finalize_saw_active_tracker == [True]
    assert seen_runner_kwargs["input"] == "hi"
    assert seen_runner_kwargs["max_turns"] == 4
    assert checkpoint_calls[0]["step_name"] == "stream-agent_openai_runner_call"
    assert checkpoint_calls[0]["cache_key"]

    assert [event["kind"] for event in fake_streams.published] == [
        "openai_agents.stream.start",
        "openai_agents.stream.event",
        "openai_agents.stream.end",
    ]
    assert [event["index"] for event in fake_streams.published] == [0, 1, 2]
    stream_ids = {event["stream_id"] for event in fake_streams.published}
    assert len(stream_ids) == 1
    assert fake_streams.published[1]["payload"]["text_delta"] == "hello"
    assert "raw" in fake_streams.published[1]["payload"]
    assert fake_streams.flush_count == 1


@pytest.mark.anyio
async def test_run_stream_rejects_calls_strategy() -> None:
    runner = KitaruRunner(SimpleNamespace(name="agent"), checkpoint_strategy="calls")

    with pytest.raises(KitaruUsageError, match="runner_call"):
        await runner.run_stream(OpenAIRunRequest.start("hi"))


@pytest.mark.anyio
async def test_run_stream_rejects_outside_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as openai_agent

    monkeypatch.setattr(openai_agent, "is_inside_flow", lambda: False)
    runner = KitaruRunner(
        SimpleNamespace(name="agent"), checkpoint_strategy="runner_call"
    )

    with pytest.raises(KitaruUsageError, match="inside a Kitaru flow"):
        await runner.run_stream(OpenAIRunRequest.start("hi"))


def test_run_stream_sync_rejects_running_event_loop() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="agent"), checkpoint_strategy="runner_call"
    )
    request = OpenAIRunRequest.start("hi")

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="already running event loop"):
            runner.run_stream_sync(request)

    asyncio.run(call_sync())


def test_stream_publisher_orders_start_event_end_and_hides_raw_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._streaming as openai_streaming

    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(
        openai_streaming,
        "_load_zenml_streams",
        lambda: fake_streams,
    )
    publisher = openai_streaming.OpenAIStreamPublisher(
        agent_name="stream agent",
        include_raw=False,
        stream_id="stream-1",
    )

    publisher.publish_start()
    publisher.publish_sdk_event({"type": "raw_response_event", "data": {"delta": "hi"}})
    publisher.publish_end(status="completed")

    assert [event["kind"] for event in fake_streams.published] == [
        "openai_agents.stream.start",
        "openai_agents.stream.event",
        "openai_agents.stream.end",
    ]
    assert [event["index"] for event in fake_streams.published] == [0, 1, 2]
    assert [event["stream_id"] for event in fake_streams.published] == [
        "stream-1",
        "stream-1",
        "stream-1",
    ]
    payload = fake_streams.published[1]["payload"]
    assert payload["category"] == "raw_response_event"
    assert payload["text_delta"] == "hi"
    assert "raw" not in payload
    assert fake_streams.flush_count == 1


def test_stream_publisher_orders_start_error_and_flushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._streaming as openai_streaming

    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(
        openai_streaming,
        "_load_zenml_streams",
        lambda: fake_streams,
    )
    publisher = openai_streaming.OpenAIStreamPublisher(
        agent_name="stream agent",
        include_raw=False,
        stream_id="stream-1",
    )

    publisher.publish_start()
    publisher.publish_error(RuntimeError("boom"))

    assert [event["kind"] for event in fake_streams.published] == [
        "openai_agents.stream.start",
        "openai_agents.stream.error",
    ]
    assert [event["index"] for event in fake_streams.published] == [0, 1]
    error_payload = fake_streams.published[1]["payload"]
    assert error_payload["status"] == "failed"
    assert error_payload["error_type"] == "RuntimeError"
    assert error_payload["message"] == "boom"
    assert fake_streams.flush_count == 1


def test_stream_publishing_degrades_when_zenml_streams_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._streaming as openai_streaming

    monkeypatch.setattr(openai_streaming, "_load_zenml_streams", lambda: None)
    publisher = openai_streaming.OpenAIStreamPublisher(
        agent_name="agent",
        include_raw=True,
        stream_id="stream-1",
    )

    publisher.publish_start()
    publisher.publish_sdk_event({"type": "raw_response_event", "data": {"delta": "hi"}})
    publisher.publish_end(status="completed")
