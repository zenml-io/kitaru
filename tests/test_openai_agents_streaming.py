"""Focused tests for OpenAI Agents runner-call streaming support."""

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("agents")

from agents import RunConfig, Runner

import kitaru.adapters.openai_agents._runner as openai_runner
import kitaru.adapters.openai_agents._streaming as openai_streaming
from kitaru.adapters.openai_agents import (
    OPENAI_STREAM_COMPLETED,
    OPENAI_STREAM_EVENT,
    OPENAI_STREAM_FAILED,
    OPENAI_STREAM_STARTED,
    KitaruRunner,
    OpenAIApprovalDecision,
    OpenAICapturePolicy,
    OpenAIRunRequest,
    OpenAIRunStateEnvelope,
)
from kitaru.errors import KitaruUsageError


class FakeStreamedResult:
    def __init__(self, events: list[Any], *, final_output: str = "ok") -> None:
        self.events = events
        self.final_output = final_output
        self.drained = False

    async def stream_events(self):
        for event in self.events:
            yield event
        self.drained = True


@contextmanager
def fake_tracker_scope(_agent_name: str):
    yield SimpleNamespace(
        event_log_artifact_name="events",
        run_summary_artifact_name="summary",
    )


@pytest.mark.anyio
async def test_streamed_bridge_forwards_context_and_drains_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = SimpleNamespace(team_id="team-a")
    event = SimpleNamespace(type="agent_updated_stream_event")
    sdk_result = FakeStreamedResult([event])
    seen: dict[str, Any] = {}
    received_events: list[Any] = []

    def fake_run_streamed(*args: Any, **kwargs: Any) -> FakeStreamedResult:
        seen["args"] = args
        seen["kwargs"] = kwargs
        return sdk_result

    monkeypatch.setattr(Runner, "run_streamed", fake_run_streamed, raising=False)

    result = await openai_runner.run_openai_agent_streamed(
        agent=SimpleNamespace(name="agent"),
        input="hello",
        max_turns=3,
        run_config=RunConfig(tracing_disabled=True),
        context=ctx,
        on_event=received_events.append,
    )

    assert result is sdk_result
    assert sdk_result.drained is True
    assert received_events == [event]
    assert seen["args"][0].name == "agent"
    assert seen["args"][1] == "hello"
    assert seen["kwargs"]["context"] is ctx
    assert seen["kwargs"]["max_turns"] == 3


@pytest.mark.anyio
async def test_streamed_bridge_propagates_stream_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenStreamedResult:
        async def stream_events(self):
            yield SimpleNamespace(type="raw_response_event")
            raise RuntimeError("stream broke")

    monkeypatch.setattr(
        Runner,
        "run_streamed",
        lambda *_args, **_kwargs: BrokenStreamedResult(),
        raising=False,
    )

    with pytest.raises(RuntimeError, match="stream broke"):
        await openai_runner.run_openai_agent_streamed(
            agent=SimpleNamespace(name="agent"),
            input="hello",
            max_turns=3,
            run_config=RunConfig(tracing_disabled=True),
        )


def test_stream_publisher_normalizes_safe_payloads_and_flushes_terminal_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any], bool]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    publisher.started()
    publisher.event(
        SimpleNamespace(
            type="raw_response_event",
            data=SimpleNamespace(type="response.output_text.delta", delta=" hello "),
        )
    )
    publisher.event(
        SimpleNamespace(
            type="run_item_stream_event",
            name="tool_called",
            item=SimpleNamespace(
                type="function_call",
                name="lookup_order",
                call_id="call_1",
                arguments="SECRET_DO_NOT_LOG",
            ),
        )
    )
    publisher.completed(status="completed")

    assert [item[0] for item in published] == [
        OPENAI_STREAM_STARTED,
        OPENAI_STREAM_EVENT,
        OPENAI_STREAM_EVENT,
        OPENAI_STREAM_COMPLETED,
    ]
    assert "text_delta" not in published[1][1]
    assert published[1][1]["display"] == "response.output_text.delta"
    assert "hello" not in repr(published[1][1])
    assert published[2][1]["display"] == "Tool called: lookup_order"
    assert "arguments" not in published[2][1]
    assert "SECRET_DO_NOT_LOG" not in repr(published[2][1])
    assert published[-1][2] is True


def test_stream_publisher_caps_text_delta_payload() -> None:
    publisher = openai_streaming.OpenAIStreamPublisher(
        agent_name="support", include_text_deltas=True
    )
    long_delta = "x" * 500

    payload = publisher.normalize_event(
        SimpleNamespace(
            type="raw_response_event",
            data=SimpleNamespace(type="response.output_text.delta", delta=long_delta),
        )
    )

    assert len(payload["text_delta"]) == 240
    assert payload["text_delta"].endswith("...")
    assert payload["display"] == payload["text_delta"]


def test_stream_publisher_tolerates_unknown_events_and_publish_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def broken_publish(*_args: Any, **_kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        raise KitaruUsageError("outside checkpoint")

    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", broken_publish)
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    publisher.event(SimpleNamespace(type="surprise_event"))
    publisher.failed(RuntimeError("boom"))

    assert calls == 2


def test_stream_publisher_includes_text_delta_only_when_opted_in() -> None:
    publisher = openai_streaming.OpenAIStreamPublisher(
        agent_name="support", include_text_deltas=True
    )

    payload = publisher.normalize_event(
        SimpleNamespace(
            type="raw_response_event",
            data=SimpleNamespace(
                type="response.output_text.delta", delta="explicit text"
            ),
        )
    )

    assert payload["text_delta"] == "explicit text"
    assert payload["display"] == "explicit text"


def test_stream_publisher_failed_tolerates_broken_exception_str(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any], bool]] = []

    class BrokenStrError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("no string for you")

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    publisher.failed(BrokenStrError())

    assert published == [
        (
            OPENAI_STREAM_FAILED,
            {
                "adapter": "openai_agents",
                "agent_name": "support",
                "category": "lifecycle",
                "display": "OpenAI Agents stream failed: BrokenStrError",
                "error_type": "BrokenStrError",
                "message": "BrokenStrError",
            },
            True,
        )
    ]


def test_stream_publisher_tolerates_normalization_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any]]] = []

    class EventWithBrokenType:
        @property
        def type(self) -> str:
            raise RuntimeError("property exploded")

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        _ = flush
        published.append((kind, payload))

    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    publisher.event(EventWithBrokenType())

    assert published == [
        (
            OPENAI_STREAM_EVENT,
            {
                "adapter": "openai_agents",
                "agent_name": "support",
                "category": "stream_event_normalization_failed",
                "display": "OpenAI stream event",
                "event_type": "EventWithBrokenType",
            },
        )
    ]


def test_stream_publisher_handles_both_handoff_spellings() -> None:
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    documented = publisher.normalize_event(
        SimpleNamespace(
            type="run_item_stream_event",
            name="handoff_occured",
            item=SimpleNamespace(type="handoff", name="billing"),
        )
    )
    defensive = publisher.normalize_event(
        SimpleNamespace(
            type="run_item_stream_event",
            name="handoff_occurred",
            item=SimpleNamespace(type="handoff", name="billing"),
        )
    )

    assert documented["display"] == "Handoff occurred: billing"
    assert defensive["display"] == "Handoff occurred: billing"


def test_stream_publisher_handles_agent_updates() -> None:
    publisher = openai_streaming.OpenAIStreamPublisher(agent_name="support")

    payload = publisher.normalize_event(
        SimpleNamespace(
            type="agent_updated_stream_event",
            new_agent=SimpleNamespace(name="triage_agent"),
        )
    )

    assert payload["category"] == "agent_updated_stream_event"
    assert payload["new_agent_name"] == "triage_agent"
    assert payload["display"] == "Agent updated: triage_agent"


def test_runner_call_stream_cache_identity_is_separate_from_run() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    runner = KitaruRunner(
        SimpleNamespace(name="cache-identity-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    stream_text_runner = KitaruRunner(
        SimpleNamespace(name="cache-identity-agent"),
        checkpoint_strategy="runner_call",
        capture=OpenAICapturePolicy(include_stream_text_deltas=True),
        run_config_factory=lambda: run_config,
    )
    redacted_payload_runner = KitaruRunner(
        SimpleNamespace(name="cache-identity-agent"),
        checkpoint_strategy="runner_call",
        capture=OpenAICapturePolicy(save_interruption_payloads=False),
        run_config_factory=lambda: run_config,
    )

    run_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    stream_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="stream",
        stream_identity=runner._stream_cache_identity(),
    )
    stream_sync_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="stream",
        stream_identity=runner._stream_cache_identity(),
    )
    content_stream_key = stream_text_runner._runner_call_cache_key(
        request,
        agent=stream_text_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="stream",
        stream_identity=stream_text_runner._stream_cache_identity(),
    )
    redacted_payload_run_key = redacted_payload_runner._runner_call_cache_key(
        request,
        agent=redacted_payload_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )

    assert run_key != stream_key
    assert stream_key == stream_sync_key
    assert stream_key != content_stream_key
    assert run_key != redacted_payload_run_key


def test_runner_call_non_stream_and_stream_cache_entries_do_not_collide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as agent_module

    cache: dict[str | None, Any] = {}
    cache_keys: list[str | None] = []
    sync_calls = 0
    streamed_calls = 0

    def fake_run_sync_in_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        cache_keys.append(cache_key)
        if cache_key not in cache:
            cache[cache_key] = kwargs["body"]()
        return cache[cache_key]

    def fake_run_sync(**_kwargs: Any) -> SimpleNamespace:
        nonlocal sync_calls
        sync_calls += 1
        return SimpleNamespace(final_output="non-stream durable result")

    def fake_streamed_sync(**kwargs: Any) -> SimpleNamespace:
        nonlocal streamed_calls
        streamed_calls += 1
        kwargs["on_event"](SimpleNamespace(type="agent_updated_stream_event"))
        return SimpleNamespace(final_output="stream durable result")

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setitem(
        KitaruRunner._run_runner_call_checkpoint_sync.__globals__,
        "run_sync_in_checkpoint",
        fake_run_sync_in_checkpoint,
    )
    runner = KitaruRunner(
        SimpleNamespace(name="cache-collision-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__, "run_openai_agent_sync", fake_run_sync
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__,
        "run_openai_agent_streamed_sync",
        fake_streamed_sync,
    )

    request = OpenAIRunRequest.start("hello")
    non_stream_first = runner.run_sync(request)
    stream_after_non_stream = runner.run_stream_sync(request)
    stream_first_again = runner.run_stream_sync(request)
    non_stream_after_stream = runner.run_sync(request)

    assert non_stream_first.final_output == "non-stream durable result"
    assert non_stream_after_stream.final_output == "non-stream durable result"
    assert stream_after_non_stream.final_output == "stream durable result"
    assert stream_first_again.final_output == "stream durable result"
    assert cache_keys == [cache_keys[0], cache_keys[1], cache_keys[1], cache_keys[0]]
    assert cache_keys[0] != cache_keys[1]
    assert sync_calls == 1
    assert streamed_calls == 1
    assert len(cache) == 2


def test_run_stream_sync_checkpoint_cache_hit_returns_result_without_streaming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as agent_module

    cache: dict[str | None, Any] = {}
    published: list[str] = []
    streamed_calls = 0

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        _ = payload, flush
        published.append(kind)

    def fake_run_sync_in_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in cache:
            cache[cache_key] = kwargs["body"]()
        return cache[cache_key]

    def fake_streamed_sync(**kwargs: Any) -> SimpleNamespace:
        nonlocal streamed_calls
        streamed_calls += 1
        kwargs["on_event"](SimpleNamespace(type="agent_updated_stream_event"))
        return SimpleNamespace(final_output="cached durable result")

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setitem(
        KitaruRunner._run_runner_call_checkpoint_sync.__globals__,
        "run_sync_in_checkpoint",
        fake_run_sync_in_checkpoint,
    )
    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)
    run_config = RunConfig(tracing_disabled=True)
    runner = KitaruRunner(
        SimpleNamespace(name="cache-stream-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__,
        "run_openai_agent_streamed_sync",
        fake_streamed_sync,
    )

    first = runner.run_stream_sync(OpenAIRunRequest.start("hello"))
    first_event_count = len(published)
    second = runner.run_stream_sync(OpenAIRunRequest.start("hello"))

    assert first.final_output == "cached durable result"
    assert second.final_output == "cached durable result"
    assert streamed_calls == 1
    assert first_event_count == 3
    assert len(published) == first_event_count


def test_run_stream_sync_publishes_lifecycle_and_stream_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any], bool]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    def fake_streamed_sync(**kwargs: Any) -> SimpleNamespace:
        kwargs["on_event"](
            SimpleNamespace(
                type="raw_response_event",
                data=SimpleNamespace(type="response.output_text.delta", delta="Hi"),
            )
        )
        return SimpleNamespace(final_output="done")

    runner = KitaruRunner(
        SimpleNamespace(name="stream-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__,
        "run_openai_agent_streamed_sync",
        fake_streamed_sync,
    )
    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)

    result = runner.run_stream_sync(OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "done"
    assert [item[0] for item in published] == [
        OPENAI_STREAM_STARTED,
        OPENAI_STREAM_EVENT,
        OPENAI_STREAM_COMPLETED,
    ]
    assert published[-1][2] is True


@pytest.mark.anyio
async def test_run_stream_async_returns_completed_result_and_tracks_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracked: list[tuple[Any, dict[str, Any]]] = []

    async def fake_streamed(**kwargs: Any) -> SimpleNamespace:
        kwargs["on_event"](SimpleNamespace(type="agent_updated_stream_event"))
        return SimpleNamespace(final_output="async done")

    runner = KitaruRunner(
        SimpleNamespace(name="async-stream-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._track_completed.__globals__,
        "track",
        lambda event, data: tracked.append((event, data)),
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_async.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_async.__globals__,
        "run_openai_agent_streamed",
        fake_streamed,
    )
    tracked.clear()

    result = await runner.run_stream(OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "async done"
    assert tracked[-1][1]["surface"] == "run_stream"


def test_run_stream_sync_interrupted_result_preserves_pending_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 3}

    def fake_streamed_sync(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            interruptions=[
                {
                    "tool_name": "send_email",
                    "call_id": "call_approval",
                    "message": "approval required",
                }
            ],
            to_state=lambda: FakeState(),
            last_response_id="resp_interrupted",
        )

    runner = KitaruRunner(
        SimpleNamespace(name="interrupted-stream-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__,
        "run_openai_agent_streamed_sync",
        fake_streamed_sync,
    )

    result = runner.run_stream_sync(OpenAIRunRequest.start("needs approval"))

    assert result.status == "interrupted"
    assert result.pending_state is not None
    assert result.pending_state.state_json == {"current_turn": 3}
    assert result.interruptions[0].tool_name == "send_email"
    assert result.interruptions[0].call_id == "call_approval"
    assert result.last_response_id == "resp_interrupted"


def test_run_stream_sync_rejects_fresh_context_on_resume() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="resume-stream-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    interrupted = OpenAIRunStateEnvelope(
        agents_sdk_version="0.15.0",
        state_json={"current_turn": 1},
    )
    request = OpenAIRunRequest.resume(
        interrupted,
        OpenAIApprovalDecision(approve=True),
    )

    with pytest.raises(KitaruUsageError, match="Fresh `context=`"):
        runner.run_stream_sync(request, context=SimpleNamespace(team_id="team-a"))


def test_run_stream_rejects_calls_strategy() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="calls-agent"), checkpoint_strategy="calls"
    )

    with pytest.raises(KitaruUsageError, match="runner_call"):
        runner.run_stream_sync(OpenAIRunRequest.start("hello"))


def test_run_stream_sync_rejects_running_event_loop() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="loop-agent"), checkpoint_strategy="runner_call"
    )

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="run_stream"):
            runner.run_stream_sync(OpenAIRunRequest.start("hello"))

    import asyncio

    asyncio.run(call_sync())


def test_run_stream_sync_failure_publishes_failed_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any], bool]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    def broken_streamed_sync(**_kwargs: Any) -> SimpleNamespace:
        raise RuntimeError("sdk exploded")

    runner = KitaruRunner(
        SimpleNamespace(name="broken-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__, "tracker_scope", fake_tracker_scope
    )
    monkeypatch.setitem(
        runner._run_sdk_stream_sync.__globals__,
        "run_openai_agent_streamed_sync",
        broken_streamed_sync,
    )
    monkeypatch.setattr(openai_streaming.kitaru_events, "publish", fake_publish)

    with pytest.raises(RuntimeError, match="sdk exploded"):
        runner.run_stream_sync(OpenAIRunRequest.start("hello"))

    assert [item[0] for item in published] == [
        OPENAI_STREAM_STARTED,
        OPENAI_STREAM_FAILED,
    ]
    assert published[-1][2] is True
    assert published[-1][1]["error_type"] == "RuntimeError"
