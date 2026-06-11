"""Focused tests for Claude Agent SDK live streaming support."""

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import sys
import types
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from kitaru.errors import KitaruRuntimeError, KitaruUsageError


def _purge_claude_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.claude_agent_sdk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    sdk = types.ModuleType("claude_agent_sdk")
    calls: list[dict[str, object]] = []
    messages: list[object] = []
    sdk.__dict__["calls"] = calls
    sdk.__dict__["messages"] = messages

    class StreamEvent:
        def __init__(self, event: dict[str, object]) -> None:
            self.event = event

    class AssistantMessage:
        def __init__(
            self,
            content: list[object] | None = None,
            *,
            model: str = "claude-sonnet",
            stop_reason: str = "end_turn",
            usage: object | None = None,
        ) -> None:
            self.content = content or []
            self.model = model
            self.stop_reason = stop_reason
            self.usage = usage

    class UserMessage:
        def __init__(
            self,
            content: str | list[object] = "user text",
            *,
            tool_use_result: object | None = None,
        ) -> None:
            self.content = content
            self.tool_use_result = tool_use_result

    class SystemMessage:
        def __init__(self, *, subtype: str = "init") -> None:
            self.subtype = subtype

    class RateLimitInfo:
        def __init__(self) -> None:
            self.status = "ok"
            self.rate_limit_type = "tokens"
            self.utilization = 0.25

    class RateLimitEvent:
        def __init__(self) -> None:
            self.rate_limit_info = RateLimitInfo()

    class ClaudeAgentOptions:
        def __init__(
            self,
            *,
            cwd: str | None = None,
            resume: str | None = None,
            max_turns: int | None = None,
            include_partial_messages: bool = False,
        ) -> None:
            self.cwd = cwd
            self.resume = resume
            self.max_turns = max_turns
            self.include_partial_messages = include_partial_messages

    class ResultMessage:
        def __init__(
            self,
            *,
            session_id: str = "session-123",
            result: str = "final secret result text",
            structured_output: object | None = None,
            is_error: bool = False,
        ) -> None:
            self.session_id = session_id
            self.result = result
            self.structured_output = structured_output
            self.is_error = is_error
            self.usage = {"input_tokens": 3, "output_tokens": 5}
            self.total_cost_usd = 0.04
            self.model_usage = {"claude-sonnet": {"input_tokens": 3}}
            self.stop_reason = "end_turn"
            self.subtype = "success"
            self.num_turns = 1
            self.duration_ms = 12.5
            self.duration_api_ms = 10.0

    async def query(*, prompt: str, options: object = None):
        calls.append({"prompt": prompt, "options": options})
        for message in messages:
            yield message

    sdk.__dict__["StreamEvent"] = StreamEvent
    sdk.__dict__["AssistantMessage"] = AssistantMessage
    sdk.__dict__["UserMessage"] = UserMessage
    sdk.__dict__["SystemMessage"] = SystemMessage
    sdk.__dict__["RateLimitEvent"] = RateLimitEvent
    sdk.__dict__["ClaudeAgentOptions"] = ClaudeAgentOptions
    sdk.__dict__["ResultMessage"] = ResultMessage
    sdk.__dict__["query"] = query
    messages[:] = [ResultMessage()]
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", sdk)
    return sdk


@pytest.fixture
def claude_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_sdk: types.ModuleType,
) -> types.ModuleType:
    _purge_claude_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.claude_agent_sdk")


@contextmanager
def _patched_scope(
    monkeypatch: pytest.MonkeyPatch,
    *,
    inside_flow: bool,
    inside_checkpoint: bool,
) -> Iterator[None]:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: inside_flow)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(tracking_module, "is_inside_flow", lambda: inside_flow)
    monkeypatch.setattr(
        tracking_module, "is_inside_checkpoint", lambda: inside_checkpoint
    )
    yield


def _patch_direct_execution_persistence(
    monkeypatch: pytest.MonkeyPatch,
    *,
    save_artifact: Callable[..., object] | None = None,
    save_event: Callable[..., object] | None = None,
    log_event: Callable[..., object] | None = None,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(
        agent_module.KitaruClaudeRunner,
        "_save_artifact",
        staticmethod(save_artifact or (lambda name, value, *, type: None)),
    )
    monkeypatch.setattr(
        tracking_module.kitaru,
        "save",
        save_event or (lambda name, value, *, type: None),
    )
    monkeypatch.setattr(
        tracking_module.kitaru,
        "log",
        log_event or (lambda **kwargs: None),
    )


def _capture_published(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str, dict[str, Any], bool]]:
    streaming_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._streaming"
    )
    published: list[tuple[str, dict[str, Any], bool]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    monkeypatch.setattr(streaming_module.kitaru_events, "publish", fake_publish)
    return published


def _run_stream_direct(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    **runner_kwargs: object,
) -> Any:
    with _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=True):
        _patch_direct_execution_persistence(monkeypatch)
        runner = claude_adapter.KitaruClaudeRunner(
            allow_direct_execution_inside_checkpoint=True,
            name="claude",
            **runner_kwargs,
        )
        return runner.run_stream_sync(claude_adapter.ClaudeRunRequest.start("hello"))


def test_public_stream_methods_exist(claude_adapter: types.ModuleType) -> None:
    assert callable(claude_adapter.KitaruClaudeRunner.run_stream)
    assert callable(claude_adapter.KitaruClaudeRunner.run_stream_sync)


def test_run_stream_sync_rejects_running_event_loop(
    claude_adapter: types.ModuleType,
) -> None:
    runner = claude_adapter.KitaruClaudeRunner(name="claude")
    request = claude_adapter.ClaudeRunRequest.start("hello")

    async def call_sync() -> None:
        with pytest.raises(
            KitaruUsageError, match=r"await KitaruClaudeRunner\.run_stream"
        ):
            runner.run_stream_sync(request)

    asyncio.run(call_sync())


def test_stream_scope_guards_match_invocation_adapter(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = claude_adapter.KitaruClaudeRunner(name="claude")
    request = claude_adapter.ClaudeRunRequest.start("hello")

    with (
        _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=False),
        pytest.raises(KitaruUsageError, match="inside a Kitaru flow body"),
    ):
        runner.run_stream_sync(request)

    with (
        _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=True),
        pytest.raises(KitaruUsageError, match="existing Kitaru checkpoint"),
    ):
        runner.run_stream_sync(request)


def test_direct_stream_execution_preserves_warning(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run_stream_direct(claude_adapter, monkeypatch)

    assert result.final_text == "final secret result text"
    assert result.metadata["direct_execution_inside_checkpoint"] is True
    assert any(
        "ran directly inside an existing Kitaru checkpoint" in warning
        for warning in result.warnings
    )


def test_stream_uses_synthetic_invocation_checkpoint_from_flow_scope(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    calls: list[dict[str, object]] = []

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        calls.append(dict(kwargs))
        with _patched_scope(monkeypatch, inside_flow=True, inside_checkpoint=True):
            return cast(Callable[[], object], kwargs["body"])()

    with _patched_scope(monkeypatch, inside_flow=True, inside_checkpoint=False):
        _patch_direct_execution_persistence(monkeypatch)
        monkeypatch.setattr(
            agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
        )
        runner = claude_adapter.KitaruClaudeRunner(
            name="claude", checkpoint_config={"cache": False, "retries": 1}
        )
        result = runner.run_stream_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.final_text == "final secret result text"
    assert calls[0]["step_name"] == "claude_claude_invocation"
    assert calls[0]["config"] == {"cache": False, "retries": 1, "type": "agent_call"}
    assert isinstance(calls[0]["cache_key"], str)


def test_stream_cache_key_differs_from_non_stream_cache_key(
    claude_adapter: types.ModuleType,
) -> None:
    runner = claude_adapter.KitaruClaudeRunner(name="claude")
    request = claude_adapter.ClaudeRunRequest.start("hello")
    options = {"allowed_tools": ["Read"]}

    run_key = runner._invocation_cache_key(request, options=options)
    stream_key = runner._invocation_cache_key(
        request, options={"allowed_tools": ["Read"]}, surface="stream"
    )

    assert stream_key != run_key


def test_stream_cache_key_varies_by_text_delta_policy(
    claude_adapter: types.ModuleType,
) -> None:
    request = claude_adapter.ClaudeRunRequest.start("hello")
    default_runner = claude_adapter.KitaruClaudeRunner(name="claude")
    text_runner = claude_adapter.KitaruClaudeRunner(
        name="claude",
        capture=claude_adapter.ClaudeCapturePolicy(include_stream_text_deltas=True),
    )

    default_key = default_runner._invocation_cache_key(
        request, options=None, surface="stream"
    )
    text_key = text_runner._invocation_cache_key(
        request, options=None, surface="stream"
    )

    assert default_key != text_key


def test_successful_fake_sdk_stream_event_order_and_safe_payloads(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    secret = "SECRET_TOOL_INPUT_SHOULD_NOT_LEAK"
    long_text = "hello " + ("x" * 400)
    fake_sdk.__dict__["messages"][:] = [
        fake_sdk.StreamEvent(
            {
                "type": "content_block_start",
                "index": 1,
                "content_block": {
                    "type": "tool_use",
                    "name": "lookup_order",
                    "id": "toolu_1",
                    "input": {"api_key": secret},
                },
            }
        ),
        fake_sdk.StreamEvent(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": long_text},
            }
        ),
        fake_sdk.StreamEvent(
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "input_json_delta", "partial_json": secret},
            }
        ),
        fake_sdk.AssistantMessage(
            [{"type": "text", "text": "FULL ASSISTANT CONTENT SECRET"}],
            usage={"input_tokens": 1},
        ),
        fake_sdk.ResultMessage(
            result="FINAL RESULT SHOULD NOT BE IN LIVE PAYLOAD",
            structured_output={"secret": "STRUCTURED_OUTPUT_SECRET"},
        ),
    ]

    result = _run_stream_direct(claude_adapter, monkeypatch)

    assert result.final_text == "FINAL RESULT SHOULD NOT BE IN LIVE PAYLOAD"
    assert [kind for kind, _, _ in published] == [
        claude_adapter.CLAUDE_STREAM_STARTED,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_COMPLETED,
    ]
    assert published[-1][2] is True

    payloads = [payload for _, payload, _ in published]
    text_payload = next(
        payload for payload in payloads if payload["category"] == "text_delta"
    )
    assert "text_delta" not in text_payload
    assert text_payload["display"] == "Claude text delta"
    assert long_text not in repr(text_payload)

    tool_payload = next(
        payload for payload in payloads if payload["category"] == "tool_input_delta"
    )
    assert tool_payload["tool_name"] == "lookup_order"
    assert "partial_json" not in tool_payload

    assistant_payload = next(
        payload for payload in payloads if payload["category"] == "assistant_message"
    )
    assert assistant_payload["content_block_count"] == 1
    assert "content" not in assistant_payload
    assert "FULL ASSISTANT CONTENT SECRET" not in repr(assistant_payload)

    result_payload = next(
        payload for payload in payloads if payload["category"] == "result_message"
    )
    assert result_payload["has_result"] is True
    assert result_payload["has_structured_output"] is True
    assert "result" not in result_payload
    assert "structured_output" not in result_payload

    all_payloads = repr(payloads)
    assert secret not in all_payloads
    assert "FINAL RESULT SHOULD NOT BE IN LIVE PAYLOAD" not in all_payloads
    assert "STRUCTURED_OUTPUT_SECRET" not in all_payloads


def test_stream_text_delta_payload_requires_explicit_opt_in(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    fake_sdk.__dict__["messages"][:] = [
        fake_sdk.StreamEvent(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "visible when opted in"},
            }
        ),
        fake_sdk.ResultMessage(result="done"),
    ]

    _run_stream_direct(
        claude_adapter,
        monkeypatch,
        capture=claude_adapter.ClaudeCapturePolicy(include_stream_text_deltas=True),
    )

    text_payload = next(
        payload for _, payload, _ in published if payload["category"] == "text_delta"
    )
    assert text_payload["text_delta"] == "visible when opted in"
    assert text_payload["display"] == "visible when opted in"


def test_tool_input_delta_does_not_reuse_tool_state_after_boundaries(
    fake_sdk: types.ModuleType,
) -> None:
    _ = fake_sdk
    streaming_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._streaming"
    )

    class StreamEvent:
        def __init__(self, event: dict[str, object]) -> None:
            self.event = event

    publisher = streaming_module.ClaudeStreamPublisher(runner_name="claude")
    first_delta = publisher.normalize_message(
        StreamEvent(
            {
                "type": "content_block_start",
                "index": 2,
                "content_block": {
                    "type": "tool_use",
                    "name": "lookup_order",
                    "id": "toolu_1",
                },
            }
        )
    )
    assert first_delta["tool_name"] == "lookup_order"
    assert first_delta["tool_id"] == "toolu_1"

    publisher.normalize_message(StreamEvent({"type": "content_block_stop", "index": 2}))
    after_block_stop = publisher.normalize_message(
        StreamEvent(
            {
                "type": "content_block_delta",
                "index": 2,
                "delta": {"type": "input_json_delta", "partial_json": "{}"},
            }
        )
    )
    assert after_block_stop["category"] == "tool_input_delta"
    assert "tool_name" not in after_block_stop
    assert "tool_id" not in after_block_stop

    publisher.normalize_message(
        StreamEvent(
            {
                "type": "content_block_start",
                "index": 3,
                "content_block": {
                    "type": "tool_use",
                    "name": "search_docs",
                    "id": "toolu_2",
                },
            }
        )
    )
    publisher.normalize_message(StreamEvent({"type": "message_stop"}))
    after_message_stop = publisher.normalize_message(
        StreamEvent(
            {
                "type": "content_block_delta",
                "index": 3,
                "delta": {"type": "input_json_delta", "partial_json": "{}"},
            }
        )
    )
    assert after_message_stop["category"] == "tool_input_delta"
    assert "tool_name" not in after_message_stop
    assert "tool_id" not in after_message_stop


def test_publisher_failure_does_not_fail_successful_run(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streaming_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._streaming"
    )
    calls = 0

    def broken_publish(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise KitaruUsageError("outside checkpoint")

    monkeypatch.setattr(streaming_module.kitaru_events, "publish", broken_publish)
    fake_sdk.__dict__["messages"][:] = [
        fake_sdk.StreamEvent(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "hi"},
            }
        ),
        fake_sdk.ResultMessage(result="done despite publisher"),
    ]

    result = _run_stream_direct(claude_adapter, monkeypatch)

    assert result.final_text == "done despite publisher"
    assert calls == 4


@pytest.mark.parametrize(
    ("messages", "match"),
    [
        (
            lambda sdk: [sdk.AssistantMessage([{"type": "text", "text": "not final"}])],
            "did not return a final ResultMessage",
        ),
        (
            lambda sdk: [sdk.ResultMessage(is_error=True, result="permission denied")],
            "error ResultMessage",
        ),
    ],
)
def test_missing_or_error_result_message_attempts_failed_event_and_reraises(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    messages: Callable[[Any], list[object]],
    match: str,
) -> None:
    published = _capture_published(monkeypatch)
    fake_sdk.__dict__["messages"][:] = messages(fake_sdk)

    with pytest.raises(RuntimeError, match=match):
        _run_stream_direct(claude_adapter, monkeypatch)

    assert [item[0] for item in published] == [
        claude_adapter.CLAUDE_STREAM_STARTED,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_FAILED,
    ]
    assert published[-1][2] is True


def test_error_result_message_does_not_leak_result_to_live_payloads(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)
    secret = "SECRET_RESULT_TEXT_SHOULD_NOT_LEAK"
    fake_sdk.__dict__["messages"][:] = [
        fake_sdk.ResultMessage(is_error=True, result=secret)
    ]

    with pytest.raises(RuntimeError) as exc_info:
        _run_stream_direct(claude_adapter, monkeypatch)

    # Non-stream/durable callers still see the detailed raised error.
    assert secret in str(exc_info.value)

    payloads = [payload for _, payload, _ in published]
    assert [kind for kind, _, _ in published] == [
        claude_adapter.CLAUDE_STREAM_STARTED,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_FAILED,
    ]
    assert secret not in repr(payloads)
    failed_payload = payloads[-1]
    assert failed_payload["category"] == "lifecycle"
    assert failed_payload["error_type"] == "ClaudeResultMessageError"
    assert failed_payload["message"] == (
        "Claude Agent SDK returned an error ResultMessage; subtype='success'"
    )
    assert failed_payload["display"] == (
        "Claude Agent SDK stream failed: Claude Agent SDK returned an error "
        "ResultMessage; subtype='success'"
    )


def test_failed_live_event_uses_safe_live_message_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streaming_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._streaming"
    )
    published: list[tuple[str, dict[str, Any], bool]] = []

    class SafeError(RuntimeError):
        safe_live_message = "Safe retryable provider failure"

        def __str__(self) -> str:
            return "SECRET provider payload"

    monkeypatch.setattr(
        streaming_module.kitaru_events,
        "publish",
        lambda kind, payload, *, flush=False: published.append((kind, payload, flush)),
    )
    publisher = streaming_module.ClaudeStreamPublisher(runner_name="claude")

    publisher.failed(SafeError())

    payload = published[0][1]
    assert payload["message"] == "Safe retryable provider failure"
    assert "SECRET provider payload" not in repr(payload)


def test_normalization_failure_does_not_stop_sdk_draining(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)

    class StreamEvent:
        @property
        def event(self) -> dict[str, object]:
            raise RuntimeError("normalization exploded")

    fake_sdk.__dict__["messages"][:] = [
        StreamEvent(),
        fake_sdk.ResultMessage(result="drained after broken event"),
    ]

    result = _run_stream_direct(claude_adapter, monkeypatch)

    assert result.final_text == "drained after broken event"
    fallback_payload = published[1][1]
    assert fallback_payload["category"] == "stream_event_normalization_failed"
    assert published[-1][0] == claude_adapter.CLAUDE_STREAM_COMPLETED


def test_durable_artifact_failure_is_not_masked_by_live_publishing(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)

    def fail_save_artifact(name: str, value: object, *, type: str) -> None:
        _ = name, value, type
        raise RuntimeError("durable messages save failed")

    with _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=True):
        _patch_direct_execution_persistence(
            monkeypatch, save_artifact=fail_save_artifact
        )
        runner = claude_adapter.KitaruClaudeRunner(
            allow_direct_execution_inside_checkpoint=True,
            name="claude",
            capture=claude_adapter.ClaudeCapturePolicy(
                fail_on_artifact_capture_error=True
            ),
        )
        with pytest.raises(KitaruRuntimeError, match="durable messages save failed"):
            runner.run_stream_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert [item[0] for item in published] == [
        claude_adapter.CLAUDE_STREAM_STARTED,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_FAILED,
    ]


def test_caller_owned_options_are_not_mutated(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclasses.dataclass
    class StaticOptions:
        cwd: str | None = None
        include_partial_messages: bool = False

    original_options = StaticOptions(cwd="/tmp/repo")

    result = _run_stream_direct(
        claude_adapter,
        monkeypatch,
        options=original_options,
    )

    assert result.final_text == "final secret result text"
    assert original_options.include_partial_messages is False
    sdk_options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0][
        "options"
    ]
    assert sdk_options is not original_options
    assert cast(Any, sdk_options).include_partial_messages is True


def test_stream_options_copy_preserves_mcp_servers(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclasses.dataclass
    class StaticOptions:
        mcp_servers: dict[str, object]
        include_partial_messages: bool = False

    server = {"type": "sdk", "name": "kitaru", "instance": object()}
    original_options = StaticOptions(mcp_servers={"kitaru": server})

    result = _run_stream_direct(
        claude_adapter,
        monkeypatch,
        options=original_options,
    )

    assert result.final_text == "final secret result text"
    assert original_options.include_partial_messages is False
    assert original_options.mcp_servers == {"kitaru": server}
    sdk_options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0][
        "options"
    ]
    assert sdk_options is not original_options
    assert cast(Any, sdk_options).include_partial_messages is True
    assert cast(Any, sdk_options).mcp_servers == {"kitaru": server}
    assert cast(Any, sdk_options).mcp_servers is original_options.mcp_servers


def test_plain_copyable_options_are_copied_for_partial_messages(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PlainOptions:
        def __init__(self, *, cwd: str | None = None) -> None:
            self.cwd = cwd
            self.include_partial_messages = False

    original_options = PlainOptions(cwd="/tmp/repo")

    result = _run_stream_direct(
        claude_adapter,
        monkeypatch,
        options=original_options,
    )

    assert result.final_text == "final secret result text"
    assert original_options.include_partial_messages is False
    sdk_options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0][
        "options"
    ]
    assert sdk_options is not original_options
    assert cast(Any, sdk_options).include_partial_messages is True


def test_failed_dataclass_replace_falls_back_to_shallow_copy(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclasses.dataclass
    class StaticOptionsWithoutPartialFlag:
        cwd: str | None = None

    original_options = StaticOptionsWithoutPartialFlag(cwd="/tmp/repo")

    result = _run_stream_direct(
        claude_adapter,
        monkeypatch,
        options=original_options,
    )

    assert result.final_text == "final secret result text"
    assert not hasattr(original_options, "include_partial_messages")
    assert not any(
        "partial messages could not be enabled" in warning
        for warning in result.warnings
    )
    sdk_options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0][
        "options"
    ]
    assert sdk_options is not original_options
    assert cast(Any, sdk_options).include_partial_messages is True


def test_uncopyable_options_degrade_to_coarse_streaming_with_warning(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UncopyableOptions:
        __slots__ = ()

        include_partial_messages = False

    options = UncopyableOptions()

    result = _run_stream_direct(claude_adapter, monkeypatch, options=options)

    sdk_options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0][
        "options"
    ]
    assert sdk_options is options
    assert options.include_partial_messages is False
    assert any(
        "partial messages could not be enabled" in warning
        for warning in result.warnings
    )


def test_raw_stream_event_is_not_stored_durably_by_default(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saved_payloads: list[object] = []
    fake_sdk.__dict__["messages"][:] = [
        fake_sdk.StreamEvent(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "live only"},
            }
        ),
        fake_sdk.AssistantMessage([{"type": "text", "text": "durable coarse"}]),
        fake_sdk.ResultMessage(result="done"),
    ]
    with _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=True):
        _patch_direct_execution_persistence(
            monkeypatch,
            save_artifact=lambda name, value, *, type: saved_payloads.append(value),
        )
        runner = claude_adapter.KitaruClaudeRunner(
            allow_direct_execution_inside_checkpoint=True,
            name="claude",
        )
        runner.run_stream_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    message_payload = cast(
        dict[str, object],
        next(
            payload
            for payload in saved_payloads
            if isinstance(payload, dict) and "messages" in payload
        ),
    )
    messages_value = message_payload["messages"]
    assert isinstance(messages_value, list)
    messages = cast(list[dict[str, object]], messages_value)
    assert len(messages) == 2
    assert all("event" not in message for message in messages)
    assert "live only" not in repr(messages)


def test_terminal_flush_failure_is_non_fatal(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streaming_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._streaming"
    )
    calls: list[tuple[str, bool]] = []

    def fail_on_flush(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        _ = payload
        calls.append((kind, flush))
        if flush:
            raise RuntimeError("flush failed")

    monkeypatch.setattr(streaming_module.kitaru_events, "publish", fail_on_flush)

    result = _run_stream_direct(claude_adapter, monkeypatch)

    assert result.final_text == "final secret result text"
    assert calls[-1] == (claude_adapter.CLAUDE_STREAM_COMPLETED, True)


def test_non_stream_run_sync_is_quiet(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = _capture_published(monkeypatch)

    with _patched_scope(monkeypatch, inside_flow=False, inside_checkpoint=True):
        _patch_direct_execution_persistence(monkeypatch)
        runner = claude_adapter.KitaruClaudeRunner(
            allow_direct_execution_inside_checkpoint=True,
            name="claude",
        )
        result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.final_text == "final secret result text"
    assert published == []
