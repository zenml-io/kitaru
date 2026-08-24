"""LangChain middleware replay contracts."""

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest
from langchain.agents.middleware.types import ModelRequest, ToolCallRequest
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command

from kitaru.api_models.v1.replay import ToolLookupMatch, ToolLookupResponse
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    PassthroughConfig,
    ReplayOverride,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru_langgraph import ToolPolicyError, ToolPolicyMissError
from kitaru_langgraph.capture import CapturePolicy
from kitaru_langgraph.codec import encode_tool_outcome
from kitaru_langgraph.langchain import KitaruLangGraphMiddleware
from kitaru_langgraph.recording import _ACTIVE_INVOCATION


class _SyncBridge:
    def run(self, coroutine: Any) -> Any:
        return asyncio.run(coroutine)


class _Recorder:
    def __init__(self, *, override: ReplayOverride | None, policy: Any = None) -> None:
        self.override = override
        self.policy = CapturePolicy()
        self.replay = (
            SimpleNamespace(id=uuid.uuid4(), tool_policy=policy)
            if policy is not None
            else None
        )
        self.recorded: list[dict[str, Any]] = []
        self.history_occurrences: dict[str, int] = {}
        self.sync_bridge = _SyncBridge()
        self.client = SimpleNamespace(
            replays=SimpleNamespace(tool_lookup=self._unexpected_lookup)
        )

    async def record_tool_substitution(self, **kwargs: Any) -> None:
        self.recorded.append(kwargs)

    async def _unexpected_lookup(self, *_: Any) -> Any:
        raise AssertionError("unexpected lookup")


def _request() -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={
            "name": "weather",
            "args": {"city": "Paris"},
            "id": "call-1",
            "type": "tool_call",
        },
        tool=None,
        state={},
        runtime=cast(Any, None),
    )


def _history_match(
    result: Any,
    *,
    status: NodeStatus = NodeStatus.COMPLETED,
    error: str | None = None,
) -> ToolLookupResponse:
    return ToolLookupResponse(
        match=ToolLookupMatch(result=result, status=status, error=error)
    )


async def _invoke_history_tool(
    recorder: _Recorder,
    live_calls: list[str],
    *,
    sync: bool,
) -> ToolMessage | Any:
    middleware = KitaruLangGraphMiddleware(requested_model=None)
    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        if sync:

            def handler(_: ToolCallRequest) -> ToolMessage:
                live_calls.append("live")
                return ToolMessage(content="live", tool_call_id="call-1")

            return await asyncio.to_thread(
                middleware.wrap_tool_call, _request(), handler
            )

        async def handler(_: ToolCallRequest) -> ToolMessage:
            live_calls.append("live")
            return ToolMessage(content="live", tool_call_id="call-1")

        return await middleware.awrap_tool_call(_request(), handler)
    finally:
        _ACTIVE_INVOCATION.reset(token)


def test_model_overrides_call_effective_model_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_model = FakeListChatModel(responses=["original"])
    replacement_model = FakeListChatModel(responses=["replacement"])
    recorder = _Recorder(
        override=ReplayOverride(
            model="replacement:model",
            prompt="new prompt",
            system_prompt="new system",
            model_params={"temperature": 0.2},
        )
    )
    middleware = KitaruLangGraphMiddleware(requested_model="original:model")
    monkeypatch.setattr(
        "kitaru_langgraph.langchain.init_chat_model", lambda _: replacement_model
    )
    observed: list[ModelRequest[Any]] = []

    def handler(request: ModelRequest[Any]) -> AIMessage:
        observed.append(request)
        return AIMessage(content="done")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        result = middleware.wrap_model_call(
            ModelRequest(
                model=original_model,
                messages=[HumanMessage(content="old prompt")],
                system_prompt="old system",
                model_settings={"temperature": 1.0},
            ),
            handler,
        )
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert result.content == "done"
    assert len(observed) == 1
    assert observed[0].model is replacement_model
    assert observed[0].messages[-1].content == "new prompt"
    assert observed[0].system_prompt == "new system"
    assert observed[0].model_settings == {"temperature": 0.2}


async def test_static_hit_skips_live_tool() -> None:
    policy = ToolPolicy(
        default=StaticConfig(
            cases=[
                StaticCase(
                    match={"city": "Paris"},
                    match_mode=StaticMatchMode.EXACT,
                    result={"weather": "sunny"},
                )
            ],
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    calls = 0

    async def handler(_: ToolCallRequest) -> ToolMessage:
        nonlocal calls
        calls += 1
        return ToolMessage(content="live", tool_call_id="call-1")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        result = await KitaruLangGraphMiddleware(requested_model=None).awrap_tool_call(
            _request(), handler
        )
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert calls == 0
    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == "call-1"
    assert result.artifact == {"weather": "sunny"}
    assert recorder.recorded[0]["policy_name"] == "static"


def test_sync_static_hit_skips_live_tool() -> None:
    policy = ToolPolicy(
        default=StaticConfig(
            cases=[
                StaticCase(
                    match={"city": "Paris"},
                    match_mode=StaticMatchMode.EXACT,
                    result={"weather": "sunny"},
                )
            ],
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    calls = 0

    def handler(_: ToolCallRequest) -> ToolMessage:
        nonlocal calls
        calls += 1
        return ToolMessage(content="live", tool_call_id="call-1")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        result = KitaruLangGraphMiddleware(requested_model=None).wrap_tool_call(
            _request(), handler
        )
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert calls == 0
    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == "call-1"
    assert result.artifact == {"weather": "sunny"}
    assert recorder.recorded[0]["policy_name"] == "static"


@pytest.mark.parametrize(
    ("on_miss", "expected_calls", "expected_error"),
    [
        (ToolPolicyOnMiss.PASSTHROUGH, 1, False),
        (ToolPolicyOnMiss.ERROR_RESULT, 0, False),
        (ToolPolicyOnMiss.FAIL, 0, True),
    ],
)
async def test_static_miss_policy_has_exact_live_call_count(
    on_miss: ToolPolicyOnMiss, expected_calls: int, expected_error: bool
) -> None:
    policy = ToolPolicy(
        default=StaticConfig(cases=[], on_miss=on_miss),
    )
    recorder = _Recorder(override=None, policy=policy)
    calls = 0

    async def handler(_: ToolCallRequest) -> ToolMessage:
        nonlocal calls
        calls += 1
        return ToolMessage(content="live", tool_call_id="call-1")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        if expected_error:
            with pytest.raises(ToolPolicyMissError):
                await KitaruLangGraphMiddleware(requested_model=None).awrap_tool_call(
                    _request(), handler
                )
        else:
            result = await KitaruLangGraphMiddleware(
                requested_model=None
            ).awrap_tool_call(_request(), handler)
            assert isinstance(result, ToolMessage)
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert calls == expected_calls


@pytest.mark.parametrize(
    ("on_miss", "expected_calls", "expected_error"),
    [
        (ToolPolicyOnMiss.PASSTHROUGH, 1, False),
        (ToolPolicyOnMiss.ERROR_RESULT, 0, False),
        (ToolPolicyOnMiss.FAIL, 0, True),
    ],
)
def test_sync_static_miss_policy_has_exact_live_call_count(
    on_miss: ToolPolicyOnMiss, expected_calls: int, expected_error: bool
) -> None:
    policy = ToolPolicy(
        default=StaticConfig(cases=[], on_miss=on_miss),
    )
    recorder = _Recorder(override=None, policy=policy)
    calls = 0

    def handler(_: ToolCallRequest) -> ToolMessage:
        nonlocal calls
        calls += 1
        return ToolMessage(content="live", tool_call_id="call-1")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        if expected_error:
            with pytest.raises(ToolPolicyMissError):
                KitaruLangGraphMiddleware(requested_model=None).wrap_tool_call(
                    _request(), handler
                )
        else:
            result = KitaruLangGraphMiddleware(requested_model=None).wrap_tool_call(
                _request(), handler
            )
            assert isinstance(result, ToolMessage)
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert calls == expected_calls


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize("outcome_kind", ["tool_message", "command"])
async def test_history_hit_decodes_current_tool_identity(
    sync: bool,
    outcome_kind: str,
) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    recorded_message = ToolMessage(content="history", tool_call_id="old", name="old")
    outcome: ToolMessage | Command[Any]
    if outcome_kind == "tool_message":
        outcome = recorded_message
    else:
        outcome = Command(update={"messages": [recorded_message], "value": None})
    envelope = encode_tool_outcome(outcome)

    async def lookup(*_: Any) -> Any:
        return _history_match(envelope)

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []
    result = await _invoke_history_tool(recorder, live_calls, sync=sync)

    assert live_calls == []
    if outcome_kind == "tool_message":
        assert isinstance(result, ToolMessage)
        message = result
    else:
        assert isinstance(result, Command)
        assert result.update["value"] is None
        message = result.update["messages"][0]
    assert message.content == "history"
    assert message.tool_call_id == "call-1"
    assert message.name == "weather"


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    ("recorded_error", "expected_error"),
    [
        ("recorded tool failure", "recorded tool failure"),
        (None, "Recorded tool call 'weather' failed"),
    ],
)
async def test_history_failed_match_raises_and_records_substitution(
    sync: bool,
    recorded_error: str | None,
    expected_error: str,
) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        )
    )
    recorder = _Recorder(override=None, policy=policy)

    async def lookup(*_: Any) -> Any:
        return _history_match(
            None,
            status=NodeStatus.FAILED,
            error=recorded_error,
        )

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []

    with pytest.raises(ToolPolicyError, match=expected_error):
        await _invoke_history_tool(recorder, live_calls, sync=sync)

    assert live_calls == []
    assert list(recorder.history_occurrences.values()) == [1]
    assert len(recorder.recorded) == 1
    assert recorder.recorded[0]["result"] is None
    assert recorder.recorded[0]["policy_name"] == "history"
    assert str(recorder.recorded[0]["error"]) == expected_error


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
async def test_history_unexpected_status_fails_closed(sync: bool) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        )
    )
    recorder = _Recorder(override=None, policy=policy)

    async def lookup(*_: Any) -> Any:
        return _history_match(None, status=NodeStatus.IN_PROGRESS)

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []

    with pytest.raises(ToolPolicyError, match="unexpected status 'in_progress'"):
        await _invoke_history_tool(recorder, live_calls, sync=sync)

    assert live_calls == []
    assert list(recorder.history_occurrences.values()) == [1]
    assert len(recorder.recorded) == 1
    assert recorder.recorded[0]["result"] is None
    assert recorder.recorded[0]["policy_name"] == "history"
    assert isinstance(recorder.recorded[0]["error"], ToolPolicyError)


async def test_history_hits_consume_baseline_occurrences_in_order() -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    requests: list[Any] = []
    envelopes = [
        encode_tool_outcome(ToolMessage(content=ticket, tool_call_id="old", name="old"))
        for ticket in ["a", "b", "c"]
    ]

    async def lookup(_: Any, request: Any) -> Any:
        requests.append(request)
        return _history_match(envelopes[len(requests) - 1])

    recorder.client.replays.tool_lookup = lookup
    middleware = KitaruLangGraphMiddleware(requested_model=None)
    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        results = [
            await middleware.awrap_tool_call(
                _request(), cast(Any, lambda _: pytest.fail("live tool called"))
            )
            for _ in range(3)
        ]
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert [result.content for result in results] == ["a", "b", "c"]
    assert [request.occurrence for request in requests] == [0, 1, 2]


async def test_history_miss_does_not_advance_occurrence() -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    requests: list[Any] = []
    envelope = encode_tool_outcome(
        ToolMessage(content="history", tool_call_id="old", name="old")
    )

    async def lookup(_: Any, request: Any) -> Any:
        requests.append(request)
        if len(requests) == 1:
            return ToolLookupResponse(match=None)
        return _history_match(envelope)

    async def handler(_: ToolCallRequest) -> ToolMessage:
        return ToolMessage(content="live", tool_call_id="call-1")

    recorder.client.replays.tool_lookup = lookup
    middleware = KitaruLangGraphMiddleware(requested_model=None)
    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        first = await middleware.awrap_tool_call(_request(), handler)
        second = await middleware.awrap_tool_call(_request(), handler)
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert isinstance(first, ToolMessage)
    assert first.content == "live"
    assert isinstance(second, ToolMessage)
    assert second.content == "history"
    assert [request.occurrence for request in requests] == [0, 0]


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize(
    "on_miss",
    [
        ToolPolicyOnMiss.PASSTHROUGH,
        ToolPolicyOnMiss.ERROR_RESULT,
        ToolPolicyOnMiss.FAIL,
    ],
)
async def test_history_genuine_miss_uses_policy_without_advancing_occurrence(
    sync: bool,
    on_miss: ToolPolicyOnMiss,
) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(scope=HistoryScope.BASELINE, on_miss=on_miss)
    )
    recorder = _Recorder(override=None, policy=policy)

    async def lookup(*_: Any) -> Any:
        return ToolLookupResponse(match=None)

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []

    if on_miss is ToolPolicyOnMiss.FAIL:
        with pytest.raises(ToolPolicyMissError):
            await _invoke_history_tool(recorder, live_calls, sync=sync)
    else:
        result = await _invoke_history_tool(recorder, live_calls, sync=sync)
        assert isinstance(result, ToolMessage)
        if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            assert result.status == "error"

    assert recorder.history_occurrences == {}
    assert live_calls == (["live"] if on_miss is ToolPolicyOnMiss.PASSTHROUGH else [])


async def test_history_non_baseline_scope_sends_no_occurrence() -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.AGENT,
            on_miss=ToolPolicyOnMiss.FAIL,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    requests: list[Any] = []
    envelope = encode_tool_outcome(
        ToolMessage(content="history", tool_call_id="old", name="old")
    )

    async def lookup(_: Any, request: Any) -> Any:
        requests.append(request)
        return _history_match(envelope)

    recorder.client.replays.tool_lookup = lookup
    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        await KitaruLangGraphMiddleware(requested_model=None).awrap_tool_call(
            _request(), cast(Any, lambda _: pytest.fail("live tool called"))
        )
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert len(requests) == 1
    assert requests[0].occurrence is None
    assert recorder.history_occurrences == {}


@pytest.mark.parametrize("sync", [False, True], ids=["async", "sync"])
@pytest.mark.parametrize("invalid_result", ["null", "malformed"])
async def test_invalid_completed_history_match_fails_closed_under_passthrough(
    sync: bool,
    invalid_result: str,
) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    result: Any = None
    if invalid_result == "malformed":
        result = encode_tool_outcome(
            ToolMessage(content="history", tool_call_id="old", name="old")
        )
        result["payload"]["additional_kwargs"] = 1

    async def lookup(*_: Any) -> Any:
        return _history_match(result)

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []

    with pytest.raises(ToolPolicyError):
        await _invoke_history_tool(recorder, live_calls, sync=sync)

    assert live_calls == []
    assert list(recorder.history_occurrences.values()) == [1]
    assert len(recorder.recorded) == 1
    assert recorder.recorded[0]["result"] is None
    assert recorder.recorded[0]["policy_name"] == "history"
    assert isinstance(recorder.recorded[0]["error"], ToolPolicyError)


@pytest.mark.parametrize(
    ("artifact", "capture_policy"),
    [
        pytest.param(
            {"api_key": "secret"},
            CapturePolicy(),
            id="redacted",
        ),
        pytest.param(
            {"value": "x" * 100},
            CapturePolicy(max_field_bytes=64),
            id="truncated",
        ),
    ],
)
async def test_lossy_history_result_fails_closed(
    artifact: dict[str, Any], capture_policy: CapturePolicy
) -> None:
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE,
            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
        )
    )
    recorder = _Recorder(override=None, policy=policy)
    envelope = encode_tool_outcome(
        ToolMessage(
            content="history",
            artifact=artifact,
            tool_call_id="old",
            name="old",
        ),
        policy=capture_policy,
    )

    async def lookup(*_: Any) -> Any:
        return _history_match(envelope)

    recorder.client.replays.tool_lookup = lookup
    live_calls: list[str] = []

    assert envelope["replayable"] is False
    with pytest.raises(ToolPolicyError, match="not replayable"):
        await _invoke_history_tool(recorder, live_calls, sync=False)
    assert live_calls == []


async def test_passthrough_policy_calls_live_tool_once() -> None:
    policy = ToolPolicy(default=PassthroughConfig())
    recorder = _Recorder(override=None, policy=policy)
    calls = 0

    async def handler(_: ToolCallRequest) -> ToolMessage:
        nonlocal calls
        calls += 1
        return ToolMessage(content="live", tool_call_id="call-1")

    token = _ACTIVE_INVOCATION.set(cast(Any, recorder))
    try:
        await KitaruLangGraphMiddleware(requested_model=None).awrap_tool_call(
            _request(), handler
        )
    finally:
        _ACTIVE_INVOCATION.reset(token)

    assert calls == 1
