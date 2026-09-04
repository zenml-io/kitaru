#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Provider-free replay tests for wrapped Claude SDK MCP tools."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    SdkMcpTool,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from mcp import types as mcp_types

import kitaru_claude_agent_sdk.runner as runner_module
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
    ToolLookupMatch,
    ToolLookupResponse,
)
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    LLMConfig,
    PassthroughConfig,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru.cache_keys import compute_tool_cache_key
from kitaru_claude_agent_sdk import (
    KitaruClaudeRunner,
    ToolPolicyError,
    ToolPolicyMissError,
    UnsupportedReplayError,
    replayable_sdk_mcp_server,
)
from kitaru_claude_agent_sdk.codec import (
    TOOL_RESULT_SCHEMA,
    decode_tool_result,
    encode_tool_result,
)

from .conftest import FakeClient, nodes


def _terminal() -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="session-1",
        total_cost_usd=0,
        usage={},
        result="done",
        uuid="result-1",
    )


def _replay(policy: ToolPolicy) -> ReplayResponse:
    now = datetime.now(UTC)
    return ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=None,
        tool_policy=policy,
        evaluators=[],
        evaluate_baselines=False,
        baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )


def _policy(tool: str, config: Any) -> ToolPolicy:
    return ToolPolicy(
        default=StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL),
        tools={tool: config},
    )


def _server(name: str, handler: Any) -> Any:
    return replayable_sdk_mcp_server(
        name=name,
        version="1.0.0",
        tools=[
            SdkMcpTool(
                name="lookup",
                description="Look up an answer.",
                input_schema={"query": str},
                handler=handler,
            )
        ],
    )


async def _invoke_replay(
    monkeypatch: pytest.MonkeyPatch,
    *,
    replay: ReplayResponse,
    server: Any,
    arguments: dict[str, Any],
    client: FakeClient | None = None,
    options: ClaudeAgentOptions | None = None,
) -> tuple[dict[str, Any], FakeClient, ClaudeAgentOptions]:
    active_client = client or FakeClient()
    active_client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: active_client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    captured_tools: list[SdkMcpTool[Any]] = []

    def fake_server_factory(**kwargs: Any) -> dict[str, Any]:
        captured_tools.extend(kwargs["tools"])
        return {"type": "sdk", "name": kwargs["name"], "instance": object()}

    async def fake_query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", fake_server_factory)
    monkeypatch.setattr(runner_module, "sdk_query", fake_query)
    supplied_options = options or ClaudeAgentOptions(tools=[])
    await anext(
        KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=supplied_options,
            replayable_servers=[server],
        )
    )
    return await captured_tools[0].handler(arguments), active_client, supplied_options


def test_codec_round_trips_ordered_text_content_and_error_flag() -> None:
    result = {
        "content": [
            {"type": "text", "text": "first"},
            {"type": "text", "text": "second"},
        ],
        "is_error": True,
    }

    encoded = encode_tool_result(result)

    assert encoded["schema"] == TOOL_RESULT_SCHEMA
    assert decode_tool_result(encoded) == result


@pytest.mark.parametrize(
    "stored",
    [
        {},
        {"schema": "future", "replayable": True, "payload": {}},
        {
            "schema": TOOL_RESULT_SCHEMA,
            "replayable": True,
            "payload": {"content": [{"type": "image", "data": "x"}]},
        },
        encode_tool_result({"content": [{"type": "text", "text": "x" * 70_000}]}),
    ],
)
def test_codec_fails_closed_for_malformed_or_non_replayable_values(stored: Any) -> None:
    with pytest.raises(ToolPolicyError):
        decode_tool_result(stored)


async def test_static_exact_and_subset_replay_never_call_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    name = "mcp__support__lookup"
    for mode, match in (
        (StaticMatchMode.EXACT, {"query": "refund", "region": "eu"}),
        (StaticMatchMode.SUBSET, {"query": "refund"}),
    ):
        configured = {
            "content": [{"type": "text", "text": mode.value}],
            "is_error": False,
        }
        result, _, _ = await _invoke_replay(
            monkeypatch,
            replay=_replay(
                _policy(
                    name,
                    StaticConfig(
                        cases=[
                            StaticCase(
                                match=match,
                                match_mode=mode,
                                result=configured,
                            )
                        ],
                        on_miss=ToolPolicyOnMiss.FAIL,
                    ),
                )
            ),
            server=_server("support", original),
            arguments={"query": "refund", "region": "eu"},
        )
        assert result == configured
    assert calls == 0


async def test_unconfigured_wrapped_tool_uses_fail_closed_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    replay = _replay(
        ToolPolicy(
            default=StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL),
            tools={},
        )
    )
    with pytest.raises(ToolPolicyMissError):
        await _invoke_replay(
            monkeypatch,
            replay=replay,
            server=_server("support", original),
            arguments={"query": "new"},
        )
    assert calls == 0


@pytest.mark.parametrize(
    ("on_miss", "raises", "live_calls", "is_error"),
    [
        (ToolPolicyOnMiss.FAIL, ToolPolicyMissError, 0, None),
        (ToolPolicyOnMiss.ERROR_RESULT, None, 0, True),
        (ToolPolicyOnMiss.PASSTHROUGH, None, 1, None),
    ],
)
async def test_static_miss_behavior(
    monkeypatch: pytest.MonkeyPatch,
    on_miss: ToolPolicyOnMiss,
    raises: type[BaseException] | None,
    live_calls: int,
    is_error: bool | None,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            StaticConfig(cases=[], on_miss=on_miss),
        )
    )
    invocation = _invoke_replay(
        monkeypatch,
        replay=replay,
        server=_server("support", original),
        arguments={"query": "new"},
    )
    if raises is not None:
        with pytest.raises(raises):
            await invocation
    else:
        result, _, _ = await invocation
        if is_error is not None:
            assert result["is_error"] is is_error
    assert calls == live_calls


async def test_history_uses_qualified_identity_normalized_arguments_and_codec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    client = FakeClient()
    recorded = {"content": [{"type": "text", "text": "recorded"}], "is_error": True}
    client.tool_lookup_responses.append(
        ToolLookupResponse(
            match=ToolLookupMatch(
                result=encode_tool_result(recorded),
                status=NodeStatus.COMPLETED,
                error=None,
            )
        )
    )
    result, _, _ = await _invoke_replay(
        monkeypatch,
        replay=_replay(
            _policy(
                "mcp__support__lookup",
                HistoryConfig(
                    scope=HistoryScope.BASELINE,
                    on_miss=ToolPolicyOnMiss.FAIL,
                ),
            )
        ),
        server=_server("support", original),
        arguments={"b": 2, "a": 1},
        client=client,
    )

    request = client.tool_lookup_requests[0]
    assert request.tool_name == "mcp__support__lookup"
    assert request.cache_key == compute_tool_cache_key(
        "mcp__support__lookup", {"a": 1, "b": 2}
    )
    assert request.occurrence == 0
    assert result == recorded
    assert calls == 0


async def test_history_consumes_failures_but_not_misses_and_never_calls_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    client = FakeClient()
    client.tool_lookup_responses.extend(
        [
            ToolLookupResponse(match=None),
            ToolLookupResponse(
                match=ToolLookupMatch(
                    result=None,
                    status=NodeStatus.FAILED,
                    error="recorded failure",
                )
            ),
            ToolLookupResponse(
                match=ToolLookupMatch(
                    result=encode_tool_result(
                        {"content": [{"type": "text", "text": "next"}]}
                    ),
                    status=NodeStatus.COMPLETED,
                    error=None,
                )
            ),
        ]
    )
    replay = _replay(
        _policy(
            "mcp__support__lookup",
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.ERROR_RESULT,
            ),
        )
    )
    server = _server("support", original)
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    wrapped: list[SdkMcpTool[Any]] = []

    def factory(**kwargs: Any) -> dict[str, Any]:
        wrapped.extend(kwargs["tools"])
        return {"type": "sdk", "name": kwargs["name"], "instance": object()}

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", factory)
    monkeypatch.setattr(runner_module, "sdk_query", query)
    await anext(
        KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[server],
        )
    )

    first = await wrapped[0].handler({"q": 1})
    assert first["is_error"] is True
    with pytest.raises(ToolPolicyError, match="recorded failure"):
        await wrapped[0].handler({"q": 1})
    assert await wrapped[0].handler({"q": 1}) == {
        "content": [{"type": "text", "text": "next"}],
        "is_error": False,
    }
    assert [request.occurrence for request in client.tool_lookup_requests] == [0, 0, 1]
    assert calls == 0


async def test_concurrent_identical_history_calls_fail_as_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    client = FakeClient()

    async def lookup(_: uuid.UUID, request: Any) -> ToolLookupResponse:
        client.tool_lookup_requests.append(request)
        entered.set()
        await release.wait()
        return ToolLookupResponse(
            match=ToolLookupMatch(
                result=encode_tool_result(
                    {"content": [{"type": "text", "text": "recorded"}]}
                ),
                status=NodeStatus.COMPLETED,
                error=None,
            )
        )

    client.replays.tool_lookup = lookup

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    active_client = client
    active_client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: active_client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    wrapped: list[SdkMcpTool[Any]] = []

    def factory(**kwargs: Any) -> dict[str, Any]:
        wrapped.extend(kwargs["tools"])
        return {"type": "sdk", "name": kwargs["name"], "instance": object()}

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", factory)
    monkeypatch.setattr(runner_module, "sdk_query", query)
    await anext(
        KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        )
    )
    first = asyncio.ensure_future(wrapped[0].handler({"q": 1}))
    await entered.wait()
    with pytest.raises(ToolPolicyError, match="Concurrent identical history"):
        await wrapped[0].handler({"q": 1})
    release.set()
    await first


async def test_parallel_same_named_tools_keep_qualified_policies_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    policy = ToolPolicy(
        default=StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL),
        tools={
            f"mcp__{server}__lookup": StaticConfig(
                cases=[
                    StaticCase(
                        match={"server": server},
                        match_mode=StaticMatchMode.EXACT,
                        result={
                            "content": [{"type": "text", "text": server}],
                            "is_error": False,
                        },
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            )
            for server in ("first", "second")
        },
    )
    replay = _replay(policy)
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    wrapped: list[SdkMcpTool[Any]] = []

    def factory(**kwargs: Any) -> dict[str, Any]:
        wrapped.extend(kwargs["tools"])
        return {"type": "sdk", "name": kwargs["name"], "instance": object()}

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", factory)
    monkeypatch.setattr(runner_module, "sdk_query", query)
    await anext(
        KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[
                _server("first", original),
                _server("second", original),
            ],
        )
    )

    results = await asyncio.gather(
        wrapped[0].handler({"server": "first"}),
        wrapped[1].handler({"server": "second"}),
    )
    assert [result["content"][0]["text"] for result in results] == [
        "first",
        "second",
    ]


async def test_preflight_rejects_unsupported_targets_before_session_or_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    unsupported = [
        ("Read", StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL)),
        (
            "mcp__external__lookup",
            HistoryConfig(scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL),
        ),
        ("mcp__support__missing", LLMConfig(model="claude")),
    ]
    for target, config in unsupported:
        client = FakeClient()
        replay = _replay(_policy(target, config))
        client.replay = replay
        monkeypatch.setattr(
            runner_module.recording_module,
            "KitaruAPIClient",
            lambda active=client: active,
        )
        monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
        sdk_calls = 0

        async def query(**_: Any) -> AsyncIterator[ResultMessage]:
            nonlocal sdk_calls
            sdk_calls += 1
            yield _terminal()

        monkeypatch.setattr(runner_module, "sdk_query", query)
        with pytest.raises(UnsupportedReplayError):
            await anext(
                KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
                    prompt="hello",
                    options=ClaudeAgentOptions(tools=[]),
                    replayable_servers=[_server("support", original)],
                )
            )
        assert sdk_calls == 0
        assert client.sessions.created == []


async def test_preflight_rejects_invalid_static_result_before_session_or_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            StaticConfig(
                cases=[
                    StaticCase(
                        match=None,
                        match_mode=StaticMatchMode.EXACT,
                        result={"content": [{"type": "image", "data": "..."}]},
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    sdk_calls = 0

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        nonlocal sdk_calls
        sdk_calls += 1
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    with pytest.raises(UnsupportedReplayError, match="text content blocks only"):
        await anext(
            KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
                prompt="hello",
                options=ClaudeAgentOptions(tools=[]),
                replayable_servers=[_server("support", original)],
            )
        )

    assert sdk_calls == 0
    assert client.sessions.created == []


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (ClaudeAgentOptions(), "tools=\\[\\]"),
        (ClaudeAgentOptions(tools=["Read"]), "tools=\\[\\]"),
        (
            ClaudeAgentOptions(
                tools=[],
                mcp_servers={
                    "external": {
                        "type": "http",
                        "url": "https://example.test",
                    }
                },
            ),
            "pre-existing MCP servers",
        ),
        (
            ClaudeAgentOptions(tools=[], allowed_tools=["Read"]),
            "allowed_tools.*Read",
        ),
    ],
)
async def test_substitution_preflight_rejects_enabled_unwrapped_tools(
    monkeypatch: pytest.MonkeyPatch,
    options: ClaudeAgentOptions,
    message: str,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            StaticConfig(
                cases=[
                    StaticCase(
                        match=None,
                        match_mode=StaticMatchMode.EXACT,
                        result={"content": [{"type": "text", "text": "safe"}]},
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    sdk_calls = 0

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        nonlocal sdk_calls
        sdk_calls += 1
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    with pytest.raises(UnsupportedReplayError, match=message):
        await anext(
            KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
                prompt="hello",
                options=options,
                replayable_servers=[_server("support", original)],
            )
        )
    assert sdk_calls == 0
    assert client.sessions.created == []
    assert client.close_count == 1


@pytest.mark.parametrize(
    ("option_updates", "message"),
    [
        ({"settings": "project"}, "settings"),
        ({"setting_sources": ["project"]}, "setting_sources"),
        ({"plugins": [cast(Any, {"type": "local", "path": "."})]}, "plugins"),
        ({"extra_args": {"mcp-config": "external.json"}}, "extra_args"),
        ({"skills": []}, "skills"),
        ({"skills": "all"}, "skills"),
        ({"agents": {}}, "agents"),
        ({"agents": {"helper": cast(Any, {})}}, "agents"),
    ],
)
async def test_substitution_preflight_rejects_explicit_topology_options(
    monkeypatch: pytest.MonkeyPatch,
    option_updates: dict[str, Any],
    message: str,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            StaticConfig(
                cases=[
                    StaticCase(
                        match=None,
                        match_mode=StaticMatchMode.EXACT,
                        result={"content": [{"type": "text", "text": "safe"}]},
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    sdk_calls = 0

    async def query(**_: Any) -> AsyncIterator[ResultMessage]:
        nonlocal sdk_calls
        sdk_calls += 1
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)
    options = ClaudeAgentOptions(tools=[])
    for name, value in option_updates.items():
        setattr(options, name, value)

    with pytest.raises(UnsupportedReplayError, match=message):
        await anext(
            KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
                prompt="hello",
                options=options,
                replayable_servers=[_server("support", original)],
            )
        )
    assert sdk_calls == 0
    assert client.sessions.created == []
    assert client.close_count == 1


async def test_all_passthrough_replay_preserves_unwrapped_tool_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay = _replay(
        ToolPolicy(
            default=PassthroughConfig(),
            tools={"Read": PassthroughConfig()},
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    options = ClaudeAgentOptions(
        allowed_tools=["Read"],
        mcp_servers={"external": {"type": "http", "url": "https://example.test"}},
        settings="project",
        setting_sources=["project"],
        plugins=[cast(Any, {"type": "local", "path": "."})],
        extra_args={"mcp-config": "external.json"},
        skills="all",
        agents={"helper": cast(Any, {})},
    )
    seen: list[ClaudeAgentOptions] = []

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        seen.append(kwargs["options"])
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    assert [
        message
        async for message in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello", options=options
        )
    ]
    assert seen[0].tools is None
    assert seen[0].allowed_tools is options.allowed_tools
    assert seen[0].mcp_servers is options.mcp_servers
    assert seen[0].settings == "project"
    assert seen[0].setting_sources == ["project"]
    assert seen[0].plugins is options.plugins
    assert seen[0].extra_args is options.extra_args
    assert seen[0].skills == "all"
    assert seen[0].agents is options.agents


async def test_materializes_fresh_servers_without_mutating_options_or_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        return {"content": [{"type": "text", "text": "live"}]}

    replay = _replay(_policy("mcp__support__lookup", PassthroughConfig()))
    clients: list[FakeClient] = []

    def client_factory() -> FakeClient:
        client = FakeClient()
        client.replay = replay
        clients.append(client)
        return client

    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", client_factory
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    servers: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> dict[str, Any]:
        server = {"type": "sdk", "name": kwargs["name"], "instance": object()}
        servers.append(server)
        return server

    seen_options: list[ClaudeAgentOptions] = []

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        seen_options.append(kwargs["options"])
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", factory)
    monkeypatch.setattr(runner_module, "sdk_query", query)
    options = ClaudeAgentOptions(
        tools=[],
        permission_mode="default",
        allowed_tools=["mcp__support__lookup"],
        disallowed_tools=["Bash"],
    )
    definition = _server("support", original)
    runner = KitaruClaudeRunner(agent_id=uuid.uuid4())
    await asyncio.gather(
        anext(
            runner.query(prompt="one", options=options, replayable_servers=[definition])
        ),
        anext(
            runner.query(prompt="two", options=options, replayable_servers=[definition])
        ),
    )

    assert len({id(server) for server in servers}) == 2
    assert all(item is not options for item in seen_options)
    assert all(item.permission_mode == options.permission_mode for item in seen_options)
    assert all(item.allowed_tools is options.allowed_tools for item in seen_options)
    assert all(
        item.disallowed_tools is options.disallowed_tools for item in seen_options
    )
    assert options.mcp_servers == {}
    assert options.setting_sources is None
    assert options.strict_mcp_config is False
    assert all(item.mcp_servers is not options.mcp_servers for item in seen_options)
    assert all(item.setting_sources == [] for item in seen_options)
    assert all(item.strict_mcp_config is True for item in seen_options)


async def test_real_sdk_server_propagates_swallowed_policy_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL),
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        instance = kwargs["options"].mcp_servers["support"]["instance"]
        request = mcp_types.CallToolRequest(
            params=mcp_types.CallToolRequestParams(
                name="lookup", arguments={"query": "missing"}
            )
        )
        result = await instance.request_handlers[mcp_types.CallToolRequest](request)
        assert result.root.isError is True
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    with pytest.raises(ToolPolicyMissError, match="No static result"):
        async for _ in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        ):
            pass

    assert client.sessions.updated[-1][1].status.value == "failed"
    assert client.sessions.updated[-1][1].error == (
        "No static result for tool 'mcp__support__lookup'"
    )


async def test_real_sdk_server_propagates_swallowed_kitaru_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    client = FakeClient()
    client.replay = replay

    async def fail_lookup(*_: Any) -> Any:
        raise OSError("history lookup failed")

    client.replays.tool_lookup = fail_lookup
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        instance = kwargs["options"].mcp_servers["support"]["instance"]
        request = mcp_types.CallToolRequest(
            params=mcp_types.CallToolRequestParams(
                name="lookup", arguments={"query": "refund"}
            )
        )
        result = await instance.request_handlers[mcp_types.CallToolRequest](request)
        assert result.root.isError is True
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    with pytest.raises(OSError, match="history lookup failed"):
        async for _ in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        ):
            pass

    assert client.sessions.updated[-1][1].status.value == "failed"


async def test_real_sdk_server_propagates_swallowed_history_decode_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("live handler must not run")

    replay = _replay(
        _policy(
            "mcp__support__lookup",
            HistoryConfig(
                scope=HistoryScope.BASELINE,
                on_miss=ToolPolicyOnMiss.FAIL,
            ),
        )
    )
    client = FakeClient()
    client.replay = replay
    client.tool_lookup_responses.append(
        ToolLookupResponse(
            match=ToolLookupMatch(
                result={"schema": "future", "replayable": True, "payload": {}},
                status=NodeStatus.COMPLETED,
                error=None,
            )
        )
    )
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        instance = kwargs["options"].mcp_servers["support"]["instance"]
        request = mcp_types.CallToolRequest(
            params=mcp_types.CallToolRequestParams(
                name="lookup", arguments={"query": "refund"}
            )
        )
        result = await instance.request_handlers[mcp_types.CallToolRequest](request)
        assert result.root.isError is True
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    with pytest.raises(ToolPolicyError, match="unknown envelope"):
        async for _ in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        ):
            pass

    assert client.sessions.updated[-1][1].status.value == "failed"


@pytest.mark.parametrize("error_type", [OSError, ToolPolicyError])
async def test_real_sdk_server_leaves_passthrough_handler_failure_with_claude(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    async def original(_: dict[str, Any]) -> dict[str, Any]:
        raise error_type("tool failed")

    replay = _replay(_policy("mcp__support__lookup", PassthroughConfig()))
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))

    async def query(**kwargs: Any) -> AsyncIterator[ResultMessage]:
        instance = kwargs["options"].mcp_servers["support"]["instance"]
        request = mcp_types.CallToolRequest(
            params=mcp_types.CallToolRequestParams(name="lookup", arguments={})
        )
        result = await instance.request_handlers[mcp_types.CallToolRequest](request)
        assert result.root.isError is True
        yield _terminal()

    monkeypatch.setattr(runner_module, "sdk_query", query)

    messages = [
        message
        async for message in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        )
    ]

    assert messages == [_terminal()]
    assert client.sessions.updated[-1][1].status.value == "completed"


async def test_explicit_passthrough_is_recorded_as_live_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def original(_: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"content": [{"type": "text", "text": "live"}]}

    replay = _replay(_policy("mcp__support__lookup", PassthroughConfig()))
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    wrapped: list[SdkMcpTool[Any]] = []

    def factory(**kwargs: Any) -> dict[str, Any]:
        wrapped.extend(kwargs["tools"])
        return {"type": "sdk", "name": kwargs["name"], "instance": object()}

    async def query(**_: Any) -> AsyncIterator[Any]:
        result = await wrapped[0].handler({"query": "refund"})
        yield AssistantMessage(
            content=[
                ToolUseBlock(
                    "tool-1",
                    "mcp__support__lookup",
                    {"query": "refund"},
                )
            ],
            model="claude-test",
            message_id="message-1",
        )
        yield UserMessage(
            content=[ToolResultBlock("tool-1", result["content"], is_error=False)]
        )
        yield _terminal()

    monkeypatch.setattr(runner_module, "create_sdk_mcp_server", factory)
    monkeypatch.setattr(runner_module, "sdk_query", query)

    assert [
        message
        async for message in KitaruClaudeRunner(agent_id=uuid.uuid4()).query(
            prompt="hello",
            options=ClaudeAgentOptions(tools=[]),
            replayable_servers=[_server("support", original)],
        )
    ]
    tool_node = [node for node in nodes(client) if node.tool_name is not None][-1]
    assert tool_node.attributes["replay"] == {
        "policy": "passthrough",
        "live": True,
    }
    assert tool_node.outputs["schema"] == TOOL_RESULT_SCHEMA
    assert calls == 1
