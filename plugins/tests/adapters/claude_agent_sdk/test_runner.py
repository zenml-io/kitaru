#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Provider-free tests for the Claude Agent SDK query facade."""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookCallback,
    HookMatcher,
    ResultMessage,
    SystemMessage,
)

import kitaru_claude_agent_sdk.runner as runner_module
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import (
    PassthroughConfig,
    ReplayOverride,
    StaticConfig,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru_claude_agent_sdk import (
    KitaruClaudeRunner,
    KitaruRecordingError,
    UnsupportedReplayError,
)

from .conftest import FakeClient


def _terminal(result: str = "done") -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=20,
        duration_api_ms=10,
        is_error=False,
        num_turns=1,
        session_id="session-1",
        total_cost_usd=0.25,
        usage={"input_tokens": 7, "output_tokens": 3},
        result=result,
        uuid=f"result-{result}",
    )


def _replay(
    *,
    override: ReplayOverride | None = None,
    tool_policy: ToolPolicy | None = None,
) -> ReplayResponse:
    now = datetime.now(UTC)
    return ReplayResponse(
        id=uuid.uuid4(),
        job_id=uuid.uuid4(),
        experiment_run_id=None,
        baseline_session_id=uuid.uuid4(),
        result_session_id=None,
        override=override,
        tool_policy=tool_policy or ToolPolicy(default=PassthroughConfig(), tools={}),
        evaluators=[],
        evaluate_baselines=False,
        baseline_evaluation_mode=BaselineEvaluationMode.NONE,
        status=ReplayStatus.PENDING,
        error=None,
        created=now,
        updated=now,
    )


class FakeQuery:
    """Capture the public query call and expose close/cancellation behavior."""

    def __init__(
        self,
        messages: list[Any],
        *,
        error: BaseException | None = None,
        wait_after_first: asyncio.Event | None = None,
    ) -> None:
        self.messages = messages
        self.error = error
        self.wait_after_first = wait_after_first
        self.calls: list[dict[str, Any]] = []
        self.closed = 0

    def __call__(self, **kwargs: Any) -> AsyncIterator[Any]:
        self.calls.append(kwargs)
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        try:
            for index, message in enumerate(self.messages):
                yield message
                if index == 0 and self.wait_after_first is not None:
                    await self.wait_after_first.wait()
            if self.error is not None:
                raise self.error
        finally:
            self.closed += 1


async def _collect(runner: KitaruClaudeRunner, **kwargs: Any) -> list[Any]:
    return [message async for message in runner.query(**kwargs)]


def _use_client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    client = FakeClient()
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    return client


async def test_yields_native_objects_in_order_and_finalizes_before_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = SystemMessage(subtype="init", data={})
    terminal = _terminal()
    fake = FakeQuery([first, terminal])
    monkeypatch.setattr(runner_module, "sdk_query", fake)
    client = _use_client(monkeypatch)

    stream = KitaruClaudeRunner(agent_id=uuid.uuid4()).query(prompt="hello")
    assert await anext(stream) is first
    assert client.sessions.updated == []

    assert await anext(stream) is terminal
    assert len(client.sessions.updated) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert fake.closed == 1


async def test_copies_options_and_preserves_hooks_permissions_and_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    caller_hook_calls: list[str] = []
    permission_calls: list[str] = []

    async def caller_hook(*_: Any) -> dict[str, Any]:
        caller_hook_calls.append("called")
        return {}

    async def can_use_tool(name: str, *_: Any) -> Any:
        permission_calls.append(name)
        return object()

    caller_matcher = HookMatcher(
        matcher="Read", hooks=[cast(HookCallback, caller_hook)], timeout=2.0
    )
    options = ClaudeAgentOptions(
        permission_mode="default",
        allowed_tools=["Read"],
        disallowed_tools=["Bash"],
        can_use_tool=can_use_tool,
        hooks={"PreToolUse": [caller_matcher]},
        mcp_servers={"external": {"type": "http", "url": "https://example.test"}},
    )
    original = replace(options)
    transport = object()
    terminal = _terminal()

    async def fake_query(**kwargs: Any) -> AsyncIterator[Any]:
        assert kwargs["transport"] is transport
        copied = kwargs["options"]
        assert copied is not options
        assert copied.permission_mode == "default"
        assert copied.allowed_tools is options.allowed_tools
        assert copied.disallowed_tools is options.disallowed_tools
        assert copied.can_use_tool is can_use_tool
        assert copied.mcp_servers is options.mcp_servers
        assert copied.hooks is not options.hooks
        assert copied.hooks["PreToolUse"][0] is caller_matcher
        await copied.hooks["PreToolUse"][0].hooks[0]({}, None, object())
        await copied.can_use_tool("Read", {}, object())
        yield terminal

    monkeypatch.setattr(runner_module, "sdk_query", fake_query)

    assert await _collect(
        KitaruClaudeRunner(agent_id=uuid.uuid4()),
        prompt="hello",
        options=options,
        transport=transport,
    ) == [terminal]
    assert options == original
    assert options.hooks == {"PreToolUse": [caller_matcher]}
    assert caller_hook_calls == ["called"]
    assert permission_calls == ["Read"]


async def test_replay_uses_recorded_input_and_supported_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay = _replay(
        override=ReplayOverride(
            prompt="recorded prompt",
            system_prompt="candidate system",
            model={"old-model": "candidate-model"},
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    options = ClaudeAgentOptions(system_prompt="old", model="old-model")
    terminal = _terminal()
    fake = FakeQuery([terminal])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    assert await _collect(
        KitaruClaudeRunner(agent_version_id=uuid.uuid4()),
        prompt="caller prompt",
        options=options,
    ) == [terminal]

    assert fake.calls[0]["prompt"] == "recorded prompt"
    copied = fake.calls[0]["options"]
    assert copied.system_prompt == "candidate system"
    assert copied.model == "candidate-model"
    assert options.system_prompt == "old"
    assert options.model == "old-model"


async def test_task_bound_query_uses_resolved_task_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    client.task_inputs = {"request": "refund", "priority": 2}
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    fake = FakeQuery([_terminal()])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    assert await _collect(KitaruClaudeRunner(), prompt="caller prompt")

    assert fake.calls[0]["prompt"] == '{"priority":2,"request":"refund"}'
    assert client.sessions.created[0].inputs == {
        "request": "refund",
        "priority": 2,
    }


async def test_non_replay_session_continuation_options_pass_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = ClaudeAgentOptions(
        resume="session-1",
        continue_conversation=True,
        fork_session=True,
        resume_session_at="message-1",
        resume_drops_turn="assistant",
    )
    fake = FakeQuery([_terminal()])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    assert await _collect(
        KitaruClaudeRunner(agent_id=uuid.uuid4()),
        prompt="hello",
        options=options,
    )

    copied = fake.calls[0]["options"]
    assert copied.resume == "session-1"
    assert copied.continue_conversation is True
    assert copied.fork_session is True
    assert copied.resume_session_at == "message-1"
    assert copied.resume_drops_turn == "assistant"


@pytest.mark.parametrize(
    ("override", "options", "message"),
    [
        (ReplayOverride(model_params={"temperature": 0.2}), None, "model_params"),
        (None, ClaudeAgentOptions(resume="session"), "resume"),
        (None, ClaudeAgentOptions(continue_conversation=True), "continue"),
        (None, ClaudeAgentOptions(fork_session=True), "fork"),
        (
            None,
            ClaudeAgentOptions(resume_session_at="message-1"),
            "resume_session_at",
        ),
        (
            None,
            ClaudeAgentOptions(resume_drops_turn="assistant"),
            "resume_drops_turn",
        ),
        (
            ReplayOverride(model={"old-model": "candidate-model"}),
            None,
            "requires ClaudeAgentOptions.model",
        ),
        (
            ReplayOverride(model={"other-model": "candidate-model"}),
            ClaudeAgentOptions(model="old-model"),
            "no entry for 'old-model'",
        ),
    ],
)
async def test_replay_preflight_rejects_unsupported_options_before_invocation(
    monkeypatch: pytest.MonkeyPatch,
    override: ReplayOverride | None,
    options: ClaudeAgentOptions | None,
    message: str,
) -> None:
    replay = _replay(override=override)
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    fake = FakeQuery([])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    with pytest.raises(UnsupportedReplayError, match=message):
        await _collect(
            KitaruClaudeRunner(agent_id=uuid.uuid4()),
            prompt="hello",
            options=options,
        )

    assert fake.calls == []
    assert client.sessions.created == []
    assert client.close_count == 1


async def test_rejects_async_prompt_before_client_or_sdk_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def prompt() -> AsyncIterator[dict[str, Any]]:
        yield {"type": "user"}

    clients: list[FakeClient] = []

    def client_factory() -> FakeClient:
        clients.append(FakeClient())
        return clients[-1]

    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", client_factory
    )
    fake = FakeQuery([])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    with pytest.raises(TypeError, match="string prompts"):
        await _collect(KitaruClaudeRunner(agent_id=uuid.uuid4()), prompt=prompt())

    assert clients == []
    assert fake.calls == []


async def test_rejects_non_passthrough_policy_before_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay = _replay(
        tool_policy=ToolPolicy(
            default=StaticConfig(cases=[], on_miss=ToolPolicyOnMiss.FAIL),
            tools={},
        )
    )
    client = FakeClient()
    client.replay = replay
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(replay.id))
    fake = FakeQuery([])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    with pytest.raises(UnsupportedReplayError, match="wrapped SDK MCP tools"):
        await _collect(KitaruClaudeRunner(agent_id=uuid.uuid4()), prompt="hello")

    assert fake.calls == []
    assert client.sessions.created == []


async def test_sdk_error_remains_primary_and_records_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = ConnectionError("sdk disconnected")
    fake = FakeQuery([SystemMessage(subtype="init", data={})], error=native)
    monkeypatch.setattr(runner_module, "sdk_query", fake)
    client = _use_client(monkeypatch)

    with pytest.raises(ConnectionError, match="sdk disconnected") as caught:
        await _collect(KitaruClaudeRunner(agent_id=uuid.uuid4()), prompt="hello")

    assert caught.value is native
    assert client.sessions.updated[-1][1].error == "sdk disconnected"
    assert fake.closed == 1


async def test_native_error_stays_primary_when_recording_finalization_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    native = RuntimeError("native failure")
    client = FakeClient()
    client.update_error = OSError("recording unavailable")
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    fake = FakeQuery([], error=native)
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    with pytest.raises(RuntimeError, match="native failure") as caught:
        await _collect(KitaruClaudeRunner(agent_id=uuid.uuid4()), prompt="hello")

    assert caught.value is native
    assert any("could not finalize" in note for note in caught.value.__notes__)
    assert fake.closed == 1


async def test_terminal_recording_error_retains_native_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminal = _terminal()
    client = FakeClient()
    client.update_error = OSError("recording unavailable")
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", lambda: client
    )
    fake = FakeQuery([terminal])
    monkeypatch.setattr(runner_module, "sdk_query", fake)

    with pytest.raises(KitaruRecordingError) as caught:
        await _collect(KitaruClaudeRunner(agent_id=uuid.uuid4()), prompt="hello")

    assert caught.value.terminal_message is terminal
    assert caught.value.phase == "finalize"
    assert fake.closed == 1


async def test_explicit_close_closes_inner_iterator_and_finalizes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeQuery([SystemMessage(subtype="init", data={}), _terminal()])
    monkeypatch.setattr(runner_module, "sdk_query", fake)
    client = _use_client(monkeypatch)
    stream = KitaruClaudeRunner(agent_id=uuid.uuid4()).query(prompt="hello")

    await anext(stream)
    await stream.aclose()

    assert fake.closed == 1
    assert len(client.sessions.updated) == 1
    assert client.sessions.updated[0][1].status.value == "failed"
    assert client.close_count == 1


async def test_aclosing_after_consumer_error_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeQuery([SystemMessage(subtype="init", data={}), _terminal()])
    monkeypatch.setattr(runner_module, "sdk_query", fake)
    client = _use_client(monkeypatch)

    with pytest.raises(RuntimeError, match="consumer failed"):
        async with contextlib.aclosing(
            KitaruClaudeRunner(agent_id=uuid.uuid4()).query(prompt="hello")
        ) as stream:
            async for _ in stream:
                raise RuntimeError("consumer failed")

    assert fake.closed == 1
    assert len(client.sessions.updated) == 1


async def test_cancellation_closes_inner_iterator_and_finalizes_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = asyncio.Event()
    fake = FakeQuery([SystemMessage(subtype="init", data={})], wait_after_first=release)
    monkeypatch.setattr(runner_module, "sdk_query", fake)
    client = _use_client(monkeypatch)
    stream = KitaruClaudeRunner(agent_id=uuid.uuid4()).query(prompt="hello")
    assert isinstance(await anext(stream), SystemMessage)

    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake.closed == 1
    assert len(client.sessions.updated) == 1


async def test_concurrent_queries_keep_options_messages_and_recorders_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ClaudeAgentOptions]] = []
    clients: list[FakeClient] = []

    def client_factory() -> FakeClient:
        client = FakeClient()
        clients.append(client)
        return client

    async def fake_query(**kwargs: Any) -> AsyncIterator[Any]:
        calls.append((kwargs["prompt"], kwargs["options"]))
        await asyncio.sleep(0)
        yield _terminal(kwargs["prompt"])

    monkeypatch.setattr(runner_module, "sdk_query", fake_query)
    monkeypatch.setattr(
        runner_module.recording_module, "KitaruAPIClient", client_factory
    )
    runner = KitaruClaudeRunner(agent_id=uuid.uuid4())
    first_options = ClaudeAgentOptions(model="first")
    second_options = ClaudeAgentOptions(model="second")

    first, second = await asyncio.gather(
        _collect(runner, prompt="first", options=first_options),
        _collect(runner, prompt="second", options=second_options),
    )

    assert first[0].result == "first"
    assert second[0].result == "second"
    assert {(prompt, options.model) for prompt, options in calls} == {
        ("first", "first"),
        ("second", "second"),
    }
    assert len({client.session_id for client in clients}) == 2
