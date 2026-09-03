#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Run-local recording tests for the Claude Agent SDK adapter."""

import asyncio
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import Any, cast

import pytest
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TaskNotificationMessage,
    TaskProgressMessage,
    TaskStartedMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.client import KitaruAPIClient
from kitaru_claude_agent_sdk.capability import KitaruRecordingError
from kitaru_claude_agent_sdk.recording import (
    InvocationRecorder,
    finalize_failure,
    finalize_terminal,
    resolve_run_input,
)

from .conftest import FakeClient, nodes


def _terminal(*, is_error: bool = False) -> ResultMessage:
    return ResultMessage(
        subtype="error" if is_error else "success",
        duration_ms=20,
        duration_api_ms=10,
        is_error=is_error,
        num_turns=1,
        session_id="session-1",
        total_cost_usd=0.25,
        usage={"input_tokens": 7, "output_tokens": 3},
        result=None if is_error else "done",
        errors=["native failure"] if is_error else None,
        uuid="result-1",
    )


async def _recorder(client: FakeClient, prompt: str = "hello") -> InvocationRecorder:
    return await InvocationRecorder.start(
        client=cast(KitaruAPIClient, client),
        inputs=prompt,
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        safe_options={"model": "claude-test", "permission_mode": "default"},
    )


async def _blocking_ingest(started: asyncio.Event, *_: Any) -> list[Any]:
    started.set()
    await asyncio.Event().wait()
    return []


async def test_resolves_task_input_and_replay_before_session_creation(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    task_id = uuid.uuid4()
    replay = object()
    payload = {"query": "refund", "limit": 2}
    fake_client.replay = replay
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps(payload))

    resolved = await resolve_run_input(
        cast(KitaruAPIClient, fake_client), "caller input"
    )

    assert resolved.recorded == payload
    assert resolved.claude == '{"limit":2,"query":"refund"}'
    assert resolved.replay is replay
    assert resolved.task_bound is True
    assert fake_client.sessions.created == []


async def test_maps_typed_messages_and_correlates_tools_and_tasks(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(UserMessage(content="hello", uuid="user-1"))
    await recorder.record_message(
        AssistantMessage(
            content=[
                TextBlock("checking"),
                ThinkingBlock("reasoning", "signature-secret"),
                ToolUseBlock("tool-1", "lookup", {"query": "refund"}),
            ],
            model="claude-test",
            message_id="message-1",
            session_id="session-1",
            usage={"input_tokens": 7, "output_tokens": 3},
        )
    )
    await recorder.record_message(
        UserMessage(
            content=[ToolResultBlock("tool-1", "eligible", is_error=False)],
            uuid="user-2",
        )
    )
    await recorder.record_message(
        TaskStartedMessage(
            subtype="task_started",
            data={"private_noise": object()},
            task_id="task-1",
            description="research",
            uuid="task-event-1",
            session_id="session-1",
            tool_use_id="tool-1",
            task_type="agent",
        )
    )
    await recorder.record_message(
        TaskProgressMessage(
            subtype="task_progress",
            data={},
            task_id="task-1",
            description="research",
            usage={"total_tokens": 2, "tool_uses": 1, "duration_ms": 4},
            uuid="task-event-2",
            session_id="session-1",
            tool_use_id="tool-1",
            last_tool_name="lookup",
        )
    )
    await recorder.record_message(
        TaskNotificationMessage(
            subtype="task_notification",
            data={},
            task_id="task-1",
            status="completed",
            output_file="/secret/path",
            summary="finished",
            uuid="task-event-3",
            session_id="session-1",
            tool_use_id="tool-1",
            usage={"total_tokens": 2, "tool_uses": 1, "duration_ms": 4},
        )
    )
    await finalize_terminal(recorder, _terminal())

    persisted = nodes(fake_client)
    latest = {node.index: node for node in persisted}
    assert [latest[index].node_type for index in sorted(latest)] == [
        NodeType.SPAN,
        NodeType.SPAN,
        NodeType.LLM_CALL,
        NodeType.TOOL_CALL,
        NodeType.SUBAGENT_CALL,
    ]
    model = latest[2]
    assert model.external_id == "message-1"
    assert model.model == "claude-test"
    assert model.reasoning == "reasoning"
    assert model.tokens.input_tokens == 7
    assert model.tokens.output_tokens == 3
    assert "signature-secret" not in model.model_dump_json()
    tool = latest[3]
    assert tool.external_id == "tool-1"
    assert tool.parent_index == model.index
    assert tool.outputs == "eligible"
    task = latest[4]
    assert task.external_id == "task-1"
    assert task.parent_index == tool.index
    assert task.status is NodeStatus.COMPLETED
    assert task.outputs == {"summary": "finished"}
    assert "/secret/path" not in task.model_dump_json()
    assert fake_client.sessions.updated[-1][1].status is SessionStatus.COMPLETED


async def test_terminal_usage_is_session_aggregate_not_duplicate_llm_node(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        AssistantMessage(
            content=[TextBlock("done")],
            model="claude-test",
            message_id="message-1",
            usage={"input_tokens": 7, "output_tokens": 3},
        )
    )
    await finalize_terminal(recorder, _terminal())

    latest = {node.index: node for node in nodes(fake_client)}
    assert [node for node in latest.values() if node.node_type is NodeType.LLM_CALL]
    assert (
        sum(
            node.tokens.input_tokens
            for node in latest.values()
            if node.tokens is not None
        )
        == 7
    )
    root = latest[0]
    assert root.cost == Decimal("0.25")
    # The terminal totals already equal the per-call counts here, so the root
    # carries no remainder and the session cannot count this turn twice.
    assert root.tokens is None
    assert all(node.cost is None for index, node in latest.items() if index != 0)
    assert fake_client.sessions.updated[-1][1].metadata == {
        "terminal": {
            "session_id": "session-1",
            "num_turns": 1,
            "duration_ms": 20,
            "duration_api_ms": 10,
            "usage": {"input_tokens": 7, "output_tokens": 3},
            "total_cost_usd": 0.25,
        }
    }


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": 7, "cache_creation_input_tokens": 5},
        {"inputTokens": 7, "cacheCreationInputTokens": 5},
    ],
)
async def test_cache_creation_tokens_count_toward_input_usage(
    fake_client: FakeClient, usage: dict[str, int]
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        AssistantMessage(
            content=[TextBlock("done")],
            model="claude-test",
            message_id="message-1",
            usage=usage,
        )
    )

    model = nodes(fake_client)[-1]
    assert model.tokens is not None
    assert model.tokens.input_tokens == 12


async def test_session_preserves_full_replay_input_while_root_is_bounded(
    fake_client: FakeClient,
) -> None:
    prompt = "x" * (16 * 1024 + 1)

    await _recorder(fake_client, prompt)

    assert fake_client.sessions.created[0].inputs == prompt
    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.inputs == {"value": prompt[:-1], "truncated": True}


async def test_hook_and_stream_views_do_not_duplicate_tool_node(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_tool_hook(
        {"tool_use_id": "tool-1", "tool_name": "lookup", "tool_input": {"q": 1}},
        event="before",
    )
    await recorder.record_message(
        StreamEvent("delta-1", "session-1", {"type": "content_block_delta"})
    )
    await recorder.record_message(
        AssistantMessage(
            content=[ToolUseBlock("tool-1", "lookup", {"q": 1})],
            model="claude-test",
            message_id="message-1",
        )
    )
    await recorder.record_tool_hook(
        {"tool_use_id": "tool-1", "tool_name": "lookup"}, event="after"
    )

    latest = {node.index: node for node in nodes(fake_client)}
    tools = [node for node in latest.values() if node.node_type is NodeType.TOOL_CALL]
    assert len(tools) == 1
    assert tools[0].attributes["hook_events"] == ["before", "after"]


async def test_stream_tool_use_reparents_hook_created_node(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_tool_hook(
        {"tool_use_id": "tool-1", "tool_name": "lookup", "tool_input": {"q": 1}},
        event="before",
    )
    await recorder.record_message(
        AssistantMessage(
            content=[ToolUseBlock("tool-1", "lookup", {"q": 1})],
            model="claude-test",
            message_id="message-1",
        )
    )

    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    tool = next(
        node for node in latest.values() if node.node_type is NodeType.TOOL_CALL
    )
    assert tool.parent_index == model.index


def _split_delivery(*blocks: Any) -> AssistantMessage:
    """Build one delivery of a turn the CLI splits across several messages."""
    return AssistantMessage(
        content=list(blocks),
        model="claude-test",
        message_id="message-1",
        session_id="session-1",
    )


async def test_split_turn_tool_delivery_stays_under_its_llm_call(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_tool_hook(
        {"tool_use_id": "tool-1", "tool_name": "lookup", "tool_input": {"q": 1}},
        event="before",
    )
    await recorder.record_message(_split_delivery(ThinkingBlock("planning", "sig")))
    await recorder.record_message(
        _split_delivery(ToolUseBlock("tool-1", "lookup", {"q": 1}))
    )

    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    tools = [node for node in latest.values() if node.node_type is NodeType.TOOL_CALL]
    assert sorted(latest) == [0, 1, 2]
    assert len(tools) == 1
    assert tools[0].parent_index == model.index
    assert tools[0].parent_index < tools[0].index
    assert model.reasoning == "planning"


async def test_split_turn_never_writes_one_message_id_at_two_indexes(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_tool_hook(
        {"tool_use_id": "tool-1", "tool_name": "lookup", "tool_input": {"q": 1}},
        event="before",
    )
    hooked = nodes(fake_client)[-1]
    await recorder.record_message(_split_delivery(ThinkingBlock("planning", "sig")))
    await recorder.record_message(
        _split_delivery(ToolUseBlock("tool-1", "lookup", {"q": 1}))
    )

    indexes_by_external_id: dict[str, set[int]] = {}
    for node in nodes(fake_client):
        if node.external_id is not None:
            indexes_by_external_id.setdefault(node.external_id, set()).add(node.index)
    assert all(len(indexes) == 1 for indexes in indexes_by_external_id.values())
    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    tool = next(
        node for node in latest.values() if node.node_type is NodeType.TOOL_CALL
    )
    assert model.index < tool.index
    # The hook created the tool node before the turn arrived, so the turn took
    # the index reserved below it and the tool node never moved.
    assert tool.index == hooked.index
    assert tool.started_at == hooked.started_at
    assert tool.parent_index == model.index


async def test_split_turn_with_two_hooked_tools_keeps_every_parent_below_its_node(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    for tool_id in ("tool-1", "tool-2"):
        await recorder.record_tool_hook(
            {"tool_use_id": tool_id, "tool_name": "lookup", "tool_input": {"q": 1}},
            event="before",
        )
    await recorder.record_message(_split_delivery(ThinkingBlock("planning", "sig")))
    await recorder.record_message(
        _split_delivery(ToolUseBlock("tool-1", "lookup", {"q": 1}))
    )
    await recorder.record_message(
        _split_delivery(ToolUseBlock("tool-2", "lookup", {"q": 2}))
    )

    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    tools = [node for node in latest.values() if node.node_type is NodeType.TOOL_CALL]
    assert sorted(latest) == [0, 1, 2, 3]
    assert len(tools) == 2
    assert all(tool.parent_index == model.index for tool in tools)
    assert all(node.parent_index < node.index for node in latest.values() if node.index)


async def test_split_turn_merges_reasoning_and_text_into_one_node(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        _split_delivery(ThinkingBlock("weighing options", "sig"))
    )
    await recorder.record_message(_split_delivery(TextBlock("final answer")))

    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    assert model.reasoning == "weighing options"
    assert model.outputs == {"text": ["final answer"]}


async def test_split_turn_keeps_text_when_tool_use_arrives_separately(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(_split_delivery(TextBlock("looking that up")))
    await recorder.record_message(
        _split_delivery(ToolUseBlock("tool-1", "lookup", {"q": 1}))
    )

    latest = {node.index: node for node in nodes(fake_client)}
    model = next(
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    )
    tool = next(
        node for node in latest.values() if node.node_type is NodeType.TOOL_CALL
    )
    assert model.outputs == {"text": ["looking that up"]}
    assert tool.parent_index == model.index


async def test_redelivered_identical_assistant_message_is_recorded_once(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    for _ in range(2):
        await recorder.record_message(
            _split_delivery(TextBlock("done"), ThinkingBlock("planning", "sig"))
        )

    latest = {node.index: node for node in nodes(fake_client)}
    model_nodes = [
        node for node in latest.values() if node.node_type is NodeType.LLM_CALL
    ]
    assert len(model_nodes) == 1
    assert model_nodes[0].outputs == {"text": ["done"]}
    assert model_nodes[0].reasoning == "planning"


async def test_replayable_tool_preserves_full_arguments_for_history_key(
    fake_client: FakeClient,
) -> None:
    tool_name = "mcp__support__lookup"
    arguments = {"query": "x" * (16 * 1024 + 1)}
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        replayable_tool_names=frozenset({tool_name}),
    )

    await recorder.record_message(
        AssistantMessage(
            content=[ToolUseBlock("tool-1", tool_name, arguments)],
            model="claude-test",
            message_id="message-1",
        )
    )

    tool = next(
        node for node in nodes(fake_client) if node.node_type is NodeType.TOOL_CALL
    )
    assert tool.inputs == arguments


@pytest.mark.parametrize("effective_before_stream", [True, False])
async def test_replayable_tool_records_effective_rewritten_arguments(
    fake_client: FakeClient, effective_before_stream: bool
) -> None:
    tool_id = "tool-1"
    tool_name = "mcp__support__lookup"
    proposed = {"query": "proposed"}
    effective = {"query": "rewritten"}
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        replayable_tool_names=frozenset({tool_name}),
    )

    async def record_stream_message() -> None:
        await recorder.record_message(
            AssistantMessage(
                content=[ToolUseBlock(tool_id, tool_name, proposed)],
                model="claude-test",
                message_id="message-1",
            )
        )

    if not effective_before_stream:
        await record_stream_message()
    await recorder.record_tool_policy(
        tool_name=tool_name,
        arguments=effective,
        policy="passthrough",
        live=True,
    )
    await recorder.record_tool_hook(
        {
            "tool_use_id": tool_id,
            "tool_name": tool_name,
            "tool_input": effective,
        },
        event="after",
    )
    if effective_before_stream:
        await record_stream_message()

    latest = {node.index: node for node in nodes(fake_client)}
    tool = next(
        node for node in latest.values() if node.node_type is NodeType.TOOL_CALL
    )
    assert tool.inputs == effective
    assert tool.attributes["replay"] == {"policy": "passthrough", "live": True}


async def test_unknown_message_is_bounded_and_opaque_fields_are_not_persisted(
    fake_client: FakeClient,
) -> None:
    @dataclass
    class FutureMessage:
        name: str
        transport: object
        headers: dict[str, str]
        environment: dict[str, str]

    secret = "sensitive-token-value"
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs="hello",
        agent_id=None,
        agent_version_id=None,
        session_name=None,
        replay=False,
        safe_options={
            "model": "claude-test",
            "transport": object(),
            "headers": {"auth": secret},
            "environment": {"KEY": secret},
        },
    )
    await recorder.record_message(
        cast(Any, FutureMessage("future", object(), {"auth": secret}, {"KEY": secret}))
    )
    await recorder.record_message(SystemMessage("init", {"cwd": "/secret"}))

    payload = "\n".join(node.model_dump_json() for node in nodes(fake_client))
    assert secret not in payload
    assert "/secret" not in payload
    assert "FutureMessage" in payload


async def test_error_terminal_records_native_failure_fields(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)

    await finalize_terminal(recorder, _terminal(is_error=True))

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.status is NodeStatus.FAILED
    assert root.error == "native failure"
    update = fake_client.sessions.updated[-1][1]
    assert update.status is SessionStatus.FAILED
    assert update.error == "native failure"


def _failed_terminal(
    *,
    subtype: str = "success",
    result: str | None = None,
    errors: list[str] | None = None,
    terminal_reason: str | None = None,
    api_error_status: int | None = None,
) -> ResultMessage:
    """Build the terminal shape the CLI emits when the provider call fails."""
    return ResultMessage(
        subtype=subtype,
        duration_ms=20,
        duration_api_ms=10,
        is_error=True,
        num_turns=1,
        session_id="session-1",
        total_cost_usd=None,
        usage=None,
        result=result,
        errors=errors,
        terminal_reason=terminal_reason,
        api_error_status=api_error_status,
        uuid="result-error",
    )


async def test_api_error_terminal_records_the_readable_cause_and_status(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)

    await finalize_terminal(
        recorder,
        _failed_terminal(result="API Error: 529 Overloaded", api_error_status=529),
    )

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.status is NodeStatus.FAILED
    assert root.error == "API Error: 529 Overloaded"
    update = fake_client.sessions.updated[-1][1]
    assert update.status is SessionStatus.FAILED
    assert update.error == "API Error: 529 Overloaded"
    assert update.metadata["terminal"]["api_error_status"] == 529


@pytest.mark.parametrize(
    ("subtype", "expected"),
    [("error", "error"), ("success", "Claude reported a failed result")],
)
async def test_failed_terminal_never_records_the_success_subtype_as_the_error(
    fake_client: FakeClient, subtype: str, expected: str
) -> None:
    recorder = await _recorder(fake_client)

    await finalize_terminal(recorder, _failed_terminal(subtype=subtype))

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.error == expected
    assert fake_client.sessions.updated[-1][1].error == expected


async def test_failed_terminal_prefers_the_terminal_reason_over_the_subtype(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)

    await finalize_terminal(
        recorder, _failed_terminal(subtype="error", terminal_reason="max_turns")
    )

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.error == "max_turns"


async def test_failed_terminal_never_records_a_completed_reason_as_the_error(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)

    await finalize_terminal(
        recorder, _failed_terminal(terminal_reason="completed", api_error_status=529)
    )

    expected = "Claude reported a failed result"
    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.error == expected
    assert fake_client.sessions.updated[-1][1].error == expected


async def test_terminal_output_tokens_reconcile_onto_the_root_span(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    for identity in ("message-1", "message-2"):
        await recorder.record_message(
            AssistantMessage(
                content=[TextBlock("part")],
                model="claude-test",
                message_id=identity,
                usage={"input_tokens": 7, "output_tokens": 3},
            )
        )
    terminal = _terminal()
    terminal.usage = {
        "input_tokens": 14,
        "output_tokens": 138,
        "cache_read_input_tokens": 5,
        "output_tokens_details": {"thinking_tokens": 90},
    }

    await finalize_terminal(recorder, terminal)

    latest = {node.index: node for node in nodes(fake_client)}
    root = latest[0]
    assert root.tokens is not None
    assert root.tokens.output_tokens == 132
    assert root.tokens.input_tokens == 0
    assert root.tokens.cached_input_tokens == 5
    assert root.tokens.reasoning_tokens == 90
    rollup = [node.tokens for node in latest.values() if node.tokens is not None]
    assert sum(tokens.input_tokens or 0 for tokens in rollup) == 14
    assert sum(tokens.output_tokens or 0 for tokens in rollup) == 138
    assert sum(tokens.cached_input_tokens or 0 for tokens in rollup) == 5
    assert sum(tokens.reasoning_tokens or 0 for tokens in rollup) == 90


async def test_recorded_tokens_above_the_terminal_total_are_never_negated(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    for identity in ("message-1", "message-2"):
        await recorder.record_message(
            AssistantMessage(
                content=[TextBlock("part")],
                model="claude-test",
                message_id=identity,
                usage={"input_tokens": 900, "output_tokens": 3},
            )
        )
    terminal = _terminal()
    terminal.usage = {"input_tokens": 900, "output_tokens": 138}

    await finalize_terminal(recorder, terminal)

    latest = {node.index: node for node in nodes(fake_client)}
    root = latest[0]
    assert root.tokens is not None
    assert root.tokens.input_tokens == 0
    assert root.tokens.output_tokens == 132
    rollup = [node.tokens for node in latest.values() if node.tokens is not None]
    assert sum(tokens.input_tokens or 0 for tokens in rollup) == 1800


async def test_failure_without_a_terminal_leaves_root_span_tokens_unset(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        AssistantMessage(
            content=[TextBlock("done")],
            model="claude-test",
            message_id="message-1",
            usage={"input_tokens": 7, "output_tokens": 3},
        )
    )

    await finalize_failure(recorder, RuntimeError("native failure"))

    latest = {node.index: node for node in nodes(fake_client)}
    assert latest[0].tokens is None
    rollup = [node.tokens for node in latest.values() if node.tokens is not None]
    assert sum(tokens.output_tokens or 0 for tokens in rollup) == 3


async def test_thinking_tokens_are_recorded_as_reasoning_tokens(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)

    await recorder.record_message(
        AssistantMessage(
            content=[TextBlock("done")],
            model="claude-test",
            message_id="message-1",
            usage={
                "input_tokens": 7,
                "output_tokens": 3,
                "output_tokens_details": {"thinking_tokens": 2},
            },
        )
    )

    model = nodes(fake_client)[-1]
    assert model.tokens is not None
    assert model.tokens.reasoning_tokens == 2


async def test_root_span_records_the_bounded_prompt_sent_to_claude(
    fake_client: FakeClient,
) -> None:
    prompt = "x" * (16 * 1024 + 1)
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs={"request": "refund"},
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        effective_prompt=prompt,
    )

    await finalize_terminal(recorder, _terminal())

    latest = {node.index: node for node in nodes(fake_client)}
    assert latest[0].attributes["effective_prompt"] == {
        "value": prompt[: 16 * 1024],
        "truncated": True,
    }
    assert fake_client.sessions.created[0].inputs == {"request": "refund"}


async def test_root_span_records_a_short_prompt_as_a_plain_string(
    fake_client: FakeClient,
) -> None:
    recorder = await InvocationRecorder.start(
        client=cast(KitaruAPIClient, fake_client),
        inputs={"request": "refund"},
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
        effective_prompt="candidate prompt",
    )

    await finalize_terminal(recorder, _terminal())

    latest = {node.index: node for node in nodes(fake_client)}
    assert latest[0].attributes["effective_prompt"] == "candidate prompt"


async def test_native_error_is_primary_when_finalization_also_fails(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    native_error = RuntimeError("native failure")
    persistence_error = OSError("persistence secret must not leak")
    fake_client.update_error = persistence_error

    secondary = await finalize_failure(recorder, native_error)

    assert secondary is persistence_error
    assert fake_client.close_count == 1


async def test_root_ingest_failure_marks_created_session_failed(
    fake_client: FakeClient,
) -> None:
    fake_client.ingest_error = OSError("root ingest failed")

    with pytest.raises(OSError, match="root ingest failed"):
        await InvocationRecorder.start(
            client=cast(KitaruAPIClient, fake_client),
            inputs="hello",
            agent_id=uuid.uuid4(),
            agent_version_id=None,
            session_name=None,
            replay=False,
        )

    assert len(fake_client.sessions.created) == 1
    assert len(fake_client.sessions.updated) == 1
    update = fake_client.sessions.updated[0][1]
    assert update.status is SessionStatus.FAILED
    assert update.error == "root ingest failed"


async def test_terminal_result_is_authoritative_and_diagnostics_are_bounded(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    terminal = _terminal()
    terminal.result = "x" * (16 * 1024 + 1)

    await finalize_terminal(recorder, terminal)

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.outputs == {"value": "x" * (16 * 1024), "truncated": True}
    assert fake_client.sessions.updated[-1][1].outputs == terminal.result

    error_recorder = await _recorder(fake_client)
    error = RuntimeError("y" * (16 * 1024 + 1))
    await error_recorder.finalize(error=error)

    latest = {node.index: node for node in nodes(fake_client)}
    assert latest[0].error == "y" * (16 * 1024)
    assert fake_client.sessions.updated[-1][1].error == "y" * (16 * 1024)


async def test_finalization_timeout_eventually_closes_client_once(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = await _recorder(fake_client)
    started = asyncio.Event()
    closed = asyncio.Event()

    original_close = fake_client.close

    async def close() -> None:
        await original_close()
        closed.set()

    monkeypatch.setattr(
        fake_client.sessions, "ingest_nodes", partial(_blocking_ingest, started)
    )
    monkeypatch.setattr(fake_client, "close", close)
    monkeypatch.setattr(
        "kitaru_claude_agent_sdk.recording.FINALIZATION_TIMEOUT_SECONDS", 0.01
    )

    failure = await finalize_failure(recorder, RuntimeError("native failure"))

    assert isinstance(failure, TimeoutError)
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(closed.wait(), timeout=1)
    assert fake_client.close_count == 1


async def test_cancelling_terminal_finalization_propagates_and_closes_client(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = await _recorder(fake_client)
    started = asyncio.Event()

    monkeypatch.setattr(
        fake_client.sessions, "ingest_nodes", partial(_blocking_ingest, started)
    )
    task = asyncio.create_task(finalize_terminal(recorder, _terminal()))
    await asyncio.wait_for(started.wait(), timeout=1)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake_client.close_count == 1


async def test_cancelling_unfinished_child_finalization_closes_client(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        AssistantMessage(
            content=[ToolUseBlock("tool-1", "lookup", {"query": "refund"})],
            model="claude-test",
            message_id="message-1",
        )
    )
    started = asyncio.Event()

    monkeypatch.setattr(
        fake_client.sessions, "ingest_nodes", partial(_blocking_ingest, started)
    )
    task = asyncio.create_task(finalize_failure(recorder, RuntimeError("failed")))
    await asyncio.wait_for(started.wait(), timeout=1)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake_client.close_count == 1


async def test_cancelling_terminal_finalization_bounds_slow_cleanup(
    fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = await _recorder(fake_client)
    ingest_started = asyncio.Event()
    release_close = asyncio.Event()
    closed = asyncio.Event()

    async def slow_close() -> None:
        await release_close.wait()
        fake_client.close_count += 1
        closed.set()

    monkeypatch.setattr(
        fake_client.sessions,
        "ingest_nodes",
        partial(_blocking_ingest, ingest_started),
    )
    monkeypatch.setattr(fake_client, "close", slow_close)
    monkeypatch.setattr(
        "kitaru_claude_agent_sdk.recording.FINALIZATION_TIMEOUT_SECONDS", 0.01
    )
    task = asyncio.create_task(finalize_terminal(recorder, _terminal()))
    await asyncio.wait_for(ingest_started.wait(), timeout=1)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert fake_client.close_count == 0

    release_close.set()
    await asyncio.wait_for(closed.wait(), timeout=1)


async def test_post_success_finalization_failure_retains_terminal(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    terminal = _terminal()
    fake_client.update_error = OSError("persistence failed")

    with pytest.raises(KitaruRecordingError) as raised:
        await finalize_terminal(recorder, terminal)

    assert raised.value.terminal_message is terminal
    assert raised.value.session_id == fake_client.session_id
    assert "persistence failed" not in str(raised.value)


async def test_cancelled_and_explicit_close_finalize_only_once(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.finalize(error=asyncio.CancelledError())
    await recorder.finalize(error=GeneratorExit())

    assert len(fake_client.sessions.updated) == 1
    assert fake_client.sessions.updated[0][1].status is SessionStatus.FAILED
    assert fake_client.close_count == 1


async def test_failure_finalization_marks_open_tool_and_task_nodes_failed(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        AssistantMessage(
            content=[ToolUseBlock("tool-1", "lookup", {"query": "refund"})],
            model="claude-test",
            message_id="message-1",
        )
    )
    await recorder.record_message(
        TaskStartedMessage(
            subtype="task_started",
            data={},
            task_id="task-1",
            description="research",
            uuid="task-event-1",
            session_id="session-1",
            tool_use_id="tool-1",
            task_type="agent",
        )
    )

    await finalize_failure(recorder, GeneratorExit())

    latest = {node.index: node for node in nodes(fake_client)}
    child_nodes = [
        node
        for node in latest.values()
        if node.node_type in {NodeType.TOOL_CALL, NodeType.SUBAGENT_CALL}
    ]
    assert len(child_nodes) == 2
    assert all(node.status is NodeStatus.FAILED for node in child_nodes)
    assert all(node.error == "GeneratorExit" for node in child_nodes)
    assert all(node.ended_at is not None for node in child_nodes)


async def test_orphan_task_stop_after_cancellation_is_retained_under_root(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    await recorder.record_message(
        TaskNotificationMessage(
            subtype="task_notification",
            data={},
            task_id="orphan-task",
            status="stopped",
            output_file="",
            summary="cancelled",
            uuid="task-event",
            session_id="session-1",
        )
    )
    await recorder.finalize(error=asyncio.CancelledError())

    latest = {node.index: node for node in nodes(fake_client)}
    task = next(node for node in latest.values() if node.external_id == "orphan-task")
    assert task.parent_index == 0
    assert task.status is NodeStatus.FAILED


async def test_concurrent_recorders_keep_overlapping_ids_isolated() -> None:
    clients = [FakeClient(), FakeClient()]
    recorders = await asyncio.gather(*(_recorder(client) for client in clients))

    async def record(recorder: InvocationRecorder, value: str) -> None:
        await recorder.record_message(
            AssistantMessage(
                content=[ToolUseBlock("same-id", "lookup", {"value": value})],
                model="claude-test",
                message_id="same-message",
            )
        )
        await recorder.record_message(
            UserMessage(content=[ToolResultBlock("same-id", value)], uuid="result")
        )

    await asyncio.gather(record(recorders[0], "first"), record(recorders[1], "second"))

    outputs = []
    for client in clients:
        latest = {node.index: node for node in nodes(client)}
        outputs.append(
            next(
                node.outputs
                for node in latest.values()
                if node.node_type is NodeType.TOOL_CALL
            )
        )
    assert outputs == ["first", "second"]


def test_cost_is_decimal_safe() -> None:
    assert Decimal(str(_terminal().total_cost_usd)) == Decimal("0.25")
