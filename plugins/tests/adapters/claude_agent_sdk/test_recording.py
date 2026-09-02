#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Run-local recording tests for the Claude Agent SDK adapter."""

import asyncio
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal
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


async def test_terminal_result_and_error_are_bounded(
    fake_client: FakeClient,
) -> None:
    recorder = await _recorder(fake_client)
    terminal = _terminal()
    terminal.result = "x" * (16 * 1024 + 1)

    await finalize_terminal(recorder, terminal)

    root = {node.index: node for node in nodes(fake_client)}[0]
    assert root.outputs == {"value": "x" * (16 * 1024), "truncated": True}
    assert fake_client.sessions.updated[-1][1].outputs == root.outputs

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

    async def slow_ingest(*_: Any) -> list[Any]:
        started.set()
        await asyncio.Event().wait()
        return []

    original_close = fake_client.close

    async def close() -> None:
        await original_close()
        closed.set()

    monkeypatch.setattr(fake_client.sessions, "ingest_nodes", slow_ingest)
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

    async def slow_ingest(*_: Any) -> list[Any]:
        started.set()
        await asyncio.Event().wait()
        return []

    monkeypatch.setattr(fake_client.sessions, "ingest_nodes", slow_ingest)
    task = asyncio.create_task(finalize_terminal(recorder, _terminal()))
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

    async def slow_ingest(*_: Any) -> list[Any]:
        ingest_started.set()
        await asyncio.Event().wait()
        return []

    async def slow_close() -> None:
        await release_close.wait()
        fake_client.close_count += 1
        closed.set()

    monkeypatch.setattr(fake_client.sessions, "ingest_nodes", slow_ingest)
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
