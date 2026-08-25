#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Lifecycle and activity recording tests for the OpenAI runner adapter."""

import asyncio
import json
import uuid
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import AsyncMock

import pytest
from agents import (
    Agent,
    ModelResponse,
    RunConfig,
    RunContextWrapper,
    RunErrorDetails,
    RunHooks,
    RunItem,
    Runner,
    RunResult,
    TResponseInputItem,
    Usage,
)
from agents.items import HandoffOutputItem, ToolCallItem, ToolCallOutputItem
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

import kitaru_openai_agents.recording as recording_module
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru.client import KitaruAPIClient
from kitaru_openai_agents import KitaruRunner
from kitaru_openai_agents.recording import (
    KitaruRecordingError,
    RunRecorder,
    UnsupportedInterruptionError,
    finalize_failure,
)


class _FakeSessions:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client
        self.created: list[Any] = []
        self.updated: list[tuple[uuid.UUID, Any]] = []
        self.node_batches: list[tuple[uuid.UUID, Any]] = []

    async def create(self, request: Any) -> Any:
        if self._client.create_error is not None:
            raise self._client.create_error
        self.created.append(request)
        self._client.events.append("session:create")
        return SimpleNamespace(id=self._client.session_id)

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        if self._client.ingest_error is not None:
            error = self._client.ingest_error
            self._client.ingest_error = None
            raise error
        self.node_batches.append((session_id, request))
        self._client.events.append("nodes:ingest")
        return []

    async def update(self, session_id: uuid.UUID, request: Any) -> Any:
        self.updated.append((session_id, request))
        self._client.events.append(f"session:{request.status}")
        if self._client.update_error is not None:
            raise self._client.update_error
        return None


class _FakeTasks:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client

    async def get_spec(self, task_id: uuid.UUID) -> Any:
        self._client.events.append("task:get_spec")
        return SimpleNamespace(
            details=AgentTaskDetails(inputs=self._client.task_inputs)
        )


class _FakeReplays:
    def __init__(self, client: "_FakeClient") -> None:
        self._client = client

    async def get(self, replay_id: uuid.UUID) -> Any:
        self._client.events.append("replay:get")
        if self._client.replay_error is not None:
            raise self._client.replay_error
        return SimpleNamespace(id=replay_id)


class _FakeClient:
    instances: ClassVar[list["_FakeClient"]] = []
    next_task_inputs: ClassVar[Any] = "task fallback"
    next_create_error: ClassVar[BaseException | None] = None
    next_ingest_error: ClassVar[BaseException | None] = None
    next_update_error: ClassVar[BaseException | None] = None
    next_replay_error: ClassVar[BaseException | None] = None

    def __init__(self) -> None:
        self.session_id = uuid.uuid4()
        self.task_inputs = type(self).next_task_inputs
        self.create_error = type(self).next_create_error
        self.ingest_error = type(self).next_ingest_error
        self.update_error = type(self).next_update_error
        self.replay_error = type(self).next_replay_error
        self.events: list[str] = []
        self.sessions = _FakeSessions(self)
        self.tasks = _FakeTasks(self)
        self.replays = _FakeReplays(self)
        self.closed = False
        type(self).instances.append(self)

    async def close(self) -> None:
        if self.closed:
            raise AssertionError("client closed twice")
        self.closed = True
        self.events.append("client:close")


@pytest.fixture(autouse=True)
def _fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.instances.clear()
    _FakeClient.next_task_inputs = "task fallback"
    _FakeClient.next_create_error = None
    _FakeClient.next_ingest_error = None
    _FakeClient.next_update_error = None
    _FakeClient.next_replay_error = None
    for name in ("KITARU_TASK_INPUTS", "KITARU_REPLAY_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(recording_module, "KitaruAPIClient", _FakeClient)


def _nodes(client: _FakeClient) -> list[Any]:
    return [node for _, batch in client.sessions.node_batches for node in batch.nodes]


async def test_records_complete_session_and_model_node(
    deterministic_agent: Agent[None],
) -> None:
    result = await KitaruRunner(agent_id=uuid.uuid4()).run(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )
    client = _FakeClient.instances[0]
    nodes = _nodes(client)

    assert result.final_output == "deterministic result"
    assert [node.index for node in nodes] == [0, 1, 0]
    assert nodes[0].node_type is NodeType.SPAN
    assert nodes[0].status is NodeStatus.IN_PROGRESS
    assert nodes[1].node_type is NodeType.LLM_CALL
    assert nodes[1].parent_index == 0
    assert nodes[1].status is NodeStatus.COMPLETED
    assert nodes[2].node_type is NodeType.SPAN
    assert nodes[2].status is NodeStatus.COMPLETED
    assert nodes[2].outputs == "deterministic result"
    assert client.sessions.updated[-1][1].status is SessionStatus.COMPLETED
    assert client.events == [
        "session:create",
        "nodes:ingest",
        "nodes:ingest",
        "nodes:ingest",
        "session:completed",
        "client:close",
    ]


async def test_task_input_environment_wins_and_preserves_json(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    task_id = uuid.uuid4()
    configured_agent_id = uuid.uuid4()
    configured_agent_version_id = uuid.uuid4()
    payload = {"query": "hello", "limit": 3}
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps(payload))
    native_result = SimpleNamespace(
        raw_responses=[], new_items=[], final_output="done", interruptions=[]
    )
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)

    result = await KitaruRunner(
        agent_id=configured_agent_id,
        agent_version_id=configured_agent_version_id,
    ).run(deterministic_agent, "caller input")
    client = _FakeClient.instances[0]

    assert result is native_result
    assert client.sessions.created[0].inputs == payload
    assert client.sessions.created[0].agent_id is None
    assert client.sessions.created[0].agent_version_id is None
    native_run.assert_awaited_once()
    awaited = native_run.await_args
    assert awaited is not None
    assert awaited.args[1] == '{"limit":3,"query":"hello"}'


async def test_task_spec_is_input_fallback(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    _FakeClient.next_task_inputs = [{"role": "user", "content": "from task spec"}]
    native_result = SimpleNamespace(
        raw_responses=[], new_items=[], final_output="done", interruptions=[]
    )
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)

    await KitaruRunner().run(deterministic_agent, "caller input")
    client = _FakeClient.instances[0]

    assert client.events[0] == "task:get_spec"
    awaited = native_run.await_args
    assert awaited is not None
    assert awaited.args[1] is _FakeClient.next_task_inputs


async def test_invalid_task_environment_list_is_serialized_deterministically(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    monkeypatch.setenv("KITARU_TASK_INPUTS", '[{"z":2,"a":1}]')
    native_result = SimpleNamespace(
        raw_responses=[], new_items=[], final_output="done", interruptions=[]
    )
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)

    await KitaruRunner().run(deterministic_agent, "caller input")

    awaited = native_run.await_args
    assert awaited is not None
    assert awaited.args[1] == '[{"a":1,"z":2}]'


async def test_invalid_task_spec_list_is_serialized_deterministically(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    _FakeClient.next_task_inputs = [{"z": 2, "a": 1}]
    native_result = SimpleNamespace(
        raw_responses=[], new_items=[], final_output="done", interruptions=[]
    )
    native_run = AsyncMock(return_value=native_result)
    monkeypatch.setattr(Runner, "run", native_run)

    await KitaruRunner().run(deterministic_agent, "caller input")

    awaited = native_run.await_args
    assert awaited is not None
    assert awaited.args[1] == '[{"a":1,"z":2}]'


async def test_task_json_rejects_non_finite_numbers(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    monkeypatch.setenv("KITARU_TASK_INPUTS", '{"value":NaN}')
    native_run = AsyncMock()
    monkeypatch.setattr(Runner, "run", native_run)

    with pytest.raises(ValueError, match="Out of range float values"):
        await KitaruRunner().run(deterministic_agent, "caller input")

    assert _FakeClient.instances[0].sessions.created == []
    native_run.assert_not_awaited()


async def test_observer_runs_after_root_ingest(
    deterministic_agent: Agent[None],
) -> None:
    observed: list[tuple[uuid.UUID, list[str]]] = []

    async def observe(session: Any) -> None:
        client = _FakeClient.instances[0]
        observed.append((session.id, list(client.events)))

    await KitaruRunner(agent_id=uuid.uuid4(), session_observer=observe).run(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )

    assert observed[0][1] == ["session:create", "nodes:ingest"]


async def test_sync_observer_runs_after_root_ingest(
    deterministic_agent: Agent[None],
) -> None:
    observed: list[tuple[uuid.UUID, list[str]]] = []

    def observe(session: Any) -> None:
        client = _FakeClient.instances[0]
        observed.append((session.id, list(client.events)))

    await KitaruRunner(agent_id=uuid.uuid4(), session_observer=observe).run(
        deterministic_agent,
        "hello",
        run_config=RunConfig(tracing_disabled=True),
    )

    assert observed[0][1] == ["session:create", "nodes:ingest"]


async def test_concurrent_runs_on_one_agent_keep_sessions_isolated(
    deterministic_agent: Agent[None],
) -> None:
    first, second = await asyncio.gather(
        KitaruRunner(agent_id=uuid.uuid4()).run(
            deterministic_agent,
            "first",
            run_config=RunConfig(tracing_disabled=True),
        ),
        KitaruRunner(agent_id=uuid.uuid4()).run(
            deterministic_agent,
            "second",
            run_config=RunConfig(tracing_disabled=True),
        ),
    )

    assert first.final_output == second.final_output == "deterministic result"
    assert len(_FakeClient.instances) == 2
    assert len({client.session_id for client in _FakeClient.instances}) == 2
    for client in _FakeClient.instances:
        assert [node.index for node in _nodes(client)] == [0, 1, 0]
        assert client.closed


async def test_replay_fetch_failure_precedes_session_and_model(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    replay_error = PermissionError("not authorized")
    _FakeClient.next_replay_error = replay_error
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    native_run = AsyncMock()
    monkeypatch.setattr(Runner, "run", native_run)

    with pytest.raises(PermissionError) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is replay_error
    client = _FakeClient.instances[0]
    assert client.sessions.created == []
    assert client.closed
    native_run.assert_not_awaited()


async def test_native_failure_remains_primary_when_failure_update_fails(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_error = RuntimeError("native failure")
    _FakeClient.next_update_error = OSError("recording failure")
    monkeypatch.setattr(Runner, "run", AsyncMock(side_effect=native_error))

    with pytest.raises(RuntimeError) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value is native_error
    assert _FakeClient.instances[0].closed


async def test_finalize_failure_returns_secondary_persistence_error() -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    persistence_error = OSError("recording failure")
    client.update_error = persistence_error

    returned = await finalize_failure(recorder, RuntimeError("native failure"))

    assert returned is persistence_error


async def test_post_success_recording_failure_retains_native_result(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_result = SimpleNamespace(
        new_items=[], raw_responses=[], final_output="done", interruptions=[]
    )
    monkeypatch.setattr(Runner, "run", AsyncMock(return_value=native_result))
    _FakeClient.next_update_error = OSError("cannot finalize")

    with pytest.raises(KitaruRecordingError) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value.result is native_result
    assert raised.value.session_id == _FakeClient.instances[0].session_id
    assert raised.value.phase == "finalize"
    assert raised.value.retry_safe is False
    assert raised.value.side_effects_possible is True


async def test_interruption_fails_closed_and_retains_result(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    native_result = SimpleNamespace(
        new_items=[],
        raw_responses=[],
        final_output=None,
        interruptions=[object()],
    )
    monkeypatch.setattr(Runner, "run", AsyncMock(return_value=native_result))

    with pytest.raises(UnsupportedInterruptionError) as raised:
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    assert raised.value.result is native_result
    update = _FakeClient.instances[0].sessions.updated[-1][1]
    assert update.status is SessionStatus.FAILED


async def test_cancellation_fails_session_and_closes_client(
    deterministic_agent: Agent[None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Runner, "run", AsyncMock(side_effect=asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await KitaruRunner(agent_id=uuid.uuid4()).run(deterministic_agent, "hello")

    client = _FakeClient.instances[0]
    assert client.sessions.updated[-1][1].status is SessionStatus.FAILED
    assert client.closed


async def test_fail_propagates_persistence_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    ingest = AsyncMock(side_effect=asyncio.CancelledError())
    update = AsyncMock()
    monkeypatch.setattr(client.sessions, "ingest_nodes", ingest)
    monkeypatch.setattr(client.sessions, "update", update)

    with pytest.raises(asyncio.CancelledError):
        await recorder.fail(RuntimeError("native failure"))

    update.assert_not_awaited()


@pytest.mark.parametrize("failed_call", [1, 2])
async def test_flush_restores_all_nodes_after_a_batch_failure(
    failed_call: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=2)
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    for index in range(3):
        await recorder._append_node(
            node_type=NodeType.SPAN,
            name=f"observation-{index}",
            parent_index=0,
            external_id=None,
            started_at=None,
            ended_at=None,
            inputs=None,
            outputs=None,
        )

    ingest_nodes = client.sessions.ingest_nodes
    call_count = 0

    async def fail_one_batch(session_id: uuid.UUID, request: Any) -> list[Any]:
        nonlocal call_count
        call_count += 1
        if call_count == failed_call:
            raise OSError("batch failed")
        return await ingest_nodes(session_id, request)

    monkeypatch.setattr(client.sessions, "ingest_nodes", fail_one_batch)
    with pytest.raises(OSError, match="batch failed"):
        await recorder.flush()

    assert [node.index for node in recorder.buffer] == [1, 2, 3]
    monkeypatch.setattr(client.sessions, "ingest_nodes", ingest_nodes)
    await recorder.flush()
    assert recorder.buffer == []


async def test_close_can_retry_after_a_client_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    close = AsyncMock(side_effect=[OSError("close failed"), None])
    monkeypatch.setattr(client, "close", close)

    with pytest.raises(OSError, match="close failed"):
        await recorder.close()
    assert recorder.closed is False

    await recorder.close()
    await recorder.close()
    assert recorder.closed is True
    assert close.await_count == 2


async def test_second_cancellation_boundedly_abandons_stuck_failure_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def ingest_nodes(session_id: uuid.UUID, request: Any) -> list[Any]:
        del session_id, request
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return []

    monkeypatch.setattr(client.sessions, "ingest_nodes", ingest_nodes)
    monkeypatch.setattr(recording_module, "FINALIZATION_TIMEOUT_SECONDS", 0.01)
    finalizing = asyncio.create_task(
        finalize_failure(recorder, asyncio.CancelledError())
    )
    await started.wait()

    finalizing.cancel()
    result = await asyncio.wait_for(finalizing, timeout=0.2)

    assert isinstance(result, asyncio.CancelledError)
    assert cancelled.is_set()
    release.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_reconciles_tools_hosted_calls_and_handoffs_by_public_ids() -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    source = Agent(name="source")
    target = Agent(name="target")
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    response = ModelResponse(
        output=[
            ResponseFunctionToolCall(
                arguments='{"city":"Paris"}',
                call_id="call-1",
                name="weather",
                type="function_call",
                id="item-1",
                status="completed",
            ),
            ResponseOutputMessage(
                id="message-1",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text="done",
                        type="output_text",
                        logprobs=[],
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            ),
        ],
        usage=Usage(input_tokens=2, output_tokens=3, total_tokens=5),
        response_id="response-1",
    )
    items: list[RunItem] = [
        ToolCallItem(
            agent=source,
            raw_item=ResponseFunctionToolCall(
                arguments='{"city":"Paris"}',
                call_id="call-1",
                name="weather",
                type="function_call",
                id="item-1",
                status="completed",
            ),
        ),
        ToolCallOutputItem(
            agent=source,
            raw_item={
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "sunny",
            },
            output={"forecast": "sunny"},
        ),
        ToolCallItem(
            agent=source,
            raw_item={
                "type": "file_search_call",
                "id": "hosted-1",
                "status": "completed",
            },
        ),
        HandoffOutputItem(
            agent=target,
            raw_item=cast(TResponseInputItem, {"type": "handoff_output_item"}),
            source_agent=source,
            target_agent=target,
        ),
    ]

    await recorder.reconcile(
        cast(
            RunResult,
            SimpleNamespace(
                raw_responses=[response],
                new_items=items,
                final_output="done",
            ),
        )
    )
    await recorder.complete("done")
    nodes = _nodes(client)

    assert [node.node_type for node in nodes] == [
        NodeType.SPAN,
        NodeType.LLM_CALL,
        NodeType.TOOL_CALL,
        NodeType.TOOL_CALL,
        NodeType.SUBAGENT_CALL,
        NodeType.SPAN,
    ]
    tool = nodes[2]
    assert tool.external_id == "call-1"
    assert tool.inputs == {"city": "Paris"}
    assert tool.outputs == {"forecast": "sunny"}
    assert nodes[3].external_id == "hosted-1"
    assert nodes[4].subagent_id == "target"
    assert nodes[1].parent_index == 0
    assert nodes[2].parent_index == 1
    assert nodes[3].parent_index == 0
    assert nodes[4].parent_index == 0
    assert nodes[5].status is NodeStatus.COMPLETED
    for node in nodes[1:5]:
        assert node.started_at is None
        assert node.ended_at is None


def test_records_user_fields_named_like_capture_metadata() -> None:
    agent = Agent(name="source")
    inputs, attributes = recording_module._capture_tool_input(
        ToolCallItem(
            agent=agent,
            raw_item=ResponseFunctionToolCall(
                arguments=(
                    '{"_kitaru_truncated":false,'
                    '"_kitaru_unsupported_type":"application_value",'
                    '"value":1}'
                ),
                call_id="call-1",
                name="lookup",
                type="function_call",
                id="item-1",
                status="completed",
            ),
        )
    )

    assert inputs == {
        "_kitaru_truncated": False,
        "_kitaru_unsupported_type": "application_value",
        "value": 1,
    }
    assert attributes == {}


def test_lone_surrogate_tool_argument_is_recorded_as_capture_loss() -> None:
    agent = Agent(name="source")
    inputs, attributes = recording_module._capture_tool_input(
        ToolCallItem(
            agent=agent,
            raw_item=ResponseFunctionToolCall(
                arguments=r'{"value":"\ud800"}',
                call_id="call-1",
                name="lookup",
                type="function_call",
                id="item-1",
                status="completed",
            ),
        )
    )

    assert inputs is None
    assert attributes == {"kitaru.tool_arguments": "capture_loss"}


async def test_same_name_tool_calls_match_outputs_and_parents_by_call_id() -> None:
    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    agent = Agent(name="source")
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )

    def raw_call(call_id: str, item_id: str) -> ResponseFunctionToolCall:
        return ResponseFunctionToolCall(
            arguments="{}",
            call_id=call_id,
            name="lookup",
            type="function_call",
            id=item_id,
            status="completed",
        )

    responses = [
        ModelResponse(
            output=[raw_call("call-1", "item-1")],
            usage=Usage(input_tokens=1, output_tokens=1, total_tokens=2),
            response_id="response-1",
        ),
        ModelResponse(
            output=[raw_call("call-2", "item-2")],
            usage=Usage(input_tokens=1, output_tokens=1, total_tokens=2),
            response_id="response-2",
        ),
    ]
    items: list[RunItem] = [
        ToolCallItem(agent=agent, raw_item=raw_call("call-1", "item-1")),
        ToolCallItem(agent=agent, raw_item=raw_call("call-2", "item-2")),
        ToolCallOutputItem(
            agent=agent,
            raw_item={"type": "function_call_output", "call_id": "call-2"},
            output="second",
        ),
        ToolCallOutputItem(
            agent=agent,
            raw_item={"type": "function_call_output", "call_id": "call-1"},
            output="first",
        ),
    ]
    run_error = RunErrorDetails(
        input="hello",
        new_items=items,
        raw_responses=responses,
        last_agent=agent,
        context_wrapper=RunContextWrapper(context=None),
        input_guardrail_results=[],
        output_guardrail_results=[],
    )

    await recorder.reconcile(run_error)
    await recorder.flush()
    nodes = _nodes(client)
    tool_nodes = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]

    assert [
        (node.external_id, node.outputs, node.parent_index) for node in tool_nodes
    ] == [
        ("call-1", "first", 1),
        ("call-2", "second", 2),
    ]
    assert all(node.started_at is None for node in tool_nodes)
    assert all(node.ended_at is None for node in tool_nodes)


async def test_capture_is_bounded_and_never_serializes_hostile_objects() -> None:
    class Hostile:
        def __str__(self) -> str:
            raise AssertionError("str called")

        def __repr__(self) -> str:
            raise AssertionError("repr called")

    client = _FakeClient()
    recorder = RunRecorder(client=cast(KitaruAPIClient, client), batch_size=20)
    agent = Agent(name="source")
    await recorder.start(
        inputs="hello",
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        replay=False,
    )
    item = ToolCallOutputItem(
        agent=agent,
        raw_item={"type": "function_call_output", "call_id": "call-1"},
        output={
            "large": "x" * 20_000,
            "deep": [[[[[[[[[["end"]]]]]]]]]],
            "many": {str(index): index for index in range(101)},
            "hostile": Hostile(),
        },
    )
    call = ToolCallItem(
        agent=agent,
        raw_item=ResponseFunctionToolCall(
            arguments="{}",
            call_id="call-1",
            name="hostile",
            type="function_call",
            id="item-1",
            status="completed",
        ),
    )

    await recorder.reconcile(
        cast(
            RunResult,
            SimpleNamespace(
                raw_responses=[], new_items=[call, item], final_output=None
            ),
        )
    )
    await recorder.complete(None)
    output = _nodes(client)[1].outputs

    assert output["large"]["_kitaru_truncated"]["reason"] == "max_characters"
    assert output["deep"][0][0][0][0][0][0][0]["_kitaru_truncated"]
    assert output["many"]["_kitaru_truncated"] == {
        "reason": "max_items_or_non_string_keys",
        "omitted": 1,
    }
    assert output["hostile"] == {
        "_kitaru_unsupported_type": f"{Hostile.__module__}.{Hostile.__qualname__}"
    }


async def test_composed_hooks_forward_caller_events_once(
    deterministic_agent: Agent[None],
) -> None:
    events: list[str] = []

    class CallerHooks(RunHooks[None]):
        async def on_llm_start(
            self,
            context: Any,
            agent: Any,
            system_prompt: str | None,
            input_items: list[Any],
        ) -> None:
            events.append("llm:start")

        async def on_llm_end(
            self, context: Any, agent: Any, response: ModelResponse
        ) -> None:
            events.append("llm:end")

    await KitaruRunner(agent_id=uuid.uuid4()).run(
        deterministic_agent,
        "hello",
        hooks=CallerHooks(),
        run_config=RunConfig(tracing_disabled=True),
    )

    assert events == ["llm:start", "llm:end"]
