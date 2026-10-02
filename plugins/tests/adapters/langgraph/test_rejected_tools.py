"""Tool results returned by middleware remain visible in recorded traces."""

import uuid
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from deepagents import create_deep_agent
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import tool

from kitaru.api_models.v1.replay_config import (
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru_langgraph import KitaruGraphRunner
from kitaru_langgraph.codec import decode_tool_outcome, encode_tool_outcome


class ToolCallingFakeModel(FakeMessagesListChatModel):
    def bind_tools(self, tools: Any, **kwargs: Any) -> Any:
        return self


class RejectSecondCall(AgentMiddleware):
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Any],
    ) -> Any:
        if request.tool_call["id"] == "call-2":
            return ToolMessage(
                content="The second attempt was rejected.",
                tool_call_id="call-2",
                name=request.tool_call["name"],
                status="error",
            )
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Any],
    ) -> Any:
        if request.tool_call["id"] == "call-2":
            return ToolMessage(
                content="The second attempt was rejected.",
                tool_call_id="call-2",
                name=request.tool_call["name"],
                status="error",
            )
        return await handler(request)


def _tool_nodes(fake_client: Any) -> list[Any]:
    return [
        node
        for _, batch in fake_client.instances[0].sessions.node_batches
        for node in batch.nodes
        if node.node_type is NodeType.TOOL_CALL
    ]


def _assert_recorded_results(
    fake_client: Any, messages: list[Any], calls: list[dict[str, Any]]
) -> None:
    nodes = _tool_nodes(fake_client)
    assert len(nodes) == len(calls)
    results = {
        message.tool_call_id: message
        for message in messages
        if isinstance(message, ToolMessage)
    }
    for call in calls:
        node = next(node for node in nodes if node.inputs == call["args"])
        assert node.name == call["name"]
        assert node.status is NodeStatus.COMPLETED
        assert node.error is None
        decoded = decode_tool_outcome(
            node.outputs, tool_call_id=call["id"], tool_name=call["name"]
        )
        assert isinstance(decoded, ToolMessage)
        assert decoded.content == results[call["id"]].content
        assert decoded.status == results[call["id"]].status
        if decoded.status == "error":
            assert node.attributes["execution"] == "short_circuited"


@pytest.mark.parametrize("sync", [True, False])
async def test_middleware_rejection_records_only_current_attempts(
    fake_client: Any, sync: bool
) -> None:
    executed: list[str] = []

    @tool
    def lookup(value: str) -> str:
        """Look up a value."""
        executed.append(value)
        return value

    calls = [
        {"name": "lookup", "args": {"value": "first"}, "id": "call-1"},
        {"name": "lookup", "args": {"value": "second"}, "id": "call-2"},
    ]
    model = ToolCallingFakeModel(
        responses=[AIMessage(content="", tool_calls=calls), AIMessage(content="done")]
    )
    runner = KitaruGraphRunner.from_agent_factory(
        create_agent,
        factory_kwargs={
            "model": model,
            "tools": [lookup],
            "middleware": [RejectSecondCall()],
        },
    )
    historical_call = {
        "name": "lookup",
        "args": {"value": "historical"},
        "id": "old-call",
    }
    inputs = {
        "messages": [
            HumanMessage(content="Earlier request"),
            AIMessage(content="", tool_calls=[historical_call]),
            ToolMessage(
                content="Earlier rejection", tool_call_id="old-call", status="error"
            ),
            HumanMessage(content="Look up first and second"),
        ]
    }
    result = runner.invoke(inputs) if sync else await runner.ainvoke(inputs)

    assert executed == ["first"]
    assert result["messages"][-1].content == "done"
    _assert_recorded_results(fake_client, result["messages"], calls)


@pytest.mark.parametrize("sync", [True, False])
@pytest.mark.parametrize("same_path", [True, False])
async def test_deep_agent_records_repeated_file_write_attempts(
    fake_client: Any, sync: bool, same_path: bool
) -> None:
    calls = [
        {
            "name": "write_file",
            "args": {"file_path": "/first.txt", "content": "first"},
            "id": "call-1",
        },
        {
            "name": "write_file",
            "args": {
                "file_path": "/first.txt" if same_path else "/second.txt",
                "content": "second",
            },
            "id": "call-2",
        },
    ]
    model = ToolCallingFakeModel(
        responses=[
            AIMessage(content="", tool_calls=calls),
            AIMessage(content="done"),
        ]
    )
    runner = KitaruGraphRunner.from_agent_factory(
        create_deep_agent, factory_kwargs={"model": model}
    )
    inputs = {"messages": [HumanMessage(content="Write the files")]}
    result = runner.invoke(inputs) if sync else await runner.ainvoke(inputs)

    assert result["messages"][-1].content == "done"
    _assert_recorded_results(fake_client, result["messages"], calls)
    if not same_path:
        assert all(
            message.status == "success"
            for message in result["messages"]
            if isinstance(message, ToolMessage)
        )


@pytest.mark.parametrize("sync", [True, False])
async def test_static_error_result_is_recorded_once(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch, sync: bool
) -> None:
    @tool
    def lookup(value: str) -> str:
        """Look up a value."""
        pytest.fail("Static substitution must not execute the tool")

    fake_client.next_replay = SimpleNamespace(
        override=None,
        tool_policy=ToolPolicy(
            default=StaticConfig(
                cases=[
                    StaticCase(
                        match={"value": "first"},
                        match_mode=StaticMatchMode.EXACT,
                        result=encode_tool_outcome(
                            ToolMessage(
                                content="Stored rejection",
                                tool_call_id="stored-call",
                                status="error",
                            )
                        ),
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            )
        ),
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    call = {"name": "lookup", "args": {"value": "first"}, "id": "call-1"}
    model = ToolCallingFakeModel(
        responses=[AIMessage(content="", tool_calls=[call]), AIMessage(content="done")]
    )
    runner = KitaruGraphRunner.from_agent_factory(
        create_agent, factory_kwargs={"model": model, "tools": [lookup]}
    )
    inputs = {"messages": [HumanMessage(content="Look up first")]}
    result = runner.invoke(inputs) if sync else await runner.ainvoke(inputs)

    assert result["messages"][-1].content == "done"
    nodes = _tool_nodes(fake_client)
    assert len(nodes) == 1
    assert nodes[0].attributes["policy"] == "static"
    decoded = decode_tool_outcome(
        nodes[0].outputs, tool_call_id="call-1", tool_name="lookup"
    )
    assert isinstance(decoded, ToolMessage)
    assert decoded.content == "Stored rejection"
    assert decoded.status == "error"
    assert nodes[0].status is NodeStatus.COMPLETED


@pytest.mark.parametrize("sync", [True, False])
async def test_reused_native_call_id_records_each_rejected_attempt(
    fake_client: Any, sync: bool
) -> None:
    @tool
    def lookup(value: str) -> str:
        """Look up a value."""
        pytest.fail("Rejected calls must not execute the tool")

    calls = [
        {"name": "lookup", "args": {"value": "first"}, "id": "call-2"},
        {"name": "lookup", "args": {"value": "second"}, "id": "call-2"},
    ]
    model = ToolCallingFakeModel(
        responses=[
            AIMessage(content="", tool_calls=[calls[0]]),
            AIMessage(content="", tool_calls=[calls[1]]),
            AIMessage(content="done"),
        ]
    )
    runner = KitaruGraphRunner.from_agent_factory(
        create_agent,
        factory_kwargs={
            "model": model,
            "tools": [lookup],
            "middleware": [RejectSecondCall()],
        },
    )
    inputs = {"messages": [HumanMessage(content="Look up first and second")]}
    result = runner.invoke(inputs) if sync else await runner.ainvoke(inputs)

    _assert_recorded_results(fake_client, result["messages"], calls)
    nodes = _tool_nodes(fake_client)
    assert len({node.external_id for node in nodes}) == 2


@pytest.mark.parametrize("sync", [True, False])
async def test_rejected_attempt_redacts_arguments_and_disables_replay(
    fake_client: Any, sync: bool
) -> None:
    @tool
    def lookup(value: str, api_key: str) -> str:
        """Look up a value with a credential."""
        pytest.fail("Rejected calls must not execute the tool")

    call = {
        "name": "lookup",
        "args": {"value": "first", "api_key": "private-credential"},
        "id": "call-2",
    }
    model = ToolCallingFakeModel(
        responses=[AIMessage(content="", tool_calls=[call]), AIMessage(content="done")]
    )
    runner = KitaruGraphRunner.from_agent_factory(
        create_agent,
        factory_kwargs={
            "model": model,
            "tools": [lookup],
            "middleware": [RejectSecondCall()],
        },
    )
    inputs = {"messages": [HumanMessage(content="Look up first")]}
    result = runner.invoke(inputs) if sync else await runner.ainvoke(inputs)

    assert result["messages"][-1].content == "done"
    nodes = _tool_nodes(fake_client)
    assert len(nodes) == 1
    assert nodes[0].inputs == {"value": "first", "api_key": "[REDACTED]"}
    assert nodes[0].outputs["replayable"] is False
    assert "private-credential" not in str(nodes[0].model_dump())
    assert call["args"]["api_key"] == "private-credential"


@pytest.mark.parametrize("sync", [True, False])
async def test_nested_tool_batches_record_rejection_once(
    fake_client: Any, sync: bool
) -> None:
    call = {
        "type": "tool_call",
        "name": "lookup",
        "args": {"value": "first"},
        "id": "call-1",
    }
    rejection = ToolMessage(
        content="Rejected by inner middleware",
        name="lookup",
        tool_call_id="call-1",
        status="error",
    )
    inner = RunnableLambda(lambda _: {"messages": [rejection]})

    def invoke_inner(inputs: Any, config: RunnableConfig) -> Any:
        return inner.invoke(inputs, config)

    async def ainvoke_inner(inputs: Any, config: RunnableConfig) -> Any:
        return await inner.ainvoke(inputs, config)

    runner = KitaruGraphRunner(RunnableLambda(invoke_inner, afunc=ainvoke_inner))
    result = runner.invoke([call]) if sync else await runner.ainvoke([call])

    assert result["messages"][0] is rejection
    _assert_recorded_results(fake_client, result["messages"], [call])
