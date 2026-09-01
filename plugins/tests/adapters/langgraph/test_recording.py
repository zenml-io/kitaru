"""Callback recording and ancestry contracts."""

import uuid
from typing import Any

from langchain_core.messages import ToolMessage
from langchain_core.outputs import LLMResult
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import tool

from kitaru.api_models.v1.session_node import NodeType
from kitaru_langgraph import KitaruGraphRunner
from kitaru_langgraph.callbacks import AsyncKitaruCallback
from kitaru_langgraph.capture import CapturePolicy
from kitaru_langgraph.recording import InvocationRecorder


async def test_key_loss_in_recorded_copy_preserves_native_tool_result(
    fake_client: Any,
) -> None:
    artifact = {1: "first", "1": "second"}
    native_results: list[ToolMessage] = []

    @tool(response_format="content_and_artifact")
    def lookup() -> tuple[str, dict[Any, str]]:
        """Return a mapping with distinct native keys."""
        return "done", artifact

    async def run(_: Any, config: RunnableConfig) -> ToolMessage:
        result = await lookup.ainvoke(
            {"name": "lookup", "args": {}, "id": "call-1", "type": "tool_call"},
            config,
        )
        assert isinstance(result, ToolMessage)
        native_results.append(result)
        return result

    returned = await KitaruGraphRunner(RunnableLambda(run)).ainvoke("hello")

    assert returned is native_results[0]
    assert returned.artifact == {1: "first", "1": "second"}
    assert artifact == {1: "first", "1": "second"}
    client = fake_client.instances[0]
    nodes = [node for _, batch in client.sessions.node_batches for node in batch.nodes]
    recorded = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)
    assert recorded.outputs["replayable"] is False
    assert set(recorded.outputs["loss_reasons"]) == {
        "non_string_key",
        "key_collision",
    }


async def test_nested_ancestor_is_persisted_before_child(fake_client: Any) -> None:
    recorder = await InvocationRecorder.setup(
        {"input": True},
        None,
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        batch_size=1,
        policy=CapturePolicy(),
    )
    callback = AsyncKitaruCallback(recorder)
    root_id = uuid.uuid4()
    nested_id = uuid.uuid4()
    model_id = uuid.uuid4()

    await callback.on_chain_start({}, {}, run_id=root_id)
    await callback.on_chain_start(
        {"name": "nested"}, {}, run_id=nested_id, parent_run_id=root_id
    )
    await callback.on_llm_start(
        {"name": "model"}, ["prompt"], run_id=model_id, parent_run_id=nested_id
    )
    await callback.on_llm_end(
        LLMResult(generations=[]),
        run_id=model_id,
        parent_run_id=nested_id,
    )
    await callback.on_chain_end({}, run_id=nested_id, parent_run_id=root_id)
    await recorder.finalize(result={"done": True})

    client = fake_client.instances[0]
    nodes = [node for _, batch in client.sessions.node_batches for node in batch.nodes]
    nested = next(node for node in nodes if node.name == "nested")
    model = next(node for node in nodes if node.node_type is NodeType.LLM_CALL)
    assert nested.index < model.index
    assert model.parent_index == nested.index
    assert all(
        node.parent_index is None or node.parent_index < node.index for node in nodes
    )


async def test_callback_failures_preserve_error_text(fake_client: Any) -> None:
    """Persist useful messages for failed chains and tool calls."""
    recorder = await InvocationRecorder.setup(
        {"input": True},
        None,
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        batch_size=1,
        policy=CapturePolicy(),
    )
    callback = AsyncKitaruCallback(recorder)
    root_id = uuid.uuid4()
    nested_id = uuid.uuid4()
    tool_id = uuid.uuid4()

    await callback.on_chain_start({}, {}, run_id=root_id)
    await callback.on_chain_start(
        {"name": "nested"}, {}, run_id=nested_id, parent_run_id=root_id
    )
    await callback.on_chain_error(RuntimeError("chain failed"), run_id=nested_id)
    await callback.on_tool_start(
        {"name": "weather"},
        "{}",
        run_id=tool_id,
        parent_run_id=root_id,
    )
    await callback.on_tool_error(RuntimeError("service down"), run_id=tool_id)
    await recorder.finalize(error=RuntimeError("graph failed"))

    client = fake_client.instances[0]
    nodes = [node for _, batch in client.sessions.node_batches for node in batch.nodes]
    nested = next(node for node in reversed(nodes) if node.name == "nested")
    tool = next(node for node in reversed(nodes) if node.name == "weather")
    root = next(node for node in reversed(nodes) if node.index == 0)
    assert nested.error == "chain failed"
    assert tool.error == "service down"
    assert root.error == "graph failed"
    assert client.sessions.updated[-1][1].error == "graph failed"
