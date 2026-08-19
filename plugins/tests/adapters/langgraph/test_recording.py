"""Callback recording and ancestry contracts."""

import json
import uuid
from typing import Any

import pytest
from langchain_core.outputs import LLMResult

from kitaru.api_models.v1.session_node import NodeType
from kitaru.client.exceptions import APIError
from kitaru_langgraph.callbacks import AsyncKitaruCallback
from kitaru_langgraph.capture import CapturePolicy
from kitaru_langgraph.recording import InvocationRecorder


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


async def test_setup_recovers_existing_result_session_on_conflict(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recover the task's result session when the create call 409s."""
    task_id = uuid.uuid4()
    result_session_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_TASK_ID", str(task_id))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps("hello"))
    fake_client.next_create_error = APIError(
        409, f"Task {task_id} already links a result session"
    )
    fake_client.next_result_session_id = result_session_id

    recorder = await InvocationRecorder.setup(
        {"input": True},
        None,
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        session_name=None,
        batch_size=1,
        policy=CapturePolicy(),
    )

    assert recorder.session_id == result_session_id
