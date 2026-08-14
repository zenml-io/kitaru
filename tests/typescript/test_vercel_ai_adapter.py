"""Cross-language proof for the compiled Vercel AI SDK support-triage example."""

import asyncio
import importlib
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import uvicorn

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session_node import NodeType, SessionNodeResponse
from kitaru.server.api.app import create_app
from kitaru.server.database.service import DatabaseService

EXPECTED_EVALUATIONS = {
    "decision_structure",
    "side_effect_safety",
    "trace_completeness",
}
EXPECTED_TOOLS = {"lookupAccount", "lookupOrder", "queueRefundReview"}
REQUESTED_MODEL_ID = "openai/gpt-5-nano"


@pytest.mark.parametrize(
    "outputs", [None, {}, {"text": None}, {"text": ""}, {"text": "   "}]
)
def test_vercel_demo_rejects_a_baseline_without_text(outputs: object) -> None:
    """Do not replay a baseline that produced no usable final answer."""
    demo = importlib.import_module("v2_examples.vercel_ai_support_triage.demo")

    with pytest.raises(RuntimeError, match="baseline session"):
        demo._require_nonempty_text(outputs)


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@asynccontextmanager
async def _network_server() -> AsyncIterator[str]:
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings)
    port = _available_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            host="127.0.0.1",
            port=port,
            lifespan="on",
            log_level="error",
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        task.result()
                    await asyncio.sleep(0.01)
        except TimeoutError as exc:
            raise RuntimeError("Timed out starting the Kitaru test server") from exc
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task
        await drop_test_database(settings)


def _tool_names(nodes: list[SessionNodeResponse]) -> set[str | None]:
    return {node.tool_name for node in nodes if node.node_type is NodeType.TOOL_CALL}


async def test_worker_records_and_history_replays_compiled_vercel_ai(
    tmp_path: Path,
) -> None:
    """Prove Worker, AI SDK, history replay, overrides, and scoring.

    The deterministic model rejects calls unless the task prompt, replay
    instructions, and max-output-token override reach AI SDK execution.
    """
    repo_root = Path(__file__).resolve().parents[2]
    for package in (
        "@zenml-io/kitaru",
        "@zenml-io/kitaru-vercel-ai",
        "@zenml-io/kitaru-example-vercel-ai-support-triage",
    ):
        subprocess.run(
            ["pnpm", "--filter", package, "build"],
            cwd=repo_root,
            check=True,
        )
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    demo = importlib.import_module("v2_examples.vercel_ai_support_triage.demo")

    async with _network_server() as api_url:
        result = await demo.run_demo(
            api_url=api_url,
            state_dir=tmp_path,
            test_model=True,
        )
        rerun = await demo.run_demo(
            api_url=api_url,
            state_dir=tmp_path,
            test_model=True,
        )

    assert result.initial_outbox_count == 1
    assert result.replay_outbox_count == 1
    assert result.replay.status is ReplayStatus.COMPLETED
    assert rerun.replay.status is ReplayStatus.COMPLETED
    assert rerun.initial_outbox_count == 1
    assert rerun.replay_outbox_count == 1
    assert result.replay.result_session_id is not None
    assert _tool_names(result.initial_nodes) == EXPECTED_TOOLS
    assert _tool_names(result.replay_nodes) == EXPECTED_TOOLS

    initial_llm_nodes = [
        node for node in result.initial_nodes if node.node_type is NodeType.LLM_CALL
    ]
    replay_llm_nodes = [
        node for node in result.replay_nodes if node.node_type is NodeType.LLM_CALL
    ]
    assert len(initial_llm_nodes) >= 2
    assert len(replay_llm_nodes) >= 2
    assert all(node.requested_model == REQUESTED_MODEL_ID for node in replay_llm_nodes)
    assert all(node.model != REQUESTED_MODEL_ID for node in replay_llm_nodes)
    assert all(node.cost is not None and node.cost > 0 for node in replay_llm_nodes)
    assert any(
        node.model_params == {"maxOutputTokens": 3000} for node in replay_llm_nodes
    )

    replay_action_nodes = [
        node
        for node in result.replay_nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name == "queueRefundReview"
    ]
    assert len(replay_action_nodes) == 1
    replay_action_attributes = replay_action_nodes[0].attributes or {}
    assert replay_action_attributes.get("mocked") is True
    assert replay_action_attributes.get("policy") == "history"

    assert {evaluation.name for evaluation in result.evaluations} == (
        EXPECTED_EVALUATIONS
    )
    assert all(evaluation.passed is True for evaluation in result.evaluations)
