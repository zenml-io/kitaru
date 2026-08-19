"""Cross-language proof for the compiled Mastra support-triage example."""

import asyncio
import importlib
import json
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
from kitaru.api_models.v1.session_node import NodeType
from kitaru.server.api.app import create_app
from kitaru.server.database.service import DatabaseService


@pytest.mark.parametrize(
    "outputs", [None, {}, {"text": None}, {"text": ""}, {"text": "   "}]
)
def test_mastra_demo_rejects_a_baseline_without_text(outputs: object) -> None:
    """Do not replay a baseline that produced no usable final answer."""
    demo = importlib.import_module("examples.typescript.mastra_support_triage.demo")

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


async def test_worker_records_and_history_replays_compiled_mastra(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Prove worker, Node/Mastra, history replay, overrides, and scoring."""
    repo_root = Path(__file__).resolve().parents[2]
    for package in (
        "@zenml-io/kitaru",
        "@zenml-io/kitaru-mastra",
        "@zenml-io/kitaru-example-mastra-support-triage",
    ):
        subprocess.run(
            ["pnpm", "--filter", package, "build"],
            cwd=repo_root,
            check=True,
        )
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    demo = importlib.import_module("examples.typescript.mastra_support_triage.demo")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    state_root = tmp_path / ".state"

    async with _network_server() as api_url:
        result = await demo.run_demo(
            api_url=api_url,
            state_dir=state_root,
            test_model=True,
        )
        rerun = await demo.run_demo(
            api_url=api_url,
            state_dir=state_root,
            test_model=True,
        )

    assert result.initial_outbox_count == 1
    assert result.replay_outbox_count == 1
    assert result.replay.status is ReplayStatus.COMPLETED
    assert rerun.replay.status is ReplayStatus.COMPLETED
    assert rerun.initial_outbox_count == 1
    assert rerun.replay_outbox_count == 1
    assert result.state_dir != rerun.state_dir
    outboxes = sorted(state_root.glob("*/refund-review-outbox.jsonl"))
    assert len(outboxes) == 2
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) == 1 for path in outboxes
    )
    assert all(evaluation.passed is True for evaluation in result.evaluations)
    assert (
        sum(node.node_type is NodeType.LLM_CALL for node in result.initial_nodes) >= 2
    )
    assert {
        node.tool_name
        for node in result.initial_nodes
        if node.node_type is NodeType.TOOL_CALL
    } == {"lookupAccount", "lookupOrder", "queueRefundReview"}
    manifests = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(state_root.glob("*/run-manifest.json"))
    ]
    assert len(manifests) == 2
    assert all(manifest["schema_version"] == 1 for manifest in manifests)
    assert all(manifest["status"] == "completed" for manifest in manifests)
    assert all(manifest["cancellations"] == [] for manifest in manifests)
    assert all(
        operation["state"] == "committed"
        for manifest in manifests
        for operation in manifest["operations"]
    )
    assert all(
        {
            "agent_id",
            "agent_version_id",
            "evaluator_blob_id",
            "evaluator_id",
            "evaluator_version_id",
            "initial_job_id",
            "initial_session_id",
            "replay_id",
            "replay_job_id",
            "result_session_id",
        }
        <= manifest["resources"].keys()
        for manifest in manifests
    )
