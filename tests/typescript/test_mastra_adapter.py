"""Cross-language proof for the compiled Mastra support-triage example."""

import asyncio
import importlib
import json
import os
import socket
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import uvicorn

from conftest import db_settings, drop_test_database, postgres_available
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeType
from kitaru.client.api_client import KitaruAPIClient
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


@pytest.fixture(scope="module")
def built_mastra_example() -> Path:
    """Build the Mastra packages and return the repository root."""
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
    return repo_root


async def _run_stream_example(
    *,
    abort: bool,
    agent_id: uuid.UUID,
    api_url: str,
    repo_root: Path,
    session_id_file: Path,
) -> dict[str, object]:
    """Run the compiled streaming example without blocking the server loop."""
    command = [
        "node",
        str(
            repo_root
            / "examples"
            / "typescript"
            / "mastra_support_triage"
            / "dist"
            / "stream.js"
        ),
        "--api-url",
        api_url,
        "--agent-id",
        str(agent_id),
    ]
    if abort:
        command.append("--abort")
    environment = os.environ.copy()
    environment["KITARU_SESSION_ID_FILE"] = str(session_id_file)
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=repo_root,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(30):
            stdout_bytes, stderr_bytes = await process.communicate()
    except TimeoutError as exc:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise RuntimeError("Timed out running the TypeScript stream example") from exc
    stdout = stdout_bytes.decode()
    if process.returncode != 0:
        detail = stderr_bytes.decode().strip() or stdout.strip()
        raise RuntimeError(
            f"The TypeScript stream example exited with {process.returncode}: {detail}"
        )
    results = [
        line.removeprefix("KITARU_STREAM_RESULT ")
        for line in stdout.splitlines()
        if line.startswith("KITARU_STREAM_RESULT ")
    ]
    if len(results) != 1:
        raise RuntimeError("The TypeScript stream example omitted its result record")
    return json.loads(results[0])


async def test_stream_persists_completed_tools_and_observable_abort(
    built_mastra_example: Path, tmp_path: Path
) -> None:
    """Prove a real server persists completed and observably aborted streams."""
    async with (
        _network_server() as api_url,
        KitaruAPIClient(base_url=api_url) as client,
    ):
        agent = await client.agents.create(
            AgentCreateRequest(name=f"mastra-stream-{uuid.uuid4().hex[:12]}")
        )
        completed_id_file = tmp_path / "completed-session-id"
        completed_result = await _run_stream_example(
            abort=False,
            agent_id=agent.id,
            api_url=api_url,
            repo_root=built_mastra_example,
            session_id_file=completed_id_file,
        )
        completed = await client.sessions.get_with_nodes(
            uuid.UUID(completed_id_file.read_text(encoding="utf-8"))
        )

        aborted_id_file = tmp_path / "aborted-session-id"
        aborted_result = await _run_stream_example(
            abort=True,
            agent_id=agent.id,
            api_url=api_url,
            repo_root=built_mastra_example,
            session_id_file=aborted_id_file,
        )
        aborted = await client.sessions.get_with_nodes(
            uuid.UUID(aborted_id_file.read_text(encoding="utf-8"))
        )

    assert completed_result == {
        "aborted": False,
        "chunks": 2,
        "text": "Order ord-1001 is delayed.",
    }
    assert completed.session.status is SessionStatus.COMPLETED
    assert completed.session.outputs == {
        "finish_reason": "stop",
        "step_count": 2,
        "text": "Order ord-1001 is delayed.",
    }
    llm_nodes = [
        node for node in completed.nodes if node.node_type is NodeType.LLM_CALL
    ]
    assert len(llm_nodes) == 2
    assert all(node.tokens is not None for node in llm_nodes)
    assert [
        (node.tokens.input_tokens, node.tokens.output_tokens)
        for node in llm_nodes
        if node.tokens is not None
    ] == [
        (5, 2),
        (6, 4),
    ]
    tool_nodes = [
        node for node in completed.nodes if node.node_type is NodeType.TOOL_CALL
    ]
    assert len(tool_nodes) == 1
    assert tool_nodes[0].tool_name == "lookupOrder"
    assert tool_nodes[0].inputs == {"orderId": "ord-1001"}
    assert tool_nodes[0].outputs == {
        "accountId": "acct-1001",
        "amountUsd": 89.5,
        "chargeCount": 2,
        "expectedDelivery": "2026-07-20",
        "orderId": "ord-1001",
        "status": "delayed",
    }

    assert aborted_result == {"aborted": True, "chunks": 1, "text": "partial"}
    assert aborted.session.status is SessionStatus.FAILED
    assert aborted.session.error == "Mastra stream aborted"
    assert all(node.status.value != "completed" for node in aborted.nodes)


async def test_worker_records_and_history_replays_compiled_mastra(
    built_mastra_example: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Prove worker, Node/Mastra, history replay, overrides, and scoring."""
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
