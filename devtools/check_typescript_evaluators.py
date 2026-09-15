"""Check the TypeScript evaluator bridge through a disposable worker stack.

Run after ``pnpm run build:packages``. Add ``--live-judge`` to make exactly two
bounded gpt-5-nano requests with OPENAI_API_KEY. The agent and its conversation
are deterministic; the judge is the only live provider integration.
"""

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

from seed import await_job
from stack import (
    create_database,
    drop_database,
    ensure_postgres,
    get_free_port,
    start_server,
    wait_for_health,
)

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayCreateRequest
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task import get_task_inputs
from kitaru.worker import Worker, WorkerConfig

ARTIFACT = Path(__file__).with_suffix(".mjs").resolve()
INPUTS = {
    "messages": [
        {"role": "user", "content": "Remember that my favorite color is blue."},
        {"role": "assistant", "content": "I will remember blue."},
        {"role": "user", "content": "What is two plus two?"},
        {"role": "assistant", "content": "Four."},
        {"role": "user", "content": "What color did I ask you to remember?"},
    ]
}


async def record_agent() -> None:
    """Record the deterministic fixture as an ordinary worker agent task."""
    async with KitaruAPIClient() as client:
        inputs = get_task_inputs()
        session = await client.sessions.create(
            SessionCreateRequest(
                agent_id=uuid.UUID(os.environ["CHECK_AGENT_ID"]),
                origin=SessionOrigin.REPLAY
                if os.environ.get("KITARU_REPLAY_ID")
                else SessionOrigin.RECORDED,
                status=SessionStatus.IN_PROGRESS,
                inputs=inputs,
                outputs=None,
                started_at=datetime.now(UTC),
                framework="deterministic-conversation-check",
            )
        )
        await client.sessions.ingest_nodes(
            session.id,
            SessionNodeBatchRequest(
                nodes=[
                    SessionNodeCreateRequest(
                        index=0,
                        node_type=NodeType.TOOL_CALL,
                        name="read_color",
                        tool_name="read_color",
                        status=NodeStatus.COMPLETED,
                        inputs={},
                        outputs={"color": "blue"},
                        attributes={},
                    )
                ]
            ),
        )
        await client.sessions.update(
            session.id,
            SessionUpdateRequest(
                status=SessionStatus.COMPLETED,
                outputs={"text": "Your favorite color is blue."},
                ended_at=datetime.now(UTC),
            ),
        )


async def check(live_judge: bool) -> None:
    """Verify persistence, replay reuse, provenance, and atomic failure."""
    if live_judge and not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required for --live-judge")
    node = shutil.which("node")
    if node is None:
        raise RuntimeError("Node 22 must be available on PATH")
    db_name = f"kitaru_ts_evaluator_{uuid.uuid4().hex[:10]}"
    await ensure_postgres()
    await create_database(db_name)
    stop = asyncio.Event()
    server = None
    worker_task = None
    with tempfile.TemporaryDirectory(prefix="kitaru-ts-evaluator-") as temporary:
        directory = Path(temporary)
        try:
            port = get_free_port()
            url = f"http://127.0.0.1:{port}"
            server = start_server(db_name, port, directory / "server.log")
            await wait_for_health(url, server, directory / "server.log")
            os.environ["KITARU_API_URL"] = url
            worker = Worker(
                WorkerConfig(
                    name="typescript-evaluator-check",
                    concurrency=1,
                    poll_interval=0.2,
                    heartbeat_interval=0.5,
                    blob_cache_root=directory / "blobs",
                    payload_cache_root=directory / "payloads",
                )
            )
            worker_task = asyncio.create_task(worker.run(stop))
            async with KitaruAPIClient(base_url=url) as client:
                agent = await client.agents.create(
                    AgentCreateRequest(name="conversation-check")
                )
                agent_version = await client.agents.create_version(
                    agent.id,
                    AgentVersionCreateRequest(
                        run_spec=RunSpec(
                            command=(
                                f'"{sys.executable}" '
                                f'"{Path(__file__).resolve()}" --agent'
                            ),
                            env={"CHECK_AGENT_ID": str(agent.id)},
                            timeout_seconds=60,
                        )
                    ),
                )
                job = await client.session_runs.create(
                    SessionRunCreateRequest(
                        agent_version_id=agent_version.id, inputs=INPUTS
                    )
                )
                recorded = await await_job(client, job.id, "recording", 60)
                assert recorded.status == JobStatus.COMPLETED, recorded.status
                sessions = [session async for session in client.sessions.iter()]
                assert len(sessions) == 1
                baseline = sessions[0]
                digest = hashlib.sha256(ARTIFACT.read_bytes()).hexdigest()
                wrapper = (
                    "from pathlib import Path\n"
                    "from kitaru.task.typescript import run_typescript_evaluator\n"
                    "async def evaluate(session, **params):\n"
                    "    return await run_typescript_evaluator(session, "
                    f"artifact=Path({str(ARTIFACT)!r}), sha256={digest!r}, "
                    f"node={node!r}, params=params, timeout_seconds=90)\n"
                )
                blob = await client.blobs.upload(
                    wrapper.encode(), media_type="text/x-python", filename="evaluate.py"
                )
                evaluator = await client.evaluators.create(
                    EvaluatorCreateRequest(name="mastra-conversation")
                )
                version = await client.evaluators.create_version(
                    evaluator.id,
                    EvaluatorVersionCreateRequest(
                        source=ScriptPluginSource(
                            blob_id=blob.id, entrypoint="evaluate"
                        ),
                        display_version="fixture-1",
                    ),
                )
                params = {"judge_model": "gpt-5-nano", "live_judge": live_judge}
                config = EvaluatorConfig(
                    evaluator="mastra-conversation", version=1, params=params
                )
                replay = await client.replays.create(
                    ReplayCreateRequest(
                        baseline_session_id=baseline.id,
                        evaluators=[config],
                        baseline_evaluation_mode=BaselineEvaluationMode.FORCE,
                    )
                )
                assert replay.job_id is not None
                completed = await await_job(
                    client, replay.job_id, "replay and evaluations", 240
                )
                assert completed.status == JobStatus.COMPLETED, completed.status
                replay = await client.replays.get(replay.id)
                rows = [row async for row in client.evaluations.iter()]
                assert len(rows) == 4, len(rows)
                assert {row.session_id for row in rows} == {
                    baseline.id,
                    replay.result_session_id,
                }
                assert {row.name for row in rows} == {
                    "complete_context",
                    "history_judge",
                }
                assert all(row.score == 1 and row.explanation for row in rows), [
                    (row.name, row.score, row.explanation) for row in rows
                ]
                assert all(
                    row.evaluator_version_id == version.id
                    and row.evaluator_params == params
                    for row in rows
                )
                failed = await client.evaluations.create(
                    EvaluationBatchCreateRequest(
                        input_session_ids=[baseline.id],
                        evaluators=[
                            EvaluatorConfig(
                                evaluator="mastra-conversation",
                                version=1,
                                params={"fail": True},
                            )
                        ],
                    )
                )
                failed = await await_job(client, failed.id, "deliberate failure", 60)
                assert failed.status == JobStatus.FAILED, failed.status
                assert len([row async for row in client.evaluations.iter()]) == 4
                print(
                    json.dumps(
                        {
                            "result": "passed",
                            "judge": "gpt-5-nano" if live_judge else "mock",
                            "live_judge_calls": 2 if live_judge else 0,
                            "sessions": 2,
                            "evaluations": 4,
                            "artifact_sha256": digest,
                            "provenance_verified": True,
                            "failure_rows": 0,
                            "results": [
                                {
                                    "name": row.name,
                                    "score": row.score,
                                    "explanation": row.explanation,
                                }
                                for row in rows
                            ],
                        },
                        indent=2,
                    )
                )
        finally:
            stop.set()
            if worker_task is not None:
                try:
                    await asyncio.wait_for(worker_task, timeout=15)
                except TimeoutError:
                    worker_task.cancel()
                    await asyncio.gather(worker_task, return_exceptions=True)
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=10)
            await drop_database(db_name)
            print("Cleaned up the temporary server, worker, database, and artifacts.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-judge", action="store_true")
    parser.add_argument("--agent", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    asyncio.run(record_agent() if args.agent else check(args.live_judge))
