"""Prove Mastra decision recording and replay on an authenticated local worker.

Run ``pnpm run build:packages`` and ``uv sync --all-extras`` first, then
``uv run python devtools/check_mastra_processor_decisions.py``.
Docker must provide the local PostgreSQL. Models are scripted; no provider
is called. Only this invocation's server, worker, and database are removed.
"""

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any

from check_mastra_memory_replay import await_job
from stack import (
    bootstrap_api_key,
    create_database,
    drop_database,
    ensure_postgres,
    get_free_port,
    start_server,
    wait_for_health,
)

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.plugin import EvaluatorConfig, ScriptPluginSource
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import ReplayOverride
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker import Worker, WorkerConfig

ARTIFACT = Path(__file__).with_suffix(".mjs").resolve()


async def require_completed_job(client: KitaruAPIClient, job_id: uuid.UUID) -> None:
    """Require a completed job and include task diagnostics on failure."""
    job = await await_job(client, job_id, "processor decision proof", 90)
    if job.status != JobStatus.COMPLETED:
        tasks = await client.jobs.list_tasks(job.id)
        raise AssertionError([(task.status, task.error) for task in tasks.items])


async def inspect_session(
    client: KitaruAPIClient, session_id: uuid.UUID, skill: str, classifier_calls: int
) -> dict[str, Any]:
    """Check hydrated persisted decisions, model requests, usage and hierarchy."""
    session = await client.sessions.get(session_id)
    assert session.status == SessionStatus.COMPLETED, session.error
    nodes = [
        node
        async for node in client.sessions.iter_nodes(
            session.id, SessionNodeListParams(include_payloads=True, size=1)
        )
    ]
    decisions = [node for node in nodes if node.name == "skill-router"]
    classifiers = [node for node in nodes if node.name == "skill-router:classifier"]
    assert len(decisions) == 1
    assert decisions[0].outputs == {"skills": [skill]}
    assert len(classifiers) == classifier_calls
    for node in classifiers:
        assert node.parent_external_id == decisions[0].external_id
        assert node.tokens and node.tokens.input_tokens == 7
        assert node.tokens.output_tokens == 3
        assert str(node.cost) == "0.0005", node.cost
        assert "Pick a skill" in json.dumps(node.inputs)
        assert skill in json.dumps(node.outputs)
    actor = [
        node for node in nodes if node.node_type == "llm_call" and node.model == "actor"
    ]
    assert len(actor) == 1
    assert f"Selected skills: {skill}" in json.dumps(actor[0].inputs)
    return {
        "session_id": str(session.id),
        "skill": skill,
        "classifier_nodes": len(classifiers),
    }


async def check(output: Path) -> None:
    """Run recording and replay with ordinary task tokens, then clean up."""
    node = shutil.which("node")
    assert node, "Node is required"
    db_name = f"kitaru_decisions_{uuid.uuid4().hex[:10]}"
    directory = Path(tempfile.mkdtemp(prefix="kitaru-decisions-proof-"))
    print(f"Proof logs: {directory}", flush=True)
    (directory / "router.txt").write_text("billing")
    stop = asyncio.Event()
    server = None
    worker_task = None
    await ensure_postgres()
    await create_database(db_name)
    try:
        port = get_free_port()
        url = f"http://127.0.0.1:{port}"
        server = start_server(
            db_name, port, directory / "server.log", auth_scheme="local"
        )
        await wait_for_health(url, server, directory / "server.log")
        key = await bootstrap_api_key(url)
        assert key
        os.environ["KITARU_API_URL"] = url
        os.environ["KITARU_API_KEY"] = key
        os.environ.pop("KITARU_API_TOKEN", None)
        worker = Worker(
            WorkerConfig(
                name=f"decision-proof-{uuid.uuid4().hex[:8]}",
                concurrency=1,
                poll_interval=0.1,
                heartbeat_interval=0.5,
                blob_cache_root=directory / "worker-blobs",
                payload_cache_root=directory / "worker-payloads",
            )
        )
        worker_task = asyncio.create_task(worker.run(stop))
        async with KitaruAPIClient(base_url=url, api_key=key) as client:
            agent = await client.agents.create(
                AgentCreateRequest(name="decision-proof")
            )

            async def version(capture: bool):
                return await client.agents.create_version(
                    agent.id,
                    AgentVersionCreateRequest(
                        run_spec=RunSpec(
                            command=f'"{node}" "{ARTIFACT}"',
                            timeout_seconds=60,
                            env={
                                "CHECK_AGENT_ID": str(agent.id),
                                "CHECK_DIRECTORY": str(directory),
                                "CHECK_CAPTURE_DECISION": "1" if capture else "0",
                            },
                        ),
                    ),
                )

            recorded_version = await version(True)
            baseline_job = await client.session_runs.create(
                SessionRunCreateRequest(
                    agent_version_id=recorded_version.id, inputs="Please help"
                )
            )
            await require_completed_job(client, baseline_job.id)
            sessions = [session async for session in client.sessions.iter()]
            assert len(sessions) == 1
            baseline = await client.sessions.get(sessions[0].id)
            assert baseline.metadata["mastra_replay_state"] == "eligible", (
                baseline.metadata
            )
            snapshot = baseline.inputs["mastra_memory_replay"]["processorDecisions"]
            assert snapshot["complete"] is True
            assert snapshot["entries"] == [
                {"name": "skill-router", "output": {"skills": ["billing"]}}
            ]
            recorded = await inspect_session(client, baseline.id, "billing", 1)
            (directory / "router.txt").write_text("returns")
            evaluator_blob = await client.blobs.upload(
                Path(__file__).with_name("evaluators.py").read_bytes(),
                media_type="text/x-python",
                filename="evaluator.py",
            )
            evaluator = await client.evaluators.create(
                EvaluatorCreateRequest(name="decision-completed")
            )
            await client.evaluators.create_version(
                evaluator.id,
                EvaluatorVersionCreateRequest(
                    source=ScriptPluginSource(
                        blob_id=evaluator_blob.id, entrypoint="evaluate_outcome"
                    )
                ),
            )
            evaluator_config = EvaluatorConfig(
                evaluator="decision-completed", version=1
            )

            async def replay(mode: str | None, baseline_id: uuid.UUID):
                return await client.replays.create(
                    ReplayCreateRequest(
                        baseline_session_id=baseline_id,
                        agent_version_id=recorded_version.id,
                        evaluators=[evaluator_config],
                        override=ReplayOverride(
                            model_params={"mastraProcessorDecisions": mode}
                        )
                        if mode
                        else None,
                    )
                )

            live = await replay(None, baseline.id)
            assert live.job_id
            await require_completed_job(client, live.job_id)
            live = await client.replays.get(live.id)
            assert live.result_session_id
            live_result = await inspect_session(
                client, live.result_session_id, "returns", 1
            )
            pinned = await replay("pinned", baseline.id)
            assert pinned.job_id
            await require_completed_job(client, pinned.job_id)
            pinned = await client.replays.get(pinned.id)
            assert pinned.result_session_id
            pinned_result = await inspect_session(
                client, pinned.result_session_id, "billing", 0
            )

            legacy_version = await version(False)
            legacy_job = await client.session_runs.create(
                SessionRunCreateRequest(
                    agent_version_id=legacy_version.id, inputs="Legacy baseline"
                )
            )
            await require_completed_job(client, legacy_job.id)
            sessions = [session async for session in client.sessions.iter()]
            legacy = next(
                session
                for session in sessions
                if session.origin == "recorded" and session.id != baseline.id
            )
            missing = await replay("pinned", legacy.id)
            assert missing.job_id
            job = await await_job(client, missing.job_id, "missing pin", 90)
            assert job.status == JobStatus.FAILED
            reports = [
                json.loads(path.read_text()) for path in directory.glob("*.json")
            ]
            failed = [report for report in reports if report["result"] == "failed"]
            assert len(failed) == 1
            assert failed[0]["actor_calls"] == failed[0]["classifier_calls"] == 0
            assert "decision" in failed[0]["error"].lower()
            successful_replays = [
                report
                for report in reports
                if report["replay_id"] and report["result"] == "passed"
            ]
            assert sorted(
                report["classifier_calls"] for report in successful_replays
            ) == [0, 1]
            assert all(report["source_calls"] == 0 for report in successful_replays)
            proof = {
                "result": "passed",
                "baseline": recorded,
                "live": live_result,
                "pinned": pinned_result,
                "missing_pin_model_calls": 0,
                "task_reports": reports,
                "provider_calls": 0,
                "logs": str(directory),
            }
            output.write_text(json.dumps(proof, indent=2) + "\n")
            print(f"PASS: processor decision proof saved to {output}", flush=True)
    finally:
        stop.set()
        if worker_task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.gather(worker_task, return_exceptions=True), 15
                )
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
        for name in ("worker-blobs", "worker-payloads"):
            shutil.rmtree(directory / name, ignore_errors=True)
        print("Removed the owned server, worker, database and caches.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/kitaru-processor-decisions-proof.json"),
    )
    asyncio.run(check(parser.parse_args().output))
