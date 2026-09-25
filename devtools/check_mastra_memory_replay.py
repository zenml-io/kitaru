"""Prove SDK, CLI and MCP memory replay on an authenticated disposable stack.

Run ``pnpm run build:packages`` first, then
``uv run python devtools/check_mastra_memory_replay.py``.
No model provider is called. Only this invocation's server and database are removed.
"""

import argparse
import asyncio
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

import asyncpg
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from stack import (
    DB_HOST,
    DB_PORT,
    DB_PWD,
    DB_USER,
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
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.plugin import EvaluatorConfig, ScriptPluginSource
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    ReplayOverride,
    ToolPolicy,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.worker import Worker, WorkerConfig

ARTIFACT = Path(__file__).with_suffix(".mjs").resolve()
OVERRIDE = {
    "system_prompt": "Replay application instruction",
    "model": "fixture/replacement",
}
POLICY = {
    "default": {"type": "history", "scope": "baseline", "on_miss": "fail"},
    "tools": {},
}


async def await_job(
    client: KitaruAPIClient, job_id: uuid.UUID, label: str, timeout: float
) -> JobResponse:
    """Wait for one owned job to reach a terminal state."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        job = await client.jobs.get(job_id)
        if job.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}:
            return job
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"{label} did not complete within {timeout}s")
        await asyncio.sleep(0.2)


async def call_mcp(session: ClientSession, tool: str, request: dict[str, Any]) -> Any:
    """Call a public MCP tool and require its structured success envelope."""
    result = await session.call_tool(tool, {"request": request})
    assert not result.is_error, result
    envelope = result.structured_content
    assert envelope and envelope["ok"], envelope
    return envelope["data"]


async def run_cli(*args: str) -> dict[str, Any]:
    """Run the installed CLI without blocking the worker's event loop."""
    process = await asyncio.create_subprocess_exec(
        str(Path(sys.executable).with_name("kitaru")),
        *args,
        "--output",
        "json",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 60)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    assert process.returncode == 0, stderr.decode() + stdout.decode()
    return json.loads(stdout)


async def inspect_session(
    client: KitaruAPIClient, session_id: uuid.UUID
) -> dict[str, Any]:
    """Require complete, hydrated request evidence and ordered memory changes."""
    session = await client.sessions.get(session_id)
    assert session.status == SessionStatus.COMPLETED, session.error
    nodes = [
        node
        async for node in client.sessions.iter_nodes(
            session_id, SessionNodeListParams(include_payloads=True, size=1)
        )
    ]
    model_nodes = [node for node in nodes if node.node_type == "llm_call"]
    mutations = [node for node in nodes if node.name == "memory_mutation"]
    assert len(model_nodes) == 2, [(node.name, node.node_type) for node in nodes]
    assert mutations, "Memory changes were not recorded"
    revisions = [node.attributes["memory_revision"] for node in mutations]
    assert revisions == sorted(set(revisions)), revisions
    assert all(node.attributes["evidence_complete"] for node in mutations)
    request_ids = {node.external_id for node in model_nodes}
    assert any(node.attributes["request_id"] in request_ids for node in mutations)
    assert all(node.attributes["request_complete"] for node in model_nodes)
    assert all(len(json.dumps(node.inputs)) > 40000 for node in model_nodes)
    first_request = json.dumps(model_nodes[0].inputs)
    assert "historical-blue" in first_request
    assert "production-today" not in first_request
    assert "Replay application instruction" in first_request
    assert "Extra context" in first_request
    assert "HISTORICAL_SKILL" in first_request
    assert "replay-green" in json.dumps(model_nodes[1].inputs)
    assert all(node.model == "replacement" for node in model_nodes)
    return {
        "session_id": str(session_id),
        "task_id": str(session.task_id),
        "node_ids": [str(node.id) for node in nodes],
        "model_nodes": len(model_nodes),
        "memory_nodes": len(mutations),
    }


async def finish_replay(
    client: KitaruAPIClient, replay_id: uuid.UUID
) -> dict[str, Any]:
    """Wait for both replay and evaluator completion, then inspect the result."""
    replay = await client.replays.get(replay_id)
    assert replay.job_id
    job = await await_job(client, replay.job_id, "memory replay", 120)
    if job.status != JobStatus.COMPLETED:
        tasks = await client.jobs.list_tasks(job.id)
        raise AssertionError(
            [(task.kind, task.status, task.error) for task in tasks.items]
        )
    replay = await client.replays.get(replay_id)
    assert replay.status == "completed", replay
    assert replay.result_session_id
    return {
        "replay_id": str(replay.id),
        **await inspect_session(client, replay.result_session_id),
    }


async def check_mcp(
    client: KitaruAPIClient,
    url: str,
    directory: Path,
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    evaluator_id: uuid.UUID,
    baseline_id: uuid.UUID,
) -> dict[str, Any]:
    """Create an experiment and inspect every result page over real MCP stdio."""
    mcp_console = str(Path(sys.executable).with_name("kitaru-mcp"))
    with (directory / "mcp.log").open("w") as mcp_log:
        parameters = StdioServerParameters(
            command=mcp_console,
            args=["--server", url, "--mode", "standard"],
            env=dict(os.environ),
        )
        async with (
            stdio_client(parameters, errlog=mcp_log) as (reader, writer),
            ClientSession(reader, writer) as mcp,
        ):
            await mcp.initialize()
            cohort = await call_mcp(
                mcp,
                "kitaru_cohorts_manage",
                {
                    "operation": "create",
                    "agent_id": str(agent_id),
                    "name": "memory-cohort",
                },
            )
            cohort_version = await call_mcp(
                mcp,
                "kitaru_cohorts_manage",
                {
                    "operation": "create_version",
                    "cohort_id": cohort["id"],
                    "add_session_ids": [str(baseline_id)],
                },
            )
            experiment = await call_mcp(
                mcp,
                "kitaru_experiments_manage",
                {
                    "operation": "create",
                    "agent_id": str(agent_id),
                    "name": "memory-experiment",
                    "override": OVERRIDE,
                    "tool_policy": POLICY,
                    "evaluators": [{"evaluator_id": str(evaluator_id), "version": 1}],
                },
            )
            started = await call_mcp(
                mcp,
                "kitaru_workflow_start",
                {
                    "operation": "experiment_run",
                    "experiment_id": experiment["id"],
                    "cohort_version_id": cohort_version["id"],
                    "agent_version_id": str(version_id),
                    "baseline_evaluation_mode": "none",
                },
            )
            run_id = started["result"]["id"]
            deadline = asyncio.get_running_loop().time() + 120
            while True:
                run = await call_mcp(
                    mcp,
                    "kitaru_activity_read",
                    {
                        "operation": "get",
                        "kind": "experiment_run",
                        "id": run_id,
                    },
                )
                if run["status"] in {"completed", "failed", "canceled"}:
                    break
                assert asyncio.get_running_loop().time() < deadline, (
                    "MCP experiment timed out"
                )
                await asyncio.sleep(0.2)
            assert run["status"] == "completed", run
            children = await call_mcp(
                mcp,
                "kitaru_activity_read",
                {
                    "operation": "list_children",
                    "kind": "experiment_run_jobs",
                    "parent_id": run_id,
                    "size": 1,
                },
            )
            assert len(children["items"]) == 1
            replay_page = await call_mcp(
                mcp,
                "kitaru_activity_read",
                {
                    "operation": "list",
                    "kind": "replay",
                    "size": 1,
                    "filter": {
                        "field": "experiment_run_id",
                        "op": "eq",
                        "value": run_id,
                    },
                },
            )
            assert len(replay_page["items"]) == 1
            mcp_replay = replay_page["items"][0]
            assert mcp_replay["status"] == "completed"
            mcp_result = await finish_replay(client, uuid.UUID(mcp_replay["id"]))
            mcp_session = await call_mcp(
                mcp,
                "kitaru_activity_read",
                {
                    "operation": "get",
                    "kind": "session",
                    "id": mcp_result["session_id"],
                },
            )
            assert mcp_session["status"] == "completed"
            assert mcp_session["inputs"]["mastra_memory_replay"]["complete"]
            mcp_nodes = []
            cursor = None
            while True:
                page = await call_mcp(
                    mcp,
                    "kitaru_activity_read",
                    {
                        "operation": "list_children",
                        "kind": "session_nodes",
                        "parent_id": mcp_result["session_id"],
                        "size": 1,
                        "cursor": cursor,
                        "include_payloads": True,
                    },
                )
                mcp_nodes.extend(page["items"])
                if not page["page"]["has_more"]:
                    break
                cursor = page["page"]["next_cursor"]
                assert cursor
            assert {item["id"] for item in mcp_nodes} == set(mcp_result["node_ids"])
            assert all(
                item["inputs"] and item["attributes"]["request_complete"]
                for item in mcp_nodes
                if item["node_type"] == "llm_call"
            )
            assert any(
                item["name"] == "memory_mutation"
                and item["attributes"]["memory_revision"] > 0
                for item in mcp_nodes
            )
        parameters = StdioServerParameters(
            command=mcp_console,
            args=["--server", url, "--mode", "read-only"],
            env=dict(os.environ),
        )
        async with (
            stdio_client(parameters, errlog=mcp_log) as (reader, writer),
            ClientSession(reader, writer) as mcp,
        ):
            await mcp.initialize()
            names = {tool.name for tool in (await mcp.list_tools()).tools}
            assert "kitaru_workflow_start" not in names
            assert "kitaru_experiments_manage" not in names
            read = await call_mcp(
                mcp,
                "kitaru_activity_read",
                {
                    "operation": "get",
                    "kind": "replay",
                    "id": mcp_result["replay_id"],
                },
            )
            assert read["result_session_id"] == mcp_result["session_id"]
            refused = await mcp.call_tool(
                "kitaru_workflow_start",
                {
                    "request": {
                        "operation": "experiment_run",
                        "experiment_id": experiment["id"],
                        "cohort_version_id": cohort_version["id"],
                        "agent_version_id": str(version_id),
                    }
                },
            )
            assert refused.is_error
    print("MCP experiment, pagination and read-only mode passed", flush=True)
    return {
        "experiment_run_id": run_id,
        **mcp_result,
        "paginated_nodes": len(mcp_nodes),
    }


async def check(output: Path) -> None:
    """Exercise all headless paths and clean up owned resources even on failure."""
    node = shutil.which("node")
    assert node, "Node 22.22 or 26 is required"
    db_name = f"kitaru_memory_{uuid.uuid4().hex[:10]}"
    directory = Path(tempfile.mkdtemp(prefix="kitaru-memory-proof-"))
    print(f"Proof logs: {directory}", flush=True)
    skills = directory / "skills" / "triage"
    skills.mkdir(parents=True)
    (skills / "SKILL.md").write_text(
        "---\nname: triage\ndescription: HISTORICAL_SKILL.\n---\n"
        "Use historical knowledge.\n"
    )
    (directory / "production.json").write_text(
        json.dumps({"preference": "historical-blue"})
    )
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
                name=f"memory-proof-{uuid.uuid4().hex[:8]}",
                concurrency=1,
                poll_interval=0.1,
                heartbeat_interval=0.5,
                blob_cache_root=directory / "worker-blobs",
                payload_cache_root=directory / "worker-payloads",
            )
        )
        worker_task = asyncio.create_task(worker.run(stop))
        async with KitaruAPIClient(base_url=url, api_key=key) as client:
            unrelated = await client.blobs.upload(
                b"unrelated private content", media_type="text/plain"
            )
            agent = await client.agents.create(AgentCreateRequest(name="memory-proof"))
            version = await client.agents.create_version(
                agent.id,
                AgentVersionCreateRequest(
                    run_spec=RunSpec(
                        command=f'"{node}" "{ARTIFACT}"',
                        timeout_seconds=90,
                        env={
                            "CHECK_AGENT_ID": str(agent.id),
                            "CHECK_DIRECTORY": str(directory),
                            "CHECK_UNRELATED_BLOB": str(unrelated.id),
                        },
                    )
                ),
            )
            job = await client.session_runs.create(
                SessionRunCreateRequest(
                    agent_version_id=version.id,
                    inputs=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "Update the preference.",
                                },
                                {
                                    "type": "file",
                                    "data": "https://files.invalid/historical.pdf?token=historical-secret",
                                    "mimeType": "application/pdf",
                                },
                            ],
                        }
                    ],
                )
            )
            job = await await_job(client, job.id, "baseline", 120)
            if job.status != JobStatus.COMPLETED:
                tasks = await client.jobs.list_tasks(job.id)
                raise AssertionError(
                    [(task.kind, task.status, task.error) for task in tasks.items]
                )
            sessions = [session async for session in client.sessions.iter()]
            assert len(sessions) == 1
            deadline = asyncio.get_running_loop().time() + 30
            while True:
                baseline = await client.sessions.get(sessions[0].id)
                if baseline.metadata.get("mastra_replay_state") != "pending":
                    break
                assert asyncio.get_running_loop().time() < deadline, (
                    "Baseline memory evidence did not finalize"
                )
                await asyncio.sleep(0.2)
            assert baseline.metadata["mastra_replay_state"] == "eligible"
            envelope = baseline.inputs["mastra_memory_replay"]
            assert envelope["complete"] is True
            assert len(envelope["initialSnapshot"]["messages"]) == 830
            assert envelope["initialSnapshot"]["records"]
            first_message = next(
                message
                for message in envelope["initialSnapshot"]["messages"]
                if message["id"] == "historical-0"
            )
            hotels = first_message["content"]["metadata"]["hotels"]
            assert len(hotels) == 1500
            assert all(len(hotel["details"]) == 10 for hotel in hotels)
            assert len(json.dumps(baseline.inputs).encode()) > 1_048_576
            assert "historical-secret" not in json.dumps(baseline.inputs)
            # Recorded files live in blobs, outside the replay input.
            assert envelope["files"]
            assert all("base64" not in file for file in envelope["files"])
            file_blob_ids = [file["blobId"] for file in envelope["files"]]
            connection = await asyncpg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PWD,
                database=db_name,
            )
            try:
                offloaded = await connection.fetchval(
                    "SELECT inputs_blob_id FROM session WHERE id = $1", baseline.id
                )
                assert offloaded, (
                    "Large session inputs did not use server payload offload"
                )
            finally:
                await connection.close()
            (directory / "production.json").write_text(
                json.dumps({"preference": "production-today"})
            )
            evaluator_blob = await client.blobs.upload(
                (Path(__file__).parent / "evaluators.py").read_bytes(),
                media_type="text/x-python",
                filename="evaluator.py",
            )
            evaluator = await client.evaluators.create(
                EvaluatorCreateRequest(name="memory-completed")
            )
            await client.evaluators.create_version(
                evaluator.id,
                EvaluatorVersionCreateRequest(
                    source=ScriptPluginSource(
                        blob_id=evaluator_blob.id, entrypoint="evaluate_outcome"
                    )
                ),
            )
            config = EvaluatorConfig(evaluator="memory-completed", version=1)
            replay = await client.replays.create(
                ReplayCreateRequest(
                    baseline_session_id=baseline.id,
                    override=ReplayOverride(**OVERRIDE),
                    tool_policy=ToolPolicy.model_validate(POLICY),
                    evaluators=[config],
                )
            )
            sdk = await finish_replay(client, replay.id)
            print("SDK replay passed", flush=True)
            created = await run_cli(
                "replay",
                "create",
                str(baseline.id),
                "--evaluator",
                "memory-completed@1",
                "--override",
                json.dumps(OVERRIDE),
                "--tool-policy",
                json.dumps(POLICY),
            )
            cli = await finish_replay(client, uuid.UUID(created["item"]["id"]))
            cli_read = await run_cli("replay", "get", cli["replay_id"])
            assert cli_read["item"]["status"] == "completed"
            assert cli_read["item"]["result_session_id"] == cli["session_id"]
            cli_nodes = await run_cli(
                "session",
                "nodes",
                cli["session_id"],
                "--include-payloads",
                "--size",
                "100",
            )
            assert {item["id"] for item in cli_nodes["items"]} == set(cli["node_ids"])
            assert all(
                item["inputs"] and item["attributes"]["request_complete"]
                for item in cli_nodes["items"]
                if item["node_type"] == "llm_call"
            )
            assert any(item["name"] == "memory_mutation" for item in cli_nodes["items"])
            print("CLI replay passed", flush=True)
            mcp_result = await check_mcp(
                client, url, directory, agent.id, version.id, evaluator.id, baseline.id
            )
            incomplete = copy.deepcopy(baseline.inputs)
            incomplete["mastra_memory_replay"]["complete"] = False
            broken = await client.sessions.create(
                SessionCreateRequest(
                    agent_id=agent.id,
                    agent_version_id=version.id,
                    framework="mastra",
                    origin=SessionOrigin.RECORDED,
                    status=SessionStatus.COMPLETED,
                    inputs=incomplete,
                    outputs={"text": "incomplete fixture"},
                )
            )
            try:
                await client.replays.create(
                    ReplayCreateRequest(
                        baseline_session_id=broken.id,
                        evaluators=[config],
                        override=ReplayOverride(**OVERRIDE),
                    )
                )
            except APIError as error:
                assert error.status_code == 409
                assert "mastra_replay_incomplete" in error.detail
            else:
                raise AssertionError("Incomplete replay was scheduled")
            reports = [
                json.loads(path.read_text())
                for path in directory.glob("*.json")
                if path.name != "production.json"
            ]
            good = [report for report in reports if report.get("result") == "passed"]
            assert len(good) == 4, reports
            assert not [
                report for report in reports if report.get("result") == "failed"
            ]
            assert all(report["unrelated_blob_status"] == 403 for report in good)
            assert all(
                not report["task_inputs_in_environment"]
                for report in good
                if report["replay_id"]
            )
            server_log = (directory / "server.log").read_text()
            for report in good:
                expected = 1 if report["replay_id"] else 0
                report["task_spec_requests"] = server_log.count(
                    f'"GET /api/v1/tasks/{report["task_id"]}/spec HTTP/1.1" 200 OK'
                )
                assert report["task_spec_requests"] == expected, report
            # Each replay task read the recorded files with its own task token.
            replays = sum(1 for report in good if report["replay_id"])
            file_blob_downloads = {
                blob_id: server_log.count(
                    f'"GET /api/v1/blobs/{blob_id}/content HTTP/1.1" 200 OK'
                )
                for blob_id in file_blob_ids
            }
            assert all(count == replays for count in file_blob_downloads.values()), (
                file_blob_downloads
            )
            connection = await asyncpg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PWD,
                database=db_name,
            )
            try:
                offloaded_requests = await connection.fetchval(
                    "SELECT count(*) FROM session_node "
                    "WHERE session_id = ANY($1::uuid[]) "
                    "AND node_type = 'llm_call' AND inputs_blob_id IS NOT NULL",
                    [uuid.UUID(item["session_id"]) for item in (sdk, cli, mcp_result)],
                )
                assert offloaded_requests == 6, offloaded_requests
            finally:
                await connection.close()
            proof = {
                "result": "passed",
                "baseline_session_id": str(baseline.id),
                "offloaded_input_blob_id": str(offloaded),
                "offloaded_request_nodes": offloaded_requests,
                "recorded_file_blob_downloads": file_blob_downloads,
                "sdk": sdk,
                "cli": cli,
                "mcp": mcp_result,
                "incomplete_baseline_id": str(broken.id),
                "read_only_mutation_denied": True,
                "task_reports": reports,
                "provider_calls": 0,
                "logs": str(directory),
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(proof, indent=2) + "\n")
            print(
                f"PASS: authenticated memory replay proof saved to {output}", flush=True
            )
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
        print(
            "Removed the owned server, worker, database and worker caches.", flush=True
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("/tmp/kitaru-memory-proof.json")
    )
    asyncio.run(check(parser.parse_args().output))
