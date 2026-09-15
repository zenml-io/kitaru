"""Run two isolated adaptive conversations and an existing Python evaluator."""

import argparse
import asyncio
import os
import sys
import tempfile
import uuid
from pathlib import Path
from subprocess import TimeoutExpired

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import SessionListParams, SessionStatus
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker import Worker, WorkerConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_DIR = Path(__file__).resolve().parent
PROMPT = "I need help with a fictional delayed parcel. What information do you need?"
TARGET_PROMPT = (
    "You assist with fictional parcel support. Ask for the order reference first. "
    "Once given FIXTURE-42, explain that this fixture is delayed and suggest "
    "contacting support. Never claim to use tools or perform real actions. "
    "Keep each answer under 80 words."
)
MODEL = "openai/gpt-5-nano"
EVALUATOR = "kitaru/output-contract"


async def _run_job(
    job_id: uuid.UUID, api_url: str, api_key: str, state_dir: Path
) -> None:
    """Run only this job's tasks, including evaluator subprocesses."""
    original = {
        key: os.environ.get(key) for key in ("KITARU_API_URL", "KITARU_API_KEY")
    }
    os.environ["KITARU_API_URL"] = api_url
    os.environ["KITARU_API_KEY"] = api_key or "local-development-key"
    try:
        worker = Worker(
            WorkerConfig(
                name=f"adaptive-demo-{job_id}",
                scope=WorkerScope(
                    claims=[WorkerClaim(kind=kind) for kind in TaskKind],
                    job_id=job_id,
                ),
                poll_interval=0.1,
                timeout=240,
                blob_cache_root=state_dir / "blobs",
                payload_cache_root=state_dir / "payloads",
            )
        )
        async with asyncio.timeout(270):
            await worker.run()
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        job = await client.jobs.get(job_id)
        if job.status is not JobStatus.COMPLETED:
            # Keep subprocess errors in local logs, where provider details belong.
            raise RuntimeError(f"Job {job.id} finished as {job.status}")


async def run_demo(api_url: str, api_key: str, state_dir: Path) -> list[str]:
    """Validate two fresh worker runs and persisted whole-transcript evaluation."""
    if not (EXAMPLE_DIR / "dist/main.js").is_file():
        raise RuntimeError(
            "Build @zenml-io/kitaru-example-mastra-adaptive-conversation first"
        )
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required")
    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        agent = await client.agents.create(
            AgentCreateRequest(name=f"mastra-adaptive-{uuid.uuid4().hex[:12]}")
        )
        version = await client.agents.create_version(
            agent.id,
            AgentVersionCreateRequest(
                display_version="v1",
                run_spec=RunSpec(
                    command=(
                        "node examples/typescript/"
                        "mastra_adaptive_conversation/dist/main.js"
                    ),
                    working_dir=str(REPO_ROOT),
                    env={"KITARU_AGENT_ID": str(agent.id)},
                    timeout_seconds=180,
                ),
            ),
        )
        session_ids: list[uuid.UUID] = []
        for repeat in range(2):
            job = await client.session_runs.create(
                SessionRunCreateRequest(
                    agent_version_id=version.id,
                    inputs={
                        "scenario_version": "parcel-fixture-v1",
                        "prompt": TARGET_PROMPT,
                    },
                    name=f"Synthetic adaptive conversation {repeat + 1}",
                )
            )
            await _run_job(job.id, api_url, api_key, state_dir)
            tasks = await client.jobs.list_tasks(job.id)
            sessions = await client.sessions.list(
                SessionListParams(
                    filter=FilterCondition(
                        field="task_id",
                        op=FilterOp.IN,
                        value=[str(task.id) for task in tasks.items],
                    )
                )
            )
            if len(sessions.items) != 1 or sessions.next_cursor is not None:
                raise RuntimeError(
                    f"Expected one conversation session, got {len(sessions.items)}"
                )
            session = await client.sessions.get(sessions.items[0].id)
            assert session.status is SessionStatus.COMPLETED
            output = session.outputs
            assert isinstance(output, dict)
            messages = output["messages"]
            assert isinstance(messages, list) and 2 <= len(messages) <= 6
            assert messages[0] == {"role": "user", "content": PROMPT}
            assert all(
                isinstance(message, dict)
                and message["role"] == ("user" if index % 2 == 0 else "assistant")
                and isinstance(message["content"], str)
                and message["content"].strip()
                for index, message in enumerate(messages)
            )
            assert output["transcript_version"] == "1"
            assert output["scenario_version"] == "parcel-fixture-v1"
            assert output["stop_reason"] in {"scenario_complete", "turn_limit"}
            assert output["tools"] == "disabled"
            assert output["target"]["model"] == MODEL
            assert len(output["branches"]) == len(messages) // 2 - 1
            assert session.id not in session_ids
            session_ids.append(session.id)
            print(
                f"repeat={repeat + 1} session={session.id} messages={len(messages)} "
                f"branches={output['branches']} stop={output['stop_reason']}"
            )

        evaluation_job = await client.evaluations.create(
            EvaluationBatchCreateRequest(
                input_session_ids=session_ids,
                evaluators=[
                    EvaluatorConfig(
                        evaluator=EVALUATOR,
                        params={
                            "required_paths": [
                                "/messages/0/content",
                                "/messages/1/content",
                                "/transcript_version",
                                "/scenario_version",
                                "/stop_reason",
                                "/target/model",
                                "/simulator/version",
                                "/judge/configuration",
                            ],
                            "type_requirements": {
                                "/messages": "array",
                                "/branches": "array",
                                "/stop_reason": "string",
                                "/transcript_version": "string",
                            },
                        },
                    )
                ],
            )
        )
        await _run_job(evaluation_job.id, api_url, api_key, state_dir)
        for session_id in session_ids:
            results = (
                await client.evaluations.list(
                    EvaluationListParams(
                        filter=FilterCondition(
                            field="session_id", op=FilterOp.EQ, value=str(session_id)
                        )
                    )
                )
            ).items
            # The availability metric is descriptive; contract verdicts must pass.
            verdicts = [result for result in results if result.passed is not None]
            assert {result.name for result in verdicts} == {
                "required_paths",
                "type_requirements",
            }
            assert all(result.passed for result in verdicts)
            print(
                f"session={session_id} evaluator={EVALUATOR} "
                f"evaluator_version_id={results[0].evaluator_version_id} "
                "contract=passed"
            )
        return [str(session_id) for session_id in session_ids]


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-url", help="Use an existing server and retain its demo records"
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="mastra-adaptive-") as temporary:
        state_dir = Path(temporary)
        if args.api_url:
            await run_demo(
                args.api_url, os.environ.get("KITARU_API_KEY", ""), state_dir
            )
            return
        # Import the repository's local stack helper only for the self-cleaning demo.
        sys.path.insert(0, str(REPO_ROOT))
        from devtools import stack

        database = f"kitaru_adaptive_{uuid.uuid4().hex}"
        await stack.ensure_postgres()
        await stack.create_database(database)
        server = None
        try:
            port = stack.get_free_port()
            log_path = state_dir / "server.log"
            server = stack.start_server(database, port, log_path)
            api_url = f"http://127.0.0.1:{port}"
            await stack.wait_for_health(api_url, server, log_path)
            await run_demo(api_url, "", state_dir)
        finally:
            if server is not None:
                server.terminate()
                try:
                    await asyncio.to_thread(server.wait, timeout=10)
                except TimeoutExpired:
                    server.kill()
                    await asyncio.to_thread(server.wait)
            await stack.drop_database(database)
    print(f"target={MODEL}; evaluator={EVALUATOR}; temporary stack/data cleaned")


if __name__ == "__main__":
    asyncio.run(_main())
