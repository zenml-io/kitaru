"""PostgreSQL proof for the TypeScript canonical returns walkthrough."""

import asyncio
import json
import os
import re
import socket
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import pytest
import uvicorn

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = REPOSITORY_ROOT / "tests"
sys.path.insert(0, str(TESTS_DIR))

from conftest import db_settings, drop_test_database, postgres_available  # noqa: E402
from kitaru.api_models.v1.agent import AgentCreateRequest  # noqa: E402
from kitaru.api_models.v1.agent_version import (  # noqa: E402
    AgentCapabilities,
    AgentVersionCreateRequest,
    RunSpec,
)
from kitaru.api_models.v1.cohort import CohortCreateRequest  # noqa: E402
from kitaru.api_models.v1.cohort_version import (  # noqa: E402
    CohortVersionCreateRequest,
)
from kitaru.api_models.v1.evaluation import (  # noqa: E402
    EvaluationBatchCreateRequest,
    EvaluationListParams,
)
from kitaru.api_models.v1.evaluator import (  # noqa: E402
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.experiment import ExperimentCreateRequest  # noqa: E402
from kitaru.api_models.v1.experiment_run import (  # noqa: E402
    ExperimentRunCreateRequest,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp  # noqa: E402
from kitaru.api_models.v1.job import JobResponse, JobStatus  # noqa: E402
from kitaru.api_models.v1.plugin import ScriptPluginSource  # noqa: E402
from kitaru.api_models.v1.replay import (  # noqa: E402
    ReplayListParams,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import (  # noqa: E402
    EvaluatorConfig,
    PassthroughConfig,
    ToolPolicy,
)
from kitaru.api_models.v1.session import (  # noqa: E402
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (  # noqa: E402
    NodeStatus,
    NodeType,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.api_models.v1.worker import WorkerScope  # noqa: E402
from kitaru.client.api_client import KitaruAPIClient  # noqa: E402
from kitaru.server.api.app import create_app  # noqa: E402
from kitaru.server.database.service import DatabaseService  # noqa: E402
from kitaru.worker import Worker, WorkerConfig  # noqa: E402

README_PATH = EXAMPLE_DIR / "README.md"
DOCUMENTED_EVALUATOR = re.compile(
    r"<!-- documented-evaluator:start -->\n```python\n(.*?)\n```\n"
    r"<!-- documented-evaluator:end -->",
    re.DOTALL,
)
TERMINAL_TOOLS = {
    "issue_refund": "refund",
    "create_replacement": "replacement",
    "escalate_to_human": "escalate",
}
INVESTIGATION_TOOLS = {
    "lookup_order",
    "get_return_policy",
    "check_shipping",
}
EXPECTED_INVESTIGATION_TOOLS = {
    "ticket-001": {"lookup_order", "get_return_policy"},
    "ticket-002": {"lookup_order", "get_return_policy"},
    "ticket-003": {"lookup_order", "get_return_policy"},
    "ticket-004": {"lookup_order", "get_return_policy"},
    "ticket-005": {"lookup_order"},
    "ticket-006": {"lookup_order", "check_shipping"},
    "ticket-007": {"lookup_order", "get_return_policy"},
    "ticket-008": {"lookup_order", "get_return_policy"},
    "ticket-009": {"lookup_order", "get_return_policy"},
    "ticket-010": {"lookup_order", "get_return_policy"},
}
REQUESTED_MODEL_ID = "openai/gpt-5-nano"
# The scripted fixture answers as its own served model, the way a provider
# returns a dated model id rather than the id the caller asked for.
SERVED_MODEL_ID = "kitaru-returns-scripted-fixture"
BASELINE_ACTIONS = {
    "ticket-001": ("refund", 98),
    "ticket-002": ("escalate", None),
    "ticket-003": ("escalate", None),
    "ticket-004": ("refund", 280),
    "ticket-005": ("escalate", None),
    "ticket-006": ("replacement", None),
    "ticket-007": ("refund", 120),
    "ticket-008": ("escalate", None),
    "ticket-009": ("refund", 80),
    "ticket-010": ("refund", 98),
}
STRICT_ACTIONS = {
    "ticket-004": ("escalate", None),
    "ticket-007": ("escalate", None),
    "ticket-001": ("refund", 98),
    "ticket-009": ("refund", 80),
    "ticket-010": ("refund", 98),
}
TARGET_TICKETS = ("ticket-004", "ticket-007")
CONTROL_TICKETS = ("ticket-001", "ticket-009", "ticket-010")
TOOLS = [
    "lookup_order",
    "get_return_policy",
    "check_shipping",
    "issue_refund",
    "create_replacement",
    "escalate_to_human",
]
SETTLED_JOB_STATUSES = {
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.CANCELED,
}
SETTLED_RUN_STATUSES = {
    ExperimentRunStatus.COMPLETED,
    ExperimentRunStatus.FAILED,
    ExperimentRunStatus.CANCELED,
}
SETTLED_REPLAY_STATUSES = {
    ReplayStatus.COMPLETED,
    ReplayStatus.FAILED,
    ReplayStatus.CANCELED,
}
REQUIRE_POSTGRES_ENVIRONMENT_VARIABLE = "KITARU_REQUIRE_POSTGRES"


async def _require_postgres() -> None:
    """Require PostgreSQL for the documented command or skip ambient collection."""
    if await postgres_available():
        return
    message = (
        "PostgreSQL is not reachable on the configured Kitaru test database port. "
        "Start it with `docker compose -f ../../docker-compose.yml up -d --build` "
        "from this example directory, then rerun `pnpm test:e2e`."
    )
    if os.environ.get(REQUIRE_POSTGRES_ENVIRONMENT_VARIABLE) == "1":
        pytest.fail(message)
    pytest.skip(message)


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@asynccontextmanager
async def _network_server() -> AsyncIterator[str]:
    await _require_postgres()
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


@contextmanager
def _worker_environment(api_url: str) -> Iterator[None]:
    names = ("KITARU_API_URL", "KITARU_API_KEY")
    original = {name: os.environ.get(name) for name in names}
    os.environ["KITARU_API_URL"] = api_url
    os.environ["KITARU_API_KEY"] = "local-development-key"
    try:
        yield
    finally:
        for name, value in original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _run(
    command: list[str], *, cwd: Path, environment: dict[str, str] | None = None
) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _build_compiled_commands() -> None:
    if not (EXAMPLE_DIR / "node_modules").exists():
        pytest.fail(
            "Standalone dependencies are missing. Run `pnpm --ignore-workspace "
            "install --frozen-lockfile` in the example directory."
        )
    _run(
        ["pnpm", "--filter", "@zenml-io/kitaru", "build"],
        cwd=REPOSITORY_ROOT,
    )
    _run(
        ["pnpm", "--filter", "@zenml-io/kitaru-vercel-ai", "build"],
        cwd=REPOSITORY_ROOT,
    )
    _run(["pnpm", "--ignore-workspace", "build"], cwd=EXAMPLE_DIR)


def _assert_documented_registration(invocation: str, *, env: tuple[str, ...]) -> None:
    """Assert the documented registration still teaches the shape that works."""
    readme = README_PATH.read_text(encoding="utf-8")
    start = readme.index(f"uv run kitaru {invocation}")
    command = readme[start : readme.index("\n```", start)]
    required = (
        '--command "node dist/main.js"',
        # A relative --working-dir resolves against whichever worker claims the
        # task, so the documented command has to capture an absolute path.
        '--working-dir "$PWD"',
        "--timeout-seconds 180",
        *(f"--env {pair}" for pair in env),
        *(f"--tool {tool}" for tool in TOOLS),
    )
    missing = [fragment for fragment in required if fragment not in command]
    assert not missing, (
        f"`uv run kitaru {invocation}` in {README_PATH} no longer documents "
        f"{missing}; the README must keep teaching a registration that works."
    )


def _documented_evaluator_source() -> str:
    matches = DOCUMENTED_EVALUATOR.findall(README_PATH.read_text(encoding="utf-8"))
    assert len(matches) == 1, "README must contain one stable evaluator source block"
    return matches[0]


def _ticket_id(session: SessionResponse) -> str:
    assert isinstance(session.inputs, str), session.inputs
    match = re.search(r"(?m)^Ticket ID:\s*(ticket-\d+)\s*$", session.inputs)
    assert match is not None, session.inputs
    return match.group(1)


def _node_output(node: SessionNodeResponse) -> dict[str, object]:
    assert isinstance(node.outputs, dict), (
        f"node {node.id} has non-object output: {node.outputs!r}"
    )
    return node.outputs


def _accepted_terminal(nodes: list[SessionNodeResponse]) -> SessionNodeResponse:
    accepted = [
        node
        for node in nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.status is NodeStatus.COMPLETED
        and node.tool_name in TERMINAL_TOOLS
        and _node_output(node).get("accepted") is True
    ]
    assert len(accepted) == 1, [
        (node.id, node.status, node.tool_name, node.outputs)
        for node in nodes
        if node.node_type is NodeType.TOOL_CALL and node.tool_name in TERMINAL_TOOLS
    ]
    return accepted[0]


def _assert_evidence(
    session: SessionResponse,
    nodes: list[SessionNodeResponse],
    expected: tuple[str, int | None],
) -> None:
    roots = [
        node
        for node in nodes
        if node.node_type is NodeType.SPAN and node.parent_index is None
    ]
    llm_nodes = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
    investigation = {
        node.tool_name
        for node in nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name in INVESTIGATION_TOOLS
    }
    assert session.status is SessionStatus.COMPLETED, (
        f"session {session.id} failed: {session.error}"
    )
    assert len(roots) == 1
    assert len(llm_nodes) >= 2
    assert all(node.requested_model == REQUESTED_MODEL_ID for node in llm_nodes)
    assert all(node.model == SERVED_MODEL_ID for node in llm_nodes)
    assert investigation == EXPECTED_INVESTIGATION_TOOLS[_ticket_id(session)]
    terminal = _accepted_terminal(nodes)
    action, amount = expected
    assert TERMINAL_TOOLS[terminal.tool_name] == action
    output = _node_output(terminal)
    if amount is not None:
        assert output.get("amount") == amount
    if action != "refund":
        assert not any(
            node.tool_name == "issue_refund"
            and node.status is NodeStatus.COMPLETED
            and _node_output(node).get("accepted") is True
            for node in nodes
            if node.node_type is NodeType.TOOL_CALL
        )


async def _nodes(
    client: KitaruAPIClient, session_id: uuid.UUID
) -> list[SessionNodeResponse]:
    return [
        node
        async for node in client.sessions.iter_nodes(
            session_id, SessionNodeListParams(include_payloads=True, size=100)
        )
    ]


async def _job_failure_details(client: KitaruAPIClient, job: JobResponse) -> str:
    tasks = await client.jobs.list_tasks(job.id)
    return json.dumps(
        {
            "job_id": str(job.id),
            "job_status": job.status,
            "job_error": job.error,
            "tasks": [
                {
                    "id": str(task.id),
                    "status": task.status,
                    "error": task.error,
                    "result_session_id": str(task.result_session_id)
                    if task.result_session_id
                    else None,
                }
                for task in tasks.items
            ],
        },
        default=str,
        indent=2,
    )


async def _wait_for_job(
    client: KitaruAPIClient, job_id: uuid.UUID, *, timeout: float = 300
) -> JobResponse:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        job = await client.jobs.get(job_id)
        if job.status in SETTLED_JOB_STATUSES:
            return job
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(f"Job {job_id} did not settle within {timeout} seconds")
        await asyncio.sleep(0.05)


async def _run_scoped_worker(
    job_id: uuid.UUID, *, api_url: str, state_dir: Path
) -> JobResponse:
    worker = Worker(
        WorkerConfig(
            name=f"vercel-returns-e2e-{job_id}",
            scope=WorkerScope(job_id=job_id),
            concurrency=4,
            poll_interval=0.05,
            timeout=300,
            blob_cache_root=state_dir / "blobs" / str(job_id),
            payload_cache_root=state_dir / "payloads" / str(job_id),
        )
    )
    with _worker_environment(api_url):
        await worker.run()
    async with KitaruAPIClient(base_url=api_url) as client:
        job = await _wait_for_job(client, job_id)
        if job.status is not JobStatus.COMPLETED:
            pytest.fail(await _job_failure_details(client, job))
        return job


async def _wait_for_run(
    client: KitaruAPIClient, run_id: uuid.UUID, *, timeout: float = 300
):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        run = await client.experiment_runs.get(run_id)
        if run.status in SETTLED_RUN_STATUSES:
            return run
        if asyncio.get_running_loop().time() >= deadline:
            jobs = await client.experiment_runs.list_jobs(run_id)
            pytest.fail(
                f"Experiment run {run_id} did not settle; jobs="
                f"{[(str(job.id), job.status, job.error) for job in jobs.items]}"
            )
        await asyncio.sleep(0.05)


async def _wait_for_replay(
    client: KitaruAPIClient, replay_id: uuid.UUID, *, timeout: float = 300
) -> ReplayResponse:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        replay = await client.replays.get(replay_id)
        if replay.status in SETTLED_REPLAY_STATUSES:
            return replay
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(
                f"Replay {replay_id} did not settle; job={replay.job_id}, "
                f"status={replay.status}, error={replay.error}"
            )
        await asyncio.sleep(0.05)


async def _sessions_in_cohort(
    client: KitaruAPIClient, cohort_version_id: uuid.UUID
) -> set[uuid.UUID]:
    page = await client.sessions.list(
        SessionListParams(
            filter=FilterCondition(
                field="cohort_version_id",
                op=FilterOp.EQ,
                value=str(cohort_version_id),
            ),
            size=100,
        )
    )
    return {session.id for session in page.items}


async def _replays_for_run(
    client: KitaruAPIClient, run_id: uuid.UUID
) -> list[ReplayResponse]:
    page = await client.replays.list(
        ReplayListParams(
            filter=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=str(run_id)
            ),
            size=100,
        )
    )
    return list(page.items)


async def test_typescript_canonical_improvement_loop(tmp_path: Path) -> None:
    """Record, score, cohort, and replay the deterministic TypeScript story."""
    _build_compiled_commands()

    async with _network_server() as api_url:
        async with KitaruAPIClient(base_url=api_url) as client:
            suffix = uuid.uuid4().hex[:10]
            agent = await client.agents.create(
                AgentCreateRequest(
                    name=f"vercel-returns-e2e-{suffix}",
                    description="Synthetic TypeScript returns resolver E2E.",
                )
            )
            _assert_documented_registration(
                "agent register", env=("RETURNS_POLICY_MODE=baseline",)
            )
            baseline_version = await client.agents.create_version(
                agent.id,
                AgentVersionCreateRequest(
                    display_version="baseline-v1",
                    description="Deterministic baseline returns policy.",
                    run_spec=RunSpec(
                        command="node dist/main.js",
                        working_dir=str(EXAMPLE_DIR),
                        env={
                            "KITARU_AGENT_ID": str(agent.id),
                            "RETURNS_POLICY_MODE": "baseline",
                        },
                        timeout_seconds=180,
                    ),
                    capabilities=AgentCapabilities(tools=TOOLS),
                ),
            )

        baseline_environment = os.environ.copy()
        baseline_environment.update(
            {
                "KITARU_API_URL": api_url,
                "KITARU_AGENT_ID": str(agent.id),
                "KITARU_AGENT_VERSION_ID": str(baseline_version.id),
            }
        )
        await asyncio.to_thread(
            _run,
            ["node", str(EXAMPLE_DIR / "dist" / "baseline.js")],
            cwd=tmp_path,
            environment=baseline_environment,
        )
        manifest = json.loads(
            (tmp_path / ".state" / "baseline-sessions.json").read_text(encoding="utf-8")
        )
        assert manifest["status"] == "completed"
        assert set(manifest["sessions"]) == set(BASELINE_ACTIONS)
        baseline_ids = {
            ticket_id: uuid.UUID(value["session_id"])
            for ticket_id, value in manifest["sessions"].items()
        }
        assert len(set(baseline_ids.values())) == 10

        async with KitaruAPIClient(base_url=api_url) as client:
            for ticket_id, session_id in baseline_ids.items():
                session = await client.sessions.get(session_id)
                assert session.agent_id == agent.id
                assert session.agent_version_id == baseline_version.id
                assert session.origin is SessionOrigin.RECORDED
                assert _ticket_id(session) == ticket_id
                _assert_evidence(
                    session,
                    await _nodes(client, session_id),
                    BASELINE_ACTIONS[ticket_id],
                )

            evaluator = await client.evaluators.create(
                EvaluatorCreateRequest(
                    name=f"returns-policy-e2e-{suffix}",
                    description="README-derived synthetic returns policy.",
                )
            )
            evaluator_blob = await client.blobs.upload(
                _documented_evaluator_source().encode(),
                media_type="text/x-python",
                filename="returns_policy.py",
            )
            evaluator_version = await client.evaluators.create_version(
                evaluator.id,
                EvaluatorVersionCreateRequest(
                    source=ScriptPluginSource(
                        blob_id=evaluator_blob.id, entrypoint="evaluate"
                    ),
                    display_version="1.0",
                ),
            )
            evaluation_job = await client.evaluations.create(
                EvaluationBatchCreateRequest(
                    input_session_ids=list(baseline_ids.values()),
                    evaluators=[EvaluatorConfig(evaluator=evaluator.name, version=1)],
                )
            )

        await _run_scoped_worker(
            evaluation_job.id, api_url=api_url, state_dir=tmp_path / "worker"
        )

        async with KitaruAPIClient(base_url=api_url) as client:
            evaluations = (
                await client.evaluations.list(
                    EvaluationListParams(
                        filter=FilterCondition(
                            field="evaluator_version_id",
                            op=FilterOp.EQ,
                            value=str(evaluator_version.id),
                        ),
                        size=100,
                    )
                )
            ).items
            assert len(evaluations) == 10
            ticket_by_session = {
                session_id: ticket_id for ticket_id, session_id in baseline_ids.items()
            }
            results = {
                ticket_by_session[evaluation.session_id]: evaluation.passed
                for evaluation in evaluations
            }
            assert sum(passed is True for passed in results.values()) == 8
            assert {
                ticket_id for ticket_id, passed in results.items() if passed is False
            } == {"ticket-004", "ticket-007"}

            target = await client.cohorts.create(
                CohortCreateRequest(
                    name=f"unsafe-refund-baseline-{suffix}",
                    agent_id=agent.id,
                )
            )
            target_version = await client.cohorts.create_version(
                target.id,
                CohortVersionCreateRequest(
                    add_session_ids=[baseline_ids[ticket] for ticket in TARGET_TICKETS],
                    display_version="baseline-targets",
                ),
            )
            control = await client.cohorts.create(
                CohortCreateRequest(
                    name=f"safe-refund-control-{suffix}",
                    agent_id=agent.id,
                )
            )
            control_version = await client.cohorts.create_version(
                control.id,
                CohortVersionCreateRequest(
                    add_session_ids=[
                        baseline_ids[ticket] for ticket in CONTROL_TICKETS
                    ],
                    display_version="baseline-controls",
                ),
            )
            assert target_version.session_count == 2
            assert control_version.session_count == 3
            assert await _sessions_in_cohort(client, target_version.id) == {
                baseline_ids[ticket] for ticket in TARGET_TICKETS
            }
            assert await _sessions_in_cohort(client, control_version.id) == {
                baseline_ids[ticket] for ticket in CONTROL_TICKETS
            }

            _assert_documented_registration(
                "agent version register",
                env=(
                    "RETURNS_POLICY_MODE=strict",
                    'KITARU_AGENT_ID="${KITARU_AGENT_ID}"',
                ),
            )
            strict_version = await client.agents.create_version(
                agent.id,
                AgentVersionCreateRequest(
                    display_version="strict-policy-v2",
                    description="Require approval for risky or oversized refunds.",
                    run_spec=RunSpec(
                        command="node dist/main.js",
                        working_dir=str(EXAMPLE_DIR),
                        env={
                            "KITARU_AGENT_ID": str(agent.id),
                            "RETURNS_POLICY_MODE": "strict",
                        },
                        timeout_seconds=180,
                    ),
                    capabilities=AgentCapabilities(tools=TOOLS),
                ),
            )
            experiment = await client.experiments.create(
                ExperimentCreateRequest(
                    name=f"improve-returns-policy-{suffix}",
                    agent_id=agent.id,
                    tool_policy=ToolPolicy(default=PassthroughConfig()),
                    evaluators=[EvaluatorConfig(evaluator=evaluator.name, version=1)],
                )
            )
            runs = [
                await client.experiments.start_run(
                    experiment.id,
                    ExperimentRunCreateRequest(
                        cohort_version_id=cohort_version.id,
                        agent_version_id=strict_version.id,
                        evaluate_baselines=True,
                    ),
                )
                for cohort_version in (target_version, control_version)
            ]
            pending_replays = [
                replay
                for run in runs
                for replay in await _replays_for_run(client, run.id)
            ]
            assert len(pending_replays) == 5
            jobs = [await client.jobs.get(replay.job_id) for replay in pending_replays]
            assert len(jobs) == 5

        for job in jobs:
            await _run_scoped_worker(
                job.id, api_url=api_url, state_dir=tmp_path / "worker"
            )

        async with KitaruAPIClient(base_url=api_url) as client:
            replays = [
                await _wait_for_replay(client, replay.id) for replay in pending_replays
            ]
            assert all(replay.status is ReplayStatus.COMPLETED for replay in replays), [
                (replay.id, replay.job_id, replay.status, replay.error)
                for replay in replays
            ]
            settled_runs = [await _wait_for_run(client, run.id) for run in runs]
            assert all(
                run.status is ExperimentRunStatus.COMPLETED for run in settled_runs
            ), [(run.id, run.status, run.error, run.progress) for run in settled_runs]
            assert sum(run.progress.completed for run in settled_runs) == 5
            assert sum(run.progress.failed for run in settled_runs) == 0

            assert len(replays) == 5
            assert {replay.experiment_run_id for replay in replays} == {
                run.id for run in settled_runs
            }
            assert all(replay.result_session_id is not None for replay in replays)

            replay_by_ticket: dict[str, SessionResponse] = {}
            for replay in replays:
                assert replay.result_session_id is not None
                session = await client.sessions.get(replay.result_session_id)
                ticket_id = _ticket_id(session)
                replay_by_ticket[ticket_id] = session
                assert session.agent_version_id == strict_version.id
                assert session.origin is SessionOrigin.REPLAY
                _assert_evidence(
                    session, await _nodes(client, session.id), STRICT_ACTIONS[ticket_id]
                )
            assert set(replay_by_ticket) == set(STRICT_ACTIONS)

            replay_ids = {session.id for session in replay_by_ticket.values()}
            all_policy = (
                await client.evaluations.list(
                    EvaluationListParams(
                        filter=FilterCondition(
                            field="evaluator_version_id",
                            op=FilterOp.EQ,
                            value=str(evaluator_version.id),
                        ),
                        size=100,
                    )
                )
            ).items
            replay_policy = [
                evaluation
                for evaluation in all_policy
                if evaluation.session_id in replay_ids
            ]
            assert len(replay_policy) == 5
            assert all(evaluation.passed is True for evaluation in replay_policy)
