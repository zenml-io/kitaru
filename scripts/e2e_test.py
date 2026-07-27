#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""End-to-end test driver for the record, replay, and experiment loop."""

import asyncio
import os
import sys
import traceback
import uuid
from pathlib import Path

from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.api_keys import ApiKeyCreateRequest
from kitaru.api_models.v1.cohorts import CohortCreateRequest
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.experiments import (
    ExperimentCreateRequest,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.jobs import (
    HistoryPolicy,
    HistoryScope,
    ImportStats,
    JobClaimRequest,
    JobKind,
    JobResponse,
    JobStatus,
    RegistryScorerConfig,
    ReplayOverride,
    ScoringPolicy,
    SourceScorerConfig,
    StaticCase,
    StaticPolicy,
    ToolPolicyConfig,
    ToolPolicyOnMiss,
    WorkerScope,
)
from kitaru.api_models.v1.replays import ReplayCreateRequest, ReplayResponse
from kitaru.api_models.v1.secrets import SecretCreateRequest
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.api_models.v1.session_runs import SessionRunCreateRequest
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionProvider,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.worker import Worker, WorkerConfig

API_URL = os.environ.get("KITARU_E2E_API_URL", "http://127.0.0.1:8300")
ACCOUNT_NAME = os.environ.get("KITARU_E2E_ACCOUNT_NAME", "default")
ACCOUNT_PASSWORD = os.environ.get("KITARU_E2E_ACCOUNT_PASSWORD", "password")

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENT_PYTHON = str(REPO_ROOT / ".venv" / "bin" / "python")
REGISTRY_SCORER_FILE = REPO_ROOT / "scripts" / "e2e_registry_scorer.py"
IMPORTER_FILE = REPO_ROOT / "importer_example" / "importer.py"
TRACE_FILE = REPO_ROOT / "importer_example" / "trace.jsonl"
ORIGINAL_MODEL = "mock-gpt-4"
OVERRIDE_MODEL = "mock-claude-opus"
SECRET_VALUE = "e2e-secret-token-123"
STATIC_CALC_RESULT = {"expression": "21 * 2", "result": 42.0, "canned": True}
SCORER_NAMES = ("answer_quality", "tool_efficiency", "token_budget")
REGISTRY_SCORER_NAME = "e2e-output-length"
REGISTRY_SCORER_ENTRYPOINT = "output_length"
IMPORTER_NAME = "e2e-trace-importer"
IMPORTER_ENTRYPOINT = "parse"
IMPORTER_PROVIDER = "otlp"
TRACE_MEDIA_TYPE = "application/x-ndjson"
IMPORTED_NODE_COUNTS = {
    "trace-2026-07-20-001": 5,
    "trace-2026-07-20-002": 3,
    "trace-2026-07-20-004": 4,
}

# Wall clock bound on every worker entry, so a stuck job fails the run
# instead of hanging it.
WORKER_TIMEOUT_SECONDS = 300.0
WORKER_POLL_INTERVAL_SECONDS = 0.5

TERMINAL_JOB_STATUSES = (
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.TIMED_OUT,
    JobStatus.CANCELED,
)

POPULATE_RUNS: list[dict[str, str] | str] = [
    '{"question": "What is the weather in Berlin, and what is 21 * 2?"}',
    "Weather in Berlin plus 21 * 2, please answer briefly.",
    {"KITARU_JOB_INPUTS": '{"question": "Berlin forecast and the answer to 21 * 2?"}'},
    {
        "KITARU_JOB_INPUTS": '{"question": "Give me Berlin weather and 21 * 2."}',
        "KITARU_JOB_SESSION_NAME": "e2e-run-4",
    },
]


def scoring_policy(registry_version: int | None = None) -> ScoringPolicy:
    """Build the scoring policy shared by the experiment and standalone replay."""
    scorers: list[SourceScorerConfig | RegistryScorerConfig] = [
        SourceScorerConfig(
            name="answer_quality",
            source="adapter_example.scorers:answer_quality",
            params={"keywords": ["answer"]},
            weight=2.0,
        ),
        SourceScorerConfig(
            name="tool_efficiency",
            source="adapter_example.scorers:tool_efficiency",
            params={"budget": 4},
            weight=1.0,
        ),
        SourceScorerConfig(
            name="token_budget",
            source="adapter_example.scorers:token_budget",
            params={"max_tokens": 4000},
            weight=0.5,
        ),
    ]
    if registry_version is not None:
        scorers.append(
            RegistryScorerConfig(
                name=REGISTRY_SCORER_NAME,
                version=registry_version,
                params={"max_chars": 400},
                weight=0.5,
            )
        )
    return ScoringPolicy(scorers=scorers, pass_threshold=0.4)


def log(message: str) -> None:
    """Print a progress line."""
    print(f"[e2e] {message}", flush=True)


def ok(message: str) -> None:
    """Print a PASS line."""
    print(f"[e2e] PASS: {message}", flush=True)


def check(condition: bool, message: str) -> None:
    """Assert a condition with a readable message."""
    if not condition:
        raise AssertionError(message)


def worker_config(name: str, scope: WorkerScope, concurrency: int = 1) -> WorkerConfig:
    """Build a worker config for one e2e scope."""
    return WorkerConfig(
        name=name,
        scope=scope,
        concurrency=concurrency,
        poll_interval=WORKER_POLL_INTERVAL_SECONDS,
        timeout=WORKER_TIMEOUT_SECONDS,
    )


async def run_experiment_run(run_id: uuid.UUID, concurrency: int = 2) -> None:
    """Drain an experiment run with a run-pinned worker."""
    await Worker(
        worker_config(
            f"e2e-run-{run_id.hex[:8]}",
            WorkerScope(experiment_run_id=run_id),
            concurrency,
        )
    ).run()


async def run_job(client: KitaruAPIClient, job_id: uuid.UUID) -> JobResponse:
    """Drain one job and its fan-out children with a job-pinned worker."""
    await Worker(
        worker_config(f"e2e-job-{job_id.hex[:8]}", WorkerScope(job_id=job_id))
    ).run()
    return await client.jobs.get(job_id)


async def run_pool_worker(
    client: KitaruAPIClient, job_id: uuid.UUID, scope: WorkerScope
) -> JobResponse:
    """Drain one pool job with an unpinned worker stopped by a watcher.

    An unpinned scope claims until it is told to stop, so a watcher polls
    the job and sets the stop event once it goes terminal.
    """
    stop = asyncio.Event()

    async def watch() -> None:
        while not stop.is_set():
            job = await client.jobs.get(job_id)
            if job.status in TERMINAL_JOB_STATUSES:
                stop.set()
                return
            await asyncio.sleep(WORKER_POLL_INTERVAL_SECONDS)

    watcher = asyncio.create_task(watch())
    try:
        await Worker(worker_config("e2e-pool-worker", scope)).run(stop)
    finally:
        stop.set()
        await watcher
    return await client.jobs.get(job_id)


async def run_agent_process(env: dict[str, str], argument: str | None = None) -> None:
    """Run the mock agent entrypoint as a regular subprocess."""
    args = [AGENT_PYTHON, "-m", "adapter_example.main"]
    if argument is not None:
        args.append(argument)
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    check(
        process.returncode == 0,
        f"Agent process exited with {process.returncode}: "
        f"{stderr.decode(errors='replace')}",
    )
    log(f"agent output: {stdout.decode(errors='replace').strip()}")


def populate_env(api_key: str, agent_id: uuid.UUID, version_id: uuid.UUID) -> dict:
    """Build the base environment for regular agent executions."""
    env = dict(os.environ)
    env.pop("KITARU_JOB_ID", None)
    env.pop("KITARU_JOB_INPUTS", None)
    env.pop("KITARU_JOB_SESSION_NAME", None)
    env["KITARU_API_URL"] = API_URL
    env["KITARU_API_KEY"] = api_key
    env["KITARU_E2E_AGENT_ID"] = str(agent_id)
    env["KITARU_E2E_AGENT_VERSION_ID"] = str(version_id)
    return env


def check_session_tree(
    session: SessionResponse, nodes: list[SessionNodeResponse]
) -> None:
    """Assert the node tree and rollups of a completed mock agent session."""
    check(session.status is SessionStatus.COMPLETED, "session is not completed")
    check(
        session.llm_call_count == 2,
        f"expected llm_call_count 2, got {session.llm_call_count}",
    )
    check(
        session.tool_call_count == 2,
        f"expected tool_call_count 2, got {session.tool_call_count}",
    )
    check(session.cost is not None and session.cost > 0, "session cost missing")
    check(session.tokens is not None, "session token rollup missing")
    assert session.tokens is not None
    check(
        (session.tokens.input_tokens or 0) > 0
        and (session.tokens.output_tokens or 0) > 0,
        "session token counts missing",
    )
    check(len(nodes) == 5, f"expected 5 nodes, got {len(nodes)}")
    root = nodes[0]
    check(root.node_type is NodeType.SPAN and root.parent_id is None, "bad root node")
    check(root.status is NodeStatus.COMPLETED, "root span not completed")
    types = [node.node_type for node in nodes]
    check(
        types
        == [
            NodeType.SPAN,
            NodeType.LLM_CALL,
            NodeType.TOOL_CALL,
            NodeType.TOOL_CALL,
            NodeType.LLM_CALL,
        ],
        f"unexpected node type order {types}",
    )
    tool_nodes = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
    check(
        {node.tool_name for node in tool_nodes} == {"get_weather", "calculate"},
        "unexpected tool names",
    )
    for node in tool_nodes:
        check(node.cache_key is not None, "tool node has no cache key")
    for node in nodes:
        check(bool(node.key), "node has no key")


def node_by_tool(
    nodes: list[SessionNodeResponse], tool_name: str
) -> SessionNodeResponse:
    """Return the single tool call node for a tool name."""
    matches = [node for node in nodes if node.tool_name == tool_name]
    check(len(matches) == 1, f"expected one {tool_name} node, got {len(matches)}")
    return matches[0]


def check_llm_models(nodes: list[SessionNodeResponse], expect_model: str) -> None:
    """Assert every LLM call node used the expected model."""
    for node in nodes:
        if node.node_type is NodeType.LLM_CALL:
            check(
                node.model == expect_model,
                f"expected model {expect_model}, got {node.model}",
            )


async def check_replay_result(
    client: KitaruAPIClient,
    replay: ReplayResponse,
    job: JobResponse,
    original: SessionResponse,
    expect_model: str,
    expect_calculate_policy: str,
    scorer_names: tuple[str, ...] = SCORER_NAMES,
) -> None:
    """Assert a settled replay, its job, its result session, and its nodes."""
    check(
        job.status is JobStatus.COMPLETED,
        f"replay job {job.id} is {job.status}: {job.error}",
    )
    check(replay.job_id == job.id, "replay is not bound to its job")
    check(replay.error is None, f"replay {replay.id} failed: {replay.error}")
    check(replay.passed is not None, "replay has no passed outcome")
    check(replay.score is not None, "replay has no score")
    check(
        replay.scores is not None and set(replay.scores) == set(scorer_names),
        f"replay scores incomplete: {replay.scores}",
    )
    check(job.result_session_id is not None, "replay job has no result session")
    assert job.result_session_id is not None
    check(
        replay.result_session_id == job.result_session_id,
        "replay result session differs from the job's",
    )
    check(job.result is None, f"replay job carries a result: {job.result}")

    children = await client.jobs.list(kind=JobKind.SCORE, page_size=50)
    scored = [child for child in children.items if child.parent_job_id == job.id]
    check(scored != [], f"replay job {job.id} fanned out no score jobs")
    for child in scored:
        check(
            child.status is JobStatus.COMPLETED,
            f"score job {child.id} is {child.status}: {child.error}",
        )
        check(
            isinstance(child.result, int | float) and 0 <= child.result <= 1,
            f"score job {child.id} carries no score result: {child.result}",
        )

    result = await client.sessions.get(job.result_session_id)
    check(result.origin is SessionOrigin.REPLAY, "result session origin is not replay")
    nodes = await client.session_nodes.list(result.id, include_payloads=True)
    check_session_tree(result, nodes)
    check_llm_models(nodes, expect_model)

    original_nodes = await client.session_nodes.list(original.id, include_payloads=True)
    weather = node_by_tool(nodes, "get_weather")
    check(
        weather.attributes == {"mocked": True, "policy": "history"},
        (f"unexpected get_weather attributes {weather.attributes}"),
    )
    check(
        weather.outputs == node_by_tool(original_nodes, "get_weather").outputs,
        "history-mocked result differs from the recorded output",
    )
    calculate = node_by_tool(nodes, "calculate")
    check(
        calculate.attributes == {"mocked": True, "policy": expect_calculate_policy},
        f"unexpected calculate attributes {calculate.attributes}",
    )
    if expect_calculate_policy == "static":
        check(
            calculate.outputs == STATIC_CALC_RESULT,
            f"static case result not returned: {calculate.outputs}",
        )


async def check_replay_diff(
    client: KitaruAPIClient,
    replay: ReplayResponse,
    job: JobResponse,
    original_model: str,
    effective_model: str,
    scorer_names: tuple[str, ...] = SCORER_NAMES,
) -> None:
    """Assert the computed diff aligns the node trees and shows the override."""
    diff = await client.replays.get_diff(replay.id)
    check(diff.replay_id == replay.id, "diff replay id")
    check(diff.original_session_id == replay.input_session_id, "diff original id")
    check(diff.result_session_id == job.result_session_id, "diff result id")
    check(
        len(diff.node_pairs) == 5,
        f"expected 5 aligned node pairs, got {len(diff.node_pairs)}",
    )
    check(diff.added_nodes == [], f"unexpected added nodes {diff.added_nodes}")
    check(diff.removed_nodes == [], f"unexpected removed nodes {diff.removed_nodes}")
    check(
        diff.input_diff.model.original == [original_model],
        f"diff original models {diff.input_diff.model.original}",
    )
    check(
        diff.input_diff.model.effective == [effective_model],
        f"diff effective models {diff.input_diff.model.effective}",
    )
    check(
        any(pair.cost_delta is not None for pair in diff.node_pairs),
        "no node pair reports a cost delta",
    )
    check(
        any(pair.token_deltas.input_tokens is not None for pair in diff.node_pairs),
        "no node pair reports token deltas",
    )
    mocked_pairs = [pair for pair in diff.node_pairs if pair.mocked]
    check(len(mocked_pairs) == 2, f"expected 2 mocked pairs, got {len(mocked_pairs)}")
    for name in scorer_names:
        delta = diff.score_deltas.get(name)
        check(delta is not None, f"diff lacks score delta for {name}")
        assert delta is not None
        check(
            delta.original is not None and delta.replay is not None,
            f"score delta for {name} has missing sides: {delta}",
        )


def check_run_summary(run: ExperimentRunResponse, replay_count: int) -> None:
    """Assert the aggregate summary of a completed experiment run."""
    check(
        run.status is ExperimentRunStatus.COMPLETED,
        f"run is {run.status}: {run.error}",
    )
    check(run.summary is not None, "run has no summary")
    assert run.summary is not None
    counts = run.summary["replay_counts_by_status"]
    check(
        counts == {"completed": replay_count},
        f"unexpected job counts {counts}",
    )
    check(run.summary["pass_rate"] == 1.0, f"pass_rate {run.summary['pass_rate']}")
    scores = run.summary["scores"]
    check(set(scores) == set(SCORER_NAMES), f"summary scorer names {set(scores)}")
    for name in SCORER_NAMES:
        for side in ("baseline", "replay"):
            stats = scores[name][side]
            check(
                stats["mean"] is not None and stats["median"] is not None,
                f"summary stats missing for {name} {side}",
            )
    total_cost = run.summary["total_cost"]
    check(
        total_cost["baseline"] is not None and total_cost["baseline"] > 0,
        f"baseline total cost {total_cost}",
    )
    check(
        total_cost["replay"] is not None and total_cost["replay"] > 0,
        f"job total cost {total_cost}",
    )
    total_tokens = run.summary["total_tokens"]
    for side in ("baseline", "replay"):
        for kind in ("input_tokens", "output_tokens"):
            count = total_tokens[side][kind]
            check(
                count is not None and count > 0,
                f"{side} total {kind} {total_tokens}",
            )


def check_import_stats(
    job: JobResponse, created: int, skipped: int, failed: int
) -> None:
    """Assert the stats an import job shipped as its result."""
    check(
        job.status is JobStatus.COMPLETED,
        f"import job {job.id} is {job.status}: {job.error}",
    )
    check(job.result is not None, "import job has no result")
    stats = ImportStats.model_validate(job.result)
    counts = (stats.created, stats.skipped, stats.failed)
    check(
        counts == (created, skipped, failed),
        f"unexpected import stats {counts}, expected {(created, skipped, failed)}",
    )
    check(
        len(stats.failures) == failed,
        f"unexpected failure sample {stats.failures}",
    )


async def check_imported_sessions(client: KitaruAPIClient, agent_id: uuid.UUID) -> None:
    """Assert the imported sessions and their node trees."""
    page = await client.sessions.list(
        agent_id=agent_id, origin=SessionOrigin.IMPORTED, page_size=50
    )
    check(page.total == 3, f"expected 3 imported sessions, got {page.total}")
    by_external_id = {session.external_id: session for session in page.items}
    check(
        set(by_external_id) == set(IMPORTED_NODE_COUNTS),
        f"unexpected imported external ids {sorted(by_external_id)}",
    )
    for external_id, expected in IMPORTED_NODE_COUNTS.items():
        session = by_external_id[external_id]
        check(
            session.provider is SessionProvider.OTLP,
            f"imported session provider {session.provider}",
        )
        check(
            session.status is SessionStatus.COMPLETED,
            f"imported session {external_id} is {session.status}",
        )
        nodes = await client.session_nodes.list(session.id, include_payloads=True)
        check(
            len(nodes) == expected,
            f"imported session {external_id} has {len(nodes)} nodes, "
            f"expected {expected}",
        )
        check(
            nodes[0].parent_id is None, f"imported root of {external_id} has a parent"
        )
        for node in nodes:
            check(bool(node.key), f"imported node of {external_id} has no key")


async def main() -> int:
    """Run the end-to-end loop against a running server."""
    log(f"target server: {API_URL}")

    # Step a: login with the default account and create an API key.
    async with KitaruAPIClient(base_url=API_URL) as anonymous:
        token = await anonymous.auth.login(ACCOUNT_NAME, ACCOUNT_PASSWORD)
    async with KitaruAPIClient(
        base_url=API_URL, api_key=token.access_token
    ) as jwt_client:
        issued = await jwt_client.api_keys.create(ApiKeyCreateRequest(name="e2e"))
    api_key = issued.key
    # The worker reads its connection from the process environment.
    os.environ["KITARU_API_URL"] = API_URL
    os.environ["KITARU_API_KEY"] = api_key
    ok(f"logged in as {ACCOUNT_NAME} and created API key {issued.name}")

    async with KitaruAPIClient(base_url=API_URL, api_key=api_key) as client:
        # Step b: secret, agent, and runnable agent version.
        secret = await client.secrets.create(
            SecretCreateRequest(
                name="e2e-secret", values={"E2E_SECRET_TOKEN": SECRET_VALUE}
            )
        )
        agent = await client.agents.create(
            AgentCreateRequest(name="e2e-mock-agent", description="E2E mock agent")
        )
        version = await client.agent_versions.create(
            agent.id,
            AgentVersionCreateRequest(
                version="v1",
                run_spec=RunSpec(
                    command=f"{AGENT_PYTHON} -m adapter_example.main",
                    working_dir=str(REPO_ROOT),
                    env={"KITARU_E2E_AGENT_ID": str(agent.id)},
                    secret_ids=[secret.id],
                    timeout_seconds=120,
                ),
            ),
        )
        # The version id only exists after creation, so patch it into the env.
        version = await client.agent_versions.update(
            version.id,
            AgentVersionUpdateRequest(
                run_spec=RunSpec(
                    command=f"{AGENT_PYTHON} -m adapter_example.main",
                    working_dir=str(REPO_ROOT),
                    env={
                        "KITARU_E2E_AGENT_ID": str(agent.id),
                        "KITARU_E2E_AGENT_VERSION_ID": str(version.id),
                    },
                    secret_ids=[secret.id],
                    timeout_seconds=120,
                ),
            ),
        )
        check(version.run_spec is not None, "agent version has no run spec")
        ok(f"created secret, agent {agent.id}, and runnable version {version.id}")

        # Step c: populate four regular recorded sessions.
        base_env = populate_env(api_key, agent.id, version.id)
        for index, variant in enumerate(POPULATE_RUNS, start=1):
            env = dict(base_env)
            argument = None
            if isinstance(variant, str):
                argument = variant
            else:
                env.update(variant)
            log(f"populate run {index}/4")
            await run_agent_process(env, argument)

        page = await client.sessions.list(agent_id=agent.id, page_size=50)
        check(page.total == 4, f"expected 4 sessions, got {page.total}")
        sessions = sorted(page.items, key=lambda session: session.created)
        for session in sessions:
            check(
                session.origin is SessionOrigin.RECORDED,
                f"session origin {session.origin}",
            )
            check(
                session.agent_version_id == version.id,
                "session agent version not recorded",
            )
            check(session.inputs is not None, "session inputs missing")
            nodes = await client.session_nodes.list(session.id, include_payloads=True)
            check_session_tree(session, nodes)
        named = [session for session in sessions if session.name == "e2e-run-4"]
        check(len(named) == 1, "KITARU_JOB_SESSION_NAME was not recorded")
        ok("4 recorded sessions completed with node trees and rollups")

        # Step d: cohort and experiment with override, tool, and scoring policy.
        cohort = await client.cohorts.create(
            CohortCreateRequest(
                name="e2e-cohort",
                agent_id=agent.id,
                session_ids=[session.id for session in sessions],
            )
        )
        check(cohort.session_count == 4, f"cohort size {cohort.session_count}")
        experiment = await client.experiments.create(
            ExperimentCreateRequest(
                name="e2e-experiment",
                cohort_id=cohort.id,
                override=ReplayOverride(model=OVERRIDE_MODEL),
                tool_policy=ToolPolicyConfig(
                    default=HistoryPolicy(
                        scope=HistoryScope.ORIGINAL_SESSION,
                        on_miss=ToolPolicyOnMiss.FAIL,
                    ),
                    tools={
                        "calculate": StaticPolicy(
                            cases=[
                                StaticCase(
                                    match={"expression": "21 * 2"},
                                    result=STATIC_CALC_RESULT,
                                )
                            ]
                        )
                    },
                ),
                scoring_policy=scoring_policy(),
            )
        )
        ok(f"created cohort {cohort.id} and experiment {experiment.id}")

        # Step e: experiment run with baseline scoring.
        run = await client.experiments.create_run(
            experiment.id, ExperimentRunCreateRequest(score_baselines=True)
        )
        check(run.agent_version_id == version.id, "run resolved the wrong version")
        check(run.score_baselines is True, "run does not score baselines")
        check(
            run.progress.pending == 4 and run.progress.total == 4,
            f"unexpected initial progress {run.progress}",
        )
        jobs_page = await client.experiment_runs.list_jobs(run.id, page_size=50)
        check(jobs_page.total == 4, f"expected 4 jobs, got {jobs_page.total}")
        spec = await client.jobs.get_spec(jobs_page.items[0].id)
        check(
            spec.secret_env == {"E2E_SECRET_TOKEN": SECRET_VALUE},
            f"secret env not resolved: {list(spec.secret_env)}",
        )
        check(spec.scorer is None, "replay spec carries a scorer")
        replays_page = await client.replays.list(experiment_run_id=run.id, page_size=50)
        check(replays_page.total == 4, f"expected 4 replays, got {replays_page.total}")
        check(
            all(replay.passed is None for replay in replays_page.items),
            "a replay is settled before the run executed",
        )
        ok("experiment run created with 4 pending jobs, replays, and secret env")

        # Step f: execute the run with a run-pinned worker.
        await run_experiment_run(run.id, concurrency=2)
        run = await client.experiment_runs.get(run.id)
        check_run_summary(run, replay_count=4)
        ok("experiment run completed with an aggregate summary")

        replays_page = await client.replays.list(experiment_run_id=run.id, page_size=50)
        check(replays_page.total == 4, f"expected 4 replays, got {replays_page.total}")
        sessions_by_id = {session.id: session for session in sessions}
        for replay in replays_page.items:
            job = await client.jobs.get(replay.job_id)
            original = sessions_by_id[replay.input_session_id]
            await check_replay_result(
                client,
                replay,
                job,
                original,
                expect_model=OVERRIDE_MODEL,
                expect_calculate_policy="static",
            )
            await check_replay_diff(client, replay, job, ORIGINAL_MODEL, OVERRIDE_MODEL)
        ok("all 4 replays completed, scored, mocked as configured, and diffed")

        for session in sessions:
            refreshed = await client.sessions.get(session.id)
            check(
                set(refreshed.scores) == set(SCORER_NAMES),
                f"baseline scores missing on {session.id}: {refreshed.scores}",
            )
        ok("baseline scores written to every original session")

        # Step g: standalone replay scored by a registered scorer as well.
        registered = await client.scorers.register(
            REGISTRY_SCORER_NAME,
            REGISTRY_SCORER_FILE,
            REGISTRY_SCORER_ENTRYPOINT,
        )
        check(
            registered.version >= 1,
            f"unexpected scorer version {registered.version}",
        )
        registry_names = (*SCORER_NAMES, REGISTRY_SCORER_NAME)
        standalone = await client.replays.create(
            ReplayCreateRequest(
                input_session_id=sessions[0].id,
                scoring_policy=scoring_policy(registry_version=registered.version),
            )
        )
        check(standalone.experiment_run_id is None, "standalone replay has a run")
        check(standalone.passed is None, "standalone replay is settled on creation")
        standalone_job = await run_job(client, standalone.job_id)
        standalone = await client.replays.get(standalone.id)
        await check_replay_result(
            client,
            standalone,
            standalone_job,
            sessions[0],
            expect_model=ORIGINAL_MODEL,
            expect_calculate_policy="history",
            scorer_names=registry_names,
        )
        await check_replay_diff(
            client,
            standalone,
            standalone_job,
            ORIGINAL_MODEL,
            ORIGINAL_MODEL,
            scorer_names=registry_names,
        )
        check(
            standalone.scores is not None
            and standalone.scores[REGISTRY_SCORER_NAME] > 0,
            f"registered scorer produced no score: {standalone.scores}",
        )
        passed_page = await client.replays.list(passed=True, page_size=50)
        check(
            standalone.id in {replay.id for replay in passed_page.items},
            "the settled standalone replay is missing from the passed filter",
        )
        ok("standalone replay completed, scored by a registered scorer, and diffed")

        # Step h: session run drained by a version-pinned pool worker.
        live_inputs = {"question": "Live run: Berlin weather and 21 * 2?"}
        live_job = await client.session_runs.create(
            SessionRunCreateRequest(
                agent_version_id=version.id,
                inputs=live_inputs,
                name="e2e-live-run",
            )
        )
        live_job = await run_pool_worker(
            client,
            live_job.id,
            WorkerScope(agent_version_ids=[version.id], kinds=[JobKind.SESSION_RUN]),
        )
        check(
            live_job.status is JobStatus.COMPLETED,
            f"session run job is {live_job.status}: {live_job.error}",
        )
        check(live_job.result_session_id is not None, "session run has no session")
        assert live_job.result_session_id is not None
        live = await client.sessions.get(live_job.result_session_id)
        check(live.origin is SessionOrigin.RECORDED, "live session origin")
        check(
            live.status is SessionStatus.COMPLETED,
            f"live session is {live.status}: {live.error}",
        )
        check(live.inputs == live_inputs, f"live session inputs {live.inputs}")
        check(live.name == "e2e-live-run", f"live session name {live.name}")
        check(
            live.agent_version_id == version.id,
            "live session agent version not recorded",
        )
        live_nodes = await client.session_nodes.list(live.id, include_payloads=True)
        check_session_tree(live, live_nodes)
        check_llm_models(live_nodes, ORIGINAL_MODEL)
        ok("session run drained by a version-pinned pool worker")

        # Step i: negative controls.
        try:
            await client.experiments.update(
                experiment.id,
                ExperimentUpdateRequest(override=ReplayOverride(model="other")),
            )
        except APIError as error:
            check(
                error.status_code == 409,
                f"expected 409 on frozen experiment, got {error.status_code}",
            )
        else:
            raise AssertionError("config update on a frozen experiment succeeded")
        negative_worker = await client.workers.create(
            WorkerCreateRequest(
                name="e2e-negative", scope=WorkerScope(experiment_run_id=run.id)
            )
        )
        check(
            negative_worker.scope.experiment_run_id == run.id,
            f"worker scope not stored: {negative_worker.scope}",
        )
        claim = await client.jobs.claim(
            JobClaimRequest(
                worker_id=negative_worker.id,
                max_jobs=10,
                scope=WorkerScope(experiment_run_id=run.id),
            )
        )
        check(claim.jobs == [], "claim on a completed run returned jobs")
        claim = await client.jobs.claim(
            JobClaimRequest(
                worker_id=negative_worker.id,
                max_jobs=10,
                scope=WorkerScope(kinds=[JobKind.REPLAY]),
            )
        )
        check(claim.jobs == [], "kind-scoped claim on drained pool work returned jobs")
        ok("frozen experiment rejects config updates, drained scopes claim nothing")

        # Step j: import recorded traces through an import job.
        registered_importer = await client.importers.register(
            IMPORTER_NAME,
            IMPORTER_FILE,
            IMPORTER_ENTRYPOINT,
            provider=IMPORTER_PROVIDER,
        )
        payload = await client.blobs.upload(TRACE_FILE.read_bytes(), TRACE_MEDIA_TYPE)
        import_request = ImportCreateRequest(
            importer=IMPORTER_NAME,
            agent_id=agent.id,
            version=registered_importer.version,
            payload_blob_id=payload.id,
        )
        import_job = await client.imports.create(import_request)
        check(import_job.agent_id == agent.id, "import job is not bound to the agent")
        import_job = await run_job(client, import_job.id)
        check_import_stats(import_job, created=3, skipped=0, failed=1)
        await check_imported_sessions(client, agent.id)
        ok("import job completed, 3 traces imported with their node trees")

        # Step k: re-importing the same payload skips every session.
        repeat = await client.imports.create(import_request)
        repeat = await run_job(client, repeat.id)
        check_import_stats(repeat, created=0, skipped=3, failed=1)
        page = await client.sessions.list(
            agent_id=agent.id, origin=SessionOrigin.IMPORTED, page_size=50
        )
        check(page.total == 3, f"re-import changed the session count to {page.total}")
        ok("re-import skipped all 3 sessions on the duplicate external ids")

    print("[e2e] ALL STEPS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:
        traceback.print_exc()
        print("[e2e] FAILED", flush=True)
        sys.exit(1)
