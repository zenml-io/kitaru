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
"""End-to-end test driver for the record, job, and experiment loop."""

import asyncio
import os
import sys
import traceback
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
# The runner imports adapter_example.scorers in-process for scoring.
sys.path.insert(0, str(REPO_ROOT))

from kitaru import Runner  # noqa: E402
from kitaru.api_models.v1.agent_versions import (  # noqa: E402
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest  # noqa: E402
from kitaru.api_models.v1.api_keys import ApiKeyCreateRequest  # noqa: E402
from kitaru.api_models.v1.cohorts import CohortCreateRequest  # noqa: E402
from kitaru.api_models.v1.experiment_runs import (  # noqa: E402
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.experiments import (  # noqa: E402
    ExperimentCreateRequest,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.jobs import (  # noqa: E402
    HistoryPolicy,
    HistoryScope,
    JobClaimRequest,
    JobResponse,
    JobStatus,
    ReplayCreateRequest,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    StaticCase,
    StaticPolicy,
    ToolPolicyConfig,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.secrets import SecretCreateRequest  # noqa: E402
from kitaru.api_models.v1.session_nodes import (  # noqa: E402
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.api_models.v1.sessions import (  # noqa: E402
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.client import KitaruAPIClient  # noqa: E402
from kitaru.client.exceptions import APIError  # noqa: E402

API_URL = os.environ.get("KITARU_E2E_API_URL", "http://127.0.0.1:8300")
ACCOUNT_NAME = os.environ.get("KITARU_E2E_ACCOUNT_NAME", "default")
ACCOUNT_PASSWORD = os.environ.get("KITARU_E2E_ACCOUNT_PASSWORD", "password")

AGENT_PYTHON = str(REPO_ROOT / ".venv" / "bin" / "python")
ORIGINAL_MODEL = "mock-gpt-4"
OVERRIDE_MODEL = "mock-claude-opus"
SECRET_VALUE = "e2e-secret-token-123"
STATIC_CALC_RESULT = {"expression": "21 * 2", "result": 42.0, "canned": True}
SCORER_NAMES = ("answer_quality", "tool_efficiency", "token_budget")

POPULATE_RUNS: list[dict[str, str] | str] = [
    '{"question": "What is the weather in Berlin, and what is 21 * 2?"}',
    "Weather in Berlin plus 21 * 2, please answer briefly.",
    {"KITARU_INPUTS": '{"question": "Berlin forecast and the answer to 21 * 2?"}'},
    {
        "KITARU_INPUTS": '{"question": "Give me Berlin weather and 21 * 2."}',
        "KITARU_SESSION_NAME": "e2e-run-4",
    },
]


def scoring_policy() -> ScoringPolicy:
    """Build the scoring policy shared by the experiment and standalone job."""
    return ScoringPolicy(
        scorers=[
            ScorerConfig(
                name="answer_quality",
                source="adapter_example.scorers:answer_quality",
                params={"keywords": ["answer"]},
                weight=2.0,
            ),
            ScorerConfig(
                name="tool_efficiency",
                source="adapter_example.scorers:tool_efficiency",
                params={"budget": 4},
                weight=1.0,
            ),
            ScorerConfig(
                name="token_budget",
                source="adapter_example.scorers:token_budget",
                params={"max_tokens": 4000},
                weight=0.5,
            ),
        ],
        pass_threshold=0.4,
    )


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
    env.pop("KITARU_INPUTS", None)
    env.pop("KITARU_SESSION_NAME", None)
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
    job: JobResponse,
    original: SessionResponse,
    expect_model: str,
    expect_calculate_policy: str,
) -> None:
    """Assert a completed job, its result session, and its node tree."""
    check(
        job.status is JobStatus.COMPLETED,
        f"job {job.id} is {job.status}: {job.error}",
    )
    check(job.passed is not None, "job has no passed outcome")
    check(job.score is not None, "job has no score")
    check(
        job.scores is not None and set(job.scores) == set(SCORER_NAMES),
        f"job scores incomplete: {job.scores}",
    )
    check(job.diff is not None, "job has no diff summary")
    assert job.diff is not None
    for field in ("cost", "tokens", "tool_calls", "score_deltas"):
        check(field in job.diff, f"diff summary lacks {field}")
    for side in ("original", "job"):
        check(
            job.diff["cost"][side] is not None and job.diff["cost"][side] > 0,
            f"diff summary lacks {side} cost: {job.diff['cost']}",
        )
        check(
            job.diff["tokens"][side]["input_tokens"] is not None,
            f"diff summary lacks {side} tokens: {job.diff['tokens']}",
        )
    check(job.result_session_id is not None, "job has no result session")
    assert job.result_session_id is not None

    result = await client.sessions.get(job.result_session_id)
    check(result.origin is SessionOrigin.REPLAY, "result session origin is not job")
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
    job: JobResponse,
    original_model: str,
    effective_model: str,
) -> None:
    """Assert the computed diff aligns the node trees and shows the override."""
    diff = await client.jobs.get_diff(job.id)
    check(diff.original_session_id == job.original_session_id, "diff original id")
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
    mocked_pairs = [pair for pair in diff.node_pairs if pair.mocked]
    check(len(mocked_pairs) == 2, f"expected 2 mocked pairs, got {len(mocked_pairs)}")
    for name in SCORER_NAMES:
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
        for side in ("baseline", "job"):
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
    for side in ("baseline", "job"):
        for kind in ("input_tokens", "output_tokens"):
            count = total_tokens[side][kind]
            check(
                count is not None and count > 0,
                f"{side} total {kind} {total_tokens}",
            )


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
        check(len(named) == 1, "KITARU_SESSION_NAME was not recorded")
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
        replays_page = await client.experiment_runs.list_jobs(run.id, page_size=50)
        check(replays_page.total == 4, f"expected 4 jobs, got {replays_page.total}")
        spec = await client.jobs.get_spec(replays_page.items[0].id)
        check(
            spec.secret_env == {"E2E_SECRET_TOKEN": SECRET_VALUE},
            f"secret env not resolved: {list(spec.secret_env)}",
        )
        check(spec.score_baselines is True, "spec does not carry score_baselines")
        ok("experiment run created with 4 pending jobs and resolved secret env")

        # Step f: execute the run with the in-process runner.
        runner = Runner(API_URL, api_key, concurrency=2)
        run = await runner.run_experiment_run(run.id)
        check_run_summary(run, replay_count=4)
        ok("experiment run completed with an aggregate summary")

        replays_page = await client.experiment_runs.list_jobs(run.id, page_size=50)
        sessions_by_id = {session.id: session for session in sessions}
        for job in replays_page.items:
            original = sessions_by_id[job.original_session_id]
            await check_replay_result(
                client,
                job,
                original,
                expect_model=OVERRIDE_MODEL,
                expect_calculate_policy="static",
            )
            await check_replay_diff(client, job, ORIGINAL_MODEL, OVERRIDE_MODEL)
        ok("all 4 jobs completed, scored, mocked as configured, and diffed")

        for session in sessions:
            refreshed = await client.sessions.get(session.id)
            check(
                set(refreshed.scores) == set(SCORER_NAMES),
                f"baseline scores missing on {session.id}: {refreshed.scores}",
            )
        ok("baseline scores written to every original session")

        # Step g: standalone job with the default tool policy.
        standalone = await client.replays.create(
            ReplayCreateRequest(
                original_session_id=sessions[0].id,
                scoring_policy=scoring_policy(),
            )
        )
        check(standalone.experiment_run_id is None, "standalone job has a run")
        standalone = await runner.run_job(standalone.id)
        await check_replay_result(
            client,
            standalone,
            sessions[0],
            expect_model=ORIGINAL_MODEL,
            expect_calculate_policy="history",
        )
        await check_replay_diff(client, standalone, ORIGINAL_MODEL, ORIGINAL_MODEL)
        ok("standalone job completed, scored, and diffed")

        # Step h: live session run with an override.
        live_inputs = {"question": "Live run: Berlin weather and 21 * 2?"}
        live = await runner.run_session(
            version.id,
            inputs=live_inputs,
            override=ReplayOverride(
                model=OVERRIDE_MODEL, system_prompt="Answer tersely."
            ),
        )
        check(live.origin is SessionOrigin.RECORDED, "live session origin")
        check(
            live.status is SessionStatus.COMPLETED,
            f"live session is {live.status}: {live.error}",
        )
        check(live.inputs == live_inputs, f"live session inputs {live.inputs}")
        check(
            live.agent_version_id == version.id,
            "live session agent version not recorded",
        )
        live_nodes = await client.session_nodes.list(live.id, include_payloads=True)
        check_session_tree(live, live_nodes)
        check_llm_models(live_nodes, OVERRIDE_MODEL)
        ok("live session recorded with the model and system prompt override")

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
        claim = await client.experiment_runs.claim(
            run.id, JobClaimRequest(worker_id="e2e-negative", max_jobs=10)
        )
        check(claim.jobs == [], "claim on a completed run returned jobs")
        ok("frozen experiment rejects config updates, drained run claims nothing")

    print("[e2e] ALL STEPS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception:
        traceback.print_exc()
        print("[e2e] FAILED", flush=True)
        sys.exit(1)
