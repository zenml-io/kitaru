"""Run the compiled Vercel AI SDK support-triage agent through Kitaru."""

import argparse
import asyncio
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    RunSpec,
)
from kitaru.api_models.v1.evaluation import (
    EvaluationListParams,
    EvaluationResponse,
)
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    HistoryScope,
    PassthroughConfig,
    ReplayOverride,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import (
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeType,
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker import Worker, WorkerConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_DIR = Path(__file__).resolve().parent
COMPILED_MAIN = EXAMPLE_DIR / "dist" / "main.js"
SCORER_SOURCE = EXAMPLE_DIR / "scorers.py"
RUN_COMMAND = "node examples/typescript/vercel_ai_support_triage/dist/main.js"
REQUESTED_MODEL_ID = "openai/gpt-5-nano"
INITIAL_PROMPT = (
    "Investigate account acct-1001 and delayed order ord-1001. "
    "The customer reports a suspected duplicate charge."
)
OVERRIDE_PROMPT = (
    "Priority escalation: investigate account acct-1001 and order ord-1001. "
    "Confirm the delayed order and suspected duplicate charge from tool evidence."
)
# The replay override replaces the agent's own instructions, so it has to restate
# the output contract the scorers grade. Asking for "the required structured
# decision" without naming the keys lets the model answer in prose instead.
OVERRIDE_SYSTEM = (
    "Follow the configured support workflow. Use the account and order lookup "
    "tools and queue one refund review for a delayed duplicate charge. Answer "
    "with a JSON object only, using exactly the keys decision, evidence, risk, "
    "and nextAction, and record the queued refund review under evidence."
)


@dataclass(frozen=True)
class DemoResult:
    """Observed record/replay proof returned to tests."""

    initial_session_id: str
    replay: ReplayResponse
    result_session_id: str
    initial_outbox_count: int
    replay_outbox_count: int
    initial_nodes: list[SessionNodeResponse]
    replay_nodes: list[SessionNodeResponse]
    evaluations: list[EvaluationResponse]


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _outbox_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


def _require_nonempty_text(outputs: object) -> str:
    if not isinstance(outputs, dict):
        raise RuntimeError("The baseline session did not record output text")
    text = outputs.get("text")
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("The baseline session recorded empty output text")
    return text


def _assert_recorded_shape(nodes: list[SessionNodeResponse]) -> None:
    roots = [
        node
        for node in nodes
        if node.node_type is NodeType.SPAN and node.parent_index is None
    ]
    llm_nodes = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
    tool_names = {
        node.tool_name for node in nodes if node.node_type is NodeType.TOOL_CALL
    }
    assert len(roots) == 1
    assert len(llm_nodes) >= 2
    assert {"lookupAccount", "lookupOrder", "queueRefundReview"} <= tool_names


@contextmanager
def _worker_environment(api_url: str, api_key: str) -> Iterator[None]:
    names = ("KITARU_API_URL", "KITARU_API_KEY")
    original = {name: os.environ.get(name) for name in names}
    os.environ["KITARU_API_URL"] = api_url
    os.environ["KITARU_API_KEY"] = api_key
    try:
        yield
    finally:
        for name, value in original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


async def _run_job(
    job_id: uuid.UUID,
    *,
    api_url: str,
    api_key: str,
    state_dir: Path,
) -> JobResponse:
    worker_key = api_key or "local-development-key"
    worker = Worker(
        WorkerConfig(
            name=f"vercel-ai-demo-{job_id}",
            scope=WorkerScope(
                claims=[WorkerClaim(kind=kind) for kind in TaskKind],
                job_id=job_id,
            ),
            poll_interval=0.05,
            timeout=180,
            blob_cache_root=state_dir / "worker-blobs",
            payload_cache_root=state_dir / "worker-payloads",
        )
    )
    with _worker_environment(api_url, worker_key):
        await worker.run()
    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        settled = await client.jobs.get(job_id)
    if settled.status is not JobStatus.COMPLETED:
        raise RuntimeError(
            f"Kitaru job {settled.id} settled as {settled.status}: {settled.error}"
        )
    return settled


async def _result_session_id(client: KitaruAPIClient, job: JobResponse) -> str:
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
    result_ids = [str(session.id) for session in sessions.items]
    if len(result_ids) != 1:
        raise RuntimeError(
            f"Kitaru job {job.id} produced {len(result_ids)} result sessions"
        )
    return result_ids[0]


async def _nodes(client: KitaruAPIClient, session_id: str) -> list[SessionNodeResponse]:
    return [
        node
        async for node in client.sessions.iter_nodes(
            uuid.UUID(session_id), SessionNodeListParams(include_payloads=True)
        )
    ]


async def run_demo(
    *,
    api_url: str,
    api_key: str = "",
    state_dir: Path | None = None,
    test_model: bool = False,
) -> DemoResult:
    """Record and history-replay one support case through current Kitaru jobs."""
    if not COMPILED_MAIN.exists():
        raise RuntimeError(
            "The compiled Node command is missing. Run: "
            "pnpm --filter @zenml-io/kitaru-example-vercel-ai-support-triage build"
        )
    if not test_model:
        _required_environment("OPENAI_API_KEY")

    state_dir = (state_dir or EXAMPLE_DIR / ".state").resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    outbox = state_dir / "refund-review-outbox.jsonl"
    outbox.unlink(missing_ok=True)
    run_env = {"KITARU_SUPPORT_TRIAGE_STATE_DIR": str(state_dir)}
    if test_model:
        run_env["KITARU_VERCEL_AI_TEST_MODEL"] = "1"
    resource_suffix = uuid.uuid4().hex[:10]

    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        agent = await client.agents.create(
            AgentCreateRequest(
                name=f"vercel-ai-support-triage-{resource_suffix}",
                description="Vercel AI SDK record/replay demo.",
            )
        )
        version = await client.agents.create_version(
            agent.id,
            AgentVersionCreateRequest(
                display_version="v1",
                description="AI SDK 7 OpenAI gpt-5-nano support triage.",
                run_spec=RunSpec(
                    command=RUN_COMMAND,
                    working_dir=str(REPO_ROOT),
                    env={**run_env, "KITARU_AGENT_ID": str(agent.id)},
                    timeout_seconds=120,
                ),
                capabilities=AgentCapabilities(
                    tools=["lookupAccount", "lookupOrder", "queueRefundReview"]
                ),
            ),
        )
        evaluator = await client.evaluators.create(
            EvaluatorCreateRequest(
                name=f"vercel-ai-support-triage-{resource_suffix}",
                description="Deterministic checks for the Vercel AI SDK demo.",
            )
        )
        scorer_blob = await client.blobs.upload(
            SCORER_SOURCE.read_bytes(),
            media_type="text/x-python",
            filename="scorers.py",
        )
        await client.evaluators.create_version(
            evaluator.id,
            EvaluatorVersionCreateRequest(
                source=ScriptPluginSource(
                    blob_id=scorer_blob.id, entrypoint="evaluate"
                ),
                display_version="v1",
            ),
        )
        initial_job = await client.session_runs.create(
            SessionRunCreateRequest(
                agent_version_id=version.id,
                inputs=INITIAL_PROMPT,
                name="Vercel AI SDK support triage baseline",
            )
        )

    initial_job = await _run_job(
        initial_job.id, api_url=api_url, api_key=api_key, state_dir=state_dir
    )
    initial_outbox_count = _outbox_count(outbox)
    assert initial_outbox_count == 1

    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        initial_session_id = await _result_session_id(client, initial_job)
        initial_session = await client.sessions.get(uuid.UUID(initial_session_id))
        assert initial_session.status is SessionStatus.COMPLETED
        _require_nonempty_text(initial_session.outputs)
        initial_nodes = await _nodes(client, initial_session_id)
        _assert_recorded_shape(initial_nodes)
        replay = await client.replays.create(
            ReplayCreateRequest(
                baseline_session_id=initial_session.id,
                agent_version_id=version.id,
                override=ReplayOverride(
                    model=REQUESTED_MODEL_ID,
                    prompt=OVERRIDE_PROMPT,
                    system_prompt=OVERRIDE_SYSTEM,
                    model_params={"maxOutputTokens": 3000},
                ),
                tool_policy=ToolPolicy(
                    default=PassthroughConfig(),
                    tools={
                        "queueRefundReview": HistoryConfig(
                            scope=HistoryScope.BASELINE,
                            on_miss=ToolPolicyOnMiss.FAIL,
                        )
                    },
                ),
                evaluators=[EvaluatorConfig(evaluator=evaluator.name)],
            )
        )

    assert replay.job_id is not None
    await _run_job(
        replay.job_id,
        api_url=api_url,
        api_key=api_key,
        state_dir=state_dir,
    )
    replay_outbox_count = _outbox_count(outbox)
    assert replay_outbox_count == 1

    async with KitaruAPIClient(base_url=api_url, api_key=api_key or None) as client:
        replay = await client.replays.get(replay.id)
        assert replay.status is ReplayStatus.COMPLETED
        assert replay.result_session_id is not None
        result_session = await client.sessions.get(replay.result_session_id)
        replay_nodes = await _nodes(client, str(replay.result_session_id))
        evaluations = list(
            (
                await client.evaluations.list(
                    EvaluationListParams(
                        filter=FilterCondition(
                            field="session_id",
                            op=FilterOp.EQ,
                            value=str(replay.result_session_id),
                        )
                    )
                )
            ).items
        )

    assert result_session.status is SessionStatus.COMPLETED
    assert result_session.origin is SessionOrigin.REPLAY
    assert result_session.inputs == {
        "prompt": OVERRIDE_PROMPT,
        "system_prompt": OVERRIDE_SYSTEM,
    }
    _assert_recorded_shape(replay_nodes)
    action_nodes = [
        node
        for node in replay_nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name == "queueRefundReview"
    ]
    assert len(action_nodes) == 1
    assert (action_nodes[0].attributes or {}).get("mocked") is True
    assert (action_nodes[0].attributes or {}).get("policy") == "history"
    llm_nodes = [node for node in replay_nodes if node.node_type is NodeType.LLM_CALL]
    assert all(node.requested_model == REQUESTED_MODEL_ID for node in llm_nodes)
    assert all(node.model != REQUESTED_MODEL_ID for node in llm_nodes)
    assert all(node.cost is not None and node.cost > 0 for node in llm_nodes)
    assert any(node.model_params == {"maxOutputTokens": 3000} for node in llm_nodes)
    assert {evaluation.name for evaluation in evaluations} == {
        "decision_structure",
        "trace_completeness",
        "side_effect_safety",
    }
    assert all(evaluation.passed is True for evaluation in evaluations)

    return DemoResult(
        initial_session_id=initial_session_id,
        replay=replay,
        result_session_id=str(replay.result_session_id),
        initial_outbox_count=initial_outbox_count,
        replay_outbox_count=replay_outbox_count,
        initial_nodes=initial_nodes,
        replay_nodes=replay_nodes,
        evaluations=evaluations,
    )


def _print_result(result: DemoResult) -> None:
    replay_action = next(
        node
        for node in result.replay_nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name == "queueRefundReview"
    )
    print(f"initial_session_id={result.initial_session_id}")
    print(f"replay_id={result.replay.id}")
    print(f"result_session_id={result.result_session_id}")
    print(f"outbox_after_record={result.initial_outbox_count}")
    print(f"outbox_after_history_replay={result.replay_outbox_count}")
    print(
        "recorded_nodes="
        f"llm:{sum(n.node_type is NodeType.LLM_CALL for n in result.initial_nodes)},"
        f"tool:{sum(n.node_type is NodeType.TOOL_CALL for n in result.initial_nodes)}"
    )
    print(
        "history_action="
        f"mocked:{(replay_action.attributes or {}).get('mocked')},"
        f"policy:{(replay_action.attributes or {}).get('policy')}"
    )
    print(
        "evaluations="
        + ",".join(
            f"{evaluation.name}:{evaluation.score}" for evaluation in result.evaluations
        )
    )


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-model", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    result = await run_demo(
        api_url=_required_environment("KITARU_API_URL"),
        api_key=os.environ.get("KITARU_API_KEY", ""),
        test_model=args.test_model,
    )
    _print_result(result)


if __name__ == "__main__":
    asyncio.run(_main())
