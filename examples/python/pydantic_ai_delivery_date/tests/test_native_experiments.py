"""Native worker, replay, experiment, and independent script evaluator contracts."""

import asyncio
import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.client import KitaruAPIClient
from kitaru.worker import Worker, WorkerConfig, task_runner
from kitaru.worker.process import TaskProcess

from delivery_date import experiments
from delivery_date.models import get_scenario
from delivery_date.persistence import record_result
from delivery_date.policy import get_default_policy
from delivery_date.runner import run_scenario
from delivery_date.scenario_library import keep_case

SERVER = os.environ.get("KITARU_DELIVERY_E2E_URL", "")
EXAMPLE = Path(__file__).resolve().parents[1]


def test_run_spec_does_not_capture_local_credentials(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-published")
    spec = experiments.get_policy_run_spec(
        get_default_policy("fix"), "gpt-6-luna", "a" * 64
    )
    assert spec.command == "uv run --frozen python -m delivery_date.worker_run"
    assert spec.working_dir is None
    assert spec.secret_ids == []
    assert spec.runtime_capabilities.overrides is False
    assert spec.runtime_capabilities.tool_policies is False
    assert "must-not-be-published" not in spec.model_dump_json()
    assert "OPENAI_API_KEY" not in spec.env


@pytest.mark.skipif(
    not SERVER, reason="Set KITARU_DELIVERY_E2E_URL to an isolated test server"
)
def test_native_policy_comparison_and_pinned_candidate_rerun(monkeypatch, tmp_path):
    """Substitute model inference while exercising real task and evaluator processes."""
    real_process = task_runner.run_task_process

    async def run_process(process, canceled):
        if process.command == experiments.WORKER_COMMAND:
            # Keep worker auth/environment/recording intact; replace only model inference.
            code = (
                "import asyncio,json,os; from delivery_date import runner,worker_run; "
                "from pydantic_ai.models.function import FunctionModel; "
                "policy=json.loads(os.environ['DELIVERY_POLICY_JSON']); "
                "fixture=runner.make_scripted_model('control' if policy['name']=='Control' else 'fix'); "
                "runner.make_openai_model=lambda _:FunctionModel(fixture.function,model_name='gpt-6-luna'); "
                "asyncio.run(worker_run.main())"
            )
            process = TaskProcess(
                [sys.executable, "-c", code],
                str(EXAMPLE),
                process.env,
                process.timeout_seconds,
            )
        return await real_process(process, canceled)

    monkeypatch.setattr(task_runner, "run_task_process", run_process)
    monkeypatch.setenv("KITARU_API_URL", SERVER)
    monkeypatch.setenv("KITARU_API_TOKEN", "local-test")
    monkeypatch.setenv("OPENAI_API_KEY", "inference-is-substituted")
    monkeypatch.setenv("PYDANTIC_AI_NO_BANNER", "1")
    monkeypatch.delenv("KITARU_API_KEY", raising=False)
    monkeypatch.chdir(EXAMPLE)

    async def exercise():
        result = await run_scenario(
            get_scenario("missing-date"), mode="tool-boundary", variant="fix"
        )
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            session_id = await record_result(
                result,
                server_url=SERVER,
                agent_name=f"native-policy-{uuid4().hex}",
                client=client,
            )
            saved = await keep_case(client, session_id)
            receipt = await experiments.start_experiment(
                client,
                experiments.ExperimentRequest(
                    cohort_version_id=UUID(saved["cohortVersionId"]),
                    baseline=get_default_policy("control"),
                    candidate=get_default_policy("fix"),
                ),
            )
            stop = asyncio.Event()
            worker = Worker(
                WorkerConfig(
                    name=f"native-test-{uuid4().hex}",
                    scope=WorkerScope(
                        claims=[
                            WorkerClaim(
                                kind=TaskKind.AGENT,
                                agent_version_id=receipt.baseline_agent_version_id,
                            ),
                            WorkerClaim(
                                kind=TaskKind.AGENT,
                                agent_version_id=receipt.candidate_agent_version_id,
                            ),
                            WorkerClaim(kind=TaskKind.EVALUATOR),
                        ]
                    ),
                    concurrency=2,
                    poll_interval=0.1,
                    blob_cache_root=tmp_path / "blobs",
                    payload_cache_root=tmp_path / "payloads",
                )
            )
            worker_task = asyncio.create_task(worker.run(stop))
            try:
                async with asyncio.timeout(45):
                    while True:
                        receipt = await experiments.read_experiment(client, receipt)
                        if receipt.status != "running":
                            break
                        await asyncio.sleep(0.1)
                assert receipt.status == "completed", receipt.model_dump_json(indent=2)
                assert receipt.baseline.passed is False
                assert receipt.candidate.passed is True
                assert receipt.ready_for_handoff is True
                baseline, candidate = (
                    receipt.baseline.cases[0],
                    receipt.candidate.cases[0],
                )
                assert baseline.checks["no_unsupported_date"] is False
                assert candidate.checks["no_unsupported_date"] is True
                assert candidate.result_session_id not in {
                    None,
                    session_id,
                    baseline.result_session_id,
                }
                session = await client.sessions.get(candidate.result_session_id)
                assert session.agent_version_id == receipt.candidate_agent_version_id
                assert session.task_id is not None
                assert session.outputs["policy_hash"] == receipt.candidate_policy_hash
                pins = experiments.ExperimentPins.model_validate(
                    experiments.serialize_pins(receipt)
                )
                assert (
                    "prompt" not in pins.model_dump_json()
                    and "transcript" not in pins.model_dump_json()
                )
                rerun = await experiments.run_pinned_candidate(client, pins, timeout=30)
                assert rerun.ready_for_handoff
                assert rerun.candidate_run_id != receipt.candidate_run_id
                assert (
                    rerun.candidate.cases[0].result_session_id
                    != candidate.result_session_id
                )

                real_iter = client.evaluations.iter

                async def manual_results(params=None):
                    async for row in real_iter(params):
                        yield row.model_copy(
                            update={"evaluator_version_id": None, "task_id": None}
                        )

                monkeypatch.setattr(client.evaluations, "iter", manual_results)
                rejected = await experiments.read_experiment(client, pins)
                assert rejected.status == "failed"
                assert not rejected.ready_for_handoff
                assert (
                    rejected.candidate.cases[0].error
                    == "Missing independent evaluator results"
                )
            finally:
                stop.set()
                await worker_task

    asyncio.run(exercise())
