"""Build and run the canonical Kitaru improvement loop."""

import asyncio
import os
import shlex
import sys
import tempfile
import uuid
from pathlib import Path

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.cohort import CohortCreateRequest
from kitaru.api_models.v1.cohort_version import CohortVersionCreateRequest
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterVersionCreateRequest,
)
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay import ReplayListParams
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.session import SessionListParams, SessionOrigin
from kitaru.api_models.v1.task import WorkerScope
from kitaru.client import KitaruAPIClient
from kitaru.worker.config import WorkerConfig
from kitaru.worker.worker import Worker

EXAMPLE_DIR = Path(__file__).parent
REPOSITORY_ROOT = EXAMPLE_DIR.parents[1]
API_URL = os.environ.get("KITARU_API_URL", "http://localhost:8000")
CACHE_ROOT = Path(tempfile.gettempdir()) / "kitaru-canonical-example"


async def _get_client() -> KitaruAPIClient:
    """Build an authenticated client for the configured server."""
    api_key = os.environ.get("KITARU_API_KEY")
    if api_key:
        return KitaruAPIClient(API_URL, api_key=api_key)
    client = KitaruAPIClient(API_URL)
    token = await client.auth.login(
        os.environ.get("KITARU_USERNAME", "default"),
        os.environ.get("KITARU_PASSWORD", "password"),
    )
    await client.close()
    os.environ["KITARU_API_KEY"] = token.access_token
    os.environ["KITARU_API_URL"] = API_URL
    return KitaruAPIClient(API_URL, api_key=token.access_token)


async def _upload_script(client: KitaruAPIClient, filename: str) -> uuid.UUID:
    """Upload one example script and return its blob id."""
    path = EXAMPLE_DIR / filename
    blob = await client.blobs.upload(
        path.read_bytes(), media_type="text/x-python", filename=path.name
    )
    return blob.id


async def _run_job(job_id: uuid.UUID) -> None:
    """Run a worker pinned to one job until it settles."""
    worker = Worker(
        WorkerConfig(
            name=f"canonical-example-{job_id}",
            scope=WorkerScope(job_id=job_id),
            concurrency=4,
            poll_interval=0.1,
            blob_cache_root=CACHE_ROOT / "blobs",
            payload_cache_root=CACHE_ROOT / "payloads",
        )
    )
    await worker.run()


async def _require_completed(client: KitaruAPIClient, job_id: uuid.UUID) -> None:
    """Raise with the job error unless a worker completed it."""
    job = await client.jobs.get(job_id)
    if job.status.value != "completed":
        raise RuntimeError(f"Job {job.id} ended as {job.status}: {job.error}")


async def main() -> None:
    """Register, import, replay, evaluate, and compare support sessions."""
    client = await _get_client()
    run_label = uuid.uuid4().hex[:8]
    try:
        print("1/5 Registering the support agent and importer")
        agent = await client.agents.create(
            AgentCreateRequest(
                name=f"support-agent-{run_label}",
                description="Support agent improved from imported production traces.",
            )
        )
        importer_blob_id = await _upload_script(client, "trace_importer.py")
        importer = await client.importers.create(
            ImporterCreateRequest(
                name=f"supportdesk-jsonl-{run_label}",
                provider=f"supportdesk-{run_label}",
                description="Import support trace JSONL.",
            )
        )
        await client.importers.create_version(
            importer.id,
            ImporterVersionCreateRequest(
                display_version="1.0",
                source=ScriptPluginSource(blob_id=importer_blob_id, entrypoint="parse"),
            ),
        )

        print("2/5 Importing production traces")
        payload_path = EXAMPLE_DIR / "production_traces.jsonl"
        payload = await client.blobs.upload(
            payload_path.read_bytes(),
            media_type="application/x-ndjson",
            filename=payload_path.name,
        )
        import_job = await client.imports.create(
            ImportCreateRequest(
                importer=importer.name,
                agent_id=agent.id,
                payload_blob_id=payload.id,
                params={"source": "supportdesk-production"},
            )
        )
        await _run_job(import_job.id)
        await _require_completed(client, import_job.id)
        baselines = [
            session
            async for session in client.sessions.iter(
                SessionListParams(agent_id=agent.id, origin=SessionOrigin.IMPORTED)
            )
        ]
        if not baselines:
            raise RuntimeError("The importer produced no sessions.")

        print("3/5 Building the cohort and evaluation policy")
        cohort = await client.cohorts.create(
            CohortCreateRequest(
                name=f"production-regressions-{run_label}",
                agent_id=agent.id,
                description="Support cases that failed the expected outcome check.",
            )
        )
        cohort_version = await client.cohorts.create_version(
            cohort.id,
            CohortVersionCreateRequest(
                add_session_ids=[session.id for session in baselines],
                display_version="production-snapshot",
            ),
        )
        evaluator_blob_id = await _upload_script(client, "evaluator.py")
        evaluator = await client.evaluators.create(
            EvaluatorCreateRequest(
                name=f"expected-outcome-{run_label}",
                description="Check the response against the expected support outcome.",
            )
        )
        await client.evaluators.create_version(
            evaluator.id,
            EvaluatorVersionCreateRequest(
                display_version="1.0",
                source=ScriptPluginSource(
                    blob_id=evaluator_blob_id, entrypoint="evaluate"
                ),
            ),
        )

        print("4/5 Registering and replaying the candidate agent")
        command = shlex.join([sys.executable, "-m", "examples.support_agent.agent"])
        candidate = await client.agents.create_version(
            agent.id,
            AgentVersionCreateRequest(
                display_version="candidate-1",
                description="Ground responses in the order lookup tool.",
                run_spec=RunSpec(
                    command=command,
                    working_dir=str(REPOSITORY_ROOT),
                    env={"KITARU_AGENT_ID": str(agent.id)},
                    timeout_seconds=60,
                ),
                capabilities=AgentCapabilities(tools=["lookup_order"]),
            ),
        )
        await client.agent_versions.update(
            candidate.id,
            AgentVersionUpdateRequest(
                run_spec=RunSpec(
                    command=command,
                    working_dir=str(REPOSITORY_ROOT),
                    env={
                        "KITARU_AGENT_ID": str(agent.id),
                        "KITARU_AGENT_VERSION_ID": str(candidate.id),
                    },
                    timeout_seconds=60,
                )
            ),
        )
        experiment = await client.experiments.create(
            ExperimentCreateRequest(
                name=f"support-agent-candidate-{run_label}",
                description="Compare the candidate against production failures.",
                evaluators=[EvaluatorConfig(evaluator=evaluator.name)],
            )
        )
        experiment_run = await client.experiments.start_run(
            experiment.id,
            ExperimentRunCreateRequest(
                cohort_version_id=cohort_version.id,
                agent_version_id=candidate.id,
                evaluate_baselines=True,
            ),
        )
        replays = [
            replay
            async for replay in client.replays.iter(
                ReplayListParams(experiment_run_id=experiment_run.id)
            )
        ]
        await asyncio.gather(*(_run_job(replay.job_id) for replay in replays))
        for replay in replays:
            await _require_completed(client, replay.job_id)

        print("5/5 Comparing baseline and candidate evaluations\n")
        replays = [
            replay
            async for replay in client.replays.iter(
                ReplayListParams(experiment_run_id=experiment_run.id)
            )
        ]
        print(f"{'case':<18} {'baseline':<10} {'candidate':<10}")
        print(f"{'-' * 18} {'-' * 10} {'-' * 10}")
        for replay in sorted(replays, key=lambda item: str(item.baseline_session_id)):
            baseline = await client.sessions.get(replay.baseline_session_id)
            baseline_scores = await client.evaluations.list(
                EvaluationListParams(session_id=baseline.id, name="expected_outcome")
            )
            candidate_scores = await client.evaluations.list(
                EvaluationListParams(
                    session_id=replay.result_session_id,
                    name="expected_outcome",
                )
            )
            print(
                f"{baseline.external_id or str(baseline.id):<18} "
                f"{bool(baseline_scores.items[0].passed)!s:<10} "
                f"{bool(candidate_scores.items[0].passed)!s:<10}"
            )
        print(f"\nExperiment run: {experiment_run.id}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
