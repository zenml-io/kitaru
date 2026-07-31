"""Run the document processing improvement loop."""

import asyncio
import os
import shlex
import sys
import tempfile
import uuid
from pathlib import Path

from examples.document_processing.corpus import CASES, download_documents
from examples.document_processing.langfuse_capture import capture_baselines
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import (
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
LANGFUSE_IMPORTER_PATH = (
    REPOSITORY_ROOT
    / "plugins"
    / "langfuse"
    / "src"
    / "kitaru_importer_langfuse"
    / "importer.py"
)
API_URL = os.environ.get("KITARU_API_URL", "http://localhost:8000")
CACHE_ROOT = Path(tempfile.gettempdir()) / "kitaru-document-processing-example"


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
    return await _upload_file(client, EXAMPLE_DIR / filename)


async def _upload_file(client: KitaruAPIClient, path: Path) -> uuid.UUID:
    """Upload one Python file and return its blob id."""
    blob = await client.blobs.upload(
        path.read_bytes(), media_type="text/x-python", filename=path.name
    )
    return blob.id


async def _run_job(job_id: uuid.UUID) -> None:
    """Run a worker pinned to one job until it settles."""
    worker = Worker(
        WorkerConfig(
            name=f"document-example-{job_id}",
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
    """Capture, import, replay, evaluate, and compare PDF extraction traces."""
    run_label = uuid.uuid4().hex[:8]
    export_path = CACHE_ROOT / run_label / "langfuse-traces.jsonl"

    print("1/6 Downloading the PDF corpus and capturing Langfuse baselines")
    await asyncio.to_thread(download_documents)
    await capture_baselines(export_path)

    client = await _get_client()
    try:
        print("2/6 Registering the agent and Langfuse importer service")
        agent = await client.agents.create(
            AgentCreateRequest(
                name=f"standards-extractor-{run_label}",
                description="Extract catalog fields from standards PDFs.",
            )
        )
        importer = await client.importers.create(
            ImporterCreateRequest(
                name=f"langfuse-jsonl-{run_label}",
                provider="langfuse",
                description="Import Langfuse traces and PydanticAI observations.",
            )
        )
        importer_blob_id = await _upload_file(client, LANGFUSE_IMPORTER_PATH)
        await client.importers.create_version(
            importer.id,
            ImporterVersionCreateRequest(
                display_version="0.1.0",
                source=ScriptPluginSource(
                    blob_id=importer_blob_id,
                    entrypoint="parse",
                ),
            ),
        )

        print("3/6 Importing the Langfuse traces")
        payload = await client.blobs.upload(
            export_path.read_bytes(),
            media_type="application/x-ndjson",
            filename=export_path.name,
        )
        import_job = await client.imports.create(
            ImportCreateRequest(
                importer=importer.name,
                agent_id=agent.id,
                payload_blob_id=payload.id,
                params={"source_instance": "nist-standards"},
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
        if len(baselines) != len(CASES):
            raise RuntimeError(
                f"Expected {len(CASES)} imported sessions, received {len(baselines)}."
            )

        print("4/6 Freezing the cohort and registering field-level evaluation")
        cohort = await client.cohorts.create(
            CohortCreateRequest(
                name=f"standards-corpus-{run_label}",
                agent_id=agent.id,
                description="Reviewed standards-document extraction cases.",
            )
        )
        cohort_version = await client.cohorts.create_version(
            cohort.id,
            CohortVersionCreateRequest(
                add_session_ids=[session.id for session in baselines],
                display_version="nist-corpus-v1",
            ),
        )
        evaluator_blob_id = await _upload_script(client, "evaluator.py")
        evaluator = await client.evaluators.create(
            EvaluatorCreateRequest(
                name=f"document-field-accuracy-{run_label}",
                description="Compare extracted fields with reviewed labels.",
            )
        )
        await client.evaluators.create_version(
            evaluator.id,
            EvaluatorVersionCreateRequest(
                display_version="1.0",
                source=ScriptPluginSource(
                    blob_id=evaluator_blob_id,
                    entrypoint="evaluate",
                ),
            ),
        )

        print("5/6 Registering and replaying the revised PydanticAI extractor")
        command = shlex.join(
            [sys.executable, "-m", "examples.document_processing.agent"]
        )
        candidate = await client.agents.create_version(
            agent.id,
            AgentVersionCreateRequest(
                display_version="prompt-v2",
                description="Extract cover metadata and framework functions.",
                run_spec=RunSpec(
                    command=command,
                    working_dir=str(REPOSITORY_ROOT),
                    env={"KITARU_AGENT_ID": str(agent.id)},
                    timeout_seconds=180,
                ),
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
                    timeout_seconds=180,
                )
            ),
        )
        experiment = await client.experiments.create(
            ExperimentCreateRequest(
                name=f"standards-extractor-prompt-v2-{run_label}",
                description="Compare two extraction prompts on the PDF corpus.",
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

        print("6/6 Comparing field accuracy\n")
        replays = [
            replay
            async for replay in client.replays.iter(
                ReplayListParams(experiment_run_id=experiment_run.id)
            )
        ]
        print(f"{'document':<34} {'baseline':>10} {'candidate':>10}")
        print(f"{'-' * 34} {'-' * 10} {'-' * 10}")
        for replay in sorted(replays, key=lambda item: str(item.baseline_session_id)):
            baseline = await client.sessions.get(replay.baseline_session_id)
            baseline_scores = await client.evaluations.list(
                EvaluationListParams(session_id=baseline.id, name="field_accuracy")
            )
            candidate_scores = await client.evaluations.list(
                EvaluationListParams(
                    session_id=replay.result_session_id,
                    name="field_accuracy",
                )
            )
            print(
                f"{baseline.metadata['langfuse.session_id']:<34} "
                f"{baseline_scores.items[0].score:>10.0%} "
                f"{candidate_scores.items[0].score:>10.0%}"
            )
        print(f"\nExperiment run: {experiment_run.id}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
