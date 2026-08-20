"""Seed a local Kitaru stack with dummy sessions, replays, and evaluations."""

import argparse
import asyncio
import os
import sys
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from typing import TypeVar

from fixtures import DEFAULT_IMPORTER_NAME, register_all
from simulation import build_session_inputs
from stack import (
    RUN_DIR,
    bootstrap_api_key,
    create_database,
    ensure_postgres,
    get_free_port,
    run_workers,
    start_server,
    wait_for_health,
)
from traces import (
    add_simulation_args,
    build_payload,
    config_from_args,
    malformed_line_count,
)

from kitaru.api_models.v1.cohort import CohortCreateRequest
from kitaru.api_models.v1.cohort_version import CohortVersionCreateRequest
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.imports import ImportCreateRequest, ImportStats
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    HistoryScope,
    ReplayOverride,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session import (
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.client.api_client import KitaruAPIClient

LIVE_INPUT_OFFSET = 10_000

TERMINAL_JOB_STATUSES = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
TERMINAL_RUN_STATUSES = {
    ExperimentRunStatus.COMPLETED,
    ExperimentRunStatus.FAILED,
    ExperimentRunStatus.CANCELED,
}

T = TypeVar("T")


async def poll_until(
    fetch: Callable[[], Awaitable[T]],
    is_done: Callable[[T], bool],
    timeout: float,
    label: str,
    interval: float = 0.2,
) -> T:
    """Poll fetch() until is_done(), bounded by timeout."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        value = await fetch()
        if is_done(value):
            return value
        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting for {label}")
        await asyncio.sleep(interval)


async def await_job(
    client: KitaruAPIClient, job_id: uuid.UUID, label: str, timeout: float
) -> JobResponse:
    """Wait for a job to settle."""
    return await poll_until(
        fetch=lambda: client.jobs.get(job_id),
        is_done=lambda job: job.status in TERMINAL_JOB_STATUSES,
        timeout=timeout,
        label=label,
    )


async def run_import(
    client: KitaruAPIClient,
    agent_id: uuid.UUID,
    payload: bytes,
    timeout: float,
    importer: str = DEFAULT_IMPORTER_NAME,
) -> tuple[JobResponse, ImportStats]:
    """Upload a trace payload, import it, and wait for the stats."""
    blob = await client.blobs.upload(
        payload, media_type="application/x-ndjson", filename="traces.jsonl"
    )
    job = await client.imports.create(
        ImportCreateRequest(
            importer=importer, agent_id=agent_id, payload_blob_id=blob.id
        )
    )
    job = await await_job(client, job.id, "import", timeout)
    tasks = await client.jobs.list_tasks(job.id)
    if not tasks.items or tasks.items[0].result is None:
        task = tasks.items[0] if tasks.items else None
        raise RuntimeError(
            f"Import job {job.id} ({job.status}) produced no stats, "
            f"task status={task.status if task else None} "
            f"error={task.error if task else None}"
        )
    return job, ImportStats.model_validate(tasks.items[0].result)


async def _seed(args: argparse.Namespace) -> int:
    """Seed the stack end to end, returning a process exit code."""
    checks: list[tuple[str, bool]] = []
    stop_event = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    server_proc = None
    config = config_from_args(args)
    db_name = args.db_name or f"kitaru_seed_{uuid.uuid4().hex[:8]}"

    if args.base_url is None:
        await ensure_postgres()
        await create_database(db_name, drop_existing=True)
        port = get_free_port()
        base_url = f"http://127.0.0.1:{port}"
        log_path = RUN_DIR / "seed-server.log"
        print(f"Starting server at {base_url} (db {db_name}, log {log_path}) ...")
        server_proc = start_server(db_name, port, log_path)
        await wait_for_health(base_url, server_proc, log_path)
        os.environ["KITARU_API_URL"] = base_url
        api_key = await bootstrap_api_key(base_url)
        if api_key is not None:
            os.environ["KITARU_API_KEY"] = api_key
    else:
        base_url = args.base_url
        os.environ["KITARU_API_URL"] = base_url

    try:
        async with KitaruAPIClient() as client:
            print("Registering dummy agent and evaluators ...")
            agent_id, agent_version_id, evaluator_ids = await register_all(client)
            print(f"  agent {agent_id}, {len(evaluator_ids)} evaluators.")

            worker_tasks = await run_workers(args.workers, args.concurrency, stop_event)

            print(f"Importing {args.sessions} generated traces ...")
            payload = await build_payload(
                config, count=args.sessions, malformed=args.malformed
            )
            (RUN_DIR / "traces.jsonl").parent.mkdir(parents=True, exist_ok=True)
            (RUN_DIR / "traces.jsonl").write_bytes(payload)
            import_job, stats = await run_import(
                client,
                agent_id,
                payload,
                timeout=60.0 + 0.1 * args.sessions,
                importer=args.importer,
            )
            print(
                f"  created={stats.created} skipped={stats.skipped} "
                f"failed={stats.failed}"
            )
            checks.append(
                ("import completed", import_job.status == JobStatus.COMPLETED)
            )
            checks.append(("import created sessions", stats.created == args.sessions))
            checks.append(
                (
                    "import flagged malformed lines",
                    stats.failed == malformed_line_count(args.malformed),
                )
            )

            print(f"Running {args.live_runs} live agent session(s) ...")
            live_jobs = [
                await client.session_runs.create(
                    SessionRunCreateRequest(
                        agent_version_id=agent_version_id,
                        inputs=build_session_inputs(config, LIVE_INPUT_OFFSET + i),
                        name=f"seed-live-{i + 1}",
                    )
                )
                for i in range(args.live_runs)
            ]
            live_results = await asyncio.gather(
                *(await_job(client, job.id, "session-run", 120.0) for job in live_jobs)
            )
            checks.append(
                (
                    "live session runs completed",
                    all(job.status == JobStatus.COMPLETED for job in live_results),
                )
            )

            if args.skip_experiment:
                return _finish(checks)

            print("Building a cohort from the imported sessions ...")
            session_ids = [
                s.id
                async for s in client.sessions.iter(
                    SessionListParams(
                        filter=AndFilter(
                            **{
                                "and": [
                                    FilterCondition(
                                        field="agent_id",
                                        op=FilterOp.EQ,
                                        value=agent_id,
                                    ),
                                    FilterCondition(
                                        field="origin",
                                        op=FilterOp.EQ,
                                        value=SessionOrigin.IMPORTED,
                                    ),
                                    FilterCondition(
                                        field="status",
                                        op=FilterOp.EQ,
                                        value=SessionStatus.COMPLETED,
                                    ),
                                ]
                            }
                        )
                    )
                )
            ]
            cohort = await client.cohorts.create(
                CohortCreateRequest(name=f"seed-cohort-{db_name}", agent_id=agent_id)
            )
            cohort_version = await client.cohorts.create_version(
                cohort.id, CohortVersionCreateRequest(add_session_ids=session_ids)
            )
            print(f"  cohort version with {cohort_version.session_count} sessions.")

            print("Running an experiment over the cohort ...")
            experiment = await client.experiments.create(
                ExperimentCreateRequest(
                    name=f"seed-experiment-{db_name}",
                    agent_id=agent_id,
                    override=ReplayOverride(model=args.model_override),
                    tool_policy=ToolPolicy(
                        default=HistoryConfig(
                            scope=HistoryScope.BASELINE,
                            on_miss=ToolPolicyOnMiss.PASSTHROUGH,
                        )
                    ),
                    evaluators=[
                        EvaluatorConfig(evaluator=name) for name in args.evaluators
                    ],
                )
            )
            run = await client.experiments.start_run(
                experiment.id,
                ExperimentRunCreateRequest(
                    cohort_version_id=cohort_version.id,
                    agent_version_id=agent_version_id,
                    evaluate_baselines=True,
                ),
            )
            run_timeout = 120.0 + 2.0 * len(session_ids) / max(1, args.workers)
            run = await poll_until(
                fetch=lambda: client.experiment_runs.get(run.id),
                is_done=lambda r: r.status in TERMINAL_RUN_STATUSES,
                timeout=run_timeout,
                label="experiment run",
            )
            print(f"  run {run.status}, progress {run.progress.model_dump()}.")
            checks.append(
                (
                    "experiment run completed",
                    run.status == ExperimentRunStatus.COMPLETED,
                )
            )

            counts: Counter[tuple[str, str]] = Counter()
            async for evaluation in client.evaluations.iter():
                counts[(evaluation.name, evaluation.data_type.value)] += 1
            print("Evaluations by name and data type:")
            for (name, data_type), count in sorted(counts.items()):
                print(f"  {name} ({data_type}): {count}")
            checks.append(("evaluations recorded", sum(counts.values()) > 0))

            return _finish(checks)
    finally:
        stop_event.set()
        if worker_tasks:
            await asyncio.wait(worker_tasks, timeout=10.0)
        if server_proc is not None:
            if args.keep:
                print(
                    f"Keeping server at {base_url} (pid {server_proc.pid}, "
                    f"db {db_name})."
                )
                print(f"export KITARU_API_URL={base_url}")
            else:
                server_proc.terminate()


def _finish(checks: list[tuple[str, bool]]) -> int:
    """Print the check summary and derive the exit code."""
    print("\nChecks:")
    failed = 0
    for label, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
        failed += 0 if passed else 1
    return 1 if failed else 0


def main() -> int:
    """Run the seed flow from CLI flags."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=25)
    parser.add_argument("--malformed", type=int, default=2)
    parser.add_argument("--live-runs", type=int, default=3)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--base-url",
        default=None,
        help="Use a running server instead of starting one.",
    )
    parser.add_argument(
        "--db-name",
        default=None,
        help="Database name, a random kitaru_seed_* name when omitted.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Leave the started server running for inspection.",
    )
    parser.add_argument("--model-override", default="dummy-llm-v2")
    parser.add_argument(
        "--importer",
        default=DEFAULT_IMPORTER_NAME,
        help="Importer name, e.g. kitaru/kitaru-jsonl for the built-in one.",
    )
    parser.add_argument(
        "--evaluators",
        nargs="+",
        default=["dummy-suite"],
        help="Evaluator names wired into the experiment.",
    )
    parser.add_argument("--skip-experiment", action="store_true")
    add_simulation_args(parser)
    args = parser.parse_args()
    return asyncio.run(_seed(args))


if __name__ == "__main__":
    sys.exit(main())
