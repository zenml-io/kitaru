"""Worker lifecycle resilience scenarios against an ephemeral local stack."""

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from fixtures import register_agent
from seed import TERMINAL_JOB_STATUSES, poll_until
from stack import (
    RUN_DIR,
    create_database,
    drop_database,
    ensure_postgres,
    get_free_port,
    start_server,
    wait_for_health,
)

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.api_models.v1.task import TaskResponse, TaskStatus
from kitaru.client.api_client import KitaruAPIClient

AGENT_SLEEP_SECONDS = 4
CLAIM_TIMEOUT_SECONDS = 30.0
CLI_CHECK_TIMEOUT_SECONDS = 60.0
RECOVERY_TIMEOUT_SECONDS = 120.0

# Reclaim killed workers' tasks within seconds instead of the defaults.
FAST_SWEEP_ENV = {
    "KITARU_SERVER_TASK_SWEEP_INTERVAL_SECONDS": "1",
    "KITARU_SERVER_TASK_HEARTBEAT_TIMEOUT_SECONDS": "3",
}

ACTIVE_TASK_STATUSES = {TaskStatus.CLAIMED, TaskStatus.RUNNING}


def resolve_cli() -> Path:
    """Return the kitaru CLI that sits next to the running interpreter."""
    cli = Path(sys.executable).with_name("kitaru")
    if not cli.exists():
        raise RuntimeError(f"kitaru CLI not found at {cli}")
    return cli


def check_cli() -> None:
    """Fail before any scenario when the CLI cannot start."""
    # Workers exec the venv CLI directly, so a half-synced environment surfaces
    # as an unexplained claim timeout instead of an import error.
    cli = resolve_cli()
    result = subprocess.run(
        [str(cli), "version"], capture_output=True, timeout=CLI_CHECK_TIMEOUT_SECONDS
    )
    if result.returncode != 0:
        output = (result.stdout + result.stderr).decode("utf-8", "replace").strip()
        raise RuntimeError(f"{cli} is not runnable, workers cannot start:\n{output}")


class WorkerProc:
    """Worker subprocess with lifecycle controls."""

    def __init__(self, name: str, base_url: str, log_path: Path) -> None:
        """Spawn a kitaru worker subprocess."""
        cli = resolve_cli()
        env = dict(os.environ)
        env.update(
            {
                "KITARU_API_URL": base_url,
                "KITARU_WORKER_NAME": name,
                "KITARU_WORKER_CONCURRENCY": "2",
                "KITARU_WORKER_POLL_INTERVAL": "0.2",
                "KITARU_WORKER_HEARTBEAT_INTERVAL": "0.5",
            }
        )
        self.name = name
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = log_path.open("wb")
        self._proc = subprocess.Popen(
            [str(cli), "worker", "start"],
            env=env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )

    @property
    def alive(self) -> bool:
        """Whether the worker process is still running."""
        return self._proc.poll() is None

    def kill(self) -> None:
        """SIGKILL the worker, orphaning any in-flight agent processes."""
        if self.alive:
            self._proc.kill()

    def suspend(self) -> None:
        """SIGSTOP the worker, freezing its heartbeats."""
        self._proc.send_signal(signal.SIGSTOP)

    def resume(self) -> None:
        """SIGCONT the worker."""
        self._proc.send_signal(signal.SIGCONT)

    def stop(self) -> None:
        """Drain the worker, escalating to SIGKILL after a timeout."""
        if self.alive:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._log_file.close()


@dataclass
class ScenarioContext:
    """Live stack handles passed to one scenario."""

    client: KitaruAPIClient
    agent_version_id: uuid.UUID
    base_url: str
    log_dir: Path
    workers: list[WorkerProc] = field(default_factory=list)
    checks: list[tuple[str, bool]] = field(default_factory=list)

    def spawn_worker(self, name: str) -> WorkerProc:
        """Start one worker subprocess and track it."""
        worker = WorkerProc(name, self.base_url, self.log_dir / f"{name}.log")
        self.workers.append(worker)
        return worker

    def check(self, label: str, passed: bool) -> None:
        """Record one scenario check."""
        self.checks.append((label, passed))

    async def submit_runs(self, count: int) -> list[JobResponse]:
        """Submit slow agent session runs."""
        return [
            await self.client.session_runs.create(
                SessionRunCreateRequest(
                    agent_version_id=self.agent_version_id,
                    inputs={
                        "question": f"resilience run {index}?",
                        "topic": "orbital debris",
                        "turns": 1,
                        "variant": index,
                    },
                    name=f"resilience-{index}",
                )
            )
            for index in range(count)
        ]

    async def job_tasks(self, jobs: list[JobResponse]) -> list[TaskResponse]:
        """Fetch every task belonging to the jobs."""
        tasks: list[TaskResponse] = []
        for job in jobs:
            page = await self.client.jobs.list_tasks(job.id)
            tasks.extend(page.items)
        return tasks

    def worker_log_tail(self, lines: int = 10) -> str:
        """Render the last lines of every worker log."""
        parts: list[str] = []
        for worker in self.workers:
            log = self.log_dir / f"{worker.name}.log"
            if not log.exists():
                continue
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[
                -lines:
            ]
            if tail:
                parts.append(f"{worker.name}: " + " | ".join(tail))
        return "\n".join(parts)

    async def wait_for_claims(self, jobs: list[JobResponse], count: int) -> None:
        """Wait until at least count tasks are claimed or running."""
        try:
            await poll_until(
                fetch=lambda: self.job_tasks(jobs),
                is_done=lambda tasks: (
                    sum(1 for t in tasks if t.status in ACTIVE_TASK_STATUSES) >= count
                ),
                timeout=CLAIM_TIMEOUT_SECONDS,
                label="task claims",
            )
        except TimeoutError as exc:
            tail = self.worker_log_tail()
            raise TimeoutError(f"{exc}\n{tail}" if tail else str(exc)) from exc

    async def wait_for_jobs(self, jobs: list[JobResponse]) -> list[JobResponse]:
        """Wait until every job settles."""

        async def fetch() -> list[JobResponse]:
            return [await self.client.jobs.get(job.id) for job in jobs]

        return await poll_until(
            fetch=fetch,
            is_done=lambda done: all(j.status in TERMINAL_JOB_STATUSES for j in done),
            timeout=RECOVERY_TIMEOUT_SECONDS,
            label="job settlement",
        )


def _all_completed(jobs: list[JobResponse]) -> bool:
    """Whether every job completed."""
    return all(job.status == JobStatus.COMPLETED for job in jobs)


async def scenario_worker_crash(ctx: ScenarioContext) -> None:
    """SIGKILL one worker mid-task and verify another finishes its work."""
    ctx.spawn_worker("crash-w1")
    ctx.spawn_worker("crash-w2")
    jobs = await ctx.submit_runs(4)
    await ctx.wait_for_claims(jobs, 3)
    ctx.workers[0].kill()
    jobs = await ctx.wait_for_jobs(jobs)
    tasks = await ctx.job_tasks(jobs)
    ctx.check("all jobs completed", _all_completed(jobs))
    ctx.check("a task was reclaimed", any(task.attempt >= 2 for task in tasks))
    ctx.check(
        "no task abandoned",
        all(task.status != TaskStatus.ABANDONED for task in tasks),
    )
    ctx.check(
        "every task has a result session",
        all(task.result_session_id is not None for task in tasks),
    )


async def scenario_worker_zombie(ctx: ScenarioContext) -> None:
    """SIGSTOP a worker, let its tasks be reclaimed, then resume it."""
    frozen = ctx.spawn_worker("zombie-w1")
    ctx.spawn_worker("zombie-w2")
    jobs = await ctx.submit_runs(4)
    await ctx.wait_for_claims(jobs, 3)
    frozen.suspend()
    jobs = await ctx.wait_for_jobs(jobs)
    ctx.check("all jobs completed while frozen", _all_completed(jobs))
    frozen.resume()
    # The resumed worker retries its stale updates, which the attempt check
    # on the server must reject without disturbing the finished tasks.
    await asyncio.sleep(3.0)
    tasks = await ctx.job_tasks(jobs)
    ctx.check(
        "tasks stayed completed after resume",
        all(task.status == TaskStatus.COMPLETED for task in tasks),
    )
    jobs = await ctx.wait_for_jobs(jobs)
    ctx.check("jobs stayed completed after resume", _all_completed(jobs))


async def scenario_fleet_death(ctx: ScenarioContext) -> None:
    """SIGKILL the whole fleet mid-task and recover with a fresh worker."""
    ctx.spawn_worker("fleet-w1")
    ctx.spawn_worker("fleet-w2")
    jobs = await ctx.submit_runs(4)
    await ctx.wait_for_claims(jobs, 3)
    for worker in ctx.workers:
        worker.kill()
    ctx.spawn_worker("fleet-w3")
    jobs = await ctx.wait_for_jobs(jobs)
    tasks = await ctx.job_tasks(jobs)
    ctx.check("all jobs completed", _all_completed(jobs))
    ctx.check("reclaimed tasks exist", any(task.attempt >= 2 for task in tasks))


async def scenario_abandonment(ctx: ScenarioContext) -> None:
    """Exhaust the retry limit and verify the task is abandoned."""
    ctx.spawn_worker("abandon-w1")
    jobs = await ctx.submit_runs(1)
    await ctx.wait_for_claims(jobs, 1)
    ctx.workers[0].kill()
    jobs = await ctx.wait_for_jobs(jobs)
    tasks = await ctx.job_tasks(jobs)
    ctx.check(
        "task abandoned at the retry limit",
        all(task.status == TaskStatus.ABANDONED for task in tasks),
    )
    ctx.check(
        "abandonment recorded an error",
        all(task.error is not None and "abandoned" in task.error for task in tasks),
    )
    ctx.check(
        "job did not complete",
        all(job.status != JobStatus.COMPLETED for job in jobs),
    )


SCENARIOS: dict[
    str, tuple[Callable[[ScenarioContext], Awaitable[None]], dict[str, str]]
] = {
    "worker-crash": (scenario_worker_crash, {}),
    "worker-zombie": (scenario_worker_zombie, {}),
    "fleet-death": (scenario_fleet_death, {}),
    "abandonment": (scenario_abandonment, {"KITARU_SERVER_TASK_RETRY_LIMIT": "1"}),
}


def scenario_db_name(name: str, base: str | None) -> str:
    """Build the database name for one scenario run."""
    if base is None:
        return f"kitaru_resil_{uuid.uuid4().hex[:8]}"
    return f"{base}_{name.replace('-', '_')}"


async def run_scenario(
    name: str, keep: bool, db_base: str | None = None
) -> list[tuple[str, bool]]:
    """Run one scenario on its own database and server."""
    scenario, extra_env = SCENARIOS[name]
    db_name = scenario_db_name(name, db_base)
    log_dir = RUN_DIR / "resilience" / name
    port = get_free_port()
    base_url = f"http://127.0.0.1:{port}"

    print(f"\n=== {name} (db {db_name}, logs {log_dir}) ===")
    await ensure_postgres()
    await create_database(db_name, drop_existing=db_base is not None)
    server = start_server(
        db_name,
        port,
        log_dir / "server.log",
        overrides={**FAST_SWEEP_ENV, **extra_env},
    )
    os.environ["KITARU_API_URL"] = base_url
    os.environ.pop("KITARU_API_KEY", None)
    ctx: ScenarioContext | None = None
    try:
        await wait_for_health(base_url, server, log_dir / "server.log")
        async with KitaruAPIClient() as client:
            _, agent_version_id = await register_agent(
                client,
                extra_env={"DUMMY_AGENT_SLEEP_SECONDS": str(AGENT_SLEEP_SECONDS)},
                display_version="dummy-slow",
            )
            ctx = ScenarioContext(
                client=client,
                agent_version_id=agent_version_id,
                base_url=base_url,
                log_dir=log_dir,
            )
            await scenario(ctx)
            return ctx.checks
    except (TimeoutError, RuntimeError) as exc:
        return [(f"scenario errored: {exc}", False)]
    finally:
        if ctx is not None:
            for worker in ctx.workers:
                worker.stop()
        server.terminate()
        failed = (
            ctx is None or not ctx.checks or any(not passed for _, passed in ctx.checks)
        )
        if keep or failed or db_base is not None:
            print(f"  keeping database {db_name} for inspection.")
        else:
            await drop_database(db_name)


async def _run(args: argparse.Namespace) -> int:
    """Run the selected scenarios and print the summary."""
    names = args.scenarios or list(SCENARIOS)
    unknown = [name for name in names if name not in SCENARIOS]
    if unknown:
        raise SystemExit(
            f"Unknown scenario(s) {unknown}, valid: {', '.join(SCENARIOS)}"
        )
    check_cli()
    results: dict[str, list[tuple[str, bool]]] = {}
    for name in names:
        results[name] = await run_scenario(name, keep=args.keep, db_base=args.db_name)
        for label, passed in results[name]:
            print(f"  [{'PASS' if passed else 'FAIL'}] {label}")

    print("\nScenarios:")
    failed = 0
    for name, checks in results.items():
        ok = all(passed for _, passed in checks)
        failed += 0 if ok else 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    return 1 if failed else 0


def main() -> int:
    """Run the resilience CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "scenarios",
        nargs="*",
        help=f"Scenarios to run, all when omitted: {', '.join(SCENARIOS)}",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep every scenario database instead of only failed ones.",
    )
    parser.add_argument(
        "--db-name",
        default=None,
        help=(
            "Base database name, suffixed per scenario and reused across runs. "
            "A random kitaru_resil_* name when omitted."
        ),
    )
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
