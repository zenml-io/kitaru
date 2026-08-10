"""Local autoscaler for a Kitaru worker pool, driven by its stats endpoint.

Spawns or terminates `kitaru worker start` subprocesses to match pending
demand. This is the bare-metal counterpart of the KEDA autoscaler in the
`kitaru-worker` Helm chart, for workers running outside Kubernetes.

Run locally::

    cd examples/features/worker_pool_supervisor
    export KITARU_API_URL=https://kitaru.example.com
    export KITARU_API_KEY=kat_...
    export KITARU_SUPERVISOR_POOL=build-pool
    uv run python supervisor.py
"""

import asyncio
import contextlib
import logging
import math
import os
import signal
import sys
from collections.abc import Iterator
from dataclasses import dataclass

import httpx

from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError

logger = logging.getLogger(__name__)

_POOL_ENV = "KITARU_SUPERVISOR_POOL"
_MIN_WORKERS_ENV = "KITARU_SUPERVISOR_MIN_WORKERS"
_MAX_WORKERS_ENV = "KITARU_SUPERVISOR_MAX_WORKERS"
_TASKS_PER_WORKER_ENV = "KITARU_SUPERVISOR_TASKS_PER_WORKER"
_POLL_SECONDS_ENV = "KITARU_SUPERVISOR_POLL_SECONDS"
_SCALE_DOWN_AFTER_ENV = "KITARU_SUPERVISOR_SCALE_DOWN_AFTER"
_WORKER_POOL_ENV = "KITARU_WORKER_POOL"
_WORKER_CONCURRENCY_ENV = "KITARU_WORKER_CONCURRENCY"
_WORKER_DRAIN_TIMEOUT_ENV = "KITARU_WORKER_DRAIN_TIMEOUT"

_DEFAULT_MIN_WORKERS = 0
_DEFAULT_MAX_WORKERS = 4
_DEFAULT_TASKS_PER_WORKER = 1
_DEFAULT_POLL_SECONDS = 15.0
_DEFAULT_SCALE_DOWN_AFTER = 3
# Fallback shutdown bound when KITARU_WORKER_DRAIN_TIMEOUT is unset, since an
# unset drain timeout tells spawned workers to wait indefinitely for held
# tasks instead of bounding the wait themselves.
_DEFAULT_DRAIN_TIMEOUT_SECONDS = 30.0
_SHUTDOWN_SLACK_SECONDS = 10.0


@dataclass(frozen=True)
class SupervisorConfig:
    """Worker pool supervisor configuration."""

    pool: str
    min_workers: int
    max_workers: int
    tasks_per_worker: int
    poll_seconds: float
    scale_down_after: int
    worker_concurrency: str | None
    worker_drain_timeout: str | None


def _env_int(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to a default."""
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    """Read a float environment variable, falling back to a default."""
    raw = os.environ.get(name)
    return float(raw) if raw else default


def load_config() -> SupervisorConfig:
    """Read supervisor configuration from the environment.

    Raises:
        RuntimeError: KITARU_SUPERVISOR_POOL is not set.

    Returns:
        Resolved supervisor configuration.
    """
    pool = os.environ.get(_POOL_ENV)
    if not pool:
        raise RuntimeError(f"{_POOL_ENV} is required.")
    return SupervisorConfig(
        pool=pool,
        min_workers=max(0, _env_int(_MIN_WORKERS_ENV, _DEFAULT_MIN_WORKERS)),
        max_workers=max(1, _env_int(_MAX_WORKERS_ENV, _DEFAULT_MAX_WORKERS)),
        tasks_per_worker=max(
            1, _env_int(_TASKS_PER_WORKER_ENV, _DEFAULT_TASKS_PER_WORKER)
        ),
        poll_seconds=_env_float(_POLL_SECONDS_ENV, _DEFAULT_POLL_SECONDS),
        scale_down_after=max(
            1, _env_int(_SCALE_DOWN_AFTER_ENV, _DEFAULT_SCALE_DOWN_AFTER)
        ),
        worker_concurrency=os.environ.get(_WORKER_CONCURRENCY_ENV),
        worker_drain_timeout=os.environ.get(_WORKER_DRAIN_TIMEOUT_ENV),
    )


def _desired_worker_count(pending_tasks: int, config: SupervisorConfig) -> int:
    """Compute the worker count needed to cover pending tasks, clamped to bounds."""
    raw = math.ceil(pending_tasks / config.tasks_per_worker)
    return max(config.min_workers, min(config.max_workers, raw))


async def _sleep_until_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for a duration, ending early when stop fires."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


def _worker_env(config: SupervisorConfig) -> dict[str, str]:
    """Build the environment for one spawned worker subprocess."""
    env = dict(os.environ)
    env[_WORKER_POOL_ENV] = config.pool
    if config.worker_concurrency is not None:
        env[_WORKER_CONCURRENCY_ENV] = config.worker_concurrency
    if config.worker_drain_timeout is not None:
        env[_WORKER_DRAIN_TIMEOUT_ENV] = config.worker_drain_timeout
    return env


async def _spawn_worker(config: SupervisorConfig) -> asyncio.subprocess.Process:
    """Start one `kitaru worker start` subprocess bound to the supervised pool."""
    process = await asyncio.create_subprocess_exec(
        "kitaru", "worker", "start", env=_worker_env(config)
    )
    logger.info(
        "Spawned worker subprocess pid=%d for pool %r.", process.pid, config.pool
    )
    return process


def _shutdown_wait_seconds(config: SupervisorConfig) -> float:
    """Bound how long shutdown waits for children: drain timeout plus slack."""
    drain_timeout = (
        float(config.worker_drain_timeout)
        if config.worker_drain_timeout is not None
        else _DEFAULT_DRAIN_TIMEOUT_SECONDS
    )
    return drain_timeout + _SHUTDOWN_SLACK_SECONDS


@contextlib.contextmanager
def _handle_shutdown_signals(stop: asyncio.Event) -> Iterator[None]:
    """Install SIGINT/SIGTERM handlers that request a graceful shutdown."""
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)

    def _request_stop(signum: int) -> None:
        logger.info("Received %s, shutting down.", signal.Signals(signum).name)
        stop.set()

    for sig in signals:
        loop.add_signal_handler(sig, _request_stop, sig)
    try:
        yield
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


class Supervisor:
    """Scales `kitaru worker start` subprocesses to match one pool's demand."""

    def __init__(self, config: SupervisorConfig, client: KitaruAPIClient) -> None:
        """Initialize the supervisor with its config and API client."""
        self._config = config
        self._client = client
        self._children: list[asyncio.subprocess.Process] = []
        self._terminated_pids: set[int] = set()
        self._low_desired_streak = 0
        self.stop = asyncio.Event()

    async def run(self) -> None:
        """Poll pool stats and reconcile worker subprocesses until stopped."""
        with _handle_shutdown_signals(self.stop):
            try:
                while not self.stop.is_set():
                    await self._tick()
                    await _sleep_until_stop(self.stop, self._config.poll_seconds)
            finally:
                await self._shutdown()

    async def _tick(self) -> None:
        """Reap exited children, fetch stats, and reconcile the desired count."""
        self._reap_exited()
        try:
            stats = await self._client.worker_pools.stats(self._config.pool)
        except (APIError, httpx.TransportError) as error:
            logger.warning(
                "Failed to fetch stats for pool %r: %s", self._config.pool, error
            )
            return

        desired = _desired_worker_count(stats.pending_tasks, self._config)
        await self._reconcile(desired)
        logger.info(
            "pool=%s pending=%d in_flight=%d live_workers=%d children=%d desired=%d",
            self._config.pool,
            stats.pending_tasks,
            stats.in_flight_tasks,
            stats.live_workers,
            len(self._children),
            desired,
        )

    async def _reconcile(self, desired: int) -> None:
        """Spawn immediately on rising demand, or count toward a delayed scale-down."""
        running = len(self._children)
        if desired > running:
            for _ in range(desired - running):
                self._children.append(await _spawn_worker(self._config))
            self._low_desired_streak = 0
        elif desired < running:
            self._low_desired_streak += 1
            if self._low_desired_streak >= self._config.scale_down_after:
                self._scale_down(running - desired)
                self._low_desired_streak = 0
        else:
            self._low_desired_streak = 0

    def _scale_down(self, surplus: int) -> None:
        """Send SIGTERM to `surplus` children, which drain and release their tasks."""
        for process in self._children[-surplus:]:
            logger.info(
                "Scaling down: terminating worker subprocess pid=%d.", process.pid
            )
            self._terminated_pids.add(process.pid)
            process.terminate()

    def _reap_exited(self) -> None:
        """Drop exited children, logging exits the supervisor did not request."""
        remaining: list[asyncio.subprocess.Process] = []
        for process in self._children:
            if process.returncode is None:
                remaining.append(process)
                continue
            expected = process.pid in self._terminated_pids
            self._terminated_pids.discard(process.pid)
            log = logger.info if expected else logger.warning
            log(
                "Worker subprocess pid=%d exited with code %d%s.",
                process.pid,
                process.returncode,
                "" if expected else " unexpectedly",
            )
        self._children = remaining

    async def _shutdown(self) -> None:
        """Terminate every child, bounded by the drain timeout plus slack."""
        if not self._children:
            return
        logger.info(
            "Shutting down: terminating %d worker subprocess(es).", len(self._children)
        )
        for process in self._children:
            process.terminate()

        bound = _shutdown_wait_seconds(self._config)
        _, pending = await asyncio.wait(
            [asyncio.ensure_future(process.wait()) for process in self._children],
            timeout=bound,
        )
        if pending:
            logger.warning(
                "%d worker subprocess(es) still running after %.0fs, leaving them.",
                len(pending),
                bound,
            )


async def _async_main() -> int:
    """Build the client and supervisor, then run until a stop signal arrives."""
    try:
        config = load_config()
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    async with KitaruAPIClient() as client:
        await Supervisor(config, client).run()
    return 0


def main() -> None:
    """Run the supervisor as a script."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
