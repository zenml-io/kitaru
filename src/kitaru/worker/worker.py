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
"""Worker lifecycle: registration, the claim loop, and stop semantics."""

import asyncio
import contextlib
import logging
import os
import platform
import re
import socket
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import httpx

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import (
    TaskClaimRequest,
    TaskStatus,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.api_models.v1.worker import WorkerCreateRequest, WorkerRuntime
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.auth import RenewingTokenAuth
from kitaru.client.exceptions import APIError
from kitaru.worker.auth import WorkerTokenSource
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.heartbeat import WorkerHeartbeat
from kitaru.worker.inflight import InflightTasks
from kitaru.worker.task_runner import TaskRunner

logger = logging.getLogger(__name__)

CLAIM_BACKOFF_MAX_SECONDS = 60.0
PAYLOAD_CACHE_MAX_BYTES = 1024**3

DEFAULT_BLOB_CACHE_ROOT = Path.home() / ".cache" / "kitaru" / "blobs"
DEFAULT_PAYLOAD_CACHE_ROOT = Path.home() / ".cache" / "kitaru" / "payloads"

_MAX_CLAIM_BATCH = 100
_SETTLED_JOB_STATUSES = frozenset(
    {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
)
_KUBERNETES_NAMESPACE_PATH = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)
_DOCKERENV_PATH = Path("/.dockerenv")
_CGROUP_PATH = Path("/proc/1/cgroup")
_CGROUP_CONTAINER_MARKERS = ("docker", "kubepods", "containerd")


def default_worker_name() -> str:
    """Derive the default worker name from the hostname and process id.

    Returns:
        Hostname-pid identifier with characters outside [A-Za-z0-9_-]
        replaced by dashes and leading or trailing dashes and underscores
        stripped.
    """
    raw = f"{socket.gethostname()}-{os.getpid()}"
    sanitized = re.sub(r"[^A-Za-z0-9_-]", "-", raw)
    return sanitized.strip("-_")


def detect_runtime() -> WorkerRuntime:
    """Detect the runtime platform the worker is registering from.

    Returns:
        Runtime reported at registration: kubernetes, docker, or bare.
    """
    hostname = socket.gethostname()
    fields = {
        "hostname": hostname,
        "os": platform.system(),
        "arch": platform.machine(),
        "python_version": platform.python_version(),
        "kitaru_version": _get_kitaru_version(),
    }
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        namespace = None
        with contextlib.suppress(OSError):
            namespace = _KUBERNETES_NAMESPACE_PATH.read_text(encoding="utf-8").strip()
        return WorkerRuntime(
            platform="kubernetes", namespace=namespace, pod=hostname, **fields
        )
    if _DOCKERENV_PATH.exists() or _cgroup_reports_container():
        return WorkerRuntime(platform="docker", **fields)
    return WorkerRuntime(platform="bare", **fields)


def _cgroup_reports_container() -> bool:
    """Report whether the init process's cgroup carries a container marker.

    Returns:
        Whether /proc/1/cgroup names a known container runtime.
    """
    try:
        content = _CGROUP_PATH.read_text(encoding="utf-8")
    except OSError:
        return False
    return any(marker in content for marker in _CGROUP_CONTAINER_MARKERS)


def _get_kitaru_version() -> str | None:
    """Read the installed kitaru package version.

    Returns:
        Installed version, or None when the package metadata is unavailable.
    """
    try:
        return version("kitaru")
    except PackageNotFoundError:
        return None


class Worker:
    """Claims tasks matching its scope and executes them as subprocesses."""

    def __init__(self, config: WorkerConfig) -> None:
        """Initialize the worker.

        Args:
            config: Worker configuration.
        """
        self._config = config
        self._inflight = InflightTasks()

    def cancel_inflight(self) -> None:
        """Request cancellation of every task the worker currently holds."""
        self._inflight.cancel_all()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Register, claim, and execute tasks until the scope drains or stops.

        Args:
            stop: Event that ends the claim loop when set, in addition to the
                scope's own completion and the configured lifetime timeout.
                Defaults to an event nothing else sets.
        """
        stop = stop if stop is not None else asyncio.Event()
        name = self._config.name or default_worker_name()
        blob_cache_root = self._config.blob_cache_root or DEFAULT_BLOB_CACHE_ROOT
        payload_cache_root = (
            self._config.payload_cache_root or DEFAULT_PAYLOAD_CACHE_ROOT
        )

        registration = WorkerCreateRequest(
            name=name,
            scope=self._config.scope,
            runtime=detect_runtime(),
            metadata=self._config.metadata,
        )
        async with KitaruAPIClient() as registration_client:
            # Task process environments inherit KITARU_API_URL from the
            # worker's environment, so pin it to the server this worker
            # registered with.
            os.environ["KITARU_API_URL"] = registration_client.base_url
            response = await registration_client.workers.create(registration)
            worker = response.worker
            logger.info("Registered worker %s (%s).", name, worker.id)
            source = WorkerTokenSource(
                registration_client, registration, response.token.get_secret_value()
            )
            client = registration_client.with_auth(RenewingTokenAuth(source))
            ctx = ExecutionContext(
                client=client,
                blob_cache=BlobCache(blob_cache_root),
                payload_cache=BlobCache(
                    payload_cache_root, max_bytes=PAYLOAD_CACHE_MAX_BYTES
                ),
            )
            heartbeat = WorkerHeartbeat(
                client=client,
                worker_id=worker.id,
                inflight=self._inflight,
                interval=self._config.heartbeat_interval,
            )
            heartbeat_task = asyncio.create_task(self._supervise_heartbeat(heartbeat))
            try:
                await self._claim_loop(ctx, stop)
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
                logger.info("Worker %s stopped.", name)

    async def _supervise_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        """Run the heartbeat, restarting it after an unexpected exception.

        Args:
            heartbeat: Heartbeat to run.
        """
        while True:
            try:
                await heartbeat.run()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Heartbeat failed unexpectedly, restarting.")
                await asyncio.sleep(self._config.heartbeat_interval)

    async def _claim_loop(
        self,
        ctx: ExecutionContext,
        stop: asyncio.Event,
    ) -> None:
        """Claim to capacity, dispatch runners, and stop when the scope drains.

        Args:
            ctx: Execution context.
            stop: Event that ends the loop when set.
        """
        deadline = None
        if self._config.timeout is not None:
            deadline = asyncio.get_running_loop().time() + self._config.timeout

        runner = TaskRunner(ctx)
        running: set[asyncio.Task[None]] = set()
        backoff = self._config.poll_interval

        try:
            while True:
                running = {task for task in running if not task.done()}
                if stop.is_set():
                    logger.info("Stop requested, ending the claim loop.")
                    break
                if (
                    deadline is not None
                    and asyncio.get_running_loop().time() >= deadline
                ):
                    logger.info("Lifetime timeout reached, ending the claim loop.")
                    break

                free_slots = self._config.concurrency - len(running)
                if free_slots <= 0:
                    await _wait_for_slot(running, stop, deadline)
                    continue

                max_tasks = min(
                    free_slots,
                    self._config.claim_batch_size or free_slots,
                    _MAX_CLAIM_BATCH,
                )
                try:
                    claimed = await ctx.client.tasks.claim(
                        TaskClaimRequest(max_tasks=max_tasks)
                    )
                except (APIError, httpx.TransportError) as exc:
                    logger.warning("Failed to claim tasks: %s", exc)
                    await _sleep_until_stop(stop, backoff, deadline)
                    backoff = min(backoff * 2, CLAIM_BACKOFF_MAX_SECONDS)
                    continue

                backoff = self._config.poll_interval
                if claimed.tasks:
                    logger.info("Claimed %d task(s).", len(claimed.tasks))
                for item in claimed.tasks:
                    running.add(asyncio.create_task(self._run_task(ctx, runner, item)))

                if len(claimed.tasks) == max_tasks:
                    continue

                if await self._should_stop(ctx, stop, deadline):
                    break
                await _sleep_until_stop(stop, self._config.poll_interval, deadline)

            running = {task for task in running if not task.done()}
            if running:
                await self._drain(running)
        finally:
            pending = {task for task in running if not task.done()}
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _drain(self, running: set[asyncio.Task[None]]) -> None:
        """Wait for the running tasks, canceling them past the drain timeout.

        Args:
            running: Running task set.
        """
        logger.info("Draining %d running task(s).", len(running))
        if self._config.drain_timeout is None:
            await asyncio.wait(running)
            return
        _, pending = await asyncio.wait(running, timeout=self._config.drain_timeout)
        if pending:
            logger.info(
                "Drain timeout reached, canceling %d remaining task(s).", len(pending)
            )
            self._inflight.cancel_all()
            await asyncio.wait(pending)

    async def _should_stop(
        self, ctx: ExecutionContext, stop: asyncio.Event, deadline: float | None
    ) -> bool:
        """Decide whether the claim loop should stop after an empty claim.

        Args:
            ctx: Execution context.
            stop: Event that ends the loop when set.
            deadline: Loop time the lifetime timeout expires at, if any.

        Returns:
            Whether the stop event is set, the deadline has passed, or the
            scope's pinned job has settled.
        """
        if stop.is_set():
            logger.info("Stop requested, ending the claim loop.")
            return True
        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
            logger.info("Lifetime timeout reached, ending the claim loop.")
            return True
        job_id = self._config.scope.job_id
        if job_id is not None:
            try:
                job = await ctx.client.jobs.get(job_id)
            except (APIError, httpx.TransportError) as exc:
                logger.warning("Failed to read job %s: %s", job_id, exc)
                return False
            if job.status in _SETTLED_JOB_STATUSES:
                logger.info(
                    "Job %s settled as %s, ending the claim loop.", job_id, job.status
                )
                return True
        return False

    async def _run_task(
        self,
        ctx: ExecutionContext,
        runner: TaskRunner,
        claimed: TaskWithSpec,
    ) -> None:
        """Register a claimed task as in flight and execute it.

        Args:
            ctx: Execution context.
            runner: Task runner.
            claimed: Claimed task and its execution spec.
        """
        task_id = claimed.task.id
        canceled = self._inflight.register(task_id)
        try:
            await runner.execute(claimed, canceled)
        except Exception as exc:
            logger.exception("Task %s runner failed.", task_id)
            await self._fail_crashed_task(ctx, claimed, exc)
        finally:
            self._inflight.unregister(task_id)

    async def _fail_crashed_task(
        self, ctx: ExecutionContext, claimed: TaskWithSpec, exc: Exception
    ) -> None:
        """Fail a task whose runner crashed, so it does not stay running.

        Args:
            ctx: Execution context.
            claimed: Claimed task and its execution spec.
            exc: Exception the runner crashed with.
        """
        client = ctx.client.with_token(claimed.token.get_secret_value())
        request = TaskUpdateRequest(
            status=TaskStatus.FAILED, error=f"Task runner failed: {exc}"
        )
        try:
            await client.tasks.update(claimed.task.id, request)
        except (APIError, httpx.TransportError) as failure:
            logger.warning(
                "Failed to update task %s to %s: %s",
                claimed.task.id,
                request.status,
                failure,
            )


async def _wait_for_slot(
    running: set[asyncio.Task[None]],
    stop: asyncio.Event,
    deadline: float | None,
) -> None:
    """Wait until a running task finishes, stop fires, or the deadline hits.

    Args:
        running: Running task set.
        stop: Event that ends the wait when set.
        deadline: Loop time the lifetime timeout expires at, if any.
    """
    stop_wait: asyncio.Task[object] = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait(
            {*running, stop_wait},
            return_when=asyncio.FIRST_COMPLETED,
            timeout=_get_remaining_time(deadline),
        )
    finally:
        stop_wait.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_wait


async def _sleep_until_stop(
    stop: asyncio.Event, duration: float, deadline: float | None
) -> None:
    """Sleep for a duration, ending early when stop fires or the deadline hits.

    Args:
        stop: Event that ends the sleep when set.
        duration: Seconds to sleep.
        deadline: Loop time the lifetime timeout expires at, if any.
    """
    remaining = _get_remaining_time(deadline)
    if remaining is not None:
        duration = min(duration, remaining)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=duration)


def _get_remaining_time(deadline: float | None) -> float | None:
    """Compute the time left until a loop deadline.

    Args:
        deadline: Loop time the lifetime timeout expires at, if any.

    Returns:
        Seconds remaining clamped at zero, or None without a deadline.
    """
    if deadline is None:
        return None
    return max(0.0, deadline - asyncio.get_running_loop().time())
