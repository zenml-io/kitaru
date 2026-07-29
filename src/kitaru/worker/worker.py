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
import uuid
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import TaskClaimRequest, TaskWithSpec
from kitaru.api_models.v1.worker import WorkerCreateRequest, WorkerRuntime
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.heartbeat import WorkerHeartbeat
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

        async with KitaruAPIClient.from_env() as client:
            ctx = ExecutionContext(
                client=client,
                blob_cache=BlobCache(blob_cache_root),
                payload_cache=BlobCache(
                    payload_cache_root, max_bytes=PAYLOAD_CACHE_MAX_BYTES
                ),
            )
            worker = await client.workers.create(
                WorkerCreateRequest(
                    name=name,
                    scope=self._config.scope,
                    runtime=detect_runtime(),
                    metadata=self._config.metadata,
                )
            )
            heartbeat = WorkerHeartbeat(
                client=client,
                worker_id=worker.id,
                interval=self._config.heartbeat_interval,
            )
            heartbeat_task = asyncio.create_task(heartbeat.run())
            try:
                await self._claim_loop(ctx, worker.id, heartbeat, stop)
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _claim_loop(
        self,
        ctx: ExecutionContext,
        worker_id: uuid.UUID,
        heartbeat: WorkerHeartbeat,
        stop: asyncio.Event,
    ) -> None:
        """Claim to capacity, dispatch runners, and stop when the scope drains.

        Args:
            ctx: Execution context.
            worker_id: Id of the registered worker.
            heartbeat: Heartbeat tracking in-flight tasks.
            stop: Event that ends the loop when set.
        """
        deadline = None
        if self._config.timeout is not None:
            deadline = asyncio.get_running_loop().time() + self._config.timeout

        runner = TaskRunner(ctx)
        running: set[asyncio.Task[None]] = set()
        backoff = self._config.poll_interval

        while True:
            free_slots = self._config.concurrency - len(running)
            if free_slots <= 0:
                _, running = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
                continue

            max_tasks = min(
                free_slots,
                self._config.claim_batch_size or free_slots,
                _MAX_CLAIM_BATCH,
            )
            try:
                claimed = await ctx.client.tasks.claim(
                    TaskClaimRequest(worker_id=worker_id, max_tasks=max_tasks)
                )
            except APIError as exc:
                logger.warning("Failed to claim tasks: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, CLAIM_BACKOFF_MAX_SECONDS)
                continue

            backoff = self._config.poll_interval
            for item in claimed.tasks:
                running.add(
                    asyncio.create_task(self._run_task(ctx, heartbeat, runner, item))
                )

            if len(claimed.tasks) == max_tasks:
                continue

            if await self._should_stop(ctx, stop, deadline):
                break
            await asyncio.sleep(self._config.poll_interval)

        if running:
            await asyncio.wait(running)

    async def _should_stop(
        self, ctx: ExecutionContext, stop: asyncio.Event, deadline: float | None
    ) -> bool:
        """Decide whether the claim loop should stop after an empty claim.

        Args:
            ctx: Execution context.
            stop: Event that ends the loop when set.
            deadline: Loop time the lifetime timeout expires at, if any.

        Raises:
            APIError: The scope's pinned job was not found.

        Returns:
            Whether the stop event is set, the deadline has passed, or the
            scope's pinned job has settled.
        """
        if stop.is_set():
            return True
        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
            return True
        job_id = self._config.scope.job_id
        if job_id is not None:
            job = await ctx.client.jobs.get(job_id)
            return job.status in _SETTLED_JOB_STATUSES
        return False

    async def _run_task(
        self,
        ctx: ExecutionContext,
        heartbeat: WorkerHeartbeat,
        runner: TaskRunner,
        claimed: TaskWithSpec,
    ) -> None:
        """Register a claimed task with the heartbeat and execute it.

        Args:
            ctx: Execution context.
            heartbeat: Heartbeat tracking in-flight tasks.
            runner: Task runner.
            claimed: Claimed task and its execution spec.
        """
        task_id = claimed.task.id
        canceled = heartbeat.register(task_id)
        try:
            await runner.execute(claimed, canceled)
        except Exception:
            logger.exception("Task %s runner failed.", task_id)
        finally:
            heartbeat.unregister(task_id)
