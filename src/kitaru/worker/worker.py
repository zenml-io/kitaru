#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Worker lifecycle: registration, heartbeat, and task claims."""

import asyncio
import contextlib
import importlib.metadata
import logging
import os
import platform
import re
import socket
import time
import uuid
from pathlib import Path

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import TaskClaimRequest, TaskWithSpec
from kitaru.api_models.v1.worker import WorkerCreateRequest, WorkerRuntime
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.heartbeat import WorkerHeartbeat
from kitaru.worker.task_runner import TaskRunner

CLAIM_BACKOFF_MAX_SECONDS = 60.0
PAYLOAD_CACHE_MAX_BYTES = 1024 * 1024 * 1024

_CODE_CACHE_ROOT = Path("~/.cache/kitaru/blobs")
_PAYLOAD_CACHE_ROOT = Path("~/.cache/kitaru/payloads")
_TERMINAL_JOB_STATUSES = {
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.CANCELED,
}

logger = logging.getLogger(__name__)


def _default_worker_name() -> str:
    """Derive a sanitized worker name from the hostname and process id.

    Returns:
        Unique worker name for the current process.
    """
    raw = f"{socket.gethostname()}-{os.getpid()}"
    return re.sub(r"[^A-Za-z0-9_-]", "-", raw).strip("-_")


def _detect_runtime() -> WorkerRuntime:
    """Describe the current worker runtime.

    Returns:
        Runtime metadata sent during worker registration.
    """
    hostname = socket.gethostname()
    namespace: str | None = None
    pod: str | None = None
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        runtime_platform = "kubernetes"
        pod = hostname
        namespace_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        try:
            namespace = namespace_path.read_text(encoding="utf-8").strip() or None
        except OSError:
            namespace = None
    elif _is_docker():
        runtime_platform = "docker"
    else:
        runtime_platform = "bare"

    try:
        kitaru_version = importlib.metadata.version("kitaru")
    except importlib.metadata.PackageNotFoundError:
        kitaru_version = None
    return WorkerRuntime(
        platform=runtime_platform,
        hostname=hostname,
        os=platform.system(),
        arch=platform.machine(),
        python_version=platform.python_version(),
        kitaru_version=kitaru_version,
        namespace=namespace,
        pod=pod,
    )


def _is_docker() -> bool:
    if Path("/.dockerenv").exists():
        return True
    try:
        cgroup = Path("/proc/1/cgroup").read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return any(marker in cgroup for marker in ("docker", "containerd", "kubepods"))


class Worker:
    """Register, claim, and execute tasks within a configured scope."""

    def __init__(self, config: WorkerConfig) -> None:
        """Initialize the worker.

        Args:
            config: Worker settings.
        """
        self._config = config
        self._name = config.name or _default_worker_name()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Run until the scope drains or a configured stop condition holds.

        Args:
            stop: Graceful stop event for an unpinned worker.
        """
        deadline = (
            time.monotonic() + self._config.timeout
            if self._config.timeout is not None
            else None
        )
        async with KitaruAPIClient.from_env() as client:
            ctx = ExecutionContext(
                client=client,
                blob_cache=BlobCache(
                    (self._config.blob_cache_root or _CODE_CACHE_ROOT).expanduser()
                ),
                payload_cache=BlobCache(
                    (
                        self._config.payload_cache_root or _PAYLOAD_CACHE_ROOT
                    ).expanduser(),
                    max_bytes=PAYLOAD_CACHE_MAX_BYTES,
                ),
            )
            registered = await client.workers.create(
                WorkerCreateRequest(
                    name=self._name,
                    scope=self._config.scope,
                    runtime=_detect_runtime(),
                    metadata=self._config.metadata,
                )
            )
            heartbeat = WorkerHeartbeat(
                client,
                registered.id,
                interval=self._config.heartbeat_interval,
            )
            heartbeat_task = asyncio.create_task(heartbeat.run())
            try:
                await self._claim_loop(
                    ctx,
                    registered.id,
                    heartbeat,
                    stop,
                    deadline,
                )
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _claim_loop(
        self,
        ctx: ExecutionContext,
        worker_id: uuid.UUID,
        heartbeat: WorkerHeartbeat,
        stop: asyncio.Event | None,
        deadline: float | None,
    ) -> None:
        runner = TaskRunner(ctx)
        running: set[asyncio.Task[None]] = set()
        backoff = self._config.poll_interval
        while True:
            done = {task for task in running if task.done()}
            if not done and len(running) >= self._config.concurrency:
                done, _ = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
            running.difference_update(done)
            for task in done:
                task.result()

            free_slots = self._config.concurrency - len(running)
            max_tasks = min(
                free_slots,
                self._config.claim_batch_size or free_slots,
                100,
            )
            try:
                response = await ctx.client.tasks.claim(
                    TaskClaimRequest(
                        worker_id=worker_id,
                        max_tasks=max_tasks,
                    )
                )
                backoff = self._config.poll_interval
            except Exception:
                logger.exception("Claim for worker %s failed", worker_id)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, CLAIM_BACKOFF_MAX_SECONDS)
                continue

            for claimed in response.tasks:
                running.add(
                    asyncio.create_task(self._execute(runner, heartbeat, claimed))
                )

            if len(response.tasks) == max_tasks:
                continue
            if not response.tasks and await self._should_stop(ctx, stop, deadline):
                break
            await asyncio.sleep(self._config.poll_interval)

        if running:
            await asyncio.gather(*running)

    async def _execute(
        self,
        runner: TaskRunner,
        heartbeat: WorkerHeartbeat,
        claimed: TaskWithSpec,
    ) -> None:
        task_id = claimed.task.id
        canceled = heartbeat.register(task_id)
        try:
            await runner.execute(claimed, canceled)
        except Exception:
            logger.exception("Runner for task %s failed", task_id)
        finally:
            heartbeat.unregister(task_id)

    async def _should_stop(
        self,
        ctx: ExecutionContext,
        stop: asyncio.Event | None,
        deadline: float | None,
    ) -> bool:
        if deadline is not None and time.monotonic() >= deadline:
            return True
        if stop is not None and stop.is_set():
            return True
        job_id = self._config.scope.job_id
        if job_id is None:
            return False
        job = await ctx.client.jobs.get(job_id)
        return job.status in _TERMINAL_JOB_STATUSES
