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
"""Worker lifecycle: registration, heartbeat, and the claim loop."""

import asyncio
import contextlib
import logging
import os
import re
import socket
import time
import uuid

from kitaru.api_models.v1.experiment_runs import ExperimentRunStatus
from kitaru.api_models.v1.jobs import ClaimedJobResponse, JobClaimRequest, JobStatus
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.blob_cache import BlobCache
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.heartbeat import WorkerHeartbeat
from kitaru.worker.job_runner import JobRunner

logger = logging.getLogger(__name__)

CLAIM_BACKOFF_MAX_SECONDS = 60
PAYLOAD_CACHE_MAX_BYTES = 1024**3

_TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    }
)

_TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)


def _default_worker_name() -> str:
    """Derive a worker name from the hostname and pid.

    Characters outside the server's worker name charset are replaced
    with dashes.

    Returns:
        Sanitized worker name.
    """
    hostname = re.sub(r"[^A-Za-z0-9_-]", "-", socket.gethostname())
    return f"{hostname}-{os.getpid()}".strip("-_")


class Worker:
    """Lifecycle owner of registration, heartbeat, and the claim loop."""

    def __init__(self, config: WorkerConfig) -> None:
        """Initialize the worker.

        Args:
            config: Worker configuration.
        """
        self._config = config
        self._name = config.name or _default_worker_name()

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Register, claim, and execute jobs until the scope drains or stops.

        Opens an API client, registers the worker by name, and runs the
        claim loop until the config scope drains, the stop event is set,
        or the lifetime timeout elapses. Callers that want the terminal
        entity of a pinned scope fetch it after this call returns.

        Args:
            stop: Event ending an unpinned scope once its claims drain,
                the loop runs until cancellation when omitted.

        Raises:
            APIError: The registration or a claim failed outside the
                backoff-retried claim errors.
        """
        deadline = (
            time.monotonic() + self._config.timeout
            if self._config.timeout is not None
            else None
        )
        async with KitaruAPIClient(
            base_url=os.environ["KITARU_API_URL"],
            api_key=os.environ["KITARU_API_KEY"],
        ) as client:
            ctx = ExecutionContext(
                client=client,
                blob_cache=BlobCache(self._config.blob_cache_root),
                payload_cache=BlobCache(
                    self._config.payload_cache_root, max_bytes=PAYLOAD_CACHE_MAX_BYTES
                ),
            )
            worker = await client.workers.create(
                WorkerCreateRequest(name=self._name, scope=self._config.scope)
            )
            heartbeat = WorkerHeartbeat(
                client, worker.id, self._config.heartbeat_interval
            )
            heartbeat_task = asyncio.create_task(heartbeat.run())
            try:
                await self._claim_loop(ctx, worker.id, heartbeat, stop, deadline)
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
        """Claim and dispatch jobs to capacity until the scope stops.

        Args:
            ctx: Execution context.
            worker_id: Id of the registered worker.
            heartbeat: Heartbeat in-flight jobs register with.
            stop: Event ending an unpinned scope.
            deadline: Lifetime deadline, unbounded when ``None``.

        Raises:
            APIError: A stop condition read failed.
        """
        job_runner = JobRunner(ctx)
        running: set[asyncio.Task[None]] = set()
        backoff = self._config.poll_interval
        while True:
            free_slots = self._config.concurrency - len(running)
            if free_slots <= 0:
                _, running = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
                continue
            max_jobs = min(free_slots, self._config.claim_batch_size or free_slots)
            try:
                response = await ctx.client.jobs.claim(
                    JobClaimRequest(
                        worker_id=worker_id, max_jobs=max_jobs, scope=self._config.scope
                    )
                )
            except APIError as exc:
                logger.warning("Claim for worker %s failed: %s", worker_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, CLAIM_BACKOFF_MAX_SECONDS)
                continue
            backoff = self._config.poll_interval
            if response.jobs:
                for claimed in response.jobs:
                    running.add(
                        asyncio.create_task(
                            self._execute(job_runner, heartbeat, claimed)
                        )
                    )
                continue
            if await self._should_stop(ctx.client, stop, deadline):
                break
            await asyncio.sleep(self._config.poll_interval)
        if running:
            await asyncio.wait(running)

    async def _execute(
        self,
        job_runner: JobRunner,
        heartbeat: WorkerHeartbeat,
        claimed: ClaimedJobResponse,
    ) -> None:
        """Execute one claimed job under the heartbeat, logging failures.

        Args:
            job_runner: Runner executing the job.
            heartbeat: Heartbeat the job registers with while it runs.
            claimed: Claimed job and its spec.
        """
        canceled = heartbeat.register(claimed.job.id)
        try:
            await job_runner.execute(claimed, canceled)
        except Exception:
            logger.exception("Job %s failed", claimed.job.id)
        finally:
            heartbeat.unregister(claimed.job.id)

    async def _should_stop(
        self,
        client: KitaruAPIClient,
        stop: asyncio.Event | None,
        deadline: float | None,
    ) -> bool:
        """Check whether the claim loop should stop claiming.

        Args:
            client: API client.
            stop: Event ending an unpinned scope.
            deadline: Lifetime deadline, unbounded when ``None``.

        Raises:
            APIError: The pinned job or run read failed, including 404
                when it no longer exists.

        Returns:
            Whether the scope is drained or the lifetime ended.
        """
        if stop is not None and stop.is_set():
            return True
        if deadline is not None and time.monotonic() >= deadline:
            return True
        scope = self._config.scope
        if scope.job_id is not None:
            job = await client.jobs.get(scope.job_id)
            return job.status in _TERMINAL_JOB_STATUSES
        if scope.experiment_run_id is not None:
            run = await client.experiment_runs.get(scope.experiment_run_id)
            return run.status in _TERMINAL_RUN_STATUSES
        return False
