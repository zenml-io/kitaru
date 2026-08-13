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
"""Job inspection, polling, and cancellation commands."""

import asyncio
import math
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.cli.output import CLIError, CommandResult, emit_event

_TERMINAL_EXIT_CODES = {
    JobStatus.COMPLETED: 0,
    JobStatus.FAILED: 8,
    JobStatus.CANCELED: 9,
}


async def get_job(client: Any, job_id: uuid.UUID, *, tasks: bool) -> CommandResult:
    """Get one job and optionally include all of its tasks."""
    job = await client.jobs.get(job_id)
    item = job.model_dump(mode="json")
    if tasks:
        item["tasks"] = [
            task.model_dump(mode="json")
            async for task in client.jobs.iter_tasks(job_id)
        ]
    return CommandResult(item=item)


async def poll_job(
    client: Any,
    job_id: uuid.UUID,
    *,
    interval: float,
    timeout: float | None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    initial_job: JobResponse | None = None,
) -> JobResponse:
    """Poll a job until it settles, emitting only meaningful changes."""
    if not math.isfinite(interval) or interval <= 0:
        raise CLIError("invalid_arguments", "--interval must be positive and finite.")
    if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
        raise CLIError("invalid_arguments", "--timeout must be positive and finite.")

    deadline = None if timeout is None else clock() + timeout
    previous = (
        None
        if initial_job is None
        else initial_job.model_dump(mode="json", exclude={"updated"})
    )

    while True:
        if deadline is None:
            job: JobResponse = await client.jobs.get(job_id)
        else:
            remaining = deadline - clock()
            if remaining <= 0:
                raise _get_poll_timeout(job_id, previous)
            try:
                job = await asyncio.wait_for(client.jobs.get(job_id), timeout=remaining)
            except TimeoutError as error:
                raise _get_poll_timeout(job_id, previous) from error
        item = job.model_dump(mode="json")
        fingerprint = dict(item)
        fingerprint.pop("updated", None)
        if fingerprint != previous:
            emit_event("snapshot", item)
            previous = fingerprint

        if job.status in _TERMINAL_EXIT_CODES:
            return job

        if deadline is not None:
            remaining = deadline - clock()
            if remaining <= 0:
                raise _get_poll_timeout(job_id, previous)
            await sleep(min(interval, remaining))
        else:
            await sleep(interval)


async def watch_job(
    client: Any,
    job_id: uuid.UUID,
    *,
    interval: float,
    timeout: float | None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> CommandResult:
    """Watch a job and map its terminal status to the public CLI contract."""
    job = await poll_job(
        client,
        job_id,
        interval=interval,
        timeout=timeout,
        clock=clock,
        sleep=sleep,
    )
    item = job.model_dump(mode="json")
    if job.status is JobStatus.COMPLETED:
        return CommandResult(item=item, event="terminal")

    emit_event("terminal", item)
    kind = "remote_failed" if job.status is JobStatus.FAILED else "remote_canceled"
    raise CLIError(
        kind,
        f"Job {job_id} settled as {job.status.value}.",
        details={"job": item},
    )


async def cancel_job(client: Any, job_id: uuid.UUID) -> CommandResult:
    """Request cancellation once without waiting for the job to settle."""
    job = await client.jobs.cancel(job_id)
    item = job.model_dump(mode="json")
    item["cancellation_requested"] = True
    return CommandResult(
        item=item,
        next_actions=[f"kitaru job watch {job_id}"],
    )


def _get_poll_timeout(job_id: uuid.UUID, previous: dict[str, Any] | None) -> CLIError:
    """Build the stable local polling-timeout error."""
    return CLIError(
        "timeout",
        f"Timed out waiting for job {job_id}; remote work continues.",
        details={
            "job_id": str(job_id),
            "last_status": previous["status"] if previous is not None else None,
        },
    )
