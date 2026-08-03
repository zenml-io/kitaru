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
"""Shared receipts for CLI operations backed by jobs."""

import asyncio
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskResponse
from kitaru.cli import jobs
from kitaru.cli.output import CLIError, CommandResult, emit_event

_DEFAULT_WAIT_INTERVAL = 2.0
_DEFAULT_WAIT_TIMEOUT = 300.0


def get_wait_settings(
    *, wait: bool, interval: float | None, timeout: float | None
) -> tuple[float, float] | None:
    """Validate and resolve create-command wait settings before mutation."""
    if not wait:
        if interval is not None or timeout is not None:
            raise CLIError(
                "invalid_arguments", "--interval and --timeout require --wait."
            )
        return None

    effective_interval = _DEFAULT_WAIT_INTERVAL if interval is None else interval
    effective_timeout = _DEFAULT_WAIT_TIMEOUT if timeout is None else timeout
    if not math.isfinite(effective_interval) or effective_interval <= 0:
        raise CLIError("invalid_arguments", "--interval must be positive and finite.")
    if not math.isfinite(effective_timeout) or effective_timeout <= 0:
        raise CLIError("invalid_arguments", "--timeout must be positive and finite.")
    return effective_interval, effective_timeout


def get_task_filter_action(resource: str, task_id: uuid.UUID | str) -> str:
    """Build an exact task-filtered list command for a CLI resource."""
    return (
        f"kitaru {resource} list --filter "
        f'\'{{"field":"task_id","op":"eq","value":"{task_id}"}}\''
    )


def created_job_result(
    operation: str,
    job: JobResponse,
    *,
    identity: Mapping[str, Any],
    next_actions: Sequence[str] = (),
) -> CommandResult:
    """Build an immediate receipt for a newly created job.

    Args:
        operation: Stable operation name.
        job: Newly created job.
        identity: Bounded request identity safe to return to the caller.
        next_actions: Additional domain-specific follow-up commands.

    Returns:
        Created-event result with common job inspection actions.
    """
    item = dict(identity)
    item.update(
        {
            "operation": operation,
            "terminal": False,
            "job": job.model_dump(mode="json"),
        }
    )
    return CommandResult(
        item=item,
        event="created",
        next_actions=[
            f"kitaru job watch {job.id}",
            f"kitaru job get {job.id} --tasks",
            *next_actions,
        ],
    )


async def wait_for_terminal_tasks(
    client: Any,
    job_id: uuid.UUID,
    *,
    interval: float = _DEFAULT_WAIT_INTERVAL,
    timeout: float = _DEFAULT_WAIT_TIMEOUT,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    initial_job: JobResponse | None = None,
) -> tuple[JobResponse, list[TaskResponse]]:
    """Wait for terminal job settlement, then fetch every task.

    Args:
        client: API client exposing the jobs resource.
        job_id: Job to wait for.
        interval: Delay between job polls.
        timeout: Finite local wait timeout.
        clock: Monotonic clock, injectable for tests.
        sleep: Async sleep function, injectable for tests.
        initial_job: Created job already emitted by the caller, when available.

    Raises:
        CLIError: Polling validation or the local wait deadline fails.

    Returns:
        Settled job and all of its tasks.
    """
    started_at = clock()
    try:
        job = await jobs.poll_job(
            client,
            job_id,
            interval=interval,
            timeout=timeout,
            clock=clock,
            sleep=sleep,
            initial_job=initial_job,
        )
    except CLIError as error:
        if error.kind != "timeout":
            raise
        details = error.details if isinstance(error.details, dict) else {}
        raise _get_wait_timeout(
            job_id,
            last_status=details.get("last_status"),
            message=error.message,
        ) from error

    remaining = timeout - (clock() - started_at)
    if remaining <= 0:
        raise _get_wait_timeout(job_id, last_status=job.status.value)
    try:
        async with asyncio.timeout(remaining):
            tasks = [task async for task in client.jobs.iter_tasks(job_id)]
    except TimeoutError as error:
        raise _get_wait_timeout(job_id, last_status=job.status.value) from error
    return job, tasks


def _get_wait_timeout(
    job_id: uuid.UUID,
    *,
    last_status: str | None,
    message: str | None = None,
) -> CLIError:
    """Build a recoverable timeout covering polling and task collection."""
    return CLIError(
        "timeout",
        message or f"Timed out waiting for job {job_id}; remote work continues.",
        details={
            "job_id": str(job_id),
            "last_status": last_status,
            "remote_continues": True,
        },
        hint=(
            "Start a worker with `kitaru worker start`, or keep waiting with "
            f"`kitaru job watch {job_id}`."
        ),
    )


def terminal_job_error(
    job: JobResponse,
    receipt: dict[str, Any],
) -> CLIError:
    """Build an enriched error for a failed or canceled job.

    Args:
        job: Settled remote job.
        receipt: Domain receipt containing task diagnostics.

    Raises:
        ValueError: The job did not fail or get canceled.

    Returns:
        Stable remote failure or cancellation error.
    """
    if job.status is JobStatus.FAILED:
        kind = "remote_failed"
    elif job.status is JobStatus.CANCELED:
        kind = "remote_canceled"
    else:
        raise ValueError("terminal_job_error requires a failed or canceled job")

    emit_event("terminal", receipt)
    return CLIError(
        kind,
        f"Job {job.id} settled as {job.status.value}.",
        details={"receipt": receipt},
    )
