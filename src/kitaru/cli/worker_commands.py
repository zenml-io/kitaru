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
"""Worker and job commands."""

import uuid
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from kitaru.cli import invocation_context, jobs, workers
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    LIST_PARAMETERS,
    UUID_READ_ERRORS,
    build_spec,
    job_app,
    register_command,
    worker_app,
)
from kitaru.cli.invocation_context import get_invocation
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec

_WORKER_START_PARAMETERS = (
    ParameterSpec("--name", "string", "option", False, "Ephemeral worker name."),
    ParameterSpec(
        "--claim",
        "agent|evaluator|importer|analyzer|agent=UUID[]",
        "option",
        False,
        "Claim the worker serves: agent, evaluator, importer, analyzer, or "
        "agent=AGENT_VERSION_ID. Repeat for multiple claims.",
    ),
    ParameterSpec(
        "--selector",
        "KEY=[VALUE[,VALUE]][]|JSON[]",
        "option",
        False,
        "Task label selectors, combined by conjunction.",
    ),
    ParameterSpec("--job-id", "UUID", "option", False, "Restrict claims to one job."),
    ParameterSpec(
        "--concurrency", "positive integer", "option", False, "Maximum held tasks."
    ),
    ParameterSpec(
        "--claim-batch-size",
        "positive integer",
        "option",
        False,
        "Maximum tasks per claim.",
    ),
    ParameterSpec(
        "--poll-interval", "positive float", "option", False, "Idle polling interval."
    ),
    ParameterSpec(
        "--heartbeat-interval", "positive float", "option", False, "Heartbeat interval."
    ),
    ParameterSpec(
        "--timeout", "positive float", "option", False, "Worker lifetime in seconds."
    ),
    ParameterSpec(
        "--drain-timeout",
        "positive float",
        "option",
        False,
        "Seconds to wait for held tasks before canceling them.",
    ),
    ParameterSpec(
        "--blob-cache-root", "path", "option", False, "Blob cache directory."
    ),
    ParameterSpec(
        "--payload-cache-root", "path", "option", False, "Payload cache directory."
    ),
    ParameterSpec(
        "--metadata", "KEY=VALUE[]", "option", False, "Worker registration metadata."
    ),
    ParameterSpec(
        "--log-level",
        "string",
        "option",
        False,
        "Log level for worker runtime logs: debug, info, warning, or error.",
    ),
)


@register_command(
    worker_app,
    build_spec(
        ("worker", "start"),
        "Run a generic local worker in the foreground without durable provider "
        "binding. The first SIGINT or SIGTERM requests a safe drain of held tasks.",
        parameters=_WORKER_START_PARAMETERS,
        read_only=False,
        side_effects=(
            "reads_local_file",
            "creates_remote_state",
            "executes_local_code",
        ),
        idempotency="non_idempotent_remote_create",
        errors=ASSET_READ_ERRORS,
        streams=True,
    ),
)
async def worker_start(
    *,
    name: str | None = None,
    claims: Annotated[
        list[str] | None,
        Parameter(
            name="--claim",
            help="Claim the worker serves: agent, evaluator, importer, analyzer, "
            "or agent=AGENT_VERSION_ID. Repeat for multiple claims.",
        ),
    ] = None,
    selectors: Annotated[
        list[str] | None,
        Parameter(name="--selector", help="KEY=[VALUE[,VALUE]] or selector JSON."),
    ] = None,
    job_id: uuid.UUID | None = None,
    concurrency: int | None = None,
    claim_batch_size: int | None = None,
    poll_interval: float | None = None,
    heartbeat_interval: float | None = None,
    timeout: float | None = None,
    drain_timeout: float | None = None,
    blob_cache_root: Path | None = None,
    payload_cache_root: Path | None = None,
    metadata: list[str] | None = None,
    log_level: Annotated[
        str,
        Parameter(
            name="--log-level",
            help="Log level for worker runtime logs: debug, info, warning, or error.",
        ),
    ] = "info",
) -> CommandResult:
    """Launch the existing generic worker as one foreground process."""
    invocation = get_invocation()
    target = invocation.resolve_target()
    return await workers.start_worker(
        target,
        log_level=log_level,
        name=name,
        claims=claims,
        selectors=selectors,
        job_id=job_id,
        concurrency=concurrency,
        claim_batch_size=claim_batch_size,
        poll_interval=poll_interval,
        heartbeat_interval=heartbeat_interval,
        timeout=timeout,
        drain_timeout=drain_timeout,
        blob_cache_root=blob_cache_root,
        payload_cache_root=payload_cache_root,
        metadata=metadata,
    )


@register_command(
    worker_app,
    build_spec(
        ("worker", "list"),
        "List live workers, or every server record with --include-stale.",
        parameters=(
            *LIST_PARAMETERS,
            ParameterSpec(
                "--include-stale",
                "boolean",
                "option",
                False,
                "Include workers past the liveness window.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def worker_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
    include_stale: Annotated[
        bool, Parameter(name="--include-stale", negative=False)
    ] = False,
) -> CommandResult:
    """List one server page of worker records."""
    async with invocation_context.open_asset_client() as client:
        return await workers.list_workers(
            client,
            size=size,
            cursor=cursor,
            sort=sort,
            filter=filter,
            include_stale=include_stale,
        )


@register_command(
    worker_app,
    build_spec(
        ("worker", "get"),
        "Get a worker by exact UUID.",
        parameters=(ParameterSpec("WORKER", "UUID", "argument", True, "Worker ID."),),
        errors=UUID_READ_ERRORS,
    ),
)
async def worker_get(worker: uuid.UUID, /) -> CommandResult:
    """Get one exact worker record."""
    async with invocation_context.open_asset_client() as client:
        return await workers.get_worker(client, worker)


_JOB_ERRORS = (
    "invalid_arguments",
    "invalid_configuration",
    "authentication_failed",
    "not_found",
    "conflict",
    "network_error",
    "internal_error",
)


@register_command(
    job_app,
    build_spec(
        ("job", "get"),
        "Get a job and optionally include a complete task snapshot.",
        parameters=(
            ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),
            ParameterSpec(
                "--tasks", "boolean", "option", False, "Include all job tasks."
            ),
        ),
        errors=_JOB_ERRORS,
    ),
)
async def job_get(job: uuid.UUID, /, *, tasks: bool = False) -> CommandResult:
    """Get one job and optionally paginate through all of its tasks."""
    async with invocation_context.open_asset_client() as client:
        return await jobs.get_job(client, job, tasks=tasks)


@register_command(
    job_app,
    build_spec(
        ("job", "watch"),
        "Poll a job until it completes, fails, or is canceled without changing it.",
        parameters=(
            ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),
            ParameterSpec(
                "--interval", "positive float", "option", False, "Polling interval."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Local wait timeout."
            ),
        ),
        errors=(*_JOB_ERRORS, "timeout", "remote_failed", "remote_canceled"),
        streams=True,
    ),
)
async def job_watch(
    job: uuid.UUID,
    /,
    *,
    interval: float = 2.0,
    timeout: float | None = None,
) -> CommandResult:
    """Wait locally for a terminal job state without canceling remote work."""
    async with invocation_context.open_asset_client() as client:
        return await jobs.watch_job(client, job, interval=interval, timeout=timeout)


@register_command(
    job_app,
    build_spec(
        ("job", "cancel"),
        "Request job cancellation once without waiting for settlement.",
        parameters=(ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="server_rejects_settled_jobs",
        errors=_JOB_ERRORS,
    ),
)
async def job_cancel(job: uuid.UUID, /) -> CommandResult:
    """Request cancellation and return immediately after server acceptance."""
    async with invocation_context.open_asset_client() as client:
        return await jobs.cancel_job(client, job)
