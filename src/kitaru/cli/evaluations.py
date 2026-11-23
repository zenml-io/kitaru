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
"""Session evaluation and stored evaluation inspection commands."""

import re
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationResult,
)
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.task import TaskKind, TaskResponse, TaskStatus
from kitaru.cli import receipts
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.registration import (
    list_params,
    page_result,
    resolve_evaluator_configs,
)
from kitaru.cli.session_selection import select_session_ids
from kitaru.client.api_client import validate_idempotency_key
from kitaru.client.exceptions import APIError

_DEFAULT_EVALUATION_PAIR_LIMIT = 100
_PAIR_CAP_PATTERN = re.compile(r"\bthe cap is (\d+)\b")

_FAILED_TASK_STATUSES = {
    TaskStatus.FAILED,
    TaskStatus.TIMED_OUT,
    TaskStatus.ABANDONED,
}
_TERMINAL_TASK_STATUSES = {
    TaskStatus.COMPLETED,
    TaskStatus.SKIPPED,
    *_FAILED_TASK_STATUSES,
    TaskStatus.CANCELED,
}


def _parse_server_pair_cap(error: APIError) -> int | None:
    """Extract a positive server pair cap from a 422 validation error."""
    if error.status_code != 422:
        return None
    match = _PAIR_CAP_PATTERN.search(error.detail)
    if match is None:
        return None
    cap = int(match.group(1))
    return cap if cap >= 1 else None


def _split_session_batches(
    session_ids: Sequence[uuid.UUID],
    *,
    evaluator_count: int,
    pair_limit: int,
) -> list[list[uuid.UUID]]:
    """Split session IDs into chunks that fit within the pair limit."""
    if evaluator_count > pair_limit:
        raise CLIError(
            "invalid_arguments",
            f"Selected {evaluator_count} evaluators, which exceeds the server's "
            f"per-request limit of {pair_limit} session-evaluator pairs.",
            details={
                "evaluator_count": evaluator_count,
                "evaluation_pair_limit": pair_limit,
            },
            hint=(
                f"Select at most {pair_limit} evaluator(s) per command, or raise "
                "KITARU_SERVER_EVALUATION_PAIR_LIMIT on the server."
            ),
        )
    sessions_per_batch = pair_limit // evaluator_count
    return [
        list(session_ids[start : start + sessions_per_batch])
        for start in range(0, len(session_ids), sessions_per_batch)
    ]


def _get_batch_idempotency_keys(
    idempotency_key: str | None, batch_count: int
) -> list[str | None]:
    """Derive deterministic per-batch idempotency keys when splitting requests."""
    if idempotency_key is None:
        return [None] * batch_count
    if batch_count == 1:
        return [idempotency_key]
    try:
        normalized = validate_idempotency_key(idempotency_key, "--idempotency-key")
    except APIError as error:
        raise CLIError("invalid_arguments", error.detail) from error
    assert normalized is not None
    keys: list[str | None] = [
        f"{normalized}:batch:{index}" for index in range(1, batch_count + 1)
    ]
    try:
        validate_idempotency_key(keys[-1], "--idempotency-key")
    except APIError as error:
        raise CLIError(
            "invalid_arguments",
            f"--idempotency-key is too long to derive {batch_count} batch keys.",
            details={"batch_count": batch_count},
        ) from error
    return keys


def _serialize_error_cause(error: Exception) -> tuple[str, dict[str, Any]]:
    """Summarize and serialize a nested exception for structured failure details."""
    if isinstance(error, APIError):
        detail = error.detail.strip() or f"HTTP {error.status_code}"
        return detail, {"status_code": error.status_code, "detail": error.detail}
    if isinstance(error, CLIError):
        return error.message, {"kind": error.kind, "message": error.message}
    message = str(error) or type(error).__name__
    return message, {"type": type(error).__name__, "message": message}


def _get_aggregate_job_status(jobs: Sequence[JobResponse]) -> str:
    """Summarize the combined status across multiple jobs."""
    statuses = {job.status for job in jobs}
    if len(statuses) == 1:
        return next(iter(statuses)).value
    for status in (
        JobStatus.FAILED,
        JobStatus.RUNNING,
        JobStatus.PENDING,
        JobStatus.CANCELED,
        JobStatus.COMPLETED,
    ):
        if status in statuses:
            return status.value
    return jobs[0].status.value


def _build_batch_submit_partial_failure(
    *,
    submitted_jobs: Sequence[JobResponse],
    total_batches: int,
    failed_batch: int,
    remaining_session_ids: Sequence[uuid.UUID],
    error: Exception,
) -> CLIError:
    """Build a partial-failure error preserving already-submitted batch jobs."""
    cause_message, cause_details = _serialize_error_cause(error)
    job_ids = [str(job.id) for job in submitted_jobs]
    return CLIError(
        "partial_failure",
        f"Submitted {len(submitted_jobs)} of {total_batches} evaluation batches "
        f"before batch {failed_batch} failed: {cause_message}",
        details={
            "operation": "session_evaluation",
            "batch_count": total_batches,
            "submitted_batches": len(submitted_jobs),
            "failed_batch": failed_batch,
            "job_ids": job_ids,
            "jobs": [job.model_dump(mode="json") for job in submitted_jobs],
            "remaining_session_ids": [str(sid) for sid in remaining_session_ids],
            "error": cause_details,
        },
        hint=(
            f"{len(submitted_jobs)} evaluation job(s) were already created "
            f"({', '.join(job_ids)}). Inspect them with `kitaru job watch <JOB_ID>` "
            f"and re-evaluate the remaining {len(remaining_session_ids)} session(s)."
        ),
    )


def _build_batch_wait_timeout(
    *,
    submitted_jobs: Sequence[JobResponse],
    settled_jobs: Sequence[JobResponse],
    timed_out_batch: int,
    timed_out_job: JobResponse,
    details: dict[str, Any] | None,
) -> CLIError:
    """Build a wait timeout error preserving every submitted and settled batch job."""
    pending_jobs = submitted_jobs[timed_out_batch - 1 :]
    return CLIError(
        "timeout",
        f"Timed out waiting for evaluation batch {timed_out_batch} of "
        f"{len(submitted_jobs)} (job {timed_out_job.id}); remote work continues.",
        details={
            **(details or {}),
            "operation": "session_evaluation",
            "batch_count": len(submitted_jobs),
            "job_id": str(timed_out_job.id),
            "job_ids": [str(job.id) for job in submitted_jobs],
            "settled_job_ids": [str(job.id) for job in settled_jobs],
            "pending_job_ids": [str(job.id) for job in pending_jobs],
            "jobs": [
                *(job.model_dump(mode="json") for job in settled_jobs),
                *(job.model_dump(mode="json") for job in pending_jobs),
            ],
            "remote_continues": True,
        },
        hint=(
            "Start a worker with `kitaru worker start`, or keep waiting with "
            f"`kitaru job watch {timed_out_job.id}`."
        ),
    )


def _build_batch_wait_partial_failure(
    *,
    submitted_jobs: Sequence[JobResponse],
    settled_jobs: Sequence[JobResponse],
    failed_batch: int,
    failed_job: JobResponse,
    error: Exception,
) -> CLIError:
    """Build a partial-failure error when polling a submitted batch fails."""
    cause_message, cause_details = _serialize_error_cause(error)
    pending_jobs = submitted_jobs[failed_batch - 1 :]
    return CLIError(
        "partial_failure",
        f"Failed while waiting for evaluation batch {failed_batch} of "
        f"{len(submitted_jobs)} (job {failed_job.id}): {cause_message}",
        details={
            "operation": "session_evaluation",
            "batch_count": len(submitted_jobs),
            "failed_batch": failed_batch,
            "job_id": str(failed_job.id),
            "job_ids": [str(job.id) for job in submitted_jobs],
            "settled_job_ids": [str(job.id) for job in settled_jobs],
            "pending_job_ids": [str(job.id) for job in pending_jobs],
            "jobs": [
                *(job.model_dump(mode="json") for job in settled_jobs),
                *(job.model_dump(mode="json") for job in pending_jobs),
            ],
            "error": cause_details,
        },
        hint=(
            "All evaluation batches were submitted. Check remaining jobs with "
            f"`kitaru job watch {failed_job.id}`."
        ),
    )


def _task_metadata(task: TaskResponse) -> dict[str, Any]:
    """Return bounded evaluator-task diagnostics without raw result data."""
    return {
        "id": str(task.id),
        "kind": task.kind.value,
        "input_session_id": str(task.input_session_id)
        if task.input_session_id is not None
        else None,
        "evaluator_version_id": str(task.plugin_version_id)
        if task.plugin_version_id is not None
        else None,
        "status": task.status.value,
        "error": task.error,
    }


def _internal_receipt_error(
    message: str,
    job: JobResponse,
    tasks: list[TaskResponse],
) -> CLIError:
    """Build a bounded internal error for an invalid evaluator task contract."""
    return CLIError(
        "internal_error",
        message,
        details={
            "job": job.model_dump(mode="json"),
            "tasks": [_task_metadata(task) for task in tasks],
        },
    )


def _parse_task_results(
    task: TaskResponse,
    *,
    required: bool,
    job: JobResponse,
    tasks: list[TaskResponse],
) -> list[dict[str, Any]]:
    """Parse bounded evaluator results, requiring them on completed tasks."""
    if not isinstance(task.result, list) or not task.result:
        if required:
            raise _internal_receipt_error(
                "A completed evaluator task must return a nonempty result list.",
                job,
                tasks,
            )
        return []
    try:
        results = [EvaluationResult.model_validate(item) for item in task.result]
    except ValidationError as error:
        if required:
            raise _internal_receipt_error(
                "A completed evaluator task returned malformed evaluation results.",
                job,
                tasks,
            ) from error
        return []
    return [result.model_dump(mode="json") for result in results]


def _build_batch_task_entries(
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    session_ids: Sequence[uuid.UUID],
    evaluator_version_ids: Sequence[uuid.UUID],
    include_job_id: bool = False,
) -> tuple[list[dict[str, Any]], int, int, int]:
    """Validate one job's evaluator tasks and return entries plus status counts."""
    expected_pairs = {
        (session_id, evaluator_version_id)
        for session_id in session_ids
        for evaluator_version_id in evaluator_version_ids
    }
    if len(tasks) != len(expected_pairs) or any(
        task.kind is not TaskKind.EVALUATOR for task in tasks
    ):
        raise _internal_receipt_error(
            "The evaluation job returned an unexpected evaluator task set.", job, tasks
        )
    if any(task.status not in _TERMINAL_TASK_STATUSES for task in tasks):
        raise _internal_receipt_error(
            "A terminal evaluation job returned a nonterminal task.", job, tasks
        )

    observed_pairs = {(task.input_session_id, task.plugin_version_id) for task in tasks}
    if observed_pairs != expected_pairs or len(observed_pairs) != len(tasks):
        raise _internal_receipt_error(
            "Evaluator tasks did not match the requested session/version pairs.",
            job,
            tasks,
        )

    task_entries: list[dict[str, Any]] = []
    for task in tasks:
        entry: dict[str, Any] = {
            "id": str(task.id),
            "input_session_id": str(task.input_session_id),
            "evaluator_version_id": str(task.plugin_version_id),
            "status": task.status.value,
            "error": task.error,
            "results": _parse_task_results(
                task,
                required=task.status is TaskStatus.COMPLETED,
                job=job,
                tasks=tasks,
            ),
        }
        if include_job_id:
            entry["job_id"] = str(task.job_id)
        task_entries.append(entry)

    completed = sum(task.status is TaskStatus.COMPLETED for task in tasks)
    failed = sum(task.status in _FAILED_TASK_STATUSES for task in tasks)
    canceled = sum(task.status is TaskStatus.CANCELED for task in tasks)
    return task_entries, completed, failed, canceled


def _terminal_evaluation_result(
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    identity: dict[str, Any],
    session_ids: list[uuid.UUID],
    evaluator_version_ids: list[uuid.UUID],
) -> CommandResult:
    """Validate evaluator tasks and map their settled outcomes to a receipt."""
    task_entries, completed, failed, canceled = _build_batch_task_entries(
        job,
        tasks,
        session_ids=session_ids,
        evaluator_version_ids=evaluator_version_ids,
    )
    task_entries.sort(
        key=lambda item: (
            item["input_session_id"],
            item["evaluator_version_id"],
            item["id"],
        )
    )

    receipt = {
        **identity,
        "operation": "session_evaluation",
        "terminal": True,
        "job": job.model_dump(mode="json"),
        "tasks": task_entries,
        "summary": {
            "total_tasks": len(tasks),
            "completed_tasks": completed,
            "failed_tasks": failed,
            "canceled_tasks": canceled,
            "result_count": sum(len(task["results"]) for task in task_entries),
        },
    }
    next_actions = [
        receipts.get_task_filter_action("evaluation", task["id"])
        for task in task_entries
    ]

    if job.status in {JobStatus.FAILED, JobStatus.CANCELED}:
        error = receipts.terminal_job_error(job, receipt)
        error.details["next_actions"] = next_actions
        raise error
    if failed:
        emit_event("terminal", receipt)
        raise CLIError(
            "remote_failed",
            f"The evaluation completed with {failed} failed task(s).",
            details={"receipt": receipt, "next_actions": next_actions},
        )
    if job.status is not JobStatus.COMPLETED or completed != len(tasks):
        raise _internal_receipt_error(
            "A completed evaluation job must have only completed evaluator tasks.",
            job,
            tasks,
        )
    return CommandResult(
        item=receipt,
        next_actions=next_actions,
        event="terminal",
    )


def _build_created_batch_result(
    submitted_jobs: Sequence[JobResponse],
    *,
    identity: dict[str, Any],
) -> CommandResult:
    """Build an immediate receipt for multiple submitted evaluation batch jobs."""
    item = {
        **identity,
        "operation": "session_evaluation",
        "terminal": False,
        "status": _get_aggregate_job_status(submitted_jobs),
        "batch_count": len(submitted_jobs),
        "job_ids": [str(job.id) for job in submitted_jobs],
        "jobs": [job.model_dump(mode="json") for job in submitted_jobs],
    }
    next_actions = [
        action
        for job in submitted_jobs
        for action in (
            f"kitaru job watch {job.id}",
            f"kitaru job get {job.id} --tasks",
        )
    ]
    return CommandResult(
        item=item,
        event="created",
        next_actions=next_actions,
    )


def _build_terminal_batch_result(
    settled_batches: Sequence[tuple[JobResponse, list[TaskResponse]]],
    *,
    batches: Sequence[Sequence[uuid.UUID]],
    identity: dict[str, Any],
    evaluator_version_ids: Sequence[uuid.UUID],
) -> CommandResult:
    """Validate and aggregate settled outcomes across multiple evaluation jobs."""
    settled_jobs: list[JobResponse] = []
    task_entries: list[dict[str, Any]] = []
    total_tasks = 0
    total_completed = 0
    total_failed = 0
    total_canceled = 0

    for (job, tasks), batch_sessions in zip(settled_batches, batches, strict=True):
        entries, completed, failed, canceled = _build_batch_task_entries(
            job,
            tasks,
            session_ids=batch_sessions,
            evaluator_version_ids=evaluator_version_ids,
            include_job_id=True,
        )
        if (
            job.status is JobStatus.COMPLETED
            and failed == 0
            and completed != len(tasks)
        ):
            raise _internal_receipt_error(
                "A completed evaluation job must have only completed evaluator tasks.",
                job,
                tasks,
            )
        settled_jobs.append(job)
        task_entries.extend(entries)
        total_tasks += len(tasks)
        total_completed += completed
        total_failed += failed
        total_canceled += canceled

    task_entries.sort(
        key=lambda item: (
            item["input_session_id"],
            item["evaluator_version_id"],
            item["id"],
        )
    )

    completed_batches = sum(job.status is JobStatus.COMPLETED for job in settled_jobs)
    failed_batches = sum(job.status is JobStatus.FAILED for job in settled_jobs)
    canceled_batches = sum(job.status is JobStatus.CANCELED for job in settled_jobs)

    if failed_batches or total_failed:
        overall_status = JobStatus.FAILED.value
    elif canceled_batches or total_canceled:
        overall_status = JobStatus.CANCELED.value
    else:
        overall_status = JobStatus.COMPLETED.value

    receipt = {
        **identity,
        "operation": "session_evaluation",
        "terminal": True,
        "status": overall_status,
        "batch_count": len(settled_jobs),
        "job_ids": [str(job.id) for job in settled_jobs],
        "jobs": [job.model_dump(mode="json") for job in settled_jobs],
        "tasks": task_entries,
        "summary": {
            "total_batches": len(settled_jobs),
            "completed_batches": completed_batches,
            "failed_batches": failed_batches,
            "canceled_batches": canceled_batches,
            "total_tasks": total_tasks,
            "completed_tasks": total_completed,
            "failed_tasks": total_failed,
            "canceled_tasks": total_canceled,
            "result_count": sum(len(task["results"]) for task in task_entries),
        },
    }
    next_actions = [f"kitaru job get {job.id} --tasks" for job in settled_jobs]

    if failed_batches or total_failed:
        emit_event("terminal", receipt)
        raise CLIError(
            "remote_failed",
            f"{failed_batches} of {len(settled_jobs)} evaluation batch(es) failed "
            f"({total_failed} failed task(s)).",
            details={"receipt": receipt, "next_actions": next_actions},
        )
    if canceled_batches or total_canceled:
        emit_event("terminal", receipt)
        raise CLIError(
            "remote_canceled",
            f"{canceled_batches} of {len(settled_jobs)} evaluation batch(es) were "
            f"canceled ({total_canceled} canceled task(s)).",
            details={"receipt": receipt, "next_actions": next_actions},
        )
    return CommandResult(
        item=receipt,
        next_actions=next_actions,
        event="terminal",
    )


async def _submit_evaluation_batches(
    client: Any,
    session_ids: list[uuid.UUID],
    configs: list[EvaluatorConfig],
    *,
    pair_limit: int,
    allow_cap_fallback: bool,
    idempotency_key: str | None,
) -> tuple[list[list[uuid.UUID]], list[JobResponse]]:
    """Split sessions into capped batches and submit one evaluation job per batch."""
    batches = _split_session_batches(
        session_ids,
        evaluator_count=len(configs),
        pair_limit=pair_limit,
    )
    batch_keys = _get_batch_idempotency_keys(idempotency_key, len(batches))

    try:
        first_job = await client.evaluations.create(
            EvaluationBatchCreateRequest(
                input_session_ids=batches[0],
                evaluators=configs,
            ),
            idempotency_key=batch_keys[0],
        )
    except APIError as error:
        if allow_cap_fallback:
            server_cap = _parse_server_pair_cap(error)
            if (
                server_cap is not None
                and server_cap < pair_limit
                and len(batches[0]) * len(configs) > server_cap
            ):
                return await _submit_evaluation_batches(
                    client,
                    session_ids,
                    configs,
                    pair_limit=server_cap,
                    allow_cap_fallback=False,
                    idempotency_key=idempotency_key,
                )
        raise

    if len(batches) == 1:
        return batches, [first_job]

    submitted_jobs = [first_job]
    for batch_index, (batch_sessions, batch_key) in enumerate(
        zip(batches[1:], batch_keys[1:], strict=True), start=2
    ):
        try:
            job = await client.evaluations.create(
                EvaluationBatchCreateRequest(
                    input_session_ids=batch_sessions,
                    evaluators=configs,
                ),
                idempotency_key=batch_key,
            )
        except Exception as error:
            remaining_sessions = [
                sid for batch in batches[batch_index - 1 :] for sid in batch
            ]
            raise _build_batch_submit_partial_failure(
                submitted_jobs=submitted_jobs,
                total_batches=len(batches),
                failed_batch=batch_index,
                remaining_session_ids=remaining_sessions,
                error=error,
            ) from error
        submitted_jobs.append(job)
    return batches, submitted_jobs


async def _wait_for_batch_evaluations(
    client: Any,
    submitted_jobs: Sequence[JobResponse],
    *,
    interval: float,
    timeout: float,
) -> list[tuple[JobResponse, list[TaskResponse]]]:
    """Wait for every submitted batch job within one shared local wait timeout."""
    deadline = time.monotonic() + timeout
    settled_batches: list[tuple[JobResponse, list[TaskResponse]]] = []
    for batch_index, job in enumerate(submitted_jobs, start=1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _build_batch_wait_timeout(
                submitted_jobs=submitted_jobs,
                settled_jobs=[settled_job for settled_job, _ in settled_batches],
                timed_out_batch=batch_index,
                timed_out_job=job,
                details=None,
            )
        try:
            terminal_job, tasks = await receipts.wait_for_terminal_tasks(
                client,
                job.id,
                interval=interval,
                timeout=remaining,
                initial_job=job,
            )
        except CLIError as error:
            if error.kind == "timeout":
                raise _build_batch_wait_timeout(
                    submitted_jobs=submitted_jobs,
                    settled_jobs=[settled_job for settled_job, _ in settled_batches],
                    timed_out_batch=batch_index,
                    timed_out_job=job,
                    details=error.details if isinstance(error.details, dict) else None,
                ) from error
            raise
        except Exception as error:
            raise _build_batch_wait_partial_failure(
                submitted_jobs=submitted_jobs,
                settled_jobs=[settled_job for settled_job, _ in settled_batches],
                failed_batch=batch_index,
                failed_job=job,
                error=error,
            ) from error
        settled_batches.append((terminal_job, tasks))
    return settled_batches


async def evaluate_sessions(
    client: Any,
    sessions: list[str] | None,
    *,
    sessions_file: Path | None,
    tag: str | None = None,
    agent: str | None = None,
    cohort: str | None = None,
    filter: str | None = None,
    all_sessions: bool = False,
    evaluators: list[str],
    evaluator_params: list[str] | None,
    evaluator_connections: list[str] | None = None,
    wait: bool,
    interval: float | None,
    timeout: float | None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create evaluation jobs over selected session/version pairs."""
    wait_settings = receipts.get_wait_settings(
        wait=wait, interval=interval, timeout=timeout
    )
    session_ids = await select_session_ids(
        client,
        sessions,
        sessions_file,
        tag=tag,
        agent=agent,
        cohort=cohort,
        filter=filter,
        all_sessions=all_sessions,
    )
    (
        configs,
        evaluator_identity,
        evaluator_version_ids,
    ) = await resolve_evaluator_configs(
        client,
        evaluators,
        evaluator_params or [],
        evaluator_connections or [],
    )

    pair_count = len(session_ids) * len(evaluator_identity)
    server_pair_limit: int | None = None
    if pair_count > 1:
        info = await client.info.get()
        server_pair_limit = info.evaluation_pair_limit
    pair_limit = (
        server_pair_limit
        if server_pair_limit is not None
        else _DEFAULT_EVALUATION_PAIR_LIMIT
    )

    identity = {
        "session_ids": [str(session_id) for session_id in session_ids],
        "evaluators": evaluator_identity,
        "session_count": len(session_ids),
        "evaluator_count": len(evaluator_identity),
        "pair_count": pair_count,
    }
    batches, submitted_jobs = await _submit_evaluation_batches(
        client,
        session_ids,
        configs,
        pair_limit=pair_limit,
        allow_cap_fallback=server_pair_limit is None and pair_count > 1,
        idempotency_key=idempotency_key,
    )

    if len(submitted_jobs) == 1:
        job = submitted_jobs[0]
        created = receipts.created_job_result(
            "session_evaluation",
            job,
            identity=identity,
        )
        if wait_settings is None:
            return created

        emit_event("created", {**created.item, "next_actions": created.next_actions})
        terminal_job, tasks = await receipts.wait_for_terminal_tasks(
            client,
            job.id,
            interval=wait_settings[0],
            timeout=wait_settings[1],
            initial_job=job,
        )
        return _terminal_evaluation_result(
            terminal_job,
            tasks,
            identity=identity,
            session_ids=session_ids,
            evaluator_version_ids=evaluator_version_ids,
        )

    created = _build_created_batch_result(submitted_jobs, identity=identity)
    if wait_settings is None:
        return created

    emit_event("created", {**created.item, "next_actions": created.next_actions})
    settled_batches = await _wait_for_batch_evaluations(
        client,
        submitted_jobs,
        interval=wait_settings[0],
        timeout=wait_settings[1],
    )
    return _build_terminal_batch_result(
        settled_batches,
        batches=batches,
        identity=identity,
        evaluator_version_ids=evaluator_version_ids,
    )


async def list_evaluations(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one bounded server page of stored evaluations."""
    params = list_params(
        "evaluation", size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.evaluations.list(params), size=size)


async def get_evaluation(client: Any, evaluation_id: uuid.UUID) -> CommandResult:
    """Get one stored evaluation by exact UUID."""
    item = await client.evaluations.get(evaluation_id)
    return CommandResult(item=item.model_dump(mode="json"))
