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
from dataclasses import dataclass, field
from typing import Any

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskKind, TaskResponse, TaskStatus
from kitaru.cli import jobs
from kitaru.cli.output import CLIError, CommandResult, emit_event

_DEFAULT_WAIT_INTERVAL = 2.0
_DEFAULT_WAIT_TIMEOUT = 300.0
_MAX_DIAGNOSTIC_TEXT = 240
_MAX_FOLLOW_UP_TASKS = 20
_MAX_FOLLOW_UP_WARNINGS = 5
_FAILED_TASK_STATUSES = frozenset(
    {TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.ABANDONED}
)


@dataclass(frozen=True, slots=True)
class ImportFollowUpSummary:
    """Bounded receipt summaries and warnings for import follow-up tasks."""

    analyzer_tasks: list[dict[str, Any]] = field(default_factory=list)
    evaluator_tasks: list[dict[str, Any]] = field(default_factory=list)
    evaluator_summary: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)
    next_actions: list[str] = field(default_factory=list)


def truncate_diagnostic(text: str | None) -> str | None:
    """Trim and bound one diagnostic message for receipts and warnings."""
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    if len(cleaned) <= _MAX_DIAGNOSTIC_TEXT:
        return cleaned
    return f"{cleaned[: _MAX_DIAGNOSTIC_TEXT - 3].rstrip()}..."


def _is_plain_int(value: Any) -> bool:
    """Return whether a value is an integer and not a boolean."""
    return isinstance(value, int) and not isinstance(value, bool)


def _get_plugin_ref_map(entries: Any) -> dict[str, str]:
    """Map resolved plugin version IDs to formatted NAME@VERSION labels."""
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        return {}
    refs: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        version_id = entry.get("version_id")
        name = entry.get("name")
        if not isinstance(version_id, str) or not isinstance(name, str) or not name:
            continue
        version = entry.get("version")
        refs[version_id] = f"{name}@{version}" if version is not None else name
    return refs


def _resolve_plugin_ref(
    task: TaskResponse,
    refs_by_version_id: Mapping[str, str],
    *,
    sole_fallback: str | None,
) -> str | None:
    """Resolve the display plugin reference for one task."""
    if task.plugin_version_id is not None:
        return refs_by_version_id.get(str(task.plugin_version_id))
    return sole_fallback


def _get_skip_reason(task: TaskResponse) -> str | None:
    """Extract a bounded human-readable skip reason from a skipped task."""
    if isinstance(task.result, Mapping):
        eligible = task.result.get("eligible_sessions")
        minimum = task.result.get("min_sessions")
        if _is_plain_int(eligible) and _is_plain_int(minimum):
            noun = "session" if eligible == 1 else "sessions"
            return f"{eligible} eligible {noun}, needs at least {minimum}"
        for key in ("message", "reason"):
            value = task.result.get(key)
            if isinstance(value, str):
                bounded = truncate_diagnostic(value)
                if bounded is not None:
                    return bounded
    return truncate_diagnostic(task.error)


def _get_analyzer_outcome_counts(
    task: TaskResponse,
) -> tuple[int | None, str | None]:
    """Extract insight count and optional empty reason from a completed analyzer."""
    if task.status is not TaskStatus.COMPLETED:
        return None, None
    if isinstance(task.result, list):
        return len(task.result), None
    if isinstance(task.result, Mapping):
        insight_count: int | None = None
        insights = task.result.get("insights")
        raw_count = task.result.get("insight_count")
        if isinstance(insights, list):
            insight_count = len(insights)
        elif _is_plain_int(raw_count):
            insight_count = raw_count
        reason: str | None = None
        for key in ("reason", "empty_reason", "message"):
            value = task.result.get(key)
            if isinstance(value, str):
                bounded = truncate_diagnostic(value)
                if bounded is not None:
                    reason = bounded
                    break
        return insight_count, reason
    return None, None


def _format_failure_status(status: TaskStatus) -> str:
    """Format a hard-failure task status as a verb phrase."""
    if status is TaskStatus.TIMED_OUT:
        return "timed out"
    if status is TaskStatus.ABANDONED:
        return "was abandoned"
    return "failed"


def _get_task_sort_priority(status: TaskStatus) -> int:
    """Order non-completed tasks ahead of completed ones when truncating."""
    if status in _FAILED_TASK_STATUSES:
        return 0
    if status is TaskStatus.CANCELED:
        return 1
    if status is TaskStatus.SKIPPED:
        return 2
    return 3


def _summarize_analyzer_task(
    task: TaskResponse,
    *,
    plugin_ref: str | None,
) -> tuple[dict[str, Any], str | None, str | None]:
    """Build a bounded summary, warning, and action for one analyzer task."""
    error_text = truncate_diagnostic(task.error)
    entry: dict[str, Any] = {
        "id": str(task.id),
        "status": task.status.value,
        "error": error_text,
    }
    if plugin_ref is not None:
        entry["analyzer"] = plugin_ref
    if task.plugin_version_id is not None:
        entry["analyzer_version_id"] = str(task.plugin_version_id)

    label = plugin_ref or f"task {task.id}"
    warning: str | None = None
    next_action: str | None = None

    if task.status is TaskStatus.SKIPPED:
        skip_reason = _get_skip_reason(task)
        if skip_reason is not None:
            entry["skip_reason"] = skip_reason
            warning = f"Analyzer {label} was skipped: {skip_reason}."
        else:
            warning = f"Analyzer {label} was skipped."
        if isinstance(task.result, Mapping):
            eligible = task.result.get("eligible_sessions")
            minimum = task.result.get("min_sessions")
            if _is_plain_int(eligible) and _is_plain_int(minimum):
                entry["eligible_sessions"] = eligible
                entry["min_sessions"] = minimum
    elif task.status in _FAILED_TASK_STATUSES:
        verb = _format_failure_status(task.status)
        if error_text is not None:
            warning = f"Analyzer {label} {verb}: {error_text}"
        else:
            warning = f"Analyzer {label} {verb}."
    elif task.status is TaskStatus.CANCELED:
        if error_text is not None:
            warning = f"Analyzer {label} was canceled: {error_text}"
        else:
            warning = f"Analyzer {label} was canceled."
    elif task.status is TaskStatus.COMPLETED:
        insight_count, empty_reason = _get_analyzer_outcome_counts(task)
        if insight_count is not None:
            entry["insight_count"] = insight_count
        if empty_reason is not None:
            entry["reason"] = empty_reason
        if insight_count == 0:
            if empty_reason is not None:
                warning = (
                    f"Analyzer {label} completed with 0 insights ({empty_reason})."
                )
            else:
                warning = f"Analyzer {label} completed with 0 insights."
        elif insight_count is not None and insight_count > 0:
            next_action = get_task_filter_action("insight", task.id)

    return entry, warning, next_action


def _summarize_evaluator_task(
    task: TaskResponse,
    *,
    plugin_ref: str | None,
) -> tuple[dict[str, Any], str | None]:
    """Build a bounded summary and optional warning for one evaluator task."""
    error_text = truncate_diagnostic(task.error)
    entry: dict[str, Any] = {
        "id": str(task.id),
        "status": task.status.value,
        "error": error_text,
    }
    if plugin_ref is not None:
        entry["evaluator"] = plugin_ref
    if task.plugin_version_id is not None:
        entry["evaluator_version_id"] = str(task.plugin_version_id)
    if task.input_session_id is not None:
        entry["input_session_id"] = str(task.input_session_id)

    label = plugin_ref or f"task {task.id}"
    target = (
        f"{label} (session {task.input_session_id})"
        if task.input_session_id is not None
        else label
    )
    warning: str | None = None

    if task.status is TaskStatus.SKIPPED:
        skip_reason = _get_skip_reason(task)
        if skip_reason is not None:
            entry["skip_reason"] = skip_reason
            warning = f"Evaluator {target} was skipped: {skip_reason}."
        else:
            warning = f"Evaluator {target} was skipped."
    elif task.status in _FAILED_TASK_STATUSES:
        verb = _format_failure_status(task.status)
        if error_text is not None:
            warning = f"Evaluator {target} {verb}: {error_text}"
        else:
            warning = f"Evaluator {target} {verb}."
    elif task.status is TaskStatus.CANCELED:
        if error_text is not None:
            warning = f"Evaluator {target} was canceled: {error_text}"
        else:
            warning = f"Evaluator {target} was canceled."
    elif task.status is TaskStatus.COMPLETED and isinstance(task.result, list):
        entry["evaluation_count"] = len(task.result)

    return entry, warning


def summarize_import_follow_up_tasks(
    tasks: Sequence[TaskResponse],
    *,
    identity: Mapping[str, Any],
) -> ImportFollowUpSummary:
    """Summarize analyzer and evaluator follow-up tasks from an import job.

    Args:
        tasks: All tasks belonging to the import job.
        identity: Bounded request identity carrying resolved analyzer and
            evaluator metadata.

    Returns:
        Bounded task summaries, warnings, and follow-up CLI actions.
    """
    raw_analyzers = [task for task in tasks if task.kind is TaskKind.ANALYZER]
    raw_evaluators = [task for task in tasks if task.kind is TaskKind.EVALUATOR]

    analyzer_refs = _get_plugin_ref_map(identity.get("analyzers"))
    sole_analyzer_ref = (
        next(iter(analyzer_refs.values()))
        if len(raw_analyzers) == 1 and len(analyzer_refs) == 1
        else None
    )
    evaluator_refs = _get_plugin_ref_map(identity.get("evaluators"))
    sole_evaluator_ref = (
        next(iter(evaluator_refs.values())) if len(evaluator_refs) == 1 else None
    )

    analyzer_entries: list[dict[str, Any]] = []
    warnings: list[str] = []
    next_actions: list[str] = []
    for task in raw_analyzers:
        plugin_ref = _resolve_plugin_ref(
            task, analyzer_refs, sole_fallback=sole_analyzer_ref
        )
        entry, warning, action = _summarize_analyzer_task(task, plugin_ref=plugin_ref)
        analyzer_entries.append(entry)
        if warning is not None:
            warnings.append(warning)
        if action is not None:
            next_actions.append(action)

    evaluator_pairs: list[
        tuple[tuple[int, str, str, str], dict[str, Any], str | None]
    ] = []
    completed_evaluators: list[TaskResponse] = []
    skipped_evaluators = 0
    failed_evaluators = 0
    canceled_evaluators = 0
    total_evaluations = 0
    for task in raw_evaluators:
        plugin_ref = _resolve_plugin_ref(
            task, evaluator_refs, sole_fallback=sole_evaluator_ref
        )
        entry, warning = _summarize_evaluator_task(task, plugin_ref=plugin_ref)
        sort_key = (
            _get_task_sort_priority(task.status),
            plugin_ref or "",
            str(task.input_session_id or ""),
            str(task.id),
        )
        evaluator_pairs.append((sort_key, entry, warning))
        if task.status is TaskStatus.COMPLETED:
            completed_evaluators.append(task)
            total_evaluations += int(entry.get("evaluation_count", 0))
        elif task.status is TaskStatus.SKIPPED:
            skipped_evaluators += 1
        elif task.status in _FAILED_TASK_STATUSES:
            failed_evaluators += 1
        elif task.status is TaskStatus.CANCELED:
            canceled_evaluators += 1

    evaluator_pairs.sort(key=lambda item: item[0])
    evaluator_entries = [
        entry for _, entry, _ in evaluator_pairs[:_MAX_FOLLOW_UP_TASKS]
    ]
    evaluator_warnings = [
        warning for _, _, warning in evaluator_pairs if warning is not None
    ]
    if len(evaluator_warnings) > _MAX_FOLLOW_UP_WARNINGS:
        parts = []
        if failed_evaluators:
            parts.append(f"{failed_evaluators} failed")
        if canceled_evaluators:
            parts.append(f"{canceled_evaluators} canceled")
        if skipped_evaluators:
            parts.append(f"{skipped_evaluators} skipped")
        breakdown = ", ".join(parts)
        warnings.append(
            f"{len(evaluator_warnings)} evaluator task(s) did not complete "
            f"({breakdown})."
        )
        warnings.extend(evaluator_warnings[:_MAX_FOLLOW_UP_WARNINGS])
    else:
        warnings.extend(evaluator_warnings)

    evaluator_summary: dict[str, Any] | None = None
    if raw_evaluators:
        evaluator_summary = {
            "total_tasks": len(raw_evaluators),
            "completed_tasks": len(completed_evaluators),
            "skipped_tasks": skipped_evaluators,
            "failed_tasks": failed_evaluators,
            "canceled_tasks": canceled_evaluators,
            "evaluation_count": total_evaluations,
            "truncated": len(raw_evaluators) > _MAX_FOLLOW_UP_TASKS,
        }
        if len(completed_evaluators) == 1:
            next_actions.append(
                get_task_filter_action("evaluation", completed_evaluators[0].id)
            )
        elif completed_evaluators:
            next_actions.append("kitaru evaluation list")

    return ImportFollowUpSummary(
        analyzer_tasks=analyzer_entries[:_MAX_FOLLOW_UP_TASKS],
        evaluator_tasks=evaluator_entries,
        evaluator_summary=evaluator_summary,
        warnings=warnings,
        next_actions=next_actions,
    )


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
            details=details,
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
    details: dict[str, Any] | None = None,
) -> CLIError:
    """Build a recoverable timeout covering polling and task collection."""
    return CLIError(
        "timeout",
        message or f"Timed out waiting for job {job_id}; remote work continues.",
        details={
            **(details or {}),
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
