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

import uuid
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationResult,
)
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskKind, TaskResponse, TaskStatus
from kitaru.cli import receipts
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.registration import (
    list_params,
    page_result,
    resolve_evaluator_configs,
)
from kitaru.cli.session_selection import select_session_ids

_FAILED_TASK_STATUSES = {
    TaskStatus.FAILED,
    TaskStatus.TIMED_OUT,
    TaskStatus.ABANDONED,
}
_TERMINAL_TASK_STATUSES = {
    TaskStatus.COMPLETED,
    *_FAILED_TASK_STATUSES,
    TaskStatus.CANCELED,
}


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


def _terminal_evaluation_result(
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    identity: dict[str, Any],
    session_ids: list[uuid.UUID],
    evaluator_version_ids: list[uuid.UUID],
) -> CommandResult:
    """Validate evaluator tasks and map their settled outcomes to a receipt."""
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
        task_entries.append(
            {
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
        )
    task_entries.sort(
        key=lambda item: (
            item["input_session_id"],
            item["evaluator_version_id"],
            item["id"],
        )
    )

    completed = sum(task.status is TaskStatus.COMPLETED for task in tasks)
    failed = sum(task.status in _FAILED_TASK_STATUSES for task in tasks)
    canceled = sum(task.status is TaskStatus.CANCELED for task in tasks)
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
    wait: bool,
    interval: float | None,
    timeout: float | None,
) -> CommandResult:
    """Create one evaluation job over selected session/version pairs."""
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
    )

    identity = {
        "session_ids": [str(session_id) for session_id in session_ids],
        "evaluators": evaluator_identity,
        "session_count": len(session_ids),
        "evaluator_count": len(evaluator_identity),
        "pair_count": len(session_ids) * len(evaluator_identity),
    }
    request = EvaluationBatchCreateRequest(
        input_session_ids=session_ids,
        evaluators=configs,
    )
    job = await client.evaluations.create(request)
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
