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
"""Executes exactly one claimed task from spec to its next status."""

import asyncio
import json
import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any

import httpx

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskResponse,
    TaskSpecResponse,
    TaskStatus,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.client.exceptions import APIError
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.process import run_task_process

logger = logging.getLogger(__name__)

MAX_RESULT_BYTES = 1024 * 1024

_LABELS: dict[TaskKind, str] = {
    TaskKind.AGENT: "Agent",
    TaskKind.EVALUATOR: "Evaluator",
    TaskKind.IMPORTER: "Importer",
}


class TaskRunner:
    """Executes exactly one claimed task from spec to its next status."""

    def __init__(self, ctx: ExecutionContext) -> None:
        """Initialize the runner.

        Args:
            ctx: Execution context.
        """
        self._ctx = ctx

    async def execute(
        self, claimed: TaskWithSpec, canceled: asyncio.Event
    ) -> TaskResponse:
        """Run a claimed task and report its outcome as the next status.

        Args:
            claimed: Claimed task and its execution spec.
            canceled: Event the heartbeat sets when the server cancels the
                task.

        Returns:
            Task carrying the transition the runner was able to write, or the
            claimed task unchanged when every write attempt was abandoned.
        """
        task, spec = claimed.task, claimed.spec
        attempt = task.attempt
        label = _LABELS[spec.kind]

        running = await self._update(
            task.id,
            attempt,
            TaskUpdateRequest(status=TaskStatus.RUNNING, attempt=attempt),
        )
        if running is None:
            return task
        task = running
        logger.info(
            "Task %s attempt %s running the %s process.", task.id, attempt, label
        )

        handler = HANDLERS[spec.kind]
        try:
            process = await handler.prepare(self._ctx, task.id, spec)
        except Exception as exc:
            return await self._fail(
                task, attempt, f"Failed to prepare the {label} process: {exc}"
            )

        with tempfile.TemporaryDirectory(prefix="kitaru-task-") as work_dir:
            result_path = Path(work_dir) / "result.json"
            process = process._replace(
                env={**process.env, "KITARU_TASK_RESULT_PATH": str(result_path)}
            )
            result = await run_task_process(process, canceled)

            if result.returncode == 0:
                return await self._succeed(task, spec, attempt, label, result_path)
            if result.returncode is not None:
                return await self._fail_exit(
                    task, attempt, label, result.returncode, result.tail, result_path
                )
            if canceled.is_set():
                logger.info("Task %s canceled by the server.", task.id)
                updated = await self._update(
                    task.id,
                    attempt,
                    TaskUpdateRequest(status=TaskStatus.CANCELED, attempt=attempt),
                )
                return updated if updated is not None else task
            logger.info(
                "Task %s timed out after %s seconds.", task.id, spec.timeout_seconds
            )
            error = _with_tail(
                f"Task timed out after {spec.timeout_seconds} seconds.", result.tail
            )
            updated = await self._update(
                task.id,
                attempt,
                TaskUpdateRequest(
                    status=TaskStatus.TIMED_OUT, attempt=attempt, error=error
                ),
            )
            return updated if updated is not None else task

    async def _succeed(
        self,
        task: TaskResponse,
        spec: TaskSpecResponse,
        attempt: int,
        label: str,
        result_path: Path,
    ) -> TaskResponse:
        """Read the result file, if any, and complete the task.

        Args:
            task: Task the transition is fenced by.
            spec: Execution spec of the task.
            attempt: Attempt the transition is fenced by.
            label: Process label used in error messages.
            result_path: Path the process was told to write its result to.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        result_value: Any = None
        if result_path.exists():
            try:
                size = result_path.stat().st_size
            except OSError:
                size = 0
            if size > MAX_RESULT_BYTES:
                error = (
                    f"{label} process wrote a result larger than "
                    f"{MAX_RESULT_BYTES} bytes."
                )
                return await self._fail(task, attempt, error)
            try:
                result_value = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return await self._fail(
                    task, attempt, f"{label} process wrote an invalid JSON result."
                )
        return await self._complete(task, spec, attempt, label, result_value)

    async def _fail_exit(
        self,
        task: TaskResponse,
        attempt: int,
        label: str,
        returncode: int,
        tail: str,
        result_path: Path,
    ) -> TaskResponse:
        """Fail a task for a nonzero exit, attaching a readable result as diagnostic.

        Args:
            task: Task the transition is fenced by.
            attempt: Attempt the transition is fenced by.
            label: Process label used in the error message.
            returncode: Exit code the process returned.
            tail: Captured stdout/stderr tail.
            result_path: Path the process was told to write its result to.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        logger.info(
            "Task %s failed: %s process exited with code %s.",
            task.id,
            label,
            returncode,
        )
        error = _with_tail(f"{label} process exited with code {returncode}.", tail)
        result_value = _read_diagnostic_result(result_path)
        updated = await self._update(
            task.id,
            attempt,
            TaskUpdateRequest(
                status=TaskStatus.FAILED,
                attempt=attempt,
                error=error,
                result=result_value,
            ),
        )
        return updated if updated is not None else task

    async def _fail(self, task: TaskResponse, attempt: int, error: str) -> TaskResponse:
        """Fail a task with an error and no result.

        Args:
            task: Task the transition is fenced by.
            attempt: Attempt the transition is fenced by.
            error: Error message.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        logger.info("Task %s failed: %s", task.id, error)
        updated = await self._update(
            task.id,
            attempt,
            TaskUpdateRequest(status=TaskStatus.FAILED, attempt=attempt, error=error),
        )
        return updated if updated is not None else task

    async def _complete(
        self,
        task: TaskResponse,
        spec: TaskSpecResponse,
        attempt: int,
        label: str,
        result_value: Any,
    ) -> TaskResponse:
        """Complete a task, resolving a 409 into a precise failure.

        Args:
            task: Task the transition is fenced by.
            spec: Execution spec of the task.
            attempt: Attempt the transition is fenced by.
            label: Process label used in a derived error message.
            result_value: Result read from the task's result file, if any.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        try:
            updated = await self._ctx.client.tasks.update(
                task.id,
                TaskUpdateRequest(
                    status=TaskStatus.COMPLETED, attempt=attempt, result=result_value
                ),
            )
            logger.info("Task %s completed.", task.id)
            return updated
        except APIError as exc:
            if exc.status_code != 409:
                logger.warning("Failed to complete task %s: %s", task.id, exc)
                return task
            return await self._resolve_completion_conflict(task, spec, attempt, label)
        except httpx.TransportError as exc:
            logger.warning("Failed to complete task %s: %s", task.id, exc)
            return task

    async def _resolve_completion_conflict(
        self, task: TaskResponse, spec: TaskSpecResponse, attempt: int, label: str
    ) -> TaskResponse:
        """Fetch the task after a rejected completion and fail it precisely.

        Args:
            task: Task the transition is fenced by.
            spec: Execution spec of the task.
            attempt: Attempt the completion was fenced by.
            label: Process label used in the derived error message.

        Returns:
            Task carrying the failed transition, the refetched task when its
            attempt no longer matches, or the original task on a further
            failure.
        """
        try:
            current = await self._ctx.client.tasks.get(task.id)
        except (APIError, httpx.TransportError) as exc:
            logger.warning(
                "Failed to fetch task %s after a completion conflict: %s", task.id, exc
            )
            return task
        if current.attempt != attempt:
            logger.info(
                "Task %s attempt %s was superseded before completion, "
                "dropping the transition.",
                task.id,
                attempt,
            )
            return current
        error = await self._build_completion_error(current, spec.kind, label)
        return await self._fail(current, attempt, error)

    async def _build_completion_error(
        self, task: TaskResponse, kind: TaskKind, label: str
    ) -> str:
        """Build the precise error a rejected completion failed with.

        Args:
            task: Freshly fetched task, still on the attempt being reported.
            kind: Kind of the task.
            label: Process label used in the fallback error message.

        Returns:
            Error message naming the missing result session, its status when
            not completed, or a missing result.
        """
        if task.result_session_id is None:
            if kind is TaskKind.AGENT:
                return (
                    "Agent process exited successfully without recording "
                    "a result session."
                )
            return f"{label} process exited successfully without writing a result."
        try:
            session = await self._ctx.client.sessions.get(task.result_session_id)
        except (APIError, httpx.TransportError) as exc:
            logger.warning(
                "Failed to fetch session %s: %s", task.result_session_id, exc
            )
            return f"{label} process exited successfully without writing a result."
        if session.status is not SessionStatus.COMPLETED:
            return f"Result session {session.id} is {session.status}, not completed."
        return f"{label} process exited successfully without writing a result."

    async def _update(
        self, task_id: uuid.UUID, attempt: int, request: TaskUpdateRequest
    ) -> TaskResponse | None:
        """Apply a status transition, logging and swallowing any failure.

        Args:
            task_id: Id of the task being transitioned.
            attempt: Attempt the transition is fenced by.
            request: Task update request.

        Returns:
            Updated task, or None when the transition was rejected or the
            request failed.
        """
        try:
            return await self._ctx.client.tasks.update(task_id, request)
        except APIError as exc:
            if exc.status_code == 409:
                logger.info(
                    "Task %s attempt %s is stale, skipping the %s transition.",
                    task_id,
                    attempt,
                    request.status,
                )
            else:
                logger.warning(
                    "Failed to update task %s to %s: %s", task_id, request.status, exc
                )
            return None
        except httpx.TransportError as exc:
            logger.warning(
                "Failed to update task %s to %s: %s", task_id, request.status, exc
            )
            return None


def _read_diagnostic_result(result_path: Path) -> Any:
    """Read a result file for a nonzero exit, ignoring an unreadable file.

    Args:
        result_path: Path the process was told to write its result to.

    Returns:
        Parsed result, or None when the file is missing, too large, or does
        not parse as JSON.
    """
    if not result_path.exists():
        return None
    try:
        if result_path.stat().st_size > MAX_RESULT_BYTES:
            return None
        return json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _with_tail(message: str, tail: str) -> str:
    """Append a captured log tail to an error message.

    Args:
        message: Error message.
        tail: Captured stdout/stderr tail, appended when non-empty.

    Returns:
        Message with the tail appended, unchanged when the tail is empty.
    """
    if not tail:
        return message
    return f"{message}\n\n{tail}"
