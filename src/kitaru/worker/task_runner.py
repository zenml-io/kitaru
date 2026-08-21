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

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionListParams, SessionStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskResponse,
    TaskSpecResponse,
    TaskStatus,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import (
    APIError,
    AuthenticationError,
    AuthorizationError,
)
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

# A task token rejected with one of these means the attempt is dead, either
# superseded by a new claim or expired, not that the worker itself lost auth.
_STALE_TOKEN_ERRORS = (AuthenticationError, AuthorizationError)


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
        token = claimed.token.get_secret_value()
        client = self._ctx.client.with_token(token)

        running = await self._update(
            client, task.id, attempt, TaskUpdateRequest(status=TaskStatus.RUNNING)
        )
        if running is None:
            return task
        task = running
        logger.info(
            "Task %s attempt %s running the %s process.", task.id, attempt, label
        )

        handler = HANDLERS[spec.kind]
        try:
            process = await handler.prepare(self._ctx, task.id, spec, token)
        except Exception as exc:
            return await self._fail(
                client, task, attempt, f"Failed to prepare the {label} process: {exc}"
            )

        with tempfile.TemporaryDirectory(
            prefix="kitaru-task-", ignore_cleanup_errors=True
        ) as work_dir:
            result_path = Path(work_dir) / "result.json"
            process = process._replace(
                env={**process.env, "KITARU_TASK_RESULT_PATH": str(result_path)}
            )
            result = await run_task_process(process, canceled)

            if result.returncode == 0:
                return await self._succeed(
                    client, task, spec, attempt, label, result_path
                )
            if result.returncode is not None:
                return await self._fail_exit(
                    client,
                    task,
                    attempt,
                    label,
                    result.returncode,
                    result.tail,
                    result_path,
                )
            if canceled.is_set():
                logger.info("Task %s canceled by the server.", task.id)
                updated = await self._update(
                    client,
                    task.id,
                    attempt,
                    TaskUpdateRequest(status=TaskStatus.CANCELED),
                )
                return updated if updated is not None else task
            logger.info(
                "Task %s timed out after %s seconds.", task.id, spec.timeout_seconds
            )
            error = _with_tail(
                f"Task timed out after {spec.timeout_seconds} seconds.", result.tail
            )
            updated = await self._update(
                client,
                task.id,
                attempt,
                TaskUpdateRequest(status=TaskStatus.TIMED_OUT, error=error),
            )
            return updated if updated is not None else task

    async def _succeed(
        self,
        client: KitaruAPIClient,
        task: TaskResponse,
        spec: TaskSpecResponse,
        attempt: int,
        label: str,
        result_path: Path,
    ) -> TaskResponse:
        """Read the result file, if any, and complete the task.

        Args:
            client: Task-token client scoped to this task and attempt.
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
                return await self._fail(client, task, attempt, error)
            try:
                result_value = json.loads(result_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return await self._fail(
                    client,
                    task,
                    attempt,
                    f"{label} process wrote an invalid JSON result.",
                )
        return await self._complete(client, task, spec, attempt, label, result_value)

    async def _fail_exit(
        self,
        client: KitaruAPIClient,
        task: TaskResponse,
        attempt: int,
        label: str,
        returncode: int,
        tail: str,
        result_path: Path,
    ) -> TaskResponse:
        """Fail a task for a nonzero exit, attaching a readable result as diagnostic.

        Args:
            client: Task-token client scoped to this task and attempt.
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
            client,
            task.id,
            attempt,
            TaskUpdateRequest(
                status=TaskStatus.FAILED,
                error=error,
                result=result_value,
            ),
        )
        return updated if updated is not None else task

    async def _fail(
        self, client: KitaruAPIClient, task: TaskResponse, attempt: int, error: str
    ) -> TaskResponse:
        """Fail a task with an error and no result.

        Args:
            client: Task-token client scoped to this task and attempt.
            task: Task the transition is fenced by.
            attempt: Attempt the transition is fenced by.
            error: Error message.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        logger.info("Task %s failed: %s", task.id, error)
        updated = await self._update(
            client,
            task.id,
            attempt,
            TaskUpdateRequest(status=TaskStatus.FAILED, error=error),
        )
        return updated if updated is not None else task

    async def _complete(
        self,
        client: KitaruAPIClient,
        task: TaskResponse,
        spec: TaskSpecResponse,
        attempt: int,
        label: str,
        result_value: Any,
    ) -> TaskResponse:
        """Complete a task, resolving a 409 into a precise failure.

        Args:
            client: Task-token client scoped to this task and attempt.
            task: Task the transition is fenced by.
            spec: Execution spec of the task.
            attempt: Attempt the transition is fenced by.
            label: Process label used in a derived error message.
            result_value: Result read from the task's result file, if any.

        Returns:
            Task carrying the transition the runner was able to write.
        """
        try:
            updated = await client.tasks.update(
                task.id,
                TaskUpdateRequest(status=TaskStatus.COMPLETED, result=result_value),
            )
            logger.info("Task %s completed.", task.id)
            return updated
        except APIError as exc:
            if exc.status_code == 409:
                return await self._resolve_completion_conflict(
                    client, task, spec, attempt, label
                )
            if isinstance(exc, _STALE_TOKEN_ERRORS):
                logger.info(
                    "Task %s attempt %s token was rejected while completing.",
                    task.id,
                    attempt,
                )
                return task
            logger.warning("Failed to complete task %s: %s", task.id, exc)
            return task
        except httpx.TransportError as exc:
            logger.warning("Failed to complete task %s: %s", task.id, exc)
            return task

    async def _resolve_completion_conflict(
        self,
        client: KitaruAPIClient,
        task: TaskResponse,
        spec: TaskSpecResponse,
        attempt: int,
        label: str,
    ) -> TaskResponse:
        """Fetch the task after a rejected completion and fail it precisely.

        Args:
            client: Task-token client scoped to this task and attempt.
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
            current = await client.tasks.get(task.id)
        except APIError as exc:
            if isinstance(exc, _STALE_TOKEN_ERRORS):
                logger.info(
                    "Task %s attempt %s token was rejected while resolving a "
                    "completion conflict.",
                    task.id,
                    attempt,
                )
            else:
                logger.warning(
                    "Failed to fetch task %s after a completion conflict: %s",
                    task.id,
                    exc,
                )
            return task
        except httpx.TransportError as exc:
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
        error = await self._build_completion_error(client, current, spec.kind, label)
        return await self._fail(client, current, attempt, error)

    async def _build_completion_error(
        self, client: KitaruAPIClient, task: TaskResponse, kind: TaskKind, label: str
    ) -> str:
        """Build the precise error a rejected completion failed with.

        Args:
            client: Task-token client scoped to this task and attempt.
            task: Freshly fetched task, still on the attempt being reported.
            kind: Kind of the task.
            label: Process label used in the fallback error message.

        Returns:
            Error message naming the missing result session, its status when
            not completed, or a missing result.
        """
        if kind is not TaskKind.AGENT:
            return f"{label} process exited successfully without writing a result."
        params = SessionListParams(
            filter=FilterCondition(field="task_id", op=FilterOp.EQ, value=str(task.id))
        )
        try:
            page = await client.sessions.list(params)
        except (APIError, httpx.TransportError) as exc:
            logger.warning("Failed to list sessions for task %s: %s", task.id, exc)
            return f"{label} process exited successfully without writing a result."
        session = page.items[0] if page.items else None
        if session is None:
            return (
                "Agent process exited successfully without recording a result session."
            )
        if session.status is not SessionStatus.COMPLETED:
            return f"Result session {session.id} is {session.status}, not completed."
        return f"{label} process exited successfully without writing a result."

    async def _update(
        self,
        client: KitaruAPIClient,
        task_id: uuid.UUID,
        attempt: int,
        request: TaskUpdateRequest,
    ) -> TaskResponse | None:
        """Apply a status transition, logging and swallowing any failure.

        Args:
            client: Task-token client scoped to this task and attempt.
            task_id: Id of the task being transitioned.
            attempt: Attempt the transition is fenced by.
            request: Task update request.

        Returns:
            Updated task, or None when the transition was rejected or the
            request failed.
        """
        try:
            return await client.tasks.update(task_id, request)
        except APIError as exc:
            if exc.status_code == 409:
                logger.info(
                    "Task %s attempt %s is stale, skipping the %s transition.",
                    task_id,
                    attempt,
                    request.status,
                )
            elif isinstance(exc, _STALE_TOKEN_ERRORS):
                logger.info(
                    "Task %s attempt %s token was rejected, skipping the %s "
                    "transition.",
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
