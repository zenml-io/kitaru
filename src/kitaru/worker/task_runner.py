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
"""Execution of one claimed task from spec to terminal status."""

import asyncio
import json
import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskResponse,
    TaskStatus,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.client.exceptions import APIError
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.process import (
    MAX_RESULT_BYTES,
    run_task_process,
)

logger = logging.getLogger(__name__)

_PROCESS_LABELS = {
    TaskKind.AGENT: "Agent",
    TaskKind.EVALUATOR: "Evaluator",
    TaskKind.IMPORTER: "Importer",
}
_MISSING = object()


def _with_tail(error: str, tail: str) -> str:
    return f"{error}\n{tail}" if tail else error


class TaskRunner:
    """Execute one claimed task and report its status."""

    def __init__(self, ctx: ExecutionContext) -> None:
        """Initialize the runner.

        Args:
            ctx: Shared execution dependencies.
        """
        self._ctx = ctx

    async def execute(
        self,
        claimed: TaskWithSpec,
        canceled: asyncio.Event,
    ) -> TaskResponse:
        """Execute one claim under its fencing attempt.

        Args:
            claimed: Task and full execution specification.
            canceled: Server-driven cancellation event.

        Returns:
            Last task response successfully observed.
        """
        task = claimed.task
        spec = claimed.spec
        label = _PROCESS_LABELS[spec.kind]
        handler = HANDLERS[spec.kind]
        running = await self._transition(
            task.id,
            task,
            TaskUpdateRequest(
                status=TaskStatus.RUNNING,
                attempt=task.attempt,
            ),
        )
        if running.status != TaskStatus.RUNNING:
            return task

        try:
            process = await handler.prepare(self._ctx, task.id, spec)
        except Exception as exc:
            return await self._fail(
                task.id,
                running,
                task.attempt,
                f"Failed to prepare the {label} process: {exc}",
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            result_path = Path(tmp_dir) / "result.json"
            env = dict(process.env)
            env["KITARU_TASK_RESULT_PATH"] = str(result_path)
            process = process._replace(env=env)
            try:
                result = await run_task_process(process, canceled)
                if result.returncode == 0:
                    return await self._succeed(
                        task.id,
                        running,
                        task.attempt,
                        label,
                        result_path,
                    )
                if result.returncode is not None:
                    partial = self._read_optional_result(result_path)
                    error = _with_tail(
                        f"{label} process exited with code {result.returncode}.",
                        result.tail,
                    )
                    return await self._fail(
                        task.id,
                        running,
                        task.attempt,
                        error,
                        result=partial,
                    )
                if canceled.is_set():
                    return await self._transition(
                        task.id,
                        running,
                        TaskUpdateRequest(
                            status=TaskStatus.CANCELED,
                            attempt=task.attempt,
                        ),
                    )
                error = _with_tail(
                    f"Task timed out after {spec.timeout_seconds} seconds.",
                    result.tail,
                )
                return await self._transition(
                    task.id,
                    running,
                    TaskUpdateRequest(
                        status=TaskStatus.TIMED_OUT,
                        attempt=task.attempt,
                        error=error,
                    ),
                )
            except Exception:
                logger.exception(
                    "Execution of task %s attempt %s failed",
                    task.id,
                    task.attempt,
                )
                return running

    async def _succeed(
        self,
        task_id: uuid.UUID,
        current: TaskResponse,
        attempt: int,
        label: str,
        result_path: Path,
    ) -> TaskResponse:
        result: Any = _MISSING
        if result_path.exists():
            try:
                if result_path.stat().st_size > MAX_RESULT_BYTES:
                    return await self._fail(
                        task_id,
                        current,
                        attempt,
                        f"{label} process wrote a result larger than "
                        f"{MAX_RESULT_BYTES} bytes.",
                    )
                result = self._parse_result(result_path)
            except (OSError, UnicodeError, ValueError):
                return await self._fail(
                    task_id,
                    current,
                    attempt,
                    f"{label} process wrote an invalid JSON result.",
                )

        kwargs: dict[str, Any] = {
            "status": TaskStatus.COMPLETED,
            "attempt": attempt,
        }
        if result is not _MISSING:
            kwargs["result"] = result
        request = TaskUpdateRequest(**kwargs)
        try:
            return await self._ctx.client.tasks.update(task_id, request)
        except APIError as exc:
            if exc.status_code != 409:
                logger.exception(
                    "Completing task %s attempt %s failed", task_id, attempt
                )
                return current

        try:
            live = await self._ctx.client.tasks.get(task_id)
        except Exception:
            logger.exception(
                "Reading task %s after rejected completion failed", task_id
            )
            return current
        if live.attempt != attempt:
            logger.info(
                "Task %s attempt %s lost its fence to attempt %s",
                task_id,
                attempt,
                live.attempt,
            )
            return live

        if live.result_session_id is None and live.kind == TaskKind.AGENT:
            error = (
                "Agent process exited successfully without recording a result session."
            )
        elif live.result_session_id is not None:
            try:
                session = await self._ctx.client.sessions.get(live.result_session_id)
            except Exception:
                logger.exception(
                    "Reading result session %s failed", live.result_session_id
                )
                return live
            if session.status != SessionStatus.COMPLETED:
                error = (
                    f"Result session {session.id} is {session.status.value}, "
                    "not completed."
                )
            else:
                error = f"{label} process exited successfully without writing a result."
        else:
            error = f"{label} process exited successfully without writing a result."
        return await self._fail(task_id, live, attempt, error)

    async def _fail(
        self,
        task_id: uuid.UUID,
        current: TaskResponse,
        attempt: int,
        error: str,
        result: Any = _MISSING,
    ) -> TaskResponse:
        kwargs: dict[str, Any] = {
            "status": TaskStatus.FAILED,
            "attempt": attempt,
            "error": error,
        }
        if result is not _MISSING:
            kwargs["result"] = result
        return await self._transition(task_id, current, TaskUpdateRequest(**kwargs))

    async def _transition(
        self,
        task_id: uuid.UUID,
        current: TaskResponse,
        request: TaskUpdateRequest,
    ) -> TaskResponse:
        try:
            return await self._ctx.client.tasks.update(task_id, request)
        except APIError as exc:
            if exc.status_code == 409:
                logger.info(
                    "Task %s transition to %s lost its fence",
                    task_id,
                    request.status,
                )
            else:
                logger.exception(
                    "Task %s transition to %s failed",
                    task_id,
                    request.status,
                )
            return current
        except Exception:
            logger.exception("Task %s transition to %s failed", task_id, request.status)
            return current

    @staticmethod
    def _read_optional_result(path: Path) -> Any:
        try:
            if not path.exists() or path.stat().st_size > MAX_RESULT_BYTES:
                return _MISSING
            return TaskRunner._parse_result(path)
        except (OSError, UnicodeError, ValueError):
            return _MISSING

    @staticmethod
    def _parse_result(path: Path) -> Any:
        def reject_constant(value: str) -> Any:
            raise ValueError(f"Invalid JSON constant: {value}")

        return json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=reject_constant,
        )
