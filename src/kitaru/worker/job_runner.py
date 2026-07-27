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
"""Execution of one claimed job from spec to its next status."""

import asyncio
import json
import tempfile
import uuid
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.jobs import (
    ClaimedJobResponse,
    JobKind,
    JobResponse,
    JobStatus,
    JobUpdateRequest,
)
from kitaru.api_models.v1.sessions import SessionStatus
from kitaru.client.exceptions import ConflictError
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.process import run_job_process

MAX_RESULT_BYTES = 1024**2

# Label of the process a job kind runs, used in status protocol messages.
_PROCESS_LABELS = {
    JobKind.REPLAY: "Agent",
    JobKind.SESSION_RUN: "Agent",
    JobKind.SCORE: "Scorer",
    JobKind.IMPORT: "Importer",
}


def _with_tail(error: str, tail: str) -> str:
    """Append a log tail to an error message.

    Args:
        error: Error message.
        tail: Captured log tail.

    Returns:
        Error message with the tail, unchanged when the tail is empty.
    """
    if not tail:
        return error
    return f"{error}\n{tail}"


class JobRunner:
    """Runner of one claimed job."""

    def __init__(self, ctx: ExecutionContext) -> None:
        """Initialize the job runner.

        Args:
            ctx: Execution context.
        """
        self._ctx = ctx

    async def execute(
        self, claimed: ClaimedJobResponse, canceled: asyncio.Event
    ) -> JobResponse:
        """Execute a claimed job from its spec to its next status.

        Args:
            claimed: Claimed job and its spec.
            canceled: Event set once the job should be abandoned.

        Raises:
            APIError: A status update was rejected outside the 409 on
                the success transition.

        Returns:
            Job in the status the run produced.
        """
        job_id = claimed.job.id
        spec = claimed.spec
        label = _PROCESS_LABELS[spec.kind]
        client = self._ctx.client
        await client.jobs.update(job_id, JobUpdateRequest(status=JobStatus.RUNNING))
        try:
            process = await HANDLERS[spec.kind].prepare(self._ctx, job_id, spec)
        except Exception as exc:
            return await self._fail(
                job_id, f"Failed to prepare the {label} process: {exc}"
            )
        with tempfile.TemporaryDirectory() as tmp_dir:
            result_path = Path(tmp_dir) / "result.json"
            process.env["KITARU_JOB_RESULT_PATH"] = str(result_path)
            result = await run_job_process(process, canceled)
            if result.returncode == 0:
                return await self._succeed(job_id, label, result_path)
            if result.returncode is not None:
                error = f"{label} process exited with code {result.returncode}."
                return await self._fail(job_id, _with_tail(error, result.tail))
            if canceled.is_set():
                return await client.jobs.update(
                    job_id, JobUpdateRequest(status=JobStatus.CANCELED)
                )
            error = f"Job timed out after {process.timeout_seconds} seconds."
            return await client.jobs.update(
                job_id,
                JobUpdateRequest(
                    status=JobStatus.TIMED_OUT, error=_with_tail(error, result.tail)
                ),
            )

    async def _succeed(
        self, job_id: uuid.UUID, label: str, result_path: Path
    ) -> JobResponse:
        """Complete a job whose process exited successfully.

        Args:
            job_id: Id of the job.
            label: Process label used in error messages.
            result_path: Path the process may have written its result to.

        Returns:
            Completed or failed job, depending on the result and the
            server's validation of the completion.
        """
        result: Any = None
        if result_path.exists():
            if result_path.stat().st_size > MAX_RESULT_BYTES:
                return await self._fail(
                    job_id,
                    f"{label} process wrote a result larger than "
                    f"{MAX_RESULT_BYTES} bytes.",
                )
            try:
                result = json.loads(result_path.read_text())
            except json.JSONDecodeError:
                return await self._fail(
                    job_id, f"{label} process wrote an invalid JSON result."
                )
        try:
            return await self._ctx.client.jobs.update(
                job_id, JobUpdateRequest(status=JobStatus.COMPLETED, result=result)
            )
        except ConflictError:
            return await self._fail(job_id, await self._conflict_error(job_id, label))

    async def _conflict_error(self, job_id: uuid.UUID, label: str) -> str:
        """Build the error of a success transition the server rejected.

        Args:
            job_id: Id of the job.
            label: Process label used in the generic fallback message.

        Returns:
            Precise error message.
        """
        job = await self._ctx.client.jobs.get(job_id)
        if job.kind in (JobKind.REPLAY, JobKind.SESSION_RUN):
            if job.result_session_id is None:
                return (
                    "Agent process exited successfully without recording "
                    "a result session."
                )
            session = await self._ctx.client.sessions.get(job.result_session_id)
            if session.status is not SessionStatus.COMPLETED:
                return (
                    f"Result session {session.id} is {session.status}, not completed."
                )
        return f"{label} process exited successfully without writing a result."

    async def _fail(self, job_id: uuid.UUID, error: str) -> JobResponse:
        """Fail the job with an error message.

        Args:
            job_id: Id of the job.
            error: Error message.

        Returns:
            Failed job.
        """
        return await self._ctx.client.jobs.update(
            job_id, JobUpdateRequest(status=JobStatus.FAILED, error=error)
        )
