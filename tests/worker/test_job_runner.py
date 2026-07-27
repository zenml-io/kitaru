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
"""Tests for the job runner's status protocol."""

import asyncio
import time
import uuid
from pathlib import Path
from typing import cast

import pytest
from fakes import (
    FakeClient,
    make_job,
    make_plugin,
    make_score_spec,
    make_session,
    make_spec,
)

from kitaru.api_models.v1.jobs import JobKind, JobSpecResponse, JobStatus
from kitaru.api_models.v1.sessions import SessionStatus
from kitaru.blob_cache import BlobCache
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import ConflictError
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.job_runner import MAX_RESULT_BYTES, JobRunner
from kitaru.worker.process import JobProcess


def make_ctx(fake: FakeClient, tmp_path: Path) -> ExecutionContext:
    """Build an execution context backed by a fake client and tmp caches."""
    return ExecutionContext(
        client=cast(KitaruAPIClient, fake),
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )


async def test_success_completes_with_no_result_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Complete a job with a null result when it wrote no result file."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id)], specs=[make_spec(job_id, command="true")]
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert final.status is JobStatus.COMPLETED
    assert fake.last_update().result is None


async def test_success_completes_with_the_result_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Attach the JSON result the process wrote to the completion call."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    command = 'echo \'{"score": 0.9}\' > "$KITARU_JOB_RESULT_PATH"'
    fake = FakeClient(
        jobs=[make_job(job_id)], specs=[make_spec(job_id, command=command)]
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.COMPLETED
    assert fake.last_update().result == {"score": 0.9}


async def test_oversized_result_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail the job when the result file exceeds the byte cap."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    command = f'head -c {MAX_RESULT_BYTES + 1} /dev/zero > "$KITARU_JOB_RESULT_PATH"'
    fake = FakeClient(
        jobs=[make_job(job_id)], specs=[make_spec(job_id, command=command)]
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error is not None
    assert "larger than" in final.error
    assert str(MAX_RESULT_BYTES) in final.error


async def test_invalid_json_result_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail the job when the result file does not parse as JSON."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    command = 'echo "not json" > "$KITARU_JOB_RESULT_PATH"'
    fake = FakeClient(
        jobs=[make_job(job_id)], specs=[make_spec(job_id, command=command)]
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error is not None
    assert "invalid JSON result" in final.error


async def test_nonzero_exit_fails_with_log_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail a job whose process exits non-zero, with the captured tail."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id)],
        specs=[make_spec(job_id, command="echo boom >&2 && exit 3")],
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error is not None
    assert "Agent process exited with code 3" in final.error
    assert "boom" in final.error


async def test_timeout_reports_timed_out_with_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Time a job out and report the elapsed timeout in the error."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id)],
        specs=[make_spec(job_id, command="sleep 30", timeout_seconds=1)],
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    started = time.monotonic()
    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert time.monotonic() - started < 10
    assert final.status is JobStatus.TIMED_OUT
    assert final.error is not None
    assert "timed out after 1 seconds" in final.error


async def test_cancel_event_outranks_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Report canceled, not timed out, when the cancel event was set."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id)],
        specs=[make_spec(job_id, command="sleep 30", timeout_seconds=30)],
    )
    runner = JobRunner(make_ctx(fake, tmp_path))
    canceled = asyncio.Event()

    async def cancel_soon() -> None:
        await asyncio.sleep(0.1)
        canceled.set()

    task = asyncio.create_task(cancel_soon())
    started = time.monotonic()
    final = await runner.execute(fake.claimed(job_id), canceled)
    await task

    assert time.monotonic() - started < 10
    assert final.status is JobStatus.CANCELED


async def test_prepare_failure_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail the job when the scorer plugin fails to materialize."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    plugin = make_plugin("0" * 64)
    spec = make_score_spec(job_id, uuid.uuid4(), plugin=plugin)
    fake = FakeClient(
        jobs=[make_job(job_id, kind=JobKind.SCORE)],
        specs=[spec],
        blob_contents={plugin.blob_id: b"tampered"},
    )
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error is not None
    assert "Failed to prepare the Scorer process" in final.error


async def test_missing_result_session_fails_with_agent_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Compose the missing-session error when the completion is rejected."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id, result_session_id=None)],
        specs=[make_spec(job_id, command="true")],
    )
    fake.update_error = ConflictError(409, "no result session")
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error == (
        "Agent process exited successfully without recording a result session."
    )


async def test_unfinished_result_session_fails_with_status_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Compose the unfinished-session error when the linked session is not done."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    session_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id, result_session_id=session_id)],
        specs=[make_spec(job_id, command="true")],
        sessions_by_id={session_id: make_session(session_id, SessionStatus.FAILED)},
    )
    fake.update_error = ConflictError(409, "session not completed")
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error == f"Result session {session_id} is failed, not completed."


class _StubHandler:
    """Handler stub always building a fixed, harmless subprocess."""

    async def prepare(
        self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
    ) -> JobProcess:
        """Build a fixed, always-succeeding process invocation."""
        _ = ctx, job_id, spec
        return JobProcess(command="true", working_dir=None, env={}, timeout_seconds=5)


async def test_score_completion_conflict_uses_the_generic_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Compose the generic no-result message for a rejected score completion."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    monkeypatch.setitem(HANDLERS, JobKind.SCORE, _StubHandler())
    job_id = uuid.uuid4()
    fake = FakeClient(
        jobs=[make_job(job_id, kind=JobKind.SCORE)],
        specs=[make_score_spec(job_id, uuid.uuid4(), plugin=None)],
    )
    fake.update_error = ConflictError(409, "missing result")
    runner = JobRunner(make_ctx(fake, tmp_path))

    final = await runner.execute(fake.claimed(job_id), asyncio.Event())

    assert final.status is JobStatus.FAILED
    assert final.error == "Scorer process exited successfully without writing a result."
