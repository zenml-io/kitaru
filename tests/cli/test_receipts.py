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
"""Shared receipt behavior for job-backed CLI operations."""

import asyncio
import io
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Literal

import pytest

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.cli import receipts
from kitaru.cli.output import (
    CLIError,
    OutputContext,
    reset_output_context,
    set_output_context,
)


def _job(job_id: uuid.UUID, status: JobStatus) -> JobResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=job_id,
        owner_id=uuid.uuid5(uuid.NAMESPACE_URL, str(job_id)),
        created=now,
        updated=now,
        status=status,
        started_at=now if status is not JobStatus.PENDING else None,
        ended_at=(
            now
            if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
            else None
        ),
        error="remote failure" if status is JobStatus.FAILED else None,
    )


def _output_context(
    mode: Literal["text", "json", "jsonl"], stdout: io.StringIO
) -> OutputContext:
    return OutputContext(
        command="session.import",
        mode=mode,
        debug=False,
        traceback=False,
        stdout=stdout,
        stderr=io.StringIO(),
        rich=False,
    )


@pytest.mark.parametrize(
    ("wait", "interval", "timeout"),
    [
        (False, 1.0, None),
        (False, None, 1.0),
        (True, 0.0, None),
        (True, None, float("nan")),
    ],
)
def test_wait_settings_reject_invalid_create_command_flags(
    wait: bool, interval: float | None, timeout: float | None
) -> None:
    """Shared wait settings reject invalid flags before remote mutation."""
    with pytest.raises(CLIError, match=r"--interval|--timeout"):
        receipts.get_wait_settings(wait=wait, interval=interval, timeout=timeout)


def test_wait_settings_apply_finite_create_command_defaults() -> None:
    """Waited create commands share the documented finite defaults."""
    assert receipts.get_wait_settings(wait=True, interval=None, timeout=None) == (
        2.0,
        300.0,
    )
    assert receipts.get_wait_settings(wait=False, interval=None, timeout=None) is None


def test_created_job_result_builds_common_receipt_and_actions() -> None:
    """Created receipts preserve bounded identity and common recovery actions."""
    job_id = uuid.uuid4()
    job = _job(job_id, JobStatus.PENDING)

    result = receipts.created_job_result(
        "session_import",
        job,
        identity={"importer_id": "importer-1", "terminal": True},
        next_actions=["kitaru session list"],
    )

    assert result.event == "created"
    assert result.item["operation"] == "session_import"
    assert result.item["terminal"] is False
    assert result.item["job"]["id"] == str(job_id)
    assert result.item["importer_id"] == "importer-1"
    assert result.next_actions == [
        f"kitaru job watch {job_id}",
        f"kitaru job get {job_id} --tasks",
        "kitaru session list",
    ]


async def test_wait_uses_finite_defaults_then_consumes_all_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task iteration starts after settlement and consumes the complete iterator."""
    job_id = uuid.uuid4()
    created = _job(job_id, JobStatus.PENDING)
    terminal = _job(job_id, JobStatus.COMPLETED)
    settled = False
    observed_tasks = [
        SimpleNamespace(id=uuid.uuid4()),
        SimpleNamespace(id=uuid.uuid4()),
    ]

    async def poll_job(*args, **kwargs) -> JobResponse:
        nonlocal settled
        assert args[1] == job_id
        assert kwargs["interval"] == 2.0
        assert kwargs["timeout"] == 300.0
        assert kwargs["initial_job"] is created
        settled = True
        return terminal

    class StubJobs:
        async def iter_tasks(self, requested: uuid.UUID):
            assert requested == job_id
            assert settled is True
            for task in observed_tasks:
                yield task

    monkeypatch.setattr(receipts.jobs, "poll_job", poll_job)

    job, tasks = await receipts.wait_for_terminal_tasks(
        SimpleNamespace(jobs=StubJobs()), job_id, initial_job=created
    )

    assert job is terminal
    assert tasks == observed_tasks


async def test_wait_enriches_timeout_without_canceling_remote_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create-command waits explain recovery and never imply cancellation."""
    job_id = uuid.uuid4()

    async def timeout(*args, **kwargs):
        del args, kwargs
        raise CLIError(
            "timeout",
            f"Timed out waiting for job {job_id}; remote work continues.",
            details={"job_id": str(job_id), "last_status": "running"},
        )

    monkeypatch.setattr(receipts.jobs, "poll_job", timeout)

    with pytest.raises(CLIError) as error:
        await receipts.wait_for_terminal_tasks(SimpleNamespace(), job_id)

    assert error.value.kind == "timeout"
    assert error.value.details == {
        "job_id": str(job_id),
        "last_status": "running",
        "remote_continues": True,
    }
    assert error.value.hint is not None
    assert "kitaru worker start" in error.value.hint
    assert f"kitaru job watch {job_id}" in error.value.hint


async def test_wait_timeout_covers_hanging_task_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The create-command deadline remains active while fetching terminal tasks."""
    job_id = uuid.uuid4()
    terminal = _job(job_id, JobStatus.COMPLETED)

    async def poll_job(*args, **kwargs) -> JobResponse:
        del args, kwargs
        return terminal

    class StubJobs:
        async def iter_tasks(self, requested: uuid.UUID):
            assert requested == job_id
            await asyncio.Event().wait()
            yield SimpleNamespace(id=uuid.uuid4())

    monkeypatch.setattr(receipts.jobs, "poll_job", poll_job)

    with pytest.raises(CLIError) as error:
        await receipts.wait_for_terminal_tasks(
            SimpleNamespace(jobs=StubJobs()), job_id, timeout=0.01
        )

    assert error.value.kind == "timeout"
    assert error.value.details == {
        "job_id": str(job_id),
        "last_status": "completed",
        "remote_continues": True,
    }
    assert error.value.hint is not None
    assert f"kitaru job watch {job_id}" in error.value.hint


@pytest.mark.parametrize(
    ("status", "kind", "exit_code"),
    [
        (JobStatus.FAILED, "remote_failed", 8),
        (JobStatus.CANCELED, "remote_canceled", 9),
    ],
)
def test_terminal_job_error_emits_enriched_receipt(
    status: JobStatus, kind: str, exit_code: int
) -> None:
    """Remote terminal errors retain the domain receipt and lifecycle event."""
    job = _job(uuid.uuid4(), status)
    receipt = {
        "operation": "session_import",
        "terminal": True,
        "job": job.model_dump(mode="json"),
        "tasks": [{"id": "task-1", "error": job.error}],
    }
    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        error = receipts.terminal_job_error(job, receipt)
    finally:
        reset_output_context(token)

    assert error.kind == kind
    assert error.exit_code == exit_code
    assert error.details == {"receipt": receipt}
    event = json.loads(stdout.getvalue())
    assert event["event"] == "terminal"
    assert event["item"] == receipt


def test_terminal_job_error_rejects_completed_job() -> None:
    """Completed jobs cannot be mislabeled as remote failures."""
    job = _job(uuid.uuid4(), JobStatus.COMPLETED)

    with pytest.raises(ValueError, match="failed or canceled"):
        receipts.terminal_job_error(job, {"terminal": True})
