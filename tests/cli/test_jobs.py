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
"""Job CLI inspection, polling, cancellation, and output behavior."""

import asyncio
import io
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal

import pytest

from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.cli import app as app_module
from kitaru.cli import jobs
from kitaru.cli.output import (
    CLIError,
    OutputContext,
    emit_result,
    reset_output_context,
    set_output_context,
)
from kitaru.cli.schema import describe_schema


def _job(
    job_id: uuid.UUID,
    status: JobStatus,
    *,
    updated_offset: int = 0,
) -> JobResponse:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return JobResponse(
        id=job_id,
        owner_id=uuid.uuid5(uuid.NAMESPACE_URL, str(job_id)),
        created=now,
        updated=now + timedelta(seconds=updated_offset),
        kind=JobKind.REPLAY,
        status=status,
        started_at=now if status is not JobStatus.PENDING else None,
        ended_at=(
            now
            if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}
            else None
        ),
        error="remote failure" if status is JobStatus.FAILED else None,
    )


class StubJobs:
    """Job resource fake with sequential observations."""

    def __init__(self, responses: list[JobResponse]) -> None:
        self.responses = responses
        self.get_calls = 0
        self.cancel_calls = 0
        self.tasks = [SimpleNamespace(model_dump=lambda **_: {"id": "task-1"})]

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        assert job_id == self.responses[0].id
        index = min(self.get_calls, len(self.responses) - 1)
        self.get_calls += 1
        return self.responses[index]

    async def iter_tasks(self, job_id: uuid.UUID):
        assert job_id == self.responses[0].id
        for task in self.tasks:
            yield task

    async def cancel(self, job_id: uuid.UUID) -> JobResponse:
        assert job_id == self.responses[0].id
        self.cancel_calls += 1
        return self.responses[0]


def _output_context(
    mode: Literal["text", "json", "jsonl"], stdout: io.StringIO
) -> OutputContext:
    return OutputContext(
        command="job.watch",
        mode=mode,
        debug=False,
        traceback=False,
        stdout=stdout,
        stderr=io.StringIO(),
        rich=False,
    )


async def test_get_can_include_complete_task_snapshot() -> None:
    """The tasks flag consumes the resource iterator rather than one page."""
    job_id = uuid.uuid4()
    resource = StubJobs([_job(job_id, JobStatus.RUNNING)])

    result = await jobs.get_job(SimpleNamespace(jobs=resource), job_id, tasks=True)

    assert result.item["id"] == str(job_id)
    assert result.item["tasks"] == [{"id": "task-1"}]
    assert resource.get_calls == 1


@pytest.mark.parametrize(
    "status",
    [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED],
)
async def test_poll_returns_every_terminal_status_without_mapping(
    status: JobStatus,
) -> None:
    """The reusable poller leaves terminal outcome policy to its caller."""
    job_id = uuid.uuid4()
    terminal = _job(job_id, status)
    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        result = await jobs.poll_job(
            SimpleNamespace(jobs=StubJobs([terminal])),
            job_id,
            interval=1,
            timeout=None,
        )
    finally:
        reset_output_context(token)

    assert result is terminal
    assert stdout.getvalue() == ""


@pytest.mark.parametrize(
    ("interval", "timeout", "message"),
    [
        (0, None, "--interval must be positive and finite."),
        (float("inf"), None, "--interval must be positive and finite."),
        (1, 0, "--timeout must be positive and finite."),
        (1, float("nan"), "--timeout must be positive and finite."),
    ],
)
async def test_poll_validates_interval_and_timeout(
    interval: float, timeout: float | None, message: str
) -> None:
    """The reusable poller owns positive finite timing validation."""
    job_id = uuid.uuid4()

    with pytest.raises(CLIError, match=message) as error:
        await jobs.poll_job(
            SimpleNamespace(jobs=StubJobs([_job(job_id, JobStatus.RUNNING)])),
            job_id,
            interval=interval,
            timeout=timeout,
        )

    assert error.value.kind == "invalid_arguments"


async def test_poll_suppresses_unchanged_created_snapshot() -> None:
    """Create-command polling does not repeat its already-emitted job state."""
    job_id = uuid.uuid4()
    created = _job(job_id, JobStatus.PENDING)
    resource = StubJobs(
        [
            _job(job_id, JobStatus.PENDING, updated_offset=1),
            _job(job_id, JobStatus.RUNNING, updated_offset=2),
            _job(job_id, JobStatus.COMPLETED, updated_offset=3),
        ]
    )

    async def sleep(_: float) -> None:
        return None

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        result = await jobs.poll_job(
            SimpleNamespace(jobs=resource),
            job_id,
            interval=1,
            timeout=None,
            sleep=sleep,
            initial_job=created,
        )
    finally:
        reset_output_context(token)

    assert result.status is JobStatus.COMPLETED
    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["item"]["status"] for event in events] == [
        "running",
        "completed",
    ]


async def test_watch_jsonl_suppresses_updated_only_changes_and_emits_terminal() -> None:
    """JSONL contains changed snapshots and exactly one final terminal event."""
    job_id = uuid.uuid4()
    resource = StubJobs(
        [
            _job(job_id, JobStatus.RUNNING),
            _job(job_id, JobStatus.RUNNING, updated_offset=1),
            _job(job_id, JobStatus.COMPLETED, updated_offset=2),
        ]
    )
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    stdout = io.StringIO()
    token = set_output_context(_output_context("jsonl", stdout))
    try:
        result = await jobs.watch_job(
            SimpleNamespace(jobs=resource),
            job_id,
            interval=0.25,
            timeout=None,
            sleep=sleep,
        )
        assert emit_result(result) == 0
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == [
        "snapshot",
        "snapshot",
        "terminal",
    ]
    assert [event["item"]["status"] for event in events] == [
        "running",
        "completed",
        "completed",
    ]
    assert sleeps == [0.25, 0.25]


async def test_watch_json_emits_only_final_document() -> None:
    """Explicit JSON suppresses intermediate polling events."""
    job_id = uuid.uuid4()
    resource = StubJobs(
        [_job(job_id, JobStatus.RUNNING), _job(job_id, JobStatus.COMPLETED)]
    )

    async def sleep(_: float) -> None:
        return None

    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        result = await jobs.watch_job(
            SimpleNamespace(jobs=resource),
            job_id,
            interval=1,
            timeout=None,
            sleep=sleep,
        )
        assert emit_result(result) == 0
    finally:
        reset_output_context(token)

    documents = stdout.getvalue().splitlines()
    assert len(documents) == 1
    payload = json.loads(documents[0])
    assert payload["item"]["status"] == "completed"
    assert "event" not in payload


@pytest.mark.parametrize(
    ("status", "kind", "exit_code"),
    [
        (JobStatus.FAILED, "remote_failed", 8),
        (JobStatus.CANCELED, "remote_canceled", 9),
    ],
)
async def test_watch_maps_remote_terminal_states(
    status: JobStatus, kind: str, exit_code: int
) -> None:
    """Failed and canceled terminal observations use the stable remote exits."""
    job_id = uuid.uuid4()
    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await jobs.watch_job(
                SimpleNamespace(jobs=StubJobs([_job(job_id, status)])),
                job_id,
                interval=1,
                timeout=None,
            )
    finally:
        reset_output_context(token)
    assert error.value.kind == kind
    assert error.value.exit_code == exit_code


async def test_watch_timeout_does_not_cancel_remote_work() -> None:
    """A local deadline reports timeout while leaving the remote job alone."""
    job_id = uuid.uuid4()
    resource = StubJobs([_job(job_id, JobStatus.RUNNING)])
    now = 0.0

    def clock() -> float:
        return now

    async def sleep(delay: float) -> None:
        nonlocal now
        now += delay

    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await jobs.watch_job(
                SimpleNamespace(jobs=resource),
                job_id,
                interval=2,
                timeout=1,
                clock=clock,
                sleep=sleep,
            )
    finally:
        reset_output_context(token)

    assert error.value.kind == "timeout"
    assert error.value.exit_code == 7
    assert error.value.details["last_status"] == "running"
    assert resource.cancel_calls == 0


async def test_watch_timeout_bounds_an_in_flight_poll() -> None:
    """The local deadline cancels a slow SDK poll instead of waiting past it."""
    job_id = uuid.uuid4()

    class SlowJobs:
        async def get(self, requested: uuid.UUID) -> JobResponse:
            assert requested == job_id
            await asyncio.sleep(1)
            return _job(job_id, JobStatus.RUNNING)

    stdout = io.StringIO()
    token = set_output_context(_output_context("json", stdout))
    try:
        with pytest.raises(CLIError) as error:
            await jobs.watch_job(
                SimpleNamespace(jobs=SlowJobs()),
                job_id,
                interval=1,
                timeout=0.05,
            )
    finally:
        reset_output_context(token)
    assert error.value.kind == "timeout"
    assert error.value.details["last_status"] is None


async def test_cancel_calls_endpoint_once_and_does_not_wait() -> None:
    """Cancellation reports a request rather than claiming terminal settlement."""
    job_id = uuid.uuid4()
    resource = StubJobs([_job(job_id, JobStatus.RUNNING)])

    result = await jobs.cancel_job(SimpleNamespace(jobs=resource), job_id)

    assert resource.cancel_calls == 1
    assert resource.get_calls == 0
    assert result.item["cancellation_requested"] is True
    assert result.item["status"] == "running"
    assert result.next_actions == [f"kitaru job watch {job_id}"]


def test_watch_ctrl_c_closes_client_without_canceling(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The shared boundary returns 130 after client cleanup and no cancellation."""
    job_id = uuid.uuid4()
    resource = StubJobs([_job(job_id, JobStatus.RUNNING)])
    closed = False

    @asynccontextmanager
    async def open_client():
        nonlocal closed
        try:
            yield SimpleNamespace(jobs=resource)
        finally:
            closed = True

    async def interrupt(*args: Any, **kwargs: Any):
        del args, kwargs
        raise KeyboardInterrupt

    monkeypatch.setattr(app_module, "_open_asset_client", open_client)
    monkeypatch.setattr(jobs, "watch_job", interrupt)

    assert (
        app_module.main(
            [
                "job",
                "watch",
                str(job_id),
                "--server",
                "https://api.example.com",
                "--machine",
            ]
        )
        == 130
    )
    captured = capsys.readouterr()
    assert closed is True
    assert resource.cancel_calls == 0
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["message"] == "Interrupted."


async def test_watch_cancellation_propagates_after_client_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asyncio SIGINT cancellation is not converted into an internal error."""
    job_id = uuid.uuid4()
    started = asyncio.Event()
    closed = False

    @asynccontextmanager
    async def open_client():
        nonlocal closed
        try:
            yield SimpleNamespace()
        finally:
            closed = True

    async def wait_forever(*args: Any, **kwargs: Any):
        del args, kwargs
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "_open_asset_client", open_client)
    monkeypatch.setattr(jobs, "watch_job", wait_forever)
    task = asyncio.create_task(
        app_module._launch(
            "job",
            "watch",
            str(job_id),
            server="https://api.example.com",
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed is True


@pytest.mark.parametrize(
    ("status", "kind", "exit_code"),
    [
        (JobStatus.FAILED, "remote_failed", 8),
        (JobStatus.CANCELED, "remote_canceled", 9),
    ],
)
def test_failed_watch_uses_structured_error_envelope(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: JobStatus,
    kind: str,
    exit_code: int,
) -> None:
    """A remote terminal failure never emits an `ok: true` success document."""
    job_id = uuid.uuid4()

    @asynccontextmanager
    async def open_client():
        yield SimpleNamespace(jobs=StubJobs([_job(job_id, status)]))

    monkeypatch.setattr(app_module, "_open_asset_client", open_client)
    assert (
        app_module.main(
            [
                "job",
                "watch",
                str(job_id),
                "--server",
                "https://api.example.com",
                "--output",
                "json",
            ]
        )
        == exit_code
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["ok"] is False
    assert payload["error"]["kind"] == kind


def test_job_schema_is_offline_and_describes_streaming_and_mutation() -> None:
    """Offline metadata mirrors job output modes, exits, and side effects."""
    specs = {item["command"]: item for item in describe_schema(("job",))}

    assert set(specs) == {"job.get", "job.watch", "job.cancel"}
    assert specs["job.watch"]["streams"] is True
    assert specs["job.watch"]["output_modes"] == ["auto", "text", "json", "jsonl"]
    watch_errors = {
        error["kind"]: error["exit_code"] for error in specs["job.watch"]["errors"]
    }
    assert watch_errors["remote_failed"] == 8
    assert specs["job.cancel"]["side_effects"]["mutates_remote_state"] is True
