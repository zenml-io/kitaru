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
"""Tests for the worker claim loop and lifecycle."""

import asyncio
import uuid

import pytest
from fakes import FakeClient, make_job, make_run, make_spec

from kitaru.api_models.v1.experiment_runs import ExperimentRunStatus
from kitaru.api_models.v1.jobs import JobKind, JobSpecResponse, JobStatus, WorkerScope
from kitaru.client.exceptions import APIError
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.process import JobProcess, ProcessResult
from kitaru.worker.worker import Worker


def patch_client(monkeypatch: pytest.MonkeyPatch, fake: FakeClient) -> None:
    """Route the worker's API client construction to a fake instance."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    monkeypatch.setattr(
        "kitaru.worker.worker.KitaruAPIClient", lambda base_url, api_key: fake
    )


def stub_successful_runs(monkeypatch: pytest.MonkeyPatch) -> list[uuid.UUID]:
    """Stub every handler and process run to succeed instantly.

    Args:
        monkeypatch: Fixture patching the handler registry and process runner.

    Returns:
        List job ids are appended to as their process is prepared.
    """
    executed: list[uuid.UUID] = []

    async def fake_run(process: JobProcess, canceled: asyncio.Event) -> ProcessResult:
        _ = process, canceled
        return ProcessResult(0, "")

    class _StubHandler:
        """Handler stub recording dispatch and building a fixed process."""

        async def prepare(
            self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
        ) -> JobProcess:
            """Record the job id and return a fixed, always-succeeding process."""
            _ = ctx, spec
            executed.append(job_id)
            return JobProcess(
                command="true", working_dir=None, env={}, timeout_seconds=5
            )

    monkeypatch.setattr("kitaru.worker.job_runner.run_job_process", fake_run)
    stub = _StubHandler()
    for kind in list(HANDLERS):
        monkeypatch.setitem(HANDLERS, kind, stub)
    return executed


async def test_registers_with_configured_name_and_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register the worker with its configured name and scope."""
    fake = FakeClient()
    patch_client(monkeypatch, fake)
    scope = WorkerScope(kinds=[JobKind.IMPORT])
    worker = Worker(WorkerConfig(name="worker-1", scope=scope))
    stop = asyncio.Event()
    stop.set()

    async with asyncio.timeout(10):
        await worker.run(stop)

    assert fake.worker_registrations[0].name == "worker-1"
    assert fake.worker_registrations[0].scope == scope


async def test_default_name_is_sanitized_hostname_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to a sanitized hostname-pid name when none is configured."""
    fake = FakeClient()
    patch_client(monkeypatch, fake)
    monkeypatch.setattr("kitaru.worker.worker.socket.gethostname", lambda: "host.local")
    worker = Worker(WorkerConfig())
    stop = asyncio.Event()
    stop.set()

    async with asyncio.timeout(10):
        await worker.run(stop)

    name = fake.worker_registrations[0].name
    assert name.startswith("host-local-")
    assert "." not in name


async def test_claims_to_capacity_up_to_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim min(free slots, batch size) jobs and dispatch one task each."""
    first_id, second_id = uuid.uuid4(), uuid.uuid4()
    first = make_job(first_id)
    second = make_job(second_id)
    fake = FakeClient(
        jobs=[first, second],
        specs=[make_spec(first_id), make_spec(second_id)],
        claim_batches=[[first, second]],
    )
    patch_client(monkeypatch, fake)
    stub_successful_runs(monkeypatch)
    worker = Worker(WorkerConfig(concurrency=2, poll_interval=0.01))
    stop = asyncio.Event()

    async with asyncio.timeout(10):
        task = asyncio.create_task(worker.run(stop))
        while len(fake.updates) < 4:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    assert fake.claim_requests[0].max_jobs == 2
    assert fake.statuses(first_id) == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert fake.statuses(second_id) == [JobStatus.RUNNING, JobStatus.COMPLETED]


async def test_unpinned_scope_stops_on_the_stop_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End the claim loop once the stop event is set after an empty claim."""
    fake = FakeClient()
    patch_client(monkeypatch, fake)
    worker = Worker(WorkerConfig(poll_interval=0.01))
    stop = asyncio.Event()

    async with asyncio.timeout(10):
        task = asyncio.create_task(worker.run(stop))
        while len(fake.claim_requests) < 2:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    assert len(fake.claim_requests) >= 2


async def test_job_scope_stops_once_the_pinned_job_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End the claim loop once the pinned job reads as terminal."""
    job_id = uuid.uuid4()
    fake = FakeClient(jobs=[make_job(job_id, status=JobStatus.COMPLETED)])
    patch_client(monkeypatch, fake)
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id), poll_interval=0.01))

    async with asyncio.timeout(10):
        await worker.run()

    assert fake.claim_requests[0].scope.job_id == job_id


async def test_run_scope_stops_once_the_pinned_run_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End the claim loop once the pinned experiment run reads as terminal."""
    run_id = uuid.uuid4()
    fake = FakeClient(run=make_run(ExperimentRunStatus.COMPLETED))
    patch_client(monkeypatch, fake)
    worker = Worker(
        WorkerConfig(scope=WorkerScope(experiment_run_id=run_id), poll_interval=0.01)
    )

    async with asyncio.timeout(10):
        await worker.run()

    assert fake.claim_requests[0].scope.experiment_run_id == run_id


async def test_lifetime_timeout_stops_an_unpinned_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End the claim loop once the lifetime deadline elapses."""
    fake = FakeClient()
    patch_client(monkeypatch, fake)
    worker = Worker(WorkerConfig(poll_interval=0.01, timeout=0.02))

    async with asyncio.timeout(10):
        await worker.run()

    assert len(fake.claim_requests) >= 1


async def test_claim_failure_backs_off_and_resets_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry a failed claim with growing backoff, resetting after success."""
    job_id = uuid.uuid4()
    job = make_job(job_id)
    fake = FakeClient(jobs=[job], specs=[make_spec(job_id)], claim_batches=[[job]])
    fake.claim_errors = [APIError(500, "boom"), APIError(500, "boom"), None]
    patch_client(monkeypatch, fake)
    stub_successful_runs(monkeypatch)
    worker = Worker(WorkerConfig(poll_interval=0.01))
    stop = asyncio.Event()

    async with asyncio.timeout(10):
        task = asyncio.create_task(worker.run(stop))
        while len(fake.claim_requests) < 3:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    assert len(fake.claim_requests) >= 3
    assert fake.statuses(job_id) == [JobStatus.RUNNING, JobStatus.COMPLETED]


async def test_graceful_stop_drains_in_flight_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wait for an in-flight job to finish before returning after stop."""
    job_id = uuid.uuid4()
    job = make_job(job_id)
    fake = FakeClient(jobs=[job], specs=[make_spec(job_id)], claim_batches=[[job]])
    patch_client(monkeypatch, fake)
    release = asyncio.Event()

    async def slow_run(process: JobProcess, canceled: asyncio.Event) -> ProcessResult:
        _ = process, canceled
        await release.wait()
        return ProcessResult(0, "")

    monkeypatch.setattr("kitaru.worker.job_runner.run_job_process", slow_run)
    worker = Worker(WorkerConfig(poll_interval=0.01))
    stop = asyncio.Event()

    async with asyncio.timeout(10):
        task = asyncio.create_task(worker.run(stop))
        while not fake.updates:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.sleep(0.05)
        assert not task.done()
        release.set()
        await task

    assert fake.statuses(job_id) == [JobStatus.RUNNING, JobStatus.COMPLETED]
