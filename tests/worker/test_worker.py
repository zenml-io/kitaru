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
"""Tests for the Worker lifecycle, runtime detection, and claim loop."""

import asyncio
import socket
import uuid
from pathlib import Path

import httpx
import pytest
from fakes import (
    FakeKitaruAPIClient,
    as_client,
    make_agent_spec,
    make_claimed,
    make_job_response,
    make_task,
)

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import (
    TaskClaimResponse,
    TaskKind,
    TaskStatus,
    WorkerScope,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.worker import worker as worker_module
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.worker import Worker, detect_runtime


def _ctx(tmp_path: Path, client: FakeKitaruAPIClient) -> ExecutionContext:
    return ExecutionContext(
        client=as_client(client),
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )


# --- Runtime detection ------------------------------------------------------


def test_detect_runtime_bare(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No k8s env, dockerenv file, or cgroup marker reports bare."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(worker_module, "_DOCKERENV_PATH", tmp_path / "missing")
    monkeypatch.setattr(worker_module, "_CGROUP_PATH", tmp_path / "missing-cgroup")

    runtime = detect_runtime()

    assert runtime.platform == "bare"
    assert runtime.hostname == socket.gethostname()
    assert runtime.os
    assert runtime.arch
    assert runtime.python_version


def test_detect_runtime_docker_via_dockerenv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A /.dockerenv file reports docker."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    dockerenv = tmp_path / ".dockerenv"
    dockerenv.write_text("")
    monkeypatch.setattr(worker_module, "_DOCKERENV_PATH", dockerenv)
    monkeypatch.setattr(worker_module, "_CGROUP_PATH", tmp_path / "missing-cgroup")

    runtime = detect_runtime()

    assert runtime.platform == "docker"


def test_detect_runtime_docker_via_cgroup_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A container marker in /proc/1/cgroup reports docker."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr(worker_module, "_DOCKERENV_PATH", tmp_path / "missing")
    cgroup = tmp_path / "cgroup"
    cgroup.write_text("1:name=systemd:/docker/abc123\n")
    monkeypatch.setattr(worker_module, "_CGROUP_PATH", cgroup)

    runtime = detect_runtime()

    assert runtime.platform == "docker"


def test_detect_runtime_kubernetes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """KUBERNETES_SERVICE_HOST reports kubernetes with namespace and pod."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("my-namespace\n")
    monkeypatch.setattr(worker_module, "_KUBERNETES_NAMESPACE_PATH", namespace_file)

    runtime = detect_runtime()

    assert runtime.platform == "kubernetes"
    assert runtime.namespace == "my-namespace"
    assert runtime.pod == socket.gethostname()


def test_detect_runtime_kubernetes_without_a_namespace_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A missing namespace file still reports kubernetes, with no namespace."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setattr(
        worker_module, "_KUBERNETES_NAMESPACE_PATH", tmp_path / "missing"
    )

    runtime = detect_runtime()

    assert runtime.platform == "kubernetes"
    assert runtime.namespace is None


# --- Stop condition ----------------------------------------------------------


async def test_should_stop_true_when_stop_event_is_set(tmp_path: Path) -> None:
    """The stop event alone ends an unpinned loop."""
    worker = Worker(WorkerConfig())
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())
    stop = asyncio.Event()
    stop.set()
    assert await worker._should_stop(ctx, stop, None) is True


async def test_should_stop_true_when_deadline_passed(tmp_path: Path) -> None:
    """A lifetime deadline in the past ends the loop."""
    worker = Worker(WorkerConfig())
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())
    past_deadline = asyncio.get_running_loop().time() - 1
    assert await worker._should_stop(ctx, asyncio.Event(), past_deadline) is True


async def test_should_stop_false_without_stop_deadline_or_job(
    tmp_path: Path,
) -> None:
    """An unpinned scope with nothing set never stops on its own."""
    worker = Worker(WorkerConfig())
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())
    assert await worker._should_stop(ctx, asyncio.Event(), None) is False


async def test_should_stop_true_when_pinned_job_settles(tmp_path: Path) -> None:
    """A job-pinned scope stops once the job reaches a terminal status."""
    job_id = uuid.uuid4()
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id)))
    client = FakeKitaruAPIClient()
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    ctx = _ctx(tmp_path, client)

    assert await worker._should_stop(ctx, asyncio.Event(), None) is True
    assert client.jobs.get_calls == [job_id]


async def test_should_stop_false_when_pinned_job_still_running(
    tmp_path: Path,
) -> None:
    """A job-pinned scope keeps polling while the job is not settled."""
    job_id = uuid.uuid4()
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id)))
    client = FakeKitaruAPIClient()
    client.jobs.get_responses.append(make_job_response(status=JobStatus.RUNNING))
    ctx = _ctx(tmp_path, client)

    assert await worker._should_stop(ctx, asyncio.Event(), None) is False


async def test_should_stop_propagates_a_missing_pinned_job(tmp_path: Path) -> None:
    """A 404 reading the pinned job propagates instead of being swallowed."""
    job_id = uuid.uuid4()
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id)))
    client = FakeKitaruAPIClient()
    client.jobs.get_responses.append(NotFoundError(404, "job not found"))
    ctx = _ctx(tmp_path, client)

    with pytest.raises(NotFoundError):
        await worker._should_stop(ctx, asyncio.Event(), None)


# --- Claim loop capacity, backoff, and dispatch ------------------------------


async def test_wait_for_stop_interrupts_a_long_poll() -> None:
    """A stop request wakes poll and backoff waits without waiting for timeout."""
    stop = asyncio.Event()
    waiting = asyncio.create_task(Worker._wait_for_stop(stop, 60.0))
    await asyncio.sleep(0)
    stop.set()
    assert await asyncio.wait_for(waiting, timeout=1.0) is True


async def test_claim_loop_pre_set_stop_does_not_claim(tmp_path: Path) -> None:
    """A graceful stop observed before a claim prevents new work acquisition."""
    client = FakeKitaruAPIClient()
    worker = Worker(WorkerConfig())
    ctx = _ctx(tmp_path, client)
    stop = asyncio.Event()
    stop.set()
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await worker._claim_loop(ctx, heartbeat, stop)

    assert client.tasks.claim_calls == []


async def test_lifetime_bounds_claim_error_backoff(tmp_path: Path) -> None:
    """Claim backoff cannot keep an expired worker alive."""
    client = FakeKitaruAPIClient()
    client.tasks.claim_responses.append(APIError(500, "boom"))
    worker = Worker(WorkerConfig(poll_interval=30.0, timeout=0.02))
    ctx = _ctx(tmp_path, client)
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await asyncio.wait_for(
        worker._claim_loop(ctx, heartbeat, asyncio.Event()),
        timeout=1.0,
    )

    assert len(client.tasks.claim_calls) == 1


async def test_lifetime_cancels_an_in_flight_claim(tmp_path: Path, monkeypatch) -> None:
    """A slow claim cannot outlive the configured worker lifetime."""
    client = FakeKitaruAPIClient()
    claim_canceled = asyncio.Event()

    async def slow_claim(request):
        client.tasks.claim_calls.append(request)
        try:
            await asyncio.Event().wait()
        finally:
            claim_canceled.set()

    monkeypatch.setattr(client.tasks, "claim", slow_claim)
    worker = Worker(WorkerConfig(timeout=0.02))
    ctx = _ctx(tmp_path, client)
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await asyncio.wait_for(
        worker._claim_loop(ctx, heartbeat, asyncio.Event()),
        timeout=1.0,
    )

    assert len(client.tasks.claim_calls) == 1
    assert claim_canceled.is_set()


async def test_lifetime_stops_claiming_while_at_full_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Capacity waits honor lifetime before another claim is attempted."""
    client = FakeKitaruAPIClient()
    task = make_task(kind=TaskKind.AGENT)
    client.tasks.claim_responses.append(
        TaskClaimResponse(tasks=[make_claimed(task, make_agent_spec(task.id))])
    )
    worker = Worker(WorkerConfig(concurrency=1, timeout=0.02))
    release = asyncio.Event()

    async def run_until_released(*args: object) -> None:
        del args
        await release.wait()

    monkeypatch.setattr(worker, "_run_task", run_until_released)
    ctx = _ctx(tmp_path, client)
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    async def release_after_deadline() -> None:
        await asyncio.sleep(0.05)
        release.set()

    await asyncio.gather(
        worker._claim_loop(ctx, heartbeat, asyncio.Event()),
        release_after_deadline(),
    )

    assert len(client.tasks.claim_calls) == 1


async def test_canceling_claim_loop_cancels_tasks_during_graceful_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """External cancellation cannot orphan a task already being drained."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    task = make_task(kind=TaskKind.AGENT, status=TaskStatus.CLAIMED, attempt=1)
    client.tasks.claim_responses.append(
        TaskClaimResponse(tasks=[make_claimed(task, make_agent_spec(task.id))])
    )
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id), concurrency=2))
    started = asyncio.Event()
    canceled = asyncio.Event()

    async def run_until_canceled(*args: object) -> None:
        del args
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            canceled.set()

    monkeypatch.setattr(worker, "_run_task", run_until_canceled)
    ctx = _ctx(tmp_path, client)
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )
    claim_loop = asyncio.create_task(
        worker._claim_loop(ctx, heartbeat, asyncio.Event())
    )
    await started.wait()
    await asyncio.sleep(0)
    claim_loop.cancel()

    with pytest.raises(asyncio.CancelledError):
        await claim_loop
    assert canceled.is_set()


async def test_claim_loop_full_claim_loops_again_without_sleeping(
    tmp_path: Path,
) -> None:
    """A claim matching the request size loops again immediately."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    config = WorkerConfig(
        scope=WorkerScope(job_id=job_id), concurrency=3, claim_batch_size=2
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)

    task_a = make_task(kind=TaskKind.AGENT)
    task_b = make_task(kind=TaskKind.AGENT)
    client.tasks.claim_responses.append(
        TaskClaimResponse(
            tasks=[
                make_claimed(task_a, make_agent_spec(task_a.id, command="true")),
                make_claimed(task_b, make_agent_spec(task_b.id, command="true")),
            ]
        )
    )
    for task in (task_a, task_b):
        running = task.model_copy(update={"status": TaskStatus.RUNNING})
        client.tasks.update_responses.append(running)
        client.tasks.update_responses.append(
            running.model_copy(update={"status": TaskStatus.COMPLETED})
        )
    # Second claim call is empty and the pinned job is settled, so the loop
    # ends right after without sleeping poll_interval away.
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )
    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert len(client.tasks.claim_calls) == 2
    # max_tasks is clamped by claim_batch_size, not the full free_slots.
    assert client.tasks.claim_calls[0].max_tasks == 2


async def test_claim_loop_respects_the_concurrency_bound(tmp_path: Path) -> None:
    """The claim request never asks for more than the free concurrency slots."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    config = WorkerConfig(scope=WorkerScope(job_id=job_id), concurrency=1)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert client.tasks.claim_calls[0].max_tasks == 1


async def test_claim_size_is_clamped_to_endpoint_limit(tmp_path: Path) -> None:
    """The claim request never exceeds the endpoint's max batch size."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    config = WorkerConfig(scope=WorkerScope(job_id=job_id), concurrency=150)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert client.tasks.claim_calls[0].max_tasks == worker_module._MAX_CLAIM_BATCH


async def test_claim_loop_short_claim_checks_stop_before_sleeping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A claim shorter than requested checks the stop condition first."""
    client = FakeKitaruAPIClient()
    config = WorkerConfig(concurrency=5, poll_interval=5.0)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    stop = asyncio.Event()
    original_claim = client.tasks.claim

    async def claim_then_stop(request):
        response = await original_claim(request)
        stop.set()
        return response

    monkeypatch.setattr(client.tasks, "claim", claim_then_stop)
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    # An immediate empty claim is a "short" claim (0 < 5). A stop requested
    # while that claim is in flight ends the loop without polling again.
    await asyncio.wait_for(worker._claim_loop(ctx, heartbeat, stop), timeout=1.0)

    assert len(client.tasks.claim_calls) == 1


async def test_claim_loop_backoff_doubles_and_resets_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Claim failures back off exponentially and reset after a success."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    config = WorkerConfig(
        scope=WorkerScope(job_id=job_id), concurrency=1, poll_interval=1.0
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)

    sleeps: list[float] = []

    async def fake_wait(stop: asyncio.Event, duration: float) -> bool:
        del stop
        sleeps.append(duration)
        return False

    monkeypatch.setattr(worker, "_wait_for_stop", fake_wait)

    client.tasks.claim_responses.append(APIError(500, "boom"))
    client.tasks.claim_responses.append(httpx.ConnectError("down"))
    client.tasks.claim_responses.append(APIError(500, "boom"))
    # The fourth call succeeds with an empty, short claim. The settled pinned
    # job ends the loop there instead of sleeping again.
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert sleeps == [1.0, 2.0, 4.0]
    assert len(client.tasks.claim_calls) == 4


async def test_claim_loop_backoff_caps_at_the_maximum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The backoff never exceeds CLAIM_BACKOFF_MAX_SECONDS."""
    client = FakeKitaruAPIClient()
    job_id = uuid.uuid4()
    config = WorkerConfig(
        scope=WorkerScope(job_id=job_id), concurrency=1, poll_interval=50.0
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)

    sleeps: list[float] = []

    async def fake_wait(stop: asyncio.Event, duration: float) -> bool:
        del stop
        sleeps.append(duration)
        return False

    monkeypatch.setattr(worker, "_wait_for_stop", fake_wait)

    for _ in range(3):
        client.tasks.claim_responses.append(APIError(500, "boom"))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))
    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )

    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert sleeps == [
        50.0,
        worker_module.CLAIM_BACKOFF_MAX_SECONDS,
        worker_module.CLAIM_BACKOFF_MAX_SECONDS,
    ]


async def test_job_pinned_loop_claims_tasks_appended_after_empty_poll(
    tmp_path: Path,
) -> None:
    """A job-pinned loop keeps polling and claims tasks appended mid-run."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(job_id=job_id), concurrency=2, poll_interval=0.01
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)

    initial = make_task(kind=TaskKind.AGENT, job_id=job_id)
    appended = make_task(kind=TaskKind.AGENT, job_id=job_id)
    for task in (initial, appended):
        running = task.model_copy(update={"status": TaskStatus.RUNNING})
        client.tasks.update_responses.append(running)
        client.tasks.update_responses.append(
            running.model_copy(update={"status": TaskStatus.COMPLETED})
        )
    client.tasks.claim_responses.append(
        TaskClaimResponse(
            tasks=[make_claimed(initial, make_agent_spec(initial.id, command="true"))]
        )
    )
    client.tasks.claim_responses.append(TaskClaimResponse(tasks=[]))
    client.tasks.claim_responses.append(
        TaskClaimResponse(
            tasks=[make_claimed(appended, make_agent_spec(appended.id, command="true"))]
        )
    )
    client.tasks.claim_responses.append(TaskClaimResponse(tasks=[]))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.RUNNING))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.RUNNING))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    heartbeat = worker_module.WorkerHeartbeat(
        as_client(client), uuid.uuid4(), interval=1000
    )
    await worker._claim_loop(ctx, heartbeat, asyncio.Event())

    assert len(client.tasks.claim_calls) == 4
    assert [task_id for task_id, _ in client.tasks.update_calls] == [
        initial.id,
        initial.id,
        appended.id,
        appended.id,
    ]


# --- Worker.run end-to-end with a fake client --------------------------------


async def test_worker_client_explicitly_uses_persisted_auth_and_request_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Only the worker seam opts into stored credentials and its HTTP timeout."""
    server = "http://test"
    path = tmp_path / "credentials.json"
    CredentialStore(path=path).set_api_key(server, "KITKEY_stored")
    monkeypatch.setenv("KITARU_API_URL", server)
    monkeypatch.setenv("KITARU_CREDENTIALS_PATH", str(path))
    monkeypatch.delenv("KITARU_API_KEY", raising=False)

    client = Worker(WorkerConfig(request_timeout=7.5))._build_client()
    try:
        assert client._auth is not None
        assert client._http.timeout.connect == 7.5
    finally:
        await client.close()


async def test_run_registers_and_drains_a_claimed_task(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """run() registers the worker, executes a claimed task, and stops cleanly."""
    client = FakeKitaruAPIClient()

    task = make_task(kind=TaskKind.AGENT, status=TaskStatus.CLAIMED, attempt=1)
    spec = make_agent_spec(task.id, command="true")
    client.tasks.claim_responses.append(
        TaskClaimResponse(tasks=[make_claimed(task, spec)])
    )
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    scope = WorkerScope(kinds=[TaskKind.AGENT])
    config = WorkerConfig(
        name="worker-under-test",
        scope=scope,
        metadata={"pool": "test"},
        concurrency=2,
        poll_interval=0.01,
        blob_cache_root=tmp_path / "blobs",
        payload_cache_root=tmp_path / "payloads",
    )
    worker = Worker(config)
    monkeypatch.setattr(worker, "_build_client", lambda: as_client(client))
    stop = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.2)
        stop.set()

    await asyncio.gather(worker.run(stop=stop), stop_soon())

    assert len(client.workers.created) == 1
    created = client.workers.created[0]
    assert created.name == "worker-under-test"
    assert created.scope == scope
    assert created.metadata == {"pool": "test"}
    assert created.runtime == detect_runtime()
    assert [call.status for _, call in client.tasks.update_calls] == [
        TaskStatus.RUNNING,
        TaskStatus.COMPLETED,
    ]
    assert client.closed is True
