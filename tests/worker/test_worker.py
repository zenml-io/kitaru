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
import os
import socket
import uuid
from pathlib import Path
from typing import Any, NoReturn

import httpx
import pytest
from fakes import (
    FakeKitaruAPIClient,
    as_client,
    make_agent_spec,
    make_claimed,
    make_job_response,
    make_task,
    make_worker_response,
)

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.task import (
    TaskClaimResponse,
    TaskKind,
    TaskResponse,
    TaskStatus,
)
from kitaru.api_models.v1.worker import (
    WorkerClaim,
    WorkerScope,
)
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.worker import worker as worker_module
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.config import WorkerConfig
from kitaru.worker.context import ExecutionContext
from kitaru.worker.task_runner import TaskRunner
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
    worker = Worker(
        WorkerConfig(
            scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id)
        )
    )
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
    worker = Worker(
        WorkerConfig(
            scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id)
        )
    )
    client = FakeKitaruAPIClient()
    client.jobs.get_responses.append(make_job_response(status=JobStatus.RUNNING))
    ctx = _ctx(tmp_path, client)

    assert await worker._should_stop(ctx, asyncio.Event(), None) is False


async def test_should_stop_false_when_the_pinned_job_read_fails(
    tmp_path: Path,
) -> None:
    """A failed pinned-job read is logged and does not stop the loop."""
    job_id = uuid.uuid4()
    worker = Worker(
        WorkerConfig(
            scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id)
        )
    )
    client = FakeKitaruAPIClient()
    client.jobs.get_responses.append(NotFoundError(404, "job not found"))
    client.jobs.get_responses.append(httpx.ConnectError("down"))
    ctx = _ctx(tmp_path, client)

    assert await worker._should_stop(ctx, asyncio.Event(), None) is False
    assert await worker._should_stop(ctx, asyncio.Event(), None) is False


# --- Claim loop capacity, backoff, and dispatch ------------------------------


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Patch the stop-aware sleep to record durations instead of sleeping."""
    sleeps: list[float] = []

    async def fake_sleep(
        stop: asyncio.Event, duration: float, deadline: float | None
    ) -> None:
        sleeps.append(duration)

    monkeypatch.setattr(worker_module, "_sleep_until_stop", fake_sleep)
    return sleeps


async def test_claim_loop_stop_set_ends_the_loop_before_claiming(
    tmp_path: Path,
) -> None:
    """A stop set before the loop starts ends it without a single claim."""
    client = FakeKitaruAPIClient()
    worker = Worker(WorkerConfig(concurrency=1))
    ctx = _ctx(tmp_path, client)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(worker._claim_loop(ctx, stop), timeout=1.0)

    assert client.tasks.claim_calls == []


async def test_claim_loop_full_claim_loops_again_without_sleeping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A claim matching the request size loops again immediately."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=3,
        claim_batch_size=1,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    sleeps = _record_sleeps(monkeypatch)

    for _ in range(2):
        task = make_task(kind=TaskKind.AGENT, job_id=job_id)
        client.tasks.claim_responses.append(
            TaskClaimResponse(
                tasks=[make_claimed(task, make_agent_spec(task.id, command="true"))]
            )
        )
        running = task.model_copy(update={"status": TaskStatus.RUNNING})
        client.tasks.update_responses.append(running)
        client.tasks.update_responses.append(
            running.model_copy(update={"status": TaskStatus.COMPLETED})
        )
    # The third claim is empty and short, and the settled job ends the loop.
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert len(client.tasks.claim_calls) == 3
    # max_tasks is clamped by claim_batch_size, not the full free_slots.
    assert client.tasks.claim_calls[0].max_tasks == 1
    assert sleeps == []


async def test_claim_loop_stop_during_a_full_claim_stops_reclaiming(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stop set while a full claim is in flight drains it without reclaiming."""
    client = FakeKitaruAPIClient()
    config = WorkerConfig(concurrency=3, claim_batch_size=2)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    stop = asyncio.Event()

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

    real_claim = client.tasks.claim

    async def claim_and_stop(request: Any) -> TaskClaimResponse:
        result = await real_claim(request)
        stop.set()
        return result

    monkeypatch.setattr(client.tasks, "claim", claim_and_stop)

    await asyncio.wait_for(worker._claim_loop(ctx, stop), timeout=5.0)

    assert len(client.tasks.claim_calls) == 1
    assert {task_id for task_id, _ in client.tasks.update_calls} == {
        task_a.id,
        task_b.id,
    }


async def test_claim_loop_respects_the_concurrency_bound(tmp_path: Path) -> None:
    """The claim request never asks for more than the free concurrency slots."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=1,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert client.tasks.claim_calls[0].max_tasks == 1


async def test_claim_size_is_clamped_to_endpoint_limit(tmp_path: Path) -> None:
    """The claim request never exceeds the endpoint's max batch size."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=150,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert client.tasks.claim_calls[0].max_tasks == worker_module._MAX_CLAIM_BATCH


async def test_claim_loop_short_claim_checks_stop_before_sleeping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A claim shorter than requested checks the stop condition first."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=5,
        poll_interval=5.0,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    sleeps = _record_sleeps(monkeypatch)
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    # An immediate empty claim is a "short" claim (0 < 5), so with the job
    # settled the loop must end without ever sleeping poll_interval.
    await asyncio.wait_for(worker._claim_loop(ctx, asyncio.Event()), timeout=1.0)

    assert len(client.tasks.claim_calls) == 1
    assert sleeps == []


async def test_claim_loop_backoff_doubles_and_resets_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Claim failures back off exponentially and reset after a success."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=1,
        poll_interval=1.0,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    sleeps = _record_sleeps(monkeypatch)

    client.tasks.claim_responses.append(APIError(500, "boom"))
    client.tasks.claim_responses.append(APIError(500, "boom"))
    client.tasks.claim_responses.append(APIError(500, "boom"))
    # The fourth call succeeds with an empty, short claim, and the settled job
    # ends the loop there instead of sleeping again.
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert sleeps == [1.0, 2.0, 4.0]
    assert len(client.tasks.claim_calls) == 4


async def test_claim_loop_backoff_caps_at_the_maximum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The backoff never exceeds CLAIM_BACKOFF_MAX_SECONDS."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=1,
        poll_interval=50.0,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    sleeps = _record_sleeps(monkeypatch)

    for _ in range(3):
        client.tasks.claim_responses.append(APIError(500, "boom"))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert sleeps == [
        50.0,
        worker_module.CLAIM_BACKOFF_MAX_SECONDS,
        worker_module.CLAIM_BACKOFF_MAX_SECONDS,
    ]


async def test_claim_loop_backs_off_on_a_transport_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transport error from a claim backs off instead of propagating."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=1,
        poll_interval=1.0,
    )
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    sleeps = _record_sleeps(monkeypatch)

    client.tasks.claim_responses.append(httpx.ConnectError("down"))
    client.jobs.get_responses.append(make_job_response(status=JobStatus.COMPLETED))

    await worker._claim_loop(ctx, asyncio.Event())

    assert sleeps == [1.0]
    assert len(client.tasks.claim_calls) == 2


async def test_claim_loop_stop_ends_the_backoff_sleep_early(tmp_path: Path) -> None:
    """A stop during the claim error backoff ends the loop promptly."""
    client = FakeKitaruAPIClient()
    config = WorkerConfig(concurrency=1, poll_interval=30.0)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    client.tasks.claim_responses.append(APIError(500, "boom"))
    stop = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    await asyncio.wait_for(
        asyncio.gather(worker._claim_loop(ctx, stop), stop_soon()), timeout=1.0
    )

    assert len(client.tasks.claim_calls) == 1


async def test_claim_loop_ends_at_the_lifetime_deadline(tmp_path: Path) -> None:
    """The loop ends once the configured lifetime timeout passes."""
    client = FakeKitaruAPIClient()
    config = WorkerConfig(concurrency=1, poll_interval=30.0, timeout=0.05)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)

    await asyncio.wait_for(worker._claim_loop(ctx, asyncio.Event()), timeout=1.0)

    assert len(client.tasks.claim_calls) >= 1


async def test_claim_loop_drain_timeout_cancels_lingering_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tasks still running past drain_timeout get their cancel events set."""
    client = FakeKitaruAPIClient()
    config = WorkerConfig(concurrency=1, drain_timeout=0.05)
    worker = Worker(config)
    ctx = _ctx(tmp_path, client)
    stop = asyncio.Event()

    async def hang_until_canceled(ctx_: object, runner: object, claimed: Any) -> None:
        canceled = worker._inflight.register(claimed.task.id)
        try:
            await canceled.wait()
        finally:
            worker._inflight.unregister(claimed.task.id)

    monkeypatch.setattr(worker, "_run_task", hang_until_canceled)

    task = make_task(kind=TaskKind.AGENT)
    client.tasks.claim_responses.append(
        TaskClaimResponse(tasks=[make_claimed(task, make_agent_spec(task.id))])
    )

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        stop.set()

    # The gather only finishes if the drain timeout cancels the hung task.
    await asyncio.wait_for(
        asyncio.gather(worker._claim_loop(ctx, stop), stop_soon()), timeout=2.0
    )

    assert worker._inflight.get_ids() == []


def test_cancel_inflight_sets_registered_cancel_events() -> None:
    """cancel_inflight() sets the cancel event of every held task."""
    worker = Worker(WorkerConfig())
    event = worker._inflight.register(uuid.uuid4())

    worker.cancel_inflight()

    assert event.is_set()


async def test_run_task_reports_a_runner_crash_as_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash escaping the runner fails the task instead of leaving it running."""
    client = FakeKitaruAPIClient()
    worker = Worker(WorkerConfig())
    ctx = _ctx(tmp_path, client)
    task = make_task(kind=TaskKind.AGENT)
    claimed = make_claimed(task, make_agent_spec(task.id))
    client.tasks.update_responses.append(make_task(status=TaskStatus.FAILED))

    async def crash(
        self_: TaskRunner, claimed_: Any, canceled_: asyncio.Event
    ) -> TaskResponse:
        raise RuntimeError("boom")

    monkeypatch.setattr(TaskRunner, "execute", crash)

    await worker._run_task(ctx, TaskRunner(ctx), claimed)

    assert len(client.tasks.update_calls) == 1
    _, request = client.tasks.update_calls[0]
    assert request.status is TaskStatus.FAILED
    assert "boom" in request.error
    assert worker._inflight.get_ids() == []


async def test_job_pinned_loop_claims_tasks_appended_after_empty_poll(
    tmp_path: Path,
) -> None:
    """A job-pinned loop keeps polling and claims tasks appended mid-run."""
    job_id = uuid.uuid4()
    client = FakeKitaruAPIClient()
    config = WorkerConfig(
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=job_id),
        concurrency=2,
        poll_interval=0.01,
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

    await worker._claim_loop(ctx, asyncio.Event())

    assert len(client.tasks.claim_calls) >= 3
    # Both tasks consumed their scripted RUNNING and COMPLETED transitions.
    assert len(client.tasks.update_calls) == 4
    assert not client.tasks.update_responses


# --- Worker.run end-to-end with a fake client --------------------------------


async def test_run_registers_and_drains_a_claimed_task(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """run() registers the worker, executes a claimed task, and stops cleanly."""
    client = FakeKitaruAPIClient()

    def _fake_client(*args: object, **kwargs: object) -> FakeKitaruAPIClient:
        return client

    monkeypatch.setattr(worker_module, "KitaruAPIClient", _fake_client)

    task = make_task(kind=TaskKind.AGENT, status=TaskStatus.CLAIMED, attempt=1)
    spec = make_agent_spec(task.id, command="true")
    client.tasks.claim_responses.append(
        TaskClaimResponse(tasks=[make_claimed(task, spec)])
    )
    running_task = task.model_copy(update={"status": TaskStatus.RUNNING})
    completed_task = running_task.model_copy(update={"status": TaskStatus.COMPLETED})
    client.tasks.update_responses.append(running_task)
    client.tasks.update_responses.append(completed_task)

    scope = WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)])
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


async def test_run_pins_the_api_url_to_the_registration_server(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """run() exports the registration client's server URL for task processes."""
    client = FakeKitaruAPIClient()
    client.base_url = "https://stored.example.com"

    def _fake_client(*args: object, **kwargs: object) -> FakeKitaruAPIClient:
        return client

    monkeypatch.setattr(worker_module, "KitaruAPIClient", _fake_client)
    monkeypatch.delenv("KITARU_API_URL", raising=False)

    config = WorkerConfig(
        name="worker-under-test",
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        poll_interval=0.01,
        blob_cache_root=tmp_path / "blobs",
        payload_cache_root=tmp_path / "payloads",
    )
    stop = asyncio.Event()
    stop.set()

    await Worker(config).run(stop=stop)

    assert os.environ["KITARU_API_URL"] == "https://stored.example.com"


def _fail_detect_runtime() -> NoReturn:
    """Fail if the pre-registered mode calls runtime detection."""
    raise AssertionError("runtime detection must be skipped")


async def test_run_pre_registered_loads_the_worker_instead_of_registering(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A pre-registered worker id loads the worker row instead of registering."""
    client = FakeKitaruAPIClient()

    def _fake_client(*args: object, **kwargs: object) -> FakeKitaruAPIClient:
        return client

    monkeypatch.setattr(worker_module, "KitaruAPIClient", _fake_client)
    monkeypatch.setattr(worker_module, "detect_runtime", _fail_detect_runtime)

    worker_id = uuid.uuid4()
    job_id = uuid.uuid4()
    loaded_scope = WorkerScope(
        claims=[WorkerClaim(kind=TaskKind.IMPORTER)], job_id=job_id
    )
    client.workers.get_response = make_worker_response(
        id=worker_id, name="job-worker", scope=loaded_scope
    )
    client.tasks.claim_responses.append(TaskClaimResponse(tasks=[]))
    client.jobs.get_responses.append(
        make_job_response(kind=JobKind.IMPORT, status=JobStatus.COMPLETED)
    )

    config = WorkerConfig(
        id=worker_id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        poll_interval=0.01,
        blob_cache_root=tmp_path / "blobs",
        payload_cache_root=tmp_path / "payloads",
    )

    await Worker(config).run()

    assert client.workers.created == []
    assert client.workers.get_calls == [worker_id]
    assert client.jobs.get_calls == [job_id]
    assert all(
        heartbeat_worker_id == worker_id
        for heartbeat_worker_id, _ in client.workers.heartbeats
    )
    assert client.closed is True


async def test_run_pre_registered_adopts_the_stored_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A pre-registered worker replaces its configured scope with the loaded one."""
    client = FakeKitaruAPIClient()

    def _fake_client(*args: object, **kwargs: object) -> FakeKitaruAPIClient:
        return client

    monkeypatch.setattr(worker_module, "KitaruAPIClient", _fake_client)
    monkeypatch.setattr(worker_module, "detect_runtime", _fail_detect_runtime)

    worker_id = uuid.uuid4()
    job_id = uuid.uuid4()
    loaded_scope = WorkerScope(
        claims=[WorkerClaim(kind=TaskKind.IMPORTER)], job_id=job_id
    )
    client.workers.get_response = make_worker_response(
        id=worker_id, name="job-worker", scope=loaded_scope
    )
    client.tasks.claim_responses.append(TaskClaimResponse(tasks=[]))
    client.jobs.get_responses.append(
        make_job_response(kind=JobKind.IMPORT, status=JobStatus.COMPLETED)
    )

    config = WorkerConfig(
        id=worker_id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        poll_interval=0.01,
        blob_cache_root=tmp_path / "blobs",
        payload_cache_root=tmp_path / "payloads",
    )
    worker = Worker(config)

    await worker.run()

    assert worker._config.scope == loaded_scope
