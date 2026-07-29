"""Worker registration, claim, and stop tests."""

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any, cast

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskResponse,
    TaskStatus,
    WorkerScope,
)
from kitaru.worker.config import WorkerConfig
from kitaru.worker.worker import Worker, _default_worker_name


def test_default_worker_name_is_sanitized(monkeypatch) -> None:
    monkeypatch.setattr("kitaru.worker.worker.socket.gethostname", lambda: "pod.a/b")
    monkeypatch.setattr("kitaru.worker.worker.os.getpid", lambda: 42)

    assert _default_worker_name() == "pod-a-b-42"


def test_runtime_detection_prefers_kubernetes(monkeypatch) -> None:
    class NamespacePath:
        def __init__(self, value) -> None:
            self.value = value

        def read_text(self, **kwargs):
            return "namespace-a\n"

    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "service")
    monkeypatch.setattr("kitaru.worker.worker.socket.gethostname", lambda: "pod-1")
    monkeypatch.setattr("kitaru.worker.worker.Path", NamespacePath)

    from kitaru.worker.worker import _detect_runtime

    runtime = _detect_runtime()

    assert runtime.platform == "kubernetes"
    assert runtime.pod == "pod-1"
    assert runtime.namespace == "namespace-a"
    assert runtime.python_version


def test_runtime_detection_distinguishes_docker_and_bare(monkeypatch) -> None:
    from kitaru.worker.worker import _detect_runtime

    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setattr("kitaru.worker.worker._is_docker", lambda: True)
    assert _detect_runtime().platform == "docker"

    monkeypatch.setattr("kitaru.worker.worker._is_docker", lambda: False)
    assert _detect_runtime().platform == "bare"


async def test_job_pinned_stop_reads_terminal_job() -> None:
    job_id = uuid.uuid4()

    class Jobs:
        async def get(self, requested):
            assert requested == job_id
            return SimpleNamespace(status=JobStatus.COMPLETED)

    ctx = SimpleNamespace(client=SimpleNamespace(jobs=Jobs()))
    worker = Worker(WorkerConfig(scope=WorkerScope(job_id=job_id)))

    assert await worker._should_stop(cast(Any, ctx), None, None)


async def test_unpinned_stop_event_and_deadline() -> None:
    worker = Worker(WorkerConfig())
    ctx = cast(Any, SimpleNamespace())
    stop = asyncio.Event()
    stop.set()

    assert await worker._should_stop(ctx, stop, None)
    assert await worker._should_stop(ctx, None, 0)


class Claims:
    def __init__(self, responses) -> None:
        self.responses = iter(responses)
        self.requests = []

    async def claim(self, request):
        self.requests.append(request)
        return next(self.responses)


class Heartbeat:
    def __init__(self) -> None:
        self.registered = []
        self.unregistered = []

    def register(self, task_id):
        self.registered.append(task_id)
        return asyncio.Event()

    def unregister(self, task_id):
        self.unregistered.append(task_id)


async def test_claim_loop_dispatches_to_capacity_and_drains(
    monkeypatch,
) -> None:
    tasks = [
        SimpleNamespace(task=make_task()),
        SimpleNamespace(task=make_task()),
    ]
    claims = Claims(
        [
            SimpleNamespace(tasks=tasks),
            SimpleNamespace(tasks=[]),
        ]
    )
    ctx = cast(
        Any,
        SimpleNamespace(
            client=SimpleNamespace(
                tasks=claims,
                jobs=SimpleNamespace(),
            )
        ),
    )
    worker = Worker(
        WorkerConfig(concurrency=2, poll_interval=0.001, claim_batch_size=5)
    )
    stop = asyncio.Event()
    stop.set()
    heartbeat = Heartbeat()
    executed = []

    async def execute(runner, heartbeat_arg, claimed):
        executed.append(claimed.task.id)
        await asyncio.sleep(0)

    monkeypatch.setattr(worker, "_execute", execute)

    await worker._claim_loop(ctx, uuid.uuid4(), cast(Any, heartbeat), stop, None)

    assert set(executed) == {task.task.id for task in tasks}
    assert [request.max_tasks for request in claims.requests] == [2, 2]


async def test_claim_loop_retries_with_exponential_backoff(
    monkeypatch,
) -> None:
    stop = asyncio.Event()
    stop.set()

    class FailingThenEmpty:
        def __init__(self) -> None:
            self.calls = 0

        async def claim(self, request):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("temporary")
            return SimpleNamespace(tasks=[])

    claims = FailingThenEmpty()
    ctx = cast(
        Any,
        SimpleNamespace(
            client=SimpleNamespace(
                tasks=claims,
                jobs=SimpleNamespace(),
            )
        ),
    )
    worker = Worker(WorkerConfig(poll_interval=0.25))
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("kitaru.worker.worker.asyncio.sleep", sleep)

    await worker._claim_loop(ctx, uuid.uuid4(), cast(Any, Heartbeat()), stop, None)

    assert claims.calls == 3
    assert sleeps == [0.25, 0.5]


async def test_claim_size_is_clamped_to_endpoint_limit() -> None:
    stop = asyncio.Event()
    stop.set()
    claims = Claims([SimpleNamespace(tasks=[])])
    ctx = cast(
        Any,
        SimpleNamespace(
            client=SimpleNamespace(
                tasks=claims,
                jobs=SimpleNamespace(),
            )
        ),
    )
    worker = Worker(WorkerConfig(concurrency=150, poll_interval=0.001))

    await worker._claim_loop(ctx, uuid.uuid4(), cast(Any, Heartbeat()), stop, None)

    assert claims.requests[0].max_tasks == 100


async def test_job_pinned_loop_claims_tasks_appended_after_empty_poll(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    initial = SimpleNamespace(task=make_task())
    appended = SimpleNamespace(task=make_task())
    claims = Claims(
        [
            SimpleNamespace(tasks=[initial]),
            SimpleNamespace(tasks=[]),
            SimpleNamespace(tasks=[appended]),
            SimpleNamespace(tasks=[]),
        ]
    )

    class Jobs:
        def __init__(self) -> None:
            self.statuses = iter([JobStatus.RUNNING, JobStatus.COMPLETED])

        async def get(self, requested):
            assert requested == job_id
            return SimpleNamespace(status=next(self.statuses))

    ctx = cast(
        Any,
        SimpleNamespace(
            client=SimpleNamespace(tasks=claims, jobs=Jobs()),
        ),
    )
    worker = Worker(
        WorkerConfig(
            scope=WorkerScope(job_id=job_id),
            concurrency=2,
            poll_interval=0.001,
        )
    )
    executed = []

    async def execute(runner, heartbeat_arg, claimed):
        executed.append(claimed.task.id)

    monkeypatch.setattr(worker, "_execute", execute)

    await worker._claim_loop(ctx, uuid.uuid4(), cast(Any, Heartbeat()), None, None)

    assert executed == [initial.task.id, appended.task.id]
    assert len(claims.requests) == 4


async def test_run_registers_scope_runtime_and_metadata(monkeypatch) -> None:
    worker_id = uuid.uuid4()
    created = []

    class Workers:
        async def create(self, request):
            created.append(request)
            return SimpleNamespace(id=worker_id)

    class Client:
        def __init__(self) -> None:
            self.workers = Workers()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    client = Client()
    monkeypatch.setattr(
        "kitaru.worker.worker.KitaruAPIClient.from_env",
        lambda: client,
    )
    scope = WorkerScope(kinds=[TaskKind.IMPORTER])
    worker = Worker(
        WorkerConfig(
            name="worker-a",
            scope=scope,
            metadata={"pool": "imports"},
            heartbeat_interval=0.01,
        )
    )

    async def claim_loop(ctx, registered_id, heartbeat, stop, deadline):
        assert ctx.client is client
        assert registered_id == worker_id

    monkeypatch.setattr(worker, "_claim_loop", claim_loop)

    await worker.run()

    assert created[0].name == "worker-a"
    assert created[0].scope == scope
    assert created[0].metadata == {"pool": "imports"}
    assert created[0].runtime.platform in {"bare", "docker", "kubernetes"}


def make_task() -> TaskResponse:
    return TaskResponse.model_construct(
        id=uuid.uuid4(),
        status=TaskStatus.CLAIMED,
    )
