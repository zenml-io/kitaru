"""Execution service and helper tests."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

from pydantic import SecretStr

from kitaru.api_models.v1.task import TaskKind as WireTaskKind
from kitaru.api_models.v1.task import WorkerScope
from kitaru.server.application.evaluation_recording import (
    record_task_evaluations,
)
from kitaru.server.application.events import (
    EventRegistry,
    JobSettled,
    ReplaySettled,
    TaskTerminal,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskUpdate
from kitaru.server.application.replay_pipeline import create_replay_pipeline
from kitaru.server.application.run_finalization import finalize_run_if_drained
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunStatus,
)
from kitaru.server.domain.job import Job, JobStatus
from kitaru.server.domain.replay import Replay, ReplayStatus
from kitaru.server.domain.replay_config import EvaluatorConfig
from kitaru.server.domain.secret import Secret
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.task import (
    AgentTask,
    AgentTaskDetails,
    EvaluationTask,
    TaskOnFailure,
    TaskStatus,
)
from kitaru.server.domain.worker import WorkerRuntime


def actor() -> AuthContext:
    return AuthContext(account=Account(name="owner"))


class EntityRepository:
    def __init__(self, entities=()) -> None:
        self.entities = {entity.id: entity for entity in entities}
        self.updated = []
        self.created = []

    async def get(self, entity_id, exclusive=False):
        return self.entities[entity_id]

    async def create(self, entity):
        self.entities[entity.id] = entity
        self.created.append(entity)
        return entity

    async def update(self, entity, expected_attempt=None):
        self.entities[entity.id] = entity
        self.updated.append((entity, expected_attempt))
        return entity


class TaskRepository(EntityRepository):
    def __init__(self, tasks=()) -> None:
        super().__init__(tasks)
        self.stale_tasks = []

    async def stale(self, stale_before, limit):
        return self.stale_tasks[:limit]

    async def claim_pending(self, worker_id, scope, max_tasks):
        claimed = []
        for task in self.entities.values():
            if task.status is TaskStatus.PENDING and len(claimed) < max_tasks:
                task.claim(worker_id)
                claimed.append(task)
        return claimed

    async def heartbeat(self, worker_id, task_ids, now):
        return []

    async def list_job_tasks(self, job_id, exclusive=False):
        return [task for task in self.entities.values() if task.job_id == job_id]

    async def query(self, task_filter):
        return list(self.entities.values()), None

    async def completed_evaluator_exists(self, session_id, plugin_version_id):
        return False


def make_task_service(
    tasks: TaskRepository,
    *,
    versions: EntityRepository | None = None,
    sessions: EntityRepository | None = None,
    secrets: EntityRepository | None = None,
    events: EventRegistry | None = None,
) -> TaskService:
    return TaskService(
        task_repository=cast(Any, tasks),
        worker_repository=cast(Any, EntityRepository()),
        agent_version_repository=cast(Any, versions or EntityRepository()),
        plugin_repository=cast(Any, EntityRepository()),
        blob_repository=cast(Any, EntityRepository()),
        secret_repository=cast(Any, secrets or EntityRepository()),
        session_repository=cast(Any, sessions or EntityRepository()),
        events=events or EventRegistry(),
    )


async def test_event_registry_dispatches_in_registration_order() -> None:
    events = EventRegistry()
    calls = []

    async def first(event):
        calls.append(("first", event.job.id))

    async def second(event):
        calls.append(("second", event.job.id))

    events.register(JobSettled, first)
    events.register(JobSettled, second)
    job = Job(owner_id=uuid.uuid4(), status=JobStatus.COMPLETED)

    await events.dispatch(JobSettled(job))

    assert calls == [("first", job.id), ("second", job.id)]


async def test_agent_spec_merges_secrets_in_declared_order() -> None:
    first = Secret(
        owner_id=uuid.uuid4(),
        name="first",
        values={"TOKEN": SecretStr("one"), "FIRST": SecretStr("yes")},
    )
    second = Secret(
        owner_id=first.owner_id,
        name="second",
        values={"TOKEN": SecretStr("two")},
    )
    version = AgentVersion(
        owner_id=first.owner_id,
        agent_id=uuid.uuid4(),
        version=1,
        run_spec=RunSpec(
            command="agent",
            secret_ids=[first.id, second.id],
            timeout_seconds=17,
        ),
    )
    task = AgentTask(
        job_id=uuid.uuid4(),
        agent_version_id=version.id,
        inputs={"prompt": "hi"},
    )
    service = make_task_service(
        TaskRepository([task]),
        versions=EntityRepository([version]),
        secrets=EntityRepository([first, second]),
    )

    spec = await service.get_spec(task.id, actor())

    assert spec.timeout_seconds == 17
    assert spec.secret_env == {"TOKEN": "two", "FIRST": "yes"}
    assert isinstance(spec.details, AgentTaskDetails)
    assert spec.details.inputs == {"prompt": "hi"}


async def test_terminal_update_dispatches_before_job_advance() -> None:
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.REPLAY,
        status=SessionStatus.COMPLETED,
    )
    task = AgentTask(
        job_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        status=TaskStatus.RUNNING,
        attempt=2,
        result_session_id=session.id,
    )
    events = EventRegistry()
    order = []

    async def terminal(event):
        order.append(("event", event.task.status))

    class Jobs:
        async def advance_job(self, advanced):
            order.append(("advance", advanced.status))

    events.register(TaskTerminal, terminal)
    service = make_task_service(
        TaskRepository([task]),
        sessions=EntityRepository([session]),
        events=events,
    )
    service.set_job_service(cast(Any, Jobs()))

    result = await service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.COMPLETED, attempt=2),
        actor(),
    )

    assert result.status is TaskStatus.COMPLETED
    assert order == [
        ("event", TaskStatus.COMPLETED),
        ("advance", TaskStatus.COMPLETED),
    ]


async def test_stale_claim_sweep_requeues_and_unlinks_result_session() -> None:
    stale = AgentTask(
        job_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        status=TaskStatus.RUNNING,
        attempt=1,
        worker_id=uuid.uuid4(),
        claimed_at=datetime.now(UTC) - timedelta(minutes=5),
        result_session_id=uuid.uuid4(),
    )
    tasks = TaskRepository([stale])
    tasks.stale_tasks = [stale]

    class Sessions(EntityRepository):
        def __init__(self):
            super().__init__()
            self.unlinked = []

        async def unlink_task(self, task_id):
            self.unlinked.append(task_id)

    sessions = Sessions()
    service = make_task_service(tasks, sessions=sessions)

    await service._sweep_stale(datetime.now(UTC))

    assert stale.status is TaskStatus.PENDING
    assert stale.result_session_id is None
    assert sessions.unlinked == [stale.id]


async def test_job_settlement_ignores_ignored_failure() -> None:
    job = Job(owner_id=uuid.uuid4(), status=JobStatus.RUNNING)
    ignored = EvaluationTask(
        job_id=job.id,
        plugin_version_id=uuid.uuid4(),
        input_session_id=uuid.uuid4(),
        status=TaskStatus.FAILED,
        on_failure=TaskOnFailure.IGNORE,
        error="ignored",
    )
    completed = EvaluationTask(
        job_id=job.id,
        plugin_version_id=uuid.uuid4(),
        input_session_id=uuid.uuid4(),
        status=TaskStatus.COMPLETED,
        result=[{"name": "score", "score": 1.0}],
    )
    jobs = EntityRepository([job])
    tasks = TaskRepository([ignored, completed])
    events = EventRegistry()
    settled = []

    async def capture(event):
        settled.append(event.job.status)

    events.register(JobSettled, capture)
    service = JobService(
        cast(Any, jobs),
        cast(Any, tasks),
        cast(Any, EntityRepository()),
        cast(Any, EntityRepository()),
        cast(Any, EntityRepository()),
        cast(Any, EntityRepository()),
        cast(Any, EntityRepository()),
        events,
    )
    service.set_task_service(cast(Any, SimpleNamespace()))

    result = await service.advance_job(completed)

    assert result is not None
    assert result.status is JobStatus.COMPLETED
    assert settled == [JobStatus.COMPLETED]


async def test_create_replay_pipeline_adds_agent_and_baseline_tasks() -> None:
    owner_id = uuid.uuid4()
    baseline = Session(
        owner_id=owner_id,
        agent_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        inputs={"prompt": "old"},
    )
    version = AgentVersion(
        owner_id=owner_id,
        agent_id=baseline.agent_id,
        version=1,
        run_spec=RunSpec(command="agent"),
    )
    evaluator = EvaluatorConfig(
        evaluator="quality",
        evaluator_version_id=uuid.uuid4(),
    )

    class Jobs:
        def __init__(self):
            self.job = Job(owner_id=owner_id)
            self.tasks = []

        async def create_job(self, requested_owner):
            assert requested_owner == owner_id
            return self.job

        async def add_task(self, job_id, task):
            self.tasks.append(task)
            return task

    class Replays:
        async def create(self, replay, config):
            return replay, config

    jobs = Jobs()
    replay, config = await create_replay_pipeline(
        owner_id=owner_id,
        baseline_session=baseline,
        agent_version=version,
        evaluators=[evaluator],
        evaluate_baselines=True,
        job_service=cast(Any, jobs),
        replay_repository=cast(Any, Replays()),
        task_repository=cast(Any, TaskRepository()),
    )

    assert config.evaluators == [evaluator]
    assert jobs.tasks[0].env == {"KITARU_REPLAY_ID": str(replay.id)}
    assert isinstance(jobs.tasks[0], AgentTask)
    assert isinstance(jobs.tasks[1], EvaluationTask)


async def test_evaluator_terminal_event_records_rows() -> None:
    session = Session(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
    )
    task = EvaluationTask(
        job_id=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        input_session_id=session.id,
        status=TaskStatus.COMPLETED,
        result=[
            {"name": "quality", "score": 0.8, "explanation": "clear"},
            {"name": "label", "value": "good", "explanation": None},
        ],
    )

    class Evaluations:
        def __init__(self):
            self.rows = []

        async def create_many(self, rows):
            self.rows.extend(rows)
            return rows

    evaluations = Evaluations()
    await record_task_evaluations(
        TaskTerminal(task, TaskStatus.RUNNING),
        cast(Any, evaluations),
        cast(Any, EntityRepository([session])),
    )

    assert [row.name for row in evaluations.rows] == ["quality", "label"]
    assert all(row.task_id == task.id for row in evaluations.rows)


async def test_run_finalization_gives_canceling_precedence() -> None:
    run = ExperimentRun(
        owner_id=uuid.uuid4(),
        experiment_id=uuid.uuid4(),
        number=1,
        cohort_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        status=ExperimentRunStatus.CANCELING,
    )
    replay = Replay(
        owner_id=run.owner_id,
        job_id=uuid.uuid4(),
        experiment_run_id=run.id,
        replay_config_id=uuid.uuid4(),
        baseline_session_id=uuid.uuid4(),
        status=ReplayStatus.FAILED,
    )

    class Replays:
        async def count_unsettled(self, run_id):
            return 0

        async def count_statuses(self, run_id):
            return {ReplayStatus.FAILED: 1}

    await finalize_run_if_drained(
        ReplaySettled(replay),
        cast(Any, Replays()),
        cast(Any, EntityRepository([run])),
    )

    assert run.status is ExperimentRunStatus.CANCELED


async def test_worker_registration_uses_atomic_upsert() -> None:
    class Workers(EntityRepository):
        def __init__(self):
            super().__init__()
            self.upserted = None

        async def upsert(self, worker):
            self.upserted = worker
            return worker

    repository = Workers()
    service = WorkerService(cast(Any, repository))

    worker, live = await service.register_worker(
        "worker-a",
        WorkerScope(kinds=[WireTaskKind.AGENT]),
        WorkerRuntime(platform="bare"),
        {"pool": "default"},
        actor(),
    )

    assert repository.upserted is worker
    assert worker.name == "worker-a"
    assert worker.metadata == {"pool": "default"}
    assert live is True
