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
"""Contract tests for task repositories."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from worker_coverage_cases import COVERAGE_CASES, CoverageCase, CoverageIds

from conftest import (
    UNSCOPED_WORKER_SCOPE,
    FakeJobRepository,
    FakeTaskRepository,
    create_job,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskKind, TaskStatus
from kitaru.api_models.v1.worker import (
    LabelSelector,
    WorkerClaim,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import Plugin, PluginKind, ScriptPluginSource
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import (
    AgentTask,
    DuplicateEvaluationTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskNotFound,
)
from kitaru.server.domain.worker import Worker
from kitaru.server.filtering import FilterCondition


class Setup(NamedTuple):
    """Task repository under test, plus rows a task can reference."""

    tasks: TaskRepository
    jobs: JobRepository
    owner_id: uuid.UUID
    job_id: uuid.UUID
    agent_version_id: uuid.UUID
    agent_version_id_2: uuid.UUID
    plugin_version_id: uuid.UUID
    session_id: uuid.UUID
    worker_id: uuid.UUID
    worker_id_2: uuid.UUID


async def _seed_postgres(session: AsyncSession, engine: AsyncEngine) -> Setup:
    """Create the account, job, and code rows postgres-backed tasks reference.

    Returns:
        Task repository and the ids of the rows it can point tasks at.
    """
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner.id, name="assistant")
    )
    agent_version = await SQLAgentVersionRepository(session).create(
        AgentVersion(owner_id=owner.id, agent_id=agent.id)
    )
    agent_version_2 = await SQLAgentVersionRepository(session).create(
        AgentVersion(owner_id=owner.id, agent_id=agent.id)
    )
    code_blob, _ = await SQLBlobRepository(session).create(
        Blob(
            owner_id=owner.id,
            sha256="1" * 64,
            size=4,
            media_type="text/x-python",
            stored_in=BlobStorageBackend.DATABASE,
        )
    )
    plugin = await SQLPluginRepository(session).create(
        Plugin(owner_id=owner.id, kind=PluginKind.EVALUATOR, name="scorer")
    )
    plugin_version = await SQLPluginRepository(session).create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="score"),
        display_version=None,
    )
    stored_session = await SQLSessionRepository(session, engine).create(
        Session(owner_id=owner.id, agent_id=agent.id, number=1, origin="recorded")
    )
    job = await SQLJobRepository(session).create(
        Job(owner_id=owner.id, kind=JobKind.SESSION_RUN)
    )
    worker = await SQLWorkerRepository(session).register(
        Worker(
            owner_id=owner.id,
            name="worker-1",
            scope=UNSCOPED_WORKER_SCOPE,
            runtime=WorkerRuntime(platform="bare"),
            last_seen_at=datetime.now(UTC),
        )
    )
    worker_2 = await SQLWorkerRepository(session).register(
        Worker(
            owner_id=owner.id,
            name="worker-2",
            scope=UNSCOPED_WORKER_SCOPE,
            runtime=WorkerRuntime(platform="bare"),
            last_seen_at=datetime.now(UTC),
        )
    )
    return Setup(
        tasks=SQLTaskRepository(session),
        jobs=SQLJobRepository(session),
        owner_id=owner.id,
        job_id=job.id,
        agent_version_id=agent_version.id,
        agent_version_id_2=agent_version_2.id,
        plugin_version_id=plugin_version.id,
        session_id=stored_session.id,
        worker_id=worker.id,
        worker_id_2=worker_2.id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each task repository implementation with a ready job to attach to."""
    if request.param == "fake":
        jobs = FakeJobRepository()
        tasks = FakeTaskRepository()
        tasks.jobs = jobs
        owner_id = uuid.uuid4()
        job = await create_job(jobs, owner_id)
        yield Setup(
            tasks=tasks,
            jobs=jobs,
            owner_id=owner_id,
            job_id=job.id,
            agent_version_id=uuid.uuid4(),
            agent_version_id_2=uuid.uuid4(),
            plugin_version_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            worker_id=uuid.uuid4(),
            worker_id_2=uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        yield await _seed_postgres(session, engine)


def _agent_task(setup: Setup, **overrides: Any) -> AgentTask:
    """Build an agent task pointed at the setup's agent version."""
    values: dict[str, Any] = {
        "job_id": setup.job_id,
        "agent_version_id": setup.agent_version_id,
    }
    values.update(overrides)
    return AgentTask(**values)


async def test_create_and_get(setup: Setup) -> None:
    """Store a task and load it back with timestamps set."""
    created = await setup.tasks.create(_agent_task(setup))
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.tasks.get(created.id)
    assert loaded == created


async def test_create_many_round_trips_tasks(setup: Setup) -> None:
    """Bulk-create persists every task in one round trip, timestamps set."""
    tasks: list[Task] = [_agent_task(setup, labels={"i": str(i)}) for i in range(3)]
    created = await setup.tasks.create_many(tasks)
    assert [task.id for task in created] == [task.id for task in tasks]
    assert all(task.created is not None for task in created)
    loaded = await setup.tasks.get_many([task.id for task in tasks])
    assert set(loaded) == {task.id for task in tasks}


async def test_create_many_empty(setup: Setup) -> None:
    """Bulk-create with no tasks is a no-op."""
    assert await setup.tasks.create_many([]) == []


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown task id."""
    missing_id = uuid.uuid4()
    with pytest.raises(TaskNotFound, match=f"Task {missing_id} was not found"):
        await setup.tasks.get(missing_id)


async def test_get_many(setup: Setup) -> None:
    """Bulk-load tasks keyed by id, missing ids omitted."""
    first = await setup.tasks.create(_agent_task(setup))
    second = await setup.tasks.create(_agent_task(setup))
    loaded = await setup.tasks.get_many([first.id, second.id, uuid.uuid4()])
    assert set(loaded) == {first.id, second.id}


async def test_evaluator_pair_uniqueness(setup: Setup) -> None:
    """The (job_id, input_session_id, plugin_version_id) key backstops duplicates."""
    task = EvaluationTask(
        job_id=setup.job_id,
        plugin_version_id=setup.plugin_version_id,
        input_session_id=setup.session_id,
    )
    await setup.tasks.create(task)
    duplicate = EvaluationTask(
        job_id=setup.job_id,
        plugin_version_id=setup.plugin_version_id,
        input_session_id=setup.session_id,
    )
    with pytest.raises(DuplicateEvaluationTask):
        await setup.tasks.create(duplicate)


async def test_import_task_round_trips_its_fields(setup: Setup) -> None:
    """An importer task round-trips its import reference."""
    import_id = uuid.uuid4()
    task = ImportTask(job_id=setup.job_id, import_id=import_id)
    created = await setup.tasks.create(task)
    assert isinstance(created, ImportTask)
    assert created.import_id == import_id


async def test_list_by_job_orders_by_id(setup: Setup) -> None:
    """List a job's tasks in creation order."""
    first = await setup.tasks.create(_agent_task(setup))
    second = await setup.tasks.create(_agent_task(setup))
    tasks = await setup.tasks.list_by_job(setup.job_id)
    assert [task.id for task in tasks] == [first.id, second.id]


async def test_query_filters(setup: Setup) -> None:
    """Filter tasks by kind and status."""
    task = await setup.tasks.create(_agent_task(setup))
    tasks, next_cursor = await setup.tasks.query(TaskFilter(job_id=setup.job_id))
    assert next_cursor is None
    assert [t.id for t in tasks] == [task.id]

    tasks, _ = await setup.tasks.query(
        TaskFilter(
            job_id=setup.job_id,
            expression=FilterCondition(
                field="kind", op=FilterOp.EQ, value=TaskKind.IMPORTER
            ),
        )
    )
    assert tasks == []

    tasks, _ = await setup.tasks.query(
        TaskFilter(
            job_id=setup.job_id,
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=TaskStatus.PENDING
            ),
        )
    )
    assert [t.id for t in tasks] == [task.id]


async def test_query_filters_by_job_id(setup: Setup) -> None:
    """Scope the query to one job, tasks of other jobs excluded."""
    matching = await setup.tasks.create(_agent_task(setup))
    other_job = await setup.jobs.create(
        Job(owner_id=setup.owner_id, kind=JobKind.SESSION_RUN)
    )
    await setup.tasks.create(_agent_task(setup, job_id=other_job.id))

    tasks, next_cursor = await setup.tasks.query(TaskFilter(job_id=setup.job_id))
    assert next_cursor is None
    assert [task.id for task in tasks] == [matching.id]


async def test_query_filters_by_stale_before(setup: Setup) -> None:
    """Filter in-flight tasks whose last heartbeat predates the bound."""
    stale = await setup.tasks.create(_agent_task(setup))
    await setup.tasks.create(_agent_task(setup))
    await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE,
        setup.worker_id,
        1,
        datetime.now(UTC) - timedelta(hours=2),
    )
    await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE, setup.worker_id, 1, datetime.now(UTC)
    )

    tasks, next_cursor = await setup.tasks.query(
        TaskFilter(stale_before=datetime.now(UTC) - timedelta(hours=1))
    )
    assert next_cursor is None
    assert [task.id for task in tasks] == [stale.id]


async def test_update_persists_status(setup: Setup) -> None:
    """Persist a status transition and renew the updated timestamp."""
    created = await setup.tasks.create(_agent_task(setup))
    created.claim(setup.worker_id, datetime.now(UTC))
    updated = await setup.tasks.update(created)
    assert updated.status is TaskStatus.CLAIMED
    assert updated.attempt == 1
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_claim_pending_orders_by_id_and_locks_rows(setup: Setup) -> None:
    """Claim hands out pending tasks oldest first, incrementing their attempt."""
    first = await setup.tasks.create(_agent_task(setup))
    second = await setup.tasks.create(_agent_task(setup))
    claimed = await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE, setup.worker_id, 1, datetime.now(UTC)
    )
    assert [task.id for task in claimed] == [first.id]
    assert claimed[0].attempt == 1
    assert claimed[0].worker_id == setup.worker_id

    remaining, _ = await setup.tasks.query(
        TaskFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=TaskStatus.PENDING
            )
        )
    )
    assert {task.id for task in remaining} == {second.id}


async def test_claim_pending_kind_filter(setup: Setup) -> None:
    """Claim respects a kind-scoped worker."""
    agent_task = await setup.tasks.create(_agent_task(setup))
    import_task = await setup.tasks.create(
        ImportTask(job_id=setup.job_id, import_id=uuid.uuid4())
    )
    claimed = await setup.tasks.claim_pending(
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed] == [import_task.id]
    assert agent_task.id != import_task.id


async def test_claim_pending_required_selector(setup: Setup) -> None:
    """A required selector matches only tasks carrying the label."""
    labeled = await setup.tasks.create(_agent_task(setup, labels={"env": "prod"}))
    await setup.tasks.create(_agent_task(setup, labels={"env": "dev"}))
    claimed = await setup.tasks.claim_pending(
        WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=True)],
        ),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed] == [labeled.id]


async def test_claim_pending_non_required_selector_matches_unlabeled(
    setup: Setup,
) -> None:
    """A non-required selector also matches tasks lacking the key."""
    matching = await setup.tasks.create(_agent_task(setup, labels={"env": "prod"}))
    unlabeled = await setup.tasks.create(_agent_task(setup))
    await setup.tasks.create(_agent_task(setup, labels={"env": "dev"}))
    claimed = await setup.tasks.claim_pending(
        WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=False)],
        ),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert {task.id for task in claimed} == {matching.id, unlabeled.id}


async def test_claim_pending_job_pin(setup: Setup) -> None:
    """A job-pinned worker only claims that job's tasks."""
    pinned = await setup.tasks.create(_agent_task(setup))
    claimed = await setup.tasks.claim_pending(
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=setup.job_id),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed] == [pinned.id]

    claimed_wrong_job = await setup.tasks.claim_pending(
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)], job_id=uuid.uuid4()),
        uuid.uuid4(),
        10,
        datetime.now(UTC),
    )
    assert claimed_wrong_job == []


async def test_claim_pending_agent_version_scoped_workers_claim_only_their_own(
    setup: Setup,
) -> None:
    """Two agent-version-scoped workers each claim only their own version's tasks."""
    first_version_tasks = [
        await setup.tasks.create(_agent_task(setup)) for _ in range(2)
    ]
    second_version_tasks = [
        await setup.tasks.create(
            _agent_task(setup, agent_version_id=setup.agent_version_id_2)
        )
        for _ in range(2)
    ]
    first_scope = WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.AGENT, agent_version_id=setup.agent_version_id)
        ]
    )
    second_scope = WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.AGENT, agent_version_id=setup.agent_version_id_2)
        ]
    )

    claimed_first = await setup.tasks.claim_pending(
        first_scope, setup.worker_id, 10, datetime.now(UTC)
    )
    claimed_second = await setup.tasks.claim_pending(
        second_scope, setup.worker_id_2, 10, datetime.now(UTC)
    )
    assert {task.id for task in claimed_first} == {
        task.id for task in first_version_tasks
    }
    assert {task.id for task in claimed_second} == {
        task.id for task in second_version_tasks
    }


async def test_claim_pending_unversioned_agent_claim_spans_every_version(
    setup: Setup,
) -> None:
    """An unversioned agent claim claims tasks across every version, oldest first."""
    first_version_task = await setup.tasks.create(_agent_task(setup))
    second_version_task = await setup.tasks.create(
        _agent_task(setup, agent_version_id=setup.agent_version_id_2)
    )
    claimed = await setup.tasks.claim_pending(
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed] == [
        first_version_task.id,
        second_version_task.id,
    ]


async def test_claim_pending_unversioned_agent_claim_does_not_starve_other_versions(
    setup: Setup,
) -> None:
    """A young backlog in one agent version does not starve an older task in another."""
    if setup.agent_version_id < setup.agent_version_id_2:
        lower_version_id, higher_version_id = (
            setup.agent_version_id,
            setup.agent_version_id_2,
        )
    else:
        lower_version_id, higher_version_id = (
            setup.agent_version_id_2,
            setup.agent_version_id,
        )
    limit = 4
    older_task = await setup.tasks.create(
        _agent_task(setup, agent_version_id=higher_version_id)
    )
    for _ in range(limit):
        await setup.tasks.create(_agent_task(setup, agent_version_id=lower_version_id))

    claimed = await setup.tasks.claim_pending(
        WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.AGENT),
                WorkerClaim(kind=TaskKind.EVALUATOR),
            ]
        ),
        setup.worker_id,
        limit,
        datetime.now(UTC),
    )
    assert older_task.id in {task.id for task in claimed}


async def test_claim_pending_kind_isolation_across_claims(setup: Setup) -> None:
    """A version-scoped agent claim and an evaluator claim never cross kinds."""
    agent_task = await setup.tasks.create(_agent_task(setup))
    evaluation_task = await setup.tasks.create(
        EvaluationTask(
            job_id=setup.job_id,
            plugin_version_id=setup.plugin_version_id,
            input_session_id=setup.session_id,
        )
    )
    await setup.tasks.create(ImportTask(job_id=setup.job_id, import_id=uuid.uuid4()))

    claimed_by_agent_scope = await setup.tasks.claim_pending(
        WorkerScope(
            claims=[
                WorkerClaim(
                    kind=TaskKind.AGENT, agent_version_id=setup.agent_version_id
                )
            ]
        ),
        setup.worker_id,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed_by_agent_scope] == [agent_task.id]

    claimed_by_evaluator_scope = await setup.tasks.claim_pending(
        WorkerScope(claims=[WorkerClaim(kind=TaskKind.EVALUATOR)]),
        setup.worker_id_2,
        10,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed_by_evaluator_scope] == [evaluation_task.id]


async def test_claim_pending_merges_claims_oldest_first(setup: Setup) -> None:
    """Mixed claims hand out the oldest tasks in exact id order, regardless of kind."""
    evaluation_task = await setup.tasks.create(
        EvaluationTask(
            job_id=setup.job_id,
            plugin_version_id=setup.plugin_version_id,
            input_session_id=setup.session_id,
        )
    )
    agent_tasks = [await setup.tasks.create(_agent_task(setup)) for _ in range(5)]

    claimed = await setup.tasks.claim_pending(
        WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.EVALUATOR),
                WorkerClaim(
                    kind=TaskKind.AGENT, agent_version_id=setup.agent_version_id
                ),
            ]
        ),
        setup.worker_id,
        4,
        datetime.now(UTC),
    )
    assert [task.id for task in claimed] == [
        evaluation_task.id,
        *[task.id for task in agent_tasks[:3]],
    ]


async def test_claim_pending_full_scope_takes_an_older_task_of_any_kind(
    setup: Setup,
) -> None:
    """A scope claiming everything takes an older task before newer agent tasks."""
    evaluation_task = await setup.tasks.create(
        EvaluationTask(
            job_id=setup.job_id,
            plugin_version_id=setup.plugin_version_id,
            input_session_id=setup.session_id,
        )
    )
    for _ in range(3):
        await setup.tasks.create(_agent_task(setup))

    claimed = await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE, setup.worker_id, 1, datetime.now(UTC)
    )
    assert [task.id for task in claimed] == [evaluation_task.id]


@pytest.mark.parametrize(
    "case", COVERAGE_CASES, ids=[case.name for case in COVERAGE_CASES]
)
async def test_claim_pending_matches_coverage_cases(
    setup: Setup, case: CoverageCase
) -> None:
    """Claim agrees with the shared worker coverage cases."""
    ids = CoverageIds(
        job_id=setup.job_id,
        other_job_id=uuid.uuid4(),
        agent_version_id=setup.agent_version_id,
        agent_version_id_2=setup.agent_version_id_2,
        plugin_version_id=setup.plugin_version_id,
        import_id=uuid.uuid4(),
        session_id=setup.session_id,
    )
    task = await setup.tasks.create(case.task(ids))
    claimed = await setup.tasks.claim_pending(
        case.scope(ids), setup.worker_id, 10, datetime.now(UTC)
    )
    expected = [task.id] if case.covered else []
    assert [claimed_task.id for claimed_task in claimed] == expected


async def test_claim_stale(setup: Setup) -> None:
    """Lock one in-flight task whose last heartbeat predates the cutoff."""
    task = await setup.tasks.create(_agent_task(setup))
    claimed = await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE,
        setup.worker_id,
        10,
        datetime.now(UTC) - timedelta(hours=1),
    )
    assert len(claimed) == 1

    stale = await setup.tasks.claim_stale(task.id, datetime.now(UTC))
    assert stale is not None
    assert stale.id == task.id

    not_yet_stale = await setup.tasks.claim_stale(
        task.id, datetime.now(UTC) - timedelta(hours=2)
    )
    assert not_yet_stale is None


async def test_list_stale_ids(setup: Setup) -> None:
    """Read the ids of in-flight tasks whose last heartbeat predates the cutoff."""
    task = await setup.tasks.create(_agent_task(setup))
    await setup.tasks.claim_pending(
        UNSCOPED_WORKER_SCOPE,
        setup.worker_id,
        10,
        datetime.now(UTC) - timedelta(hours=1),
    )

    assert await setup.tasks.list_stale_ids(datetime.now(UTC), 10) == [task.id]
    assert (
        await setup.tasks.list_stale_ids(datetime.now(UTC) - timedelta(hours=2), 10)
        == []
    )


async def test_lock_by_jobs(setup: Setup) -> None:
    """Lock the job's non-terminal task rows without altering them."""
    task = await setup.tasks.create(_agent_task(setup))
    await setup.tasks.lock_by_jobs([setup.job_id])
    await setup.tasks.lock_by_jobs([setup.job_id], nowait=True)
    await setup.tasks.lock_by_jobs([])
    assert await setup.tasks.get(task.id) == task


async def test_stamp_cancel_requested_skips_terminal_tasks(setup: Setup) -> None:
    """Stamp cancel_requested_at on non-terminal tasks, leaving terminal ones alone."""
    pending = await setup.tasks.create(_agent_task(setup))
    completed = _agent_task(setup)
    completed.claim(setup.worker_id, datetime.now(UTC))
    completed.start(datetime.now(UTC))
    completed.complete(None, datetime.now(UTC))
    stored_completed = await setup.tasks.create(completed)

    now = datetime.now(UTC)
    await setup.tasks.stamp_cancel_requested([setup.job_id], now)

    reloaded_pending = await setup.tasks.get(pending.id)
    assert reloaded_pending.cancel_requested_at == now

    reloaded_completed = await setup.tasks.get(stored_completed.id)
    assert reloaded_completed.cancel_requested_at is None


async def test_stamp_heartbeats_writes_only_the_heartbeat_column(
    setup: Setup,
) -> None:
    """Stamp heartbeat_at on owned in-flight tasks, leaving other fields alone."""
    start = datetime.now(UTC)
    held = _agent_task(setup)
    held.claim(setup.worker_id, start)
    held.start(start)
    stored_held = await setup.tasks.create(held)

    canceling = _agent_task(setup)
    canceling.claim(setup.worker_id, start)
    canceling.start(start)
    canceling.request_cancel(start)
    stored_canceling = await setup.tasks.create(canceling)

    completed = EvaluationTask(
        job_id=setup.job_id,
        plugin_version_id=setup.plugin_version_id,
        input_session_id=setup.session_id,
    )
    completed.claim(setup.worker_id, start)
    completed.start(start)
    completed.complete([{"name": "exact_match", "score": 1.0}], start)
    stored_completed = await setup.tasks.create(completed)

    now = datetime.now(UTC)
    stamped, skipped = await setup.tasks.stamp_heartbeats(
        [stored_held.id, stored_canceling.id, stored_completed.id],
        setup.worker_id,
        now,
    )
    assert stamped == {
        stored_held.id: None,
        stored_canceling.id: stored_canceling.cancel_requested_at,
    }
    assert skipped == set()
    assert await setup.tasks.stamp_heartbeats([stored_held.id], uuid.uuid4(), now) == (
        {},
        set(),
    )

    reloaded_held = await setup.tasks.get(stored_held.id)
    assert isinstance(reloaded_held, AgentTask)
    assert reloaded_held.heartbeat_at == now
    assert reloaded_held.status is TaskStatus.RUNNING

    reloaded_completed = await setup.tasks.get(stored_completed.id)
    assert reloaded_completed.heartbeat_at != now
    assert reloaded_completed.status is TaskStatus.COMPLETED
    assert reloaded_completed.result == [{"name": "exact_match", "score": 1.0}]


async def test_stamp_heartbeats_falls_back_to_the_jobs_cancel_request(
    setup: Setup,
) -> None:
    """Report a job-level cancel request the task row does not carry yet."""
    start = datetime.now(UTC)
    task = _agent_task(setup)
    task.claim(setup.worker_id, start)
    task.start(start)
    stored = await setup.tasks.create(task)
    assert stored.cancel_requested_at is None

    now = datetime.now(UTC)
    assert await setup.tasks.stamp_heartbeats([stored.id], setup.worker_id, now) == (
        {stored.id: None},
        set(),
    )

    job = await setup.jobs.get(setup.job_id)
    job.request_cancel(now)
    await setup.jobs.update(job)

    stamped, skipped = await setup.tasks.stamp_heartbeats(
        [stored.id], setup.worker_id, now
    )
    assert stamped == {stored.id: job.cancel_requested_at}
    assert skipped == set()
    # The stamp is read through, not written onto the task row.
    assert (await setup.tasks.get(stored.id)).cancel_requested_at is None


async def test_claim_pending_skip_locked_never_double_claims() -> None:
    """Concurrent claims never hand the same pending task to two workers."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (seed_session, engine):
        setup = await _seed_postgres(seed_session, engine)
        tasks = [
            await SQLTaskRepository(seed_session).create(_agent_task(setup))
            for _ in range(10)
        ]
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )

        async def claim_one() -> list[Task]:
            async with session_factory() as session:
                claimed = await SQLTaskRepository(session).claim_pending(
                    UNSCOPED_WORKER_SCOPE, setup.worker_id, 1, datetime.now(UTC)
                )
                await session.commit()
                return claimed

        results = await asyncio.gather(*(claim_one() for _ in range(len(tasks))))
        claimed_ids = [task.id for batch in results for task in batch]
        assert len(claimed_ids) == len(tasks)
        assert len(set(claimed_ids)) == len(tasks)
        assert set(claimed_ids) == {task.id for task in tasks}


async def test_stamp_heartbeats_skips_a_task_row_locked_elsewhere() -> None:
    """A heartbeat on a task locked by another transaction returns promptly.

    The locked task comes back neither stamped nor cancelable, and a sibling
    task in the same request still gets stamped.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (seed_session, engine):
        setup = await _seed_postgres(seed_session, engine)
        start = datetime.now(UTC)
        locked_task = _agent_task(setup)
        locked_task.claim(setup.worker_id, start)
        locked_task.start(start)
        stored_locked = await SQLTaskRepository(seed_session).create(locked_task)

        free_task = _agent_task(setup)
        free_task.claim(setup.worker_id, start)
        free_task.start(start)
        stored_free = await SQLTaskRepository(seed_session).create(free_task)
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )

        async with session_factory() as holder_session:
            await holder_session.execute(
                select(TaskORM).where(TaskORM.id == stored_locked.id).with_for_update()
            )

            async with session_factory() as heartbeat_session:
                now = datetime.now(UTC)
                stamped, skipped = await asyncio.wait_for(
                    SQLTaskRepository(heartbeat_session).stamp_heartbeats(
                        [stored_locked.id, stored_free.id], setup.worker_id, now
                    ),
                    timeout=5,
                )
                await heartbeat_session.commit()
                reloaded_locked = await SQLTaskRepository(heartbeat_session).get(
                    stored_locked.id
                )
                reloaded_free = await SQLTaskRepository(heartbeat_session).get(
                    stored_free.id
                )

            await holder_session.rollback()

        assert skipped == {stored_locked.id}
        assert stamped == {stored_free.id: None}
        assert reloaded_locked.heartbeat_at is None
        assert reloaded_free.heartbeat_at == now
