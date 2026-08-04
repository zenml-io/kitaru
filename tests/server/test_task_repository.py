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
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import (
    FakeJobRepository,
    FakeTaskRepository,
    create_job,
    pg_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import LabelSelector, TaskKind, TaskStatus, WorkerScope
from kitaru.api_models.v1.worker import WorkerRuntime
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
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.blob import Blob
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
    owner_id: uuid.UUID
    job_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID
    plugin_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    session_id: uuid.UUID
    worker_id: uuid.UUID


async def _seed_postgres(session: AsyncSession) -> Setup:
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
    code_blob, _ = await SQLBlobRepository(session).create(
        Blob(
            owner_id=owner.id,
            sha256="1" * 64,
            size=4,
            media_type="text/x-python",
            data=b"code",
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
    payload, _ = await SQLBlobRepository(session).create(
        Blob(
            owner_id=owner.id,
            sha256="0" * 64,
            size=4,
            media_type="text/csv",
            data=b"data",
        )
    )
    stored_session = await SQLSessionRepository(session).create(
        Session(owner_id=owner.id, agent_id=agent.id, origin="recorded")
    )
    job = await SQLJobRepository(session).create(
        Job(owner_id=owner.id, kind=JobKind.SESSION_RUN)
    )
    worker = await SQLWorkerRepository(session).register(
        Worker(
            owner_id=owner.id,
            name="worker-1",
            scope=WorkerScope(),
            runtime=WorkerRuntime(platform="bare"),
            last_seen_at=datetime.now(UTC),
        )
    )
    return Setup(
        tasks=SQLTaskRepository(session),
        owner_id=owner.id,
        job_id=job.id,
        agent_id=agent.id,
        agent_version_id=agent_version.id,
        plugin_version_id=plugin_version.id,
        payload_blob_id=payload.id,
        session_id=stored_session.id,
        worker_id=worker.id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each task repository implementation with a ready job to attach to."""
    if request.param == "fake":
        jobs = FakeJobRepository()
        owner_id = uuid.uuid4()
        job = await create_job(jobs, owner_id)
        yield Setup(
            tasks=FakeTaskRepository(),
            owner_id=owner_id,
            job_id=job.id,
            agent_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            plugin_version_id=uuid.uuid4(),
            payload_blob_id=uuid.uuid4(),
            session_id=uuid.uuid4(),
            worker_id=uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        yield await _seed_postgres(session)


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


async def test_get_scored_evaluator_version_ids_many(setup: Setup) -> None:
    """Group completed evaluator tasks by session id, other ids omitted."""
    completed = EvaluationTask(
        job_id=setup.job_id,
        plugin_version_id=setup.plugin_version_id,
        input_session_id=setup.session_id,
    )
    completed.claim(setup.worker_id, datetime.now(UTC))
    completed.start(datetime.now(UTC))
    completed.complete([{"name": "exact_match", "score": 1.0}], datetime.now(UTC))
    await setup.tasks.create(completed)

    other_session_id = uuid.uuid4()
    scored = await setup.tasks.get_scored_evaluator_version_ids_many(
        [setup.session_id, other_session_id]
    )
    assert scored == {setup.session_id: {setup.plugin_version_id}}


async def test_get_scored_evaluator_version_ids_many_empty(setup: Setup) -> None:
    """An empty id list needs no lookup and returns no matches."""
    assert await setup.tasks.get_scored_evaluator_version_ids_many([]) == {}


async def test_import_task_round_trips_its_fields(setup: Setup) -> None:
    """An importer task round-trips its plugin, payload, and agent references."""
    task = ImportTask(
        job_id=setup.job_id,
        plugin_version_id=setup.plugin_version_id,
        payload_blob_id=setup.payload_blob_id,
        agent_id=setup.agent_id,
        params={"delimiter": ","},
    )
    created = await setup.tasks.create(task)
    assert isinstance(created, ImportTask)
    assert created.plugin_version_id == setup.plugin_version_id
    assert created.payload_blob_id == setup.payload_blob_id
    assert created.params == {"delimiter": ","}


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
        WorkerScope(), setup.worker_id, 1, datetime.now(UTC)
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
        ImportTask(
            job_id=setup.job_id,
            plugin_version_id=setup.plugin_version_id,
            payload_blob_id=setup.payload_blob_id,
            agent_id=setup.agent_id,
        )
    )
    claimed = await setup.tasks.claim_pending(
        WorkerScope(kinds=[TaskKind.IMPORTER]), setup.worker_id, 10, datetime.now(UTC)
    )
    assert [task.id for task in claimed] == [import_task.id]
    assert agent_task.id != import_task.id


async def test_claim_pending_required_selector(setup: Setup) -> None:
    """A required selector matches only tasks carrying the label."""
    labeled = await setup.tasks.create(_agent_task(setup, labels={"env": "prod"}))
    await setup.tasks.create(_agent_task(setup, labels={"env": "dev"}))
    claimed = await setup.tasks.claim_pending(
        WorkerScope(
            selectors=[LabelSelector(key="env", values=["prod"], required=True)]
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
            selectors=[LabelSelector(key="env", values=["prod"], required=False)]
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
        WorkerScope(job_id=setup.job_id), setup.worker_id, 10, datetime.now(UTC)
    )
    assert [task.id for task in claimed] == [pinned.id]

    claimed_wrong_job = await setup.tasks.claim_pending(
        WorkerScope(job_id=uuid.uuid4()), uuid.uuid4(), 10, datetime.now(UTC)
    )
    assert claimed_wrong_job == []


async def test_claim_stale(setup: Setup) -> None:
    """Lock in-flight tasks whose last heartbeat predates the cutoff."""
    task = await setup.tasks.create(_agent_task(setup))
    claimed = await setup.tasks.claim_pending(
        WorkerScope(), setup.worker_id, 10, datetime.now(UTC) - timedelta(hours=1)
    )
    assert len(claimed) == 1

    stale = await setup.tasks.claim_stale(datetime.now(UTC), 10)
    assert [t.id for t in stale] == [task.id]

    not_yet_stale = await setup.tasks.claim_stale(
        datetime.now(UTC) - timedelta(hours=2), 10
    )
    assert not_yet_stale == []


async def test_stamp_cancel_requested_skips_terminal_tasks(setup: Setup) -> None:
    """Stamp cancel_requested_at on non-terminal tasks, leaving terminal ones alone."""
    pending = await setup.tasks.create(_agent_task(setup))
    completed = _agent_task(setup)
    completed.claim(setup.worker_id, datetime.now(UTC))
    completed.start(datetime.now(UTC))
    completed.link_result_session(setup.session_id)
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
    held.link_result_session(setup.session_id)
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
    stamped = await setup.tasks.stamp_heartbeats(
        [stored_held.id, stored_canceling.id, stored_completed.id],
        setup.worker_id,
        now,
    )
    assert stamped == {
        stored_held.id: None,
        stored_canceling.id: stored_canceling.cancel_requested_at,
    }
    assert await setup.tasks.stamp_heartbeats([stored_held.id], uuid.uuid4(), now) == {}

    reloaded_held = await setup.tasks.get(stored_held.id)
    assert isinstance(reloaded_held, AgentTask)
    assert reloaded_held.heartbeat_at == now
    assert reloaded_held.status is TaskStatus.RUNNING
    assert reloaded_held.result_session_id == setup.session_id

    reloaded_completed = await setup.tasks.get(stored_completed.id)
    assert reloaded_completed.heartbeat_at != now
    assert reloaded_completed.status is TaskStatus.COMPLETED
    assert reloaded_completed.result == [{"name": "exact_match", "score": 1.0}]


async def test_claim_pending_skip_locked_never_double_claims() -> None:
    """Concurrent claims never hand the same pending task to two workers."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (seed_session, engine):
        setup = await _seed_postgres(seed_session)
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
                    WorkerScope(), setup.worker_id, 1, datetime.now(UTC)
                )
                await session.commit()
                return claimed

        results = await asyncio.gather(*(claim_one() for _ in range(len(tasks))))
        claimed_ids = [task.id for batch in results for task in batch]
        assert len(claimed_ids) == len(tasks)
        assert len(set(claimed_ids)) == len(tasks)
        assert set(claimed_ids) == {task.id for task in tasks}
