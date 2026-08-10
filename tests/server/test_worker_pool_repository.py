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
"""Contract tests for worker pool repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import (
    FakeTaskRepository,
    FakeWorkerPoolRepository,
    FakeWorkerRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerRuntime, WorkerScope
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_pool_repository import (
    SQLWorkerPoolRepository,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.interfaces.worker_pool_repository import (
    WorkerPoolRepository,
)
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.worker_pool import WorkerPoolFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.job import Job
from kitaru.server.domain.task import AgentTask, QueueStats
from kitaru.server.domain.worker import Worker
from kitaru.server.domain.worker_pool import (
    DuplicateWorkerPoolName,
    WorkerPool,
    WorkerPoolInUse,
    WorkerPoolNotFound,
)
from kitaru.server.filtering import FilterCondition

Setup = tuple[WorkerPoolRepository, uuid.UUID]
WorkerSetup = tuple[WorkerPoolRepository, WorkerRepository, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each worker pool repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeWorkerPoolRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield SQLWorkerPoolRepository(session), owner.id


@pytest.fixture(params=["fake", "postgres"])
async def worker_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[WorkerSetup, None]:
    """Provide a worker pool repository wired to a worker repository and an owner id."""
    if request.param == "fake":
        workers = FakeWorkerRepository()
        yield FakeWorkerPoolRepository(worker_repository=workers), workers, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield SQLWorkerPoolRepository(session), SQLWorkerRepository(session), owner.id


class TaskSetup(NamedTuple):
    """Task repository under test, plus an owner and a ready job."""

    tasks: TaskRepository
    owner_id: uuid.UUID
    job_id: uuid.UUID
    agent_version_id: uuid.UUID
    worker_id: uuid.UUID


async def _seed_postgres_task_setup(session: AsyncSession) -> TaskSetup:
    """Create the account, agent version, job, and worker rows tasks reference.

    Args:
        session: Database session.

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
    return TaskSetup(
        tasks=SQLTaskRepository(session),
        owner_id=owner.id,
        job_id=job.id,
        agent_version_id=agent_version.id,
        worker_id=worker.id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def task_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[TaskSetup, None]:
    """Provide each task repository implementation with a ready job to attach to."""
    if request.param == "fake":
        yield TaskSetup(
            tasks=FakeTaskRepository(),
            owner_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            worker_id=uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        yield await _seed_postgres_task_setup(session)


def _agent_task(setup: TaskSetup, **overrides: Any) -> AgentTask:
    """Build an agent task pointed at the setup's agent version.

    Args:
        setup: Task repository setup.
        overrides: Field overrides layered onto the defaults.

    Returns:
        Unstored agent task.
    """
    values: dict[str, Any] = {
        "job_id": setup.job_id,
        "agent_version_id": setup.agent_version_id,
    }
    values.update(overrides)
    return AgentTask(**values)


def _worker_pool(
    owner_id: uuid.UUID,
    name: str = "pool-1",
    scope: WorkerScope | None = None,
) -> WorkerPool:
    """Build a worker pool for repository tests.

    Args:
        owner_id: Id of the owning account.
        name: Worker pool name.
        scope: Tasks the pool's workers claim.

    Returns:
        Unstored worker pool.
    """
    return WorkerPool(
        owner_id=owner_id,
        name=name,
        scope=scope if scope is not None else WorkerScope(),
    )


def _referencing_worker(
    owner_id: uuid.UUID,
    pool_id: uuid.UUID,
    name: str = "worker-1",
    concurrency: int = 1,
    last_seen_at: datetime | None = None,
) -> Worker:
    """Build a worker joined to a pool for repository tests.

    Args:
        owner_id: Id of the owning account.
        pool_id: Pool the worker joins.
        name: Worker name.
        concurrency: Concurrent task capacity the worker reports.
        last_seen_at: Time of the worker's last heartbeat.

    Returns:
        Unstored worker.
    """
    return Worker(
        owner_id=owner_id,
        name=name,
        pool_id=pool_id,
        scope=WorkerScope(),
        runtime=WorkerRuntime(platform="bare"),
        concurrency=concurrency,
        last_seen_at=last_seen_at if last_seen_at is not None else datetime.now(UTC),
    )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new worker pool with both timestamps set."""
    repository, owner_id = setup
    worker_pool = await repository.create(
        _worker_pool(owner_id, scope=WorkerScope(kinds=["agent"]))
    )
    assert worker_pool.name == "pool-1"
    assert worker_pool.owner_id == owner_id
    assert worker_pool.scope == WorkerScope(kinds=["agent"])
    assert worker_pool.created is not None
    assert worker_pool.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second worker pool with the same name."""
    repository, owner_id = setup
    await repository.create(_worker_pool(owner_id, name="pool-1"))
    with pytest.raises(
        DuplicateWorkerPoolName,
        match="Worker pool name 'pool-1' is already registered",
    ):
        await repository.create(_worker_pool(owner_id, name="pool-1"))


async def test_get(setup: Setup) -> None:
    """Load a stored worker pool by id."""
    repository, owner_id = setup
    created = await repository.create(_worker_pool(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown worker pool id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        WorkerPoolNotFound, match=f"Worker pool {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_get_by_name(setup: Setup) -> None:
    """Load a worker pool by name."""
    repository, owner_id = setup
    created = await repository.create(_worker_pool(owner_id, name="pool-1"))
    loaded = await repository.get_by_name("pool-1")
    assert loaded == created


async def test_get_by_name_not_found(setup: Setup) -> None:
    """Raise for an unknown worker pool name."""
    repository, _ = setup
    with pytest.raises(WorkerPoolNotFound, match="Worker pool missing was not found"):
        await repository.get_by_name("missing")


async def test_query_filters_by_name(setup: Setup) -> None:
    """Filter worker pools by exact name."""
    repository, owner_id = setup
    await repository.create(_worker_pool(owner_id, name="alpha"))
    await repository.create(_worker_pool(owner_id, name="beta"))
    worker_pools, next_cursor = await repository.query(
        WorkerPoolFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="beta")
        )
    )
    assert next_cursor is None
    assert [worker_pool.name for worker_pool in worker_pools] == ["beta"]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id = setup
    created = [
        await repository.create(_worker_pool(owner_id, name=f"pool-{index}"))
        for index in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[WorkerPool] = []
    cursor = None
    while True:
        worker_pools, next_cursor = await repository.query(
            WorkerPoolFilter(cursor=cursor, size=2)
        )
        collected.extend(worker_pools)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id = setup
    created = await repository.create(
        _worker_pool(owner_id, scope=WorkerScope(kinds=["agent"]))
    )
    created.update_name("renamed")
    created.update_scope(WorkerScope(kinds=["importer"]))
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.scope == WorkerScope(kinds=["importer"])
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown worker pool id."""
    repository, owner_id = setup
    worker_pool = _worker_pool(owner_id)
    with pytest.raises(
        WorkerPoolNotFound, match=f"Worker pool {worker_pool.id} was not found"
    ):
        await repository.update(worker_pool)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a worker pool to a registered name."""
    repository, owner_id = setup
    await repository.create(_worker_pool(owner_id, name="alpha"))
    other = await repository.create(_worker_pool(owner_id, name="beta"))
    other.update_name("alpha")
    with pytest.raises(DuplicateWorkerPoolName):
        await repository.update(other)


async def test_delete(setup: Setup) -> None:
    """Delete a stored worker pool."""
    repository, owner_id = setup
    created = await repository.create(_worker_pool(owner_id))
    await repository.delete(created.id)
    with pytest.raises(WorkerPoolNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown worker pool id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        WorkerPoolNotFound, match=f"Worker pool {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_delete_in_use(worker_setup: WorkerSetup) -> None:
    """Reject deleting a worker pool a worker references."""
    repository, worker_repository, owner_id = worker_setup
    worker_pool = await repository.create(_worker_pool(owner_id))
    await worker_repository.register(_referencing_worker(owner_id, worker_pool.id))

    with pytest.raises(
        WorkerPoolInUse, match=f"Worker pool {worker_pool.id} is in use"
    ):
        await repository.delete(worker_pool.id)


async def test_delete_after_the_worker_is_deleted(worker_setup: WorkerSetup) -> None:
    """Delete a worker pool once the referencing worker is gone."""
    repository, worker_repository, owner_id = worker_setup
    worker_pool = await repository.create(_worker_pool(owner_id))
    worker = await worker_repository.register(
        _referencing_worker(owner_id, worker_pool.id)
    )
    await worker_repository.delete(worker.id)

    await repository.delete(worker_pool.id)
    with pytest.raises(WorkerPoolNotFound):
        await repository.get(worker_pool.id)


async def test_get_queue_stats_empty(task_setup: TaskSetup) -> None:
    """Report zero counts and no oldest pending task when nothing is queued."""
    stats = await task_setup.tasks.get_queue_stats(WorkerScope())
    assert stats == QueueStats(pending=0, in_flight=0, oldest_pending_created=None)


async def test_get_queue_stats_counts_pending_and_in_flight(
    task_setup: TaskSetup,
) -> None:
    """Count pending tasks separately from claimed and running tasks."""
    pending = await task_setup.tasks.create(_agent_task(task_setup))
    in_flight = await task_setup.tasks.create(_agent_task(task_setup))
    in_flight.claim(task_setup.worker_id, datetime.now(UTC))
    await task_setup.tasks.update(in_flight)

    stats = await task_setup.tasks.get_queue_stats(WorkerScope())
    assert stats.pending == 1
    assert stats.in_flight == 1
    assert stats.oldest_pending_created == pending.created


async def test_get_queue_stats_narrows_by_kind(task_setup: TaskSetup) -> None:
    """A kind scope only counts tasks of that kind."""
    await task_setup.tasks.create(_agent_task(task_setup))

    stats = await task_setup.tasks.get_queue_stats(
        WorkerScope(kinds=[TaskKind.IMPORTER])
    )
    assert stats.pending == 0

    stats = await task_setup.tasks.get_queue_stats(WorkerScope(kinds=[TaskKind.AGENT]))
    assert stats.pending == 1


async def test_get_queue_stats_narrows_by_required_selector(
    task_setup: TaskSetup,
) -> None:
    """A required selector only counts tasks carrying the matching label."""
    await task_setup.tasks.create(_agent_task(task_setup, labels={"env": "prod"}))
    await task_setup.tasks.create(_agent_task(task_setup, labels={"env": "dev"}))

    stats = await task_setup.tasks.get_queue_stats(
        WorkerScope(
            selectors=[LabelSelector(key="env", values=["prod"], required=True)]
        )
    )
    assert stats.pending == 1


async def test_get_queue_stats_oldest_created(task_setup: TaskSetup) -> None:
    """Report the earliest matching pending task's created time."""
    first = await task_setup.tasks.create(_agent_task(task_setup))
    await task_setup.tasks.create(_agent_task(task_setup))

    stats = await task_setup.tasks.get_queue_stats(WorkerScope())
    assert stats.oldest_pending_created == first.created


async def test_count_live_by_pool(worker_setup: WorkerSetup) -> None:
    """Count the pool's live workers and sum their concurrency."""
    repository, worker_repository, owner_id = worker_setup
    pool = await repository.create(_worker_pool(owner_id))
    other_pool = await repository.create(_worker_pool(owner_id, name="pool-2"))
    now = datetime.now(UTC)
    await worker_repository.register(
        _referencing_worker(
            owner_id, pool.id, name="live", concurrency=4, last_seen_at=now
        )
    )
    await worker_repository.register(
        _referencing_worker(
            owner_id,
            pool.id,
            name="stale",
            concurrency=8,
            last_seen_at=now - timedelta(hours=1),
        )
    )
    await worker_repository.register(
        _referencing_worker(
            owner_id, other_pool.id, name="other-pool", concurrency=2, last_seen_at=now
        )
    )

    live = await worker_repository.count_live_by_pool(
        pool.id, now - timedelta(minutes=1)
    )
    assert live.count == 1
    assert live.capacity == 4


async def test_count_live_by_pool_empty(worker_setup: WorkerSetup) -> None:
    """Report zero count and capacity for a pool with no workers."""
    repository, worker_repository, owner_id = worker_setup
    pool = await repository.create(_worker_pool(owner_id))
    live = await worker_repository.count_live_by_pool(pool.id, datetime.now(UTC))
    assert live.count == 0
    assert live.capacity == 0
