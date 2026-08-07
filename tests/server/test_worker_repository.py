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
"""Contract tests for worker repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import (
    FakeTaskRepository,
    FakeWorkerRepository,
    pg_session,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskStatus
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
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.job import Job
from kitaru.server.domain.task import AgentTask
from kitaru.server.domain.worker import Worker, WorkerNotFound
from kitaru.server.filtering import FilterCondition

Setup = tuple[WorkerRepository, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each worker repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeWorkerRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield SQLWorkerRepository(session), owner.id


class PruneSetup(NamedTuple):
    """Worker repository under test, plus a task repository and a ready job."""

    workers: WorkerRepository
    tasks: TaskRepository
    owner_id: uuid.UUID
    job_id: uuid.UUID
    agent_version_id: uuid.UUID


async def _seed_postgres_prune_setup(session: AsyncSession) -> PruneSetup:
    """Create the account, agent version, and job rows a task can reference.

    Args:
        session: Database session.

    Returns:
        Worker and task repositories and the ids of the rows a task can
        point at.
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
    return PruneSetup(
        workers=SQLWorkerRepository(session),
        tasks=SQLTaskRepository(session),
        owner_id=owner.id,
        job_id=job.id,
        agent_version_id=agent_version.id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def prune_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[PruneSetup, None]:
    """Provide a worker repository wired to a task repository with a ready job."""
    if request.param == "fake":
        tasks = FakeTaskRepository()
        yield PruneSetup(
            workers=FakeWorkerRepository(tasks=tasks),
            tasks=tasks,
            owner_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        yield await _seed_postgres_prune_setup(session)


def _worker(
    owner_id: uuid.UUID,
    name: str = "worker-1",
    scope: WorkerScope | None = None,
    runtime: WorkerRuntime | None = None,
    metadata: dict[str, str] | None = None,
    last_seen_at: datetime | None = None,
) -> Worker:
    """Build a worker for repository tests.

    Args:
        owner_id: Id of the owning account.
        name: Worker name.
        scope: Claim scope the worker reports.
        runtime: Runtime the worker reports.
        metadata: Arbitrary metadata.
        last_seen_at: Time of the worker's last heartbeat.

    Returns:
        Unstored worker.
    """
    return Worker(
        owner_id=owner_id,
        name=name,
        scope=scope if scope is not None else WorkerScope(),
        runtime=runtime if runtime is not None else WorkerRuntime(platform="bare"),
        metadata=metadata if metadata is not None else {},
        last_seen_at=last_seen_at if last_seen_at is not None else datetime.now(UTC),
    )


def _agent_task(setup: PruneSetup, **overrides: Any) -> AgentTask:
    """Build an agent task pointed at the setup's agent version.

    Args:
        setup: Prune repository setup.
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


async def test_register_sets_timestamps(setup: Setup) -> None:
    """Store a new worker with both timestamps set."""
    repository, owner_id = setup
    worker = await repository.register(_worker(owner_id))
    assert worker.name == "worker-1"
    assert worker.owner_id == owner_id
    assert worker.created is not None
    assert worker.updated is not None


async def test_register_upsert_keeps_id_and_created(setup: Setup) -> None:
    """Re-registering under the same name keeps the id and created time."""
    repository, owner_id = setup
    first = await repository.register(
        _worker(owner_id, scope=WorkerScope(kinds=["agent"]))
    )
    second = await repository.register(
        _worker(owner_id, scope=WorkerScope(kinds=["importer"]))
    )
    assert second.id == first.id
    assert second.created == first.created
    assert second.scope == WorkerScope(kinds=["importer"])


async def test_register_upsert_renews_updated_and_last_seen_at(setup: Setup) -> None:
    """Re-registering renews the updated timestamp and last_seen_at."""
    repository, owner_id = setup
    first = await repository.register(_worker(owner_id))
    second = await repository.register(
        _worker(owner_id, last_seen_at=datetime.now(UTC) + timedelta(seconds=1))
    )
    assert second.updated is not None
    assert first.updated is not None
    assert second.updated > first.updated
    assert second.last_seen_at > first.last_seen_at


async def test_register_upsert_refreshes_scope_runtime_and_metadata(
    setup: Setup,
) -> None:
    """Re-registering replaces the scope, runtime, and metadata."""
    repository, owner_id = setup
    await repository.register(
        _worker(
            owner_id,
            scope=WorkerScope(kinds=["agent"]),
            runtime=WorkerRuntime(platform="bare"),
            metadata={"region": "eu"},
        )
    )
    second = await repository.register(
        _worker(
            owner_id,
            scope=WorkerScope(kinds=["importer"]),
            runtime=WorkerRuntime(platform="docker", hostname="worker-a"),
            metadata={"region": "us"},
        )
    )
    assert second.scope == WorkerScope(kinds=["importer"])
    assert second.runtime == WorkerRuntime(platform="docker", hostname="worker-a")
    assert second.metadata == {"region": "us"}


async def test_register_upsert_distinct_names_coexist(setup: Setup) -> None:
    """Registering under a different name creates a separate worker."""
    repository, owner_id = setup
    first = await repository.register(_worker(owner_id, name="worker-1"))
    second = await repository.register(_worker(owner_id, name="worker-2"))
    assert first.id != second.id


async def test_get(setup: Setup) -> None:
    """Load a stored worker by id."""
    repository, owner_id = setup
    created = await repository.register(_worker(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query workers newest-first with filters."""
    repository, owner_id = setup
    first = await repository.register(_worker(owner_id, name="worker-1"))
    await repository.register(_worker(owner_id, name="worker-2"))
    third = await repository.register(_worker(owner_id, name="worker-3"))

    workers, next_cursor = await repository.query(WorkerFilter())
    assert next_cursor is None
    assert [worker.name for worker in workers] == ["worker-3", "worker-2", "worker-1"]

    workers, next_cursor = await repository.query(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="worker-1")
        )
    )
    assert next_cursor is None
    assert workers[0] == first

    workers, next_cursor = await repository.query(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        )
    )
    assert next_cursor is None
    assert workers == []
    assert third.name == "worker-3"


async def test_query_seen_after(setup: Setup) -> None:
    """Filter workers by last_seen_at, an internal filter field."""
    repository, owner_id = setup
    now = datetime.now(UTC)
    await repository.register(
        _worker(owner_id, name="stale", last_seen_at=now - timedelta(hours=1))
    )
    fresh = await repository.register(_worker(owner_id, name="fresh", last_seen_at=now))

    workers, next_cursor = await repository.query(
        WorkerFilter(seen_after=now - timedelta(minutes=1))
    )
    assert next_cursor is None
    assert [worker.id for worker in workers] == [fresh.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id = setup
    created = [
        await repository.register(_worker(owner_id, name=f"worker-{i}"))
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Worker] = []
    cursor = None
    while True:
        workers, next_cursor = await repository.query(
            WorkerFilter(cursor=cursor, size=2)
        )
        collected.extend(workers)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({worker.id for worker in collected}) == 5


async def test_query_invalid_cursor(setup: Setup) -> None:
    """Raise for a cursor string that fails to decode."""
    repository, _ = setup
    with pytest.raises(ValidationError):
        await repository.query(WorkerFilter(cursor="not-a-valid-cursor"))


async def test_delete(setup: Setup) -> None:
    """Delete a stored worker."""
    repository, owner_id = setup
    created = await repository.register(_worker(owner_id))
    await repository.delete(created.id)
    with pytest.raises(WorkerNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_stale_deletes_an_unreferenced_worker(
    prune_setup: PruneSetup,
) -> None:
    """A stale worker with no in-flight task is deleted."""
    now = datetime.now(UTC)
    stale = await prune_setup.workers.register(
        _worker(prune_setup.owner_id, last_seen_at=now - timedelta(days=2))
    )

    deleted = await prune_setup.workers.delete_stale(now, 100)

    assert deleted == 1
    with pytest.raises(WorkerNotFound):
        await prune_setup.workers.get(stale.id)


async def test_delete_stale_keeps_a_worker_with_an_in_flight_task(
    prune_setup: PruneSetup,
) -> None:
    """A stale worker still claimed by an in-flight task survives the prune."""
    now = datetime.now(UTC)
    worker = await prune_setup.workers.register(
        _worker(prune_setup.owner_id, last_seen_at=now - timedelta(days=2))
    )
    task = await prune_setup.tasks.create(_agent_task(prune_setup))
    task.claim(worker.id, now)
    await prune_setup.tasks.update(task)

    deleted = await prune_setup.workers.delete_stale(now, 100)

    assert deleted == 0
    assert await prune_setup.workers.get(worker.id) == worker


async def test_delete_stale_keeps_a_worker_seen_after_the_cutoff(
    prune_setup: PruneSetup,
) -> None:
    """A worker seen at or after the cutoff survives the prune."""
    now = datetime.now(UTC)
    live = await prune_setup.workers.register(
        _worker(prune_setup.owner_id, last_seen_at=now)
    )

    deleted = await prune_setup.workers.delete_stale(now - timedelta(days=1), 100)

    assert deleted == 0
    assert await prune_setup.workers.get(live.id) == live


async def test_delete_stale_respects_the_limit(prune_setup: PruneSetup) -> None:
    """A limit narrower than the stale set deletes only up to that many."""
    now = datetime.now(UTC)
    stale_time = now - timedelta(days=2)
    for i in range(3):
        await prune_setup.workers.register(
            _worker(prune_setup.owner_id, name=f"worker-{i}", last_seen_at=stale_time)
        )

    deleted = await prune_setup.workers.delete_stale(now, 1)

    assert deleted == 1
    remaining, _ = await prune_setup.workers.query(WorkerFilter())
    assert len(remaining) == 2


async def test_delete_stale_nulls_the_worker_id_on_a_terminal_task() -> None:
    """A pruned worker's id is nulled on a terminal task, whose row survives."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        setup = await _seed_postgres_prune_setup(session)
        now = datetime.now(UTC)
        worker = await setup.workers.register(
            _worker(setup.owner_id, last_seen_at=now - timedelta(days=2))
        )
        task = await setup.tasks.create(_agent_task(setup))
        task.claim(worker.id, now)
        task.fail(error="boom", result=None, now=now)
        stored = await setup.tasks.update(task)

        deleted = await setup.workers.delete_stale(now, 100)
        assert deleted == 1
        await session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as verify_session:
            reloaded = await SQLTaskRepository(verify_session).get(stored.id)
            assert reloaded.worker_id is None
            assert reloaded.status is TaskStatus.FAILED
            with pytest.raises(WorkerNotFound):
                await SQLWorkerRepository(verify_session).get(worker.id)


async def test_scope_round_trip_selectors_and_job_pin(setup: Setup) -> None:
    """Round-trip a scope carrying selectors, kinds, and a job pin through JSONB."""
    repository, owner_id = setup
    job_id = uuid.uuid4()
    scope = WorkerScope(
        kinds=["agent", "evaluator"],
        selectors=[
            LabelSelector(key="agent_version", values=["v1", "v2"]),
            LabelSelector(key="team", values=["core"], required=True),
        ],
        job_id=job_id,
    )
    created = await repository.register(_worker(owner_id, scope=scope))
    loaded = await repository.get(created.id)
    assert loaded.scope == scope
    assert loaded.scope.job_id == job_id


async def test_runtime_round_trip(setup: Setup) -> None:
    """Round-trip a runtime object through JSONB."""
    repository, owner_id = setup
    runtime = WorkerRuntime(
        platform="kubernetes",
        hostname="pod-1",
        os="linux",
        arch="arm64",
        python_version="3.12.0",
        kitaru_version="0.1.0",
        namespace="default",
        pod="pod-1",
    )
    created = await repository.register(_worker(owner_id, runtime=runtime))
    loaded = await repository.get(created.id)
    assert loaded.runtime == runtime
