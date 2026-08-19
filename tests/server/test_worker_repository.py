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

import pytest

from conftest import (
    UNSCOPED_WORKER_SCOPE,
    FakeWorkerRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    LabelSelector,
    WorkerClaim,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.worker_repository import WorkerRepository
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.worker import Worker, WorkerNotFound
from kitaru.server.filtering import FilterCondition

Setup = tuple[WorkerRepository, uuid.UUID]

_CUTOFF = datetime.now(UTC) - timedelta(minutes=1)


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
        scope=scope if scope is not None else UNSCOPED_WORKER_SCOPE,
        runtime=runtime if runtime is not None else WorkerRuntime(platform="bare"),
        metadata=metadata if metadata is not None else {},
        last_seen_at=last_seen_at if last_seen_at is not None else datetime.now(UTC),
    )


async def test_register_sets_timestamps(setup: Setup) -> None:
    """Store a new worker with both timestamps set."""
    repository, owner_id = setup
    worker = await repository.register(_worker(owner_id))
    assert worker.name == "worker-1"
    assert worker.owner_id == owner_id
    assert worker.created is not None
    assert worker.updated is not None


async def test_register_same_name_creates_a_second_worker(setup: Setup) -> None:
    """Registering under an existing name creates a separate worker."""
    repository, owner_id = setup
    first = await repository.register(
        _worker(owner_id, scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]))
    )
    second = await repository.register(
        _worker(
            owner_id, scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)])
        )
    )
    assert second.id != first.id
    assert (await repository.get(first.id)).scope == first.scope
    assert (await repository.get(second.id)).scope == second.scope


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

    workers, next_cursor = await repository.query(WorkerFilter(), _CUTOFF)
    assert next_cursor is None
    assert [worker.name for worker in workers] == ["worker-3", "worker-2", "worker-1"]

    workers, next_cursor = await repository.query(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="worker-1")
        ),
        _CUTOFF,
    )
    assert next_cursor is None
    assert workers[0] == first

    workers, next_cursor = await repository.query(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        ),
        _CUTOFF,
    )
    assert next_cursor is None
    assert workers == []
    assert third.name == "worker-3"


async def test_query_hides_stale_unless_included(setup: Setup) -> None:
    """Leave workers past the cutoff out unless no cutoff is given."""
    repository, owner_id = setup
    now = datetime.now(UTC)
    stale = await repository.register(
        _worker(owner_id, name="stale", last_seen_at=now - timedelta(hours=1))
    )
    fresh = await repository.register(_worker(owner_id, name="fresh", last_seen_at=now))
    cutoff = now - timedelta(minutes=1)

    workers, next_cursor = await repository.query(WorkerFilter(), cutoff)
    assert next_cursor is None
    assert [worker.id for worker in workers] == [fresh.id]

    workers, _ = await repository.query(WorkerFilter(), None)
    assert {worker.id for worker in workers} == {stale.id, fresh.id}


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
            WorkerFilter(cursor=cursor, size=2), _CUTOFF
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
        await repository.query(WorkerFilter(cursor="not-a-valid-cursor"), _CUTOFF)


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


async def test_scope_round_trip_selectors_and_job_pin(setup: Setup) -> None:
    """Round-trip a scope carrying claims, selectors, and a job pin through JSONB."""
    repository, owner_id = setup
    job_id = uuid.uuid4()
    scope = WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.AGENT, agent_version_id=uuid.uuid4()),
            WorkerClaim(kind=TaskKind.EVALUATOR),
        ],
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
