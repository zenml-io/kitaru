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

import pytest

from conftest import FakeWorkerPoolRepository, pg_session, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.worker_pool_repository import (
    SQLWorkerPoolRepository,
)
from kitaru.server.application.interfaces.worker_pool_repository import (
    WorkerPoolRepository,
)
from kitaru.server.application.models.worker_pool import WorkerPoolFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.worker_pool import (
    DuplicateWorkerPoolName,
    WorkerPool,
    WorkerPoolNotFound,
)
from kitaru.server.filtering import FilterCondition

Setup = tuple[WorkerPoolRepository, uuid.UUID]


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
