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
from datetime import UTC, datetime
from typing import Any

import pytest

from conftest import FakeWorkerRepository, pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.worker_repository import (
    WorkerRepository,
)
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.job import WorkerScope
from kitaru.server.domain.worker import (
    DuplicateWorkerName,
    Worker,
    WorkerNotFound,
)

Setup = tuple[WorkerRepository, uuid.UUID]


def worker(
    owner_id: uuid.UUID,
    name: str = "runner",
    scope: WorkerScope | None = None,
    metadata: dict[str, Any] | None = None,
) -> Worker:
    """Build an unstored worker.

    Args:
        owner_id: Id of the owning account.
        name: Worker name.
        scope: Claim scope.
        metadata: Worker metadata.

    Returns:
        Worker with the last seen time set to now.
    """
    return Worker(
        owner_id=owner_id,
        name=name,
        scope=scope or WorkerScope(),
        last_seen_at=datetime.now(UTC),
        metadata=metadata or {},
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each worker repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeWorkerRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield SQLWorkerRepository(session), owner.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new worker with both timestamps set."""
    repository, owner_id = setup
    version_id = uuid.uuid4()
    created = await repository.create(
        worker(
            owner_id,
            scope=WorkerScope(agent_version_ids=[version_id]),
            metadata={"hostname": "pool-1"},
        )
    )
    assert created.name == "runner"
    assert created.owner_id == owner_id
    assert created.scope.agent_version_ids == [version_id]
    assert created.metadata == {"hostname": "pool-1"}
    assert created.created is not None
    assert created.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second worker with the same name."""
    repository, owner_id = setup
    await repository.create(worker(owner_id))
    with pytest.raises(
        DuplicateWorkerName, match="Worker name 'runner' is already registered"
    ):
        await repository.create(worker(owner_id))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id = setup
    await repository.create(worker(owner_id))
    with pytest.raises(DuplicateWorkerName):
        await repository.create(worker(owner_id))
    created = await repository.create(worker(owner_id, name="other"))
    assert created.name == "other"


async def test_get(setup: Setup) -> None:
    """Load a stored worker by id."""
    repository, owner_id = setup
    created = await repository.create(worker(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_by_name(setup: Setup) -> None:
    """Load a stored worker by name."""
    repository, owner_id = setup
    created = await repository.create(worker(owner_id))
    loaded = await repository.get_by_name("runner")
    assert loaded == created


async def test_get_by_name_not_found(setup: Setup) -> None:
    """Raise for an unknown worker name."""
    repository, _ = setup
    with pytest.raises(WorkerNotFound, match="Worker missing was not found"):
        await repository.get_by_name("missing")


async def test_query(setup: Setup) -> None:
    """Query workers with filters and pagination."""
    repository, owner_id = setup
    a = await repository.create(worker(owner_id, name="a"))
    await repository.create(worker(owner_id, name="b"))
    await repository.create(worker(owner_id, name="c"))

    workers, total = await repository.query(WorkerFilter())
    assert total == 3
    assert [item.name for item in workers] == ["a", "b", "c"]

    workers, total = await repository.query(WorkerFilter(name="a"))
    assert total == 1
    assert workers[0] == a

    workers, total = await repository.query(WorkerFilter(page=2, page_size=2))
    assert total == 3
    assert [item.name for item in workers] == ["c"]

    workers, total = await repository.query(WorkerFilter(name="missing"))
    assert total == 0
    assert workers == []


async def test_query_agent_version_id_filter(setup: Setup) -> None:
    """Query workers serving an agent, including catch-all workers."""
    repository, owner_id = setup
    version_id = uuid.uuid4()
    await repository.create(
        worker(
            owner_id, name="serving", scope=WorkerScope(agent_version_ids=[version_id])
        )
    )
    await repository.create(
        worker(
            owner_id, name="other", scope=WorkerScope(agent_version_ids=[uuid.uuid4()])
        )
    )
    await repository.create(worker(owner_id, name="catch-all"))

    workers, total = await repository.query(WorkerFilter(agent_version_id=version_id))
    assert total == 2
    assert [item.name for item in workers] == ["serving", "catch-all"]

    workers, total = await repository.query(WorkerFilter(agent_version_id=uuid.uuid4()))
    assert total == 1
    assert [item.name for item in workers] == ["catch-all"]


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id = setup
    created = await repository.create(worker(owner_id))
    version_id = uuid.uuid4()
    created.refresh(
        scope=WorkerScope(agent_version_ids=[version_id]),
        metadata={"hostname": "pool-2"},
    )
    updated = await repository.update(created)
    assert updated.scope.agent_version_ids == [version_id]
    assert updated.metadata == {"hostname": "pool-2"}
    assert updated.last_seen_at == created.last_seen_at
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, owner_id = setup
    unstored = worker(owner_id)
    with pytest.raises(WorkerNotFound, match=f"Worker {unstored.id} was not found"):
        await repository.update(unstored)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a worker to a registered name."""
    repository, owner_id = setup
    await repository.create(worker(owner_id))
    other = await repository.create(worker(owner_id, name="other"))
    other.name = "runner"
    with pytest.raises(
        DuplicateWorkerName, match="Worker name 'runner' is already registered"
    ):
        await repository.update(other)


async def test_touch(setup: Setup) -> None:
    """Bump only the last seen time of a stored worker."""
    repository, owner_id = setup
    version_id = uuid.uuid4()
    created = await repository.create(
        worker(
            owner_id,
            scope=WorkerScope(agent_version_ids=[version_id]),
            metadata={"hostname": "pool-1"},
        )
    )
    last_seen_at = datetime.now(UTC)
    await repository.touch(created.id, last_seen_at)
    loaded = await repository.get(created.id)
    assert loaded.last_seen_at == last_seen_at
    assert loaded.scope.agent_version_ids == [version_id]
    assert loaded.metadata == {"hostname": "pool-1"}
    assert loaded.updated is not None
    assert created.updated is not None
    assert loaded.updated > created.updated


async def test_touch_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await repository.touch(missing_id, datetime.now(UTC))


async def test_delete(setup: Setup) -> None:
    """Delete a stored worker."""
    repository, owner_id = setup
    created = await repository.create(worker(owner_id))
    await repository.delete(created.id)
    with pytest.raises(WorkerNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown worker id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await repository.delete(missing_id)
