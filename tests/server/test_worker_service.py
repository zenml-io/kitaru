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
"""Tests for worker use cases."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from conftest import FakeWorkerRepository, create_worker
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.workers import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.job import WorkerScope
from kitaru.server.domain.worker import WorkerNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
def service(repository: FakeWorkerRepository) -> WorkerService:
    """Provide a worker service backed by the fake repository."""
    return WorkerService(repository=repository, liveness_timeout_seconds=60)


async def test_register_worker(service: WorkerService) -> None:
    """Register a worker owned by the caller."""
    version_id = uuid.uuid4()
    worker = await service.register_worker(
        name="runner",
        scope=WorkerScope(agent_version_ids=[version_id]),
        metadata={"hostname": "pool-1"},
        actor=ACTOR,
    )
    assert worker.name == "runner"
    assert worker.owner_id == ACTOR.account.id
    assert worker.scope.agent_version_ids == [version_id]
    assert worker.metadata == {"hostname": "pool-1"}
    assert worker.last_seen_at is not None
    assert worker.created is not None
    assert worker.updated is not None


async def test_register_worker_upserts_by_name(service: WorkerService) -> None:
    """Update an existing worker on a repeated registration."""
    created = await service.register_worker(
        name="runner",
        scope=WorkerScope(agent_version_ids=[uuid.uuid4()]),
        metadata={"hostname": "pool-1"},
        actor=ACTOR,
    )
    version_id = uuid.uuid4()
    registered = await service.register_worker(
        name="runner",
        scope=WorkerScope(agent_version_ids=[version_id]),
        metadata={"hostname": "pool-2"},
        actor=ACTOR,
    )
    assert registered.id == created.id
    assert registered.scope.agent_version_ids == [version_id]
    assert registered.metadata == {"hostname": "pool-2"}
    assert registered.last_seen_at > created.last_seen_at
    assert registered.created == created.created
    assert registered.updated is not None
    assert created.updated is not None
    assert registered.updated > created.updated


async def test_register_worker_after_duplicate_failure(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """Fall back to the update path when the name is already stored."""
    created = await create_worker(repository, ACTOR.account.id, name="runner")
    registered = await service.register_worker(
        name="runner", scope=WorkerScope(), metadata={}, actor=ACTOR
    )
    assert registered.id == created.id
    assert registered.last_seen_at > created.last_seen_at


async def test_get_worker(service: WorkerService) -> None:
    """Get a stored worker by id."""
    created = await service.register_worker(
        name="runner", scope=WorkerScope(), metadata={}, actor=ACTOR
    )
    loaded = await service.get_worker(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await service.get_worker(missing_id, actor=ACTOR)


async def test_list_workers(service: WorkerService) -> None:
    """List workers with filters and pagination."""
    version_id = uuid.uuid4()
    for name, version_ids in [("a", [version_id]), ("b", [uuid.uuid4()]), ("c", None)]:
        await service.register_worker(
            name=name,
            scope=WorkerScope(agent_version_ids=version_ids),
            metadata={},
            actor=ACTOR,
        )

    workers, total = await service.list_workers(WorkerFilter(), actor=ACTOR)
    assert total == 3
    assert [worker.name for worker in workers] == ["a", "b", "c"]

    workers, total = await service.list_workers(WorkerFilter(name="b"), actor=ACTOR)
    assert total == 1
    assert workers[0].name == "b"

    workers, total = await service.list_workers(
        WorkerFilter(agent_version_id=version_id), actor=ACTOR
    )
    assert total == 2
    assert [worker.name for worker in workers] == ["a", "c"]

    workers, total = await service.list_workers(
        WorkerFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [worker.name for worker in workers] == ["c"]


async def test_delete_worker(service: WorkerService) -> None:
    """Delete a stored worker."""
    created = await service.register_worker(
        name="runner", scope=WorkerScope(), metadata={}, actor=ACTOR
    )
    await service.delete_worker(created.id, actor=ACTOR)
    with pytest.raises(WorkerNotFound):
        await service.get_worker(created.id, actor=ACTOR)


async def test_delete_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await service.delete_worker(missing_id, actor=ACTOR)


async def test_worker_liveness(service: WorkerService) -> None:
    """Report liveness from the last seen time and the timeout."""
    worker = await service.register_worker(
        name="runner", scope=WorkerScope(), metadata={}, actor=ACTOR
    )
    assert worker.is_live(service.liveness_timeout_seconds)
    stale = worker.model_copy(
        update={"last_seen_at": datetime.now(UTC) - timedelta(seconds=61)}
    )
    assert not stale.is_live(service.liveness_timeout_seconds)
