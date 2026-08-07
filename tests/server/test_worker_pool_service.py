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
"""Tests for worker pool use cases."""

import uuid

import pytest

from conftest import FakeWorkerPoolRepository
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.worker import WorkerScope
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.worker_pool import (
    WorkerPoolFilter,
    WorkerPoolUpdate,
)
from kitaru.server.application.services.worker_pool_service import WorkerPoolService
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.worker_pool import (
    DuplicateWorkerPoolName,
    WorkerPoolNotFound,
    WorkerPoolScopePinsJob,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def repository() -> FakeWorkerPoolRepository:
    """Provide a fake worker pool repository."""
    return FakeWorkerPoolRepository()


@pytest.fixture
def service(repository: FakeWorkerPoolRepository) -> WorkerPoolService:
    """Provide a worker pool service backed by the fake repository."""
    return WorkerPoolService(repository=repository)


async def test_create_worker_pool(service: WorkerPoolService) -> None:
    """Create a new worker pool."""
    worker_pool = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(kinds=["agent"]), actor=ACTOR
    )
    assert worker_pool.name == "pool-1"
    assert worker_pool.owner_id == ACTOR.account.id
    assert worker_pool.scope == WorkerScope(kinds=["agent"])
    assert worker_pool.created is not None
    assert worker_pool.updated is not None


async def test_create_worker_pool_duplicate_name(service: WorkerPoolService) -> None:
    """Reject a second worker pool with the same name."""
    await service.create_worker_pool(name="pool-1", scope=WorkerScope(), actor=ACTOR)
    with pytest.raises(
        DuplicateWorkerPoolName,
        match="Worker pool name 'pool-1' is already registered",
    ):
        await service.create_worker_pool(
            name="pool-1", scope=WorkerScope(), actor=ACTOR
        )


async def test_create_worker_pool_scope_pins_job(service: WorkerPoolService) -> None:
    """Reject a scope that names a job."""
    with pytest.raises(WorkerPoolScopePinsJob):
        await service.create_worker_pool(
            name="pool-1", scope=WorkerScope(job_id=uuid.uuid4()), actor=ACTOR
        )


async def test_get_worker_pool(service: WorkerPoolService) -> None:
    """Load a stored worker pool by id."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    loaded = await service.get_worker_pool(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_worker_pool_not_found(service: WorkerPoolService) -> None:
    """Raise for an unknown worker pool id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        WorkerPoolNotFound, match=f"Worker pool {missing_id} was not found"
    ):
        await service.get_worker_pool(missing_id, actor=ACTOR)


async def test_list_worker_pools(service: WorkerPoolService) -> None:
    """List worker pools newest-first with a name filter."""
    for name in ["pool-1", "pool-2", "pool-3"]:
        await service.create_worker_pool(name=name, scope=WorkerScope(), actor=ACTOR)

    worker_pools, next_cursor = await service.list_worker_pools(
        WorkerPoolFilter(), actor=ACTOR
    )
    assert next_cursor is None
    assert [worker_pool.name for worker_pool in worker_pools] == [
        "pool-3",
        "pool-2",
        "pool-1",
    ]

    worker_pools, next_cursor = await service.list_worker_pools(
        WorkerPoolFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="pool-2")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert worker_pools[0].name == "pool-2"


async def test_update_worker_pool_name_and_scope(service: WorkerPoolService) -> None:
    """Update a worker pool's name and scope."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(kinds=["agent"]), actor=ACTOR
    )
    updated = await service.update_worker_pool(
        created.id,
        WorkerPoolUpdate(name="renamed", scope=WorkerScope(kinds=["importer"])),
        actor=ACTOR,
    )
    assert updated.name == "renamed"
    assert updated.scope == WorkerScope(kinds=["importer"])
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_worker_pool_cannot_clear_name(
    service: WorkerPoolService,
) -> None:
    """Reject an update that clears the worker pool name."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    with pytest.raises(ValidationError, match="Worker pool name cannot be cleared"):
        await service.update_worker_pool(
            created.id, WorkerPoolUpdate(name=None), actor=ACTOR
        )


async def test_update_worker_pool_cannot_clear_scope(
    service: WorkerPoolService,
) -> None:
    """Reject an update that clears the worker pool scope."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    with pytest.raises(ValidationError, match="Worker pool scope cannot be cleared"):
        await service.update_worker_pool(
            created.id, WorkerPoolUpdate(scope=None), actor=ACTOR
        )


async def test_update_worker_pool_scope_pins_job(service: WorkerPoolService) -> None:
    """Reject an update whose new scope names a job."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    with pytest.raises(WorkerPoolScopePinsJob):
        await service.update_worker_pool(
            created.id,
            WorkerPoolUpdate(scope=WorkerScope(job_id=uuid.uuid4())),
            actor=ACTOR,
        )


async def test_update_worker_pool_not_found(service: WorkerPoolService) -> None:
    """Raise for an unknown worker pool id."""
    with pytest.raises(WorkerPoolNotFound):
        await service.update_worker_pool(
            uuid.uuid4(), WorkerPoolUpdate(name="renamed"), actor=ACTOR
        )


async def test_delete_worker_pool(service: WorkerPoolService) -> None:
    """Delete a stored worker pool."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    await service.delete_worker_pool(created.id, actor=ACTOR)
    with pytest.raises(WorkerPoolNotFound):
        await service.get_worker_pool(created.id, actor=ACTOR)


async def test_delete_worker_pool_not_found(service: WorkerPoolService) -> None:
    """Raise for an unknown worker pool id."""
    with pytest.raises(WorkerPoolNotFound):
        await service.delete_worker_pool(uuid.uuid4(), actor=ACTOR)
