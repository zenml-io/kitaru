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
from datetime import UTC, datetime, timedelta

import pytest

from conftest import (
    FakeTaskRepository,
    FakeWorkerPoolRepository,
    FakeWorkerRepository,
    create_agent_task,
    create_import_task,
    create_worker,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerScope
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
    WorkerPoolStats,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
LIVENESS_TIMEOUT_SECONDS = 60


@pytest.fixture
def repository() -> FakeWorkerPoolRepository:
    """Provide a fake worker pool repository."""
    return FakeWorkerPoolRepository()


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide a fake task repository."""
    return FakeTaskRepository()


@pytest.fixture
def worker_repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
def service(
    repository: FakeWorkerPoolRepository,
    task_repository: FakeTaskRepository,
    worker_repository: FakeWorkerRepository,
) -> WorkerPoolService:
    """Provide a worker pool service backed by the fake repositories."""
    return WorkerPoolService(
        repository=repository,
        task_repository=task_repository,
        worker_repository=worker_repository,
        liveness_timeout_seconds=LIVENESS_TIMEOUT_SECONDS,
    )


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


async def test_get_worker_pool_stats_empty_pool(service: WorkerPoolService) -> None:
    """Report zero counts and no oldest pending task for an empty pool."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    worker_pool, stats = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert worker_pool == created
    assert stats == WorkerPoolStats(
        pending_tasks=0,
        in_flight_tasks=0,
        oldest_pending_seconds=None,
        live_workers=0,
    )


async def test_get_worker_pool_stats_pending_matches_scope(
    service: WorkerPoolService, task_repository: FakeTaskRepository
) -> None:
    """Count only pending tasks the pool's scope matches, by kind and label."""
    created = await service.create_worker_pool(
        name="pool-1",
        scope=WorkerScope(
            kinds=[TaskKind.AGENT],
            selectors=[LabelSelector(key="env", values=["prod"], required=True)],
        ),
        actor=ACTOR,
    )
    await create_agent_task(task_repository, uuid.uuid4(), labels={"env": "prod"})
    await create_agent_task(task_repository, uuid.uuid4(), labels={"env": "dev"})
    await create_import_task(task_repository, uuid.uuid4())

    _, stats = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert stats.pending_tasks == 1


async def test_get_worker_pool_stats_in_flight_excludes_terminal(
    service: WorkerPoolService, task_repository: FakeTaskRepository
) -> None:
    """Count claimed and running tasks, excluding terminal statuses."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    worker_id = uuid.uuid4()
    now = datetime.now(UTC)

    claimed = await create_agent_task(task_repository, uuid.uuid4())
    claimed.claim(worker_id, now)
    await task_repository.update(claimed)

    running = await create_agent_task(task_repository, uuid.uuid4())
    running.claim(worker_id, now)
    running.start(now)
    await task_repository.update(running)

    completed = await create_agent_task(task_repository, uuid.uuid4())
    completed.claim(worker_id, now)
    completed.start(now)
    completed.link_result_session(uuid.uuid4())
    completed.complete(None, now)
    await task_repository.update(completed)

    _, stats = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert stats.pending_tasks == 0
    assert stats.in_flight_tasks == 2


async def test_get_worker_pool_stats_oldest_pending_seconds(
    service: WorkerPoolService, task_repository: FakeTaskRepository
) -> None:
    """Derive the oldest pending task's age in seconds."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    await create_agent_task(task_repository, uuid.uuid4())

    _, stats = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert stats.oldest_pending_seconds is not None
    assert stats.oldest_pending_seconds >= 0


async def test_get_worker_pool_stats_live_workers_honors_cutoff(
    service: WorkerPoolService, worker_repository: FakeWorkerRepository
) -> None:
    """Count only live workers joined to the pool."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    other_pool = await service.create_worker_pool(
        name="pool-2", scope=WorkerScope(), actor=ACTOR
    )
    now = datetime.now(UTC)
    await create_worker(
        worker_repository,
        ACTOR.account.id,
        name="live",
        pool_id=created.id,
        last_seen_at=now,
    )
    await create_worker(
        worker_repository,
        ACTOR.account.id,
        name="stale",
        pool_id=created.id,
        last_seen_at=now - timedelta(hours=1),
    )
    await create_worker(
        worker_repository,
        ACTOR.account.id,
        name="other-pool",
        pool_id=other_pool.id,
        last_seen_at=now,
    )

    _, stats = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert stats.live_workers == 1


async def test_get_worker_pool_stats_resolves_by_name(
    service: WorkerPoolService,
) -> None:
    """Resolve by name when the reference does not parse as a UUID."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    worker_pool, _ = await service.get_worker_pool_stats("pool-1", actor=ACTOR)
    assert worker_pool == created


async def test_get_worker_pool_stats_resolves_by_id(service: WorkerPoolService) -> None:
    """Resolve by id when the reference parses as a UUID."""
    created = await service.create_worker_pool(
        name="pool-1", scope=WorkerScope(), actor=ACTOR
    )
    worker_pool, _ = await service.get_worker_pool_stats(created.id, actor=ACTOR)
    assert worker_pool == created


async def test_get_worker_pool_stats_not_found(service: WorkerPoolService) -> None:
    """Raise for an unknown worker pool id or name."""
    with pytest.raises(WorkerPoolNotFound):
        await service.get_worker_pool_stats(uuid.uuid4(), actor=ACTOR)
    with pytest.raises(WorkerPoolNotFound):
        await service.get_worker_pool_stats("missing", actor=ACTOR)


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
