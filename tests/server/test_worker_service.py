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
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.worker import LabelSelector, WorkerRuntime, WorkerScope
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.worker import WorkerNotFound
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
def service(repository: FakeWorkerRepository) -> WorkerService:
    """Provide a worker service backed by the fake repository."""
    return WorkerService(repository=repository)


async def test_register_worker(service: WorkerService) -> None:
    """Register a new worker."""
    worker = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(),
        runtime=WorkerRuntime(platform="bare"),
        metadata={"region": "eu"},
        actor=ACTOR,
    )
    assert worker.name == "worker-1"
    assert worker.owner_id == ACTOR.account.id
    assert worker.metadata == {"region": "eu"}
    assert worker.created is not None
    assert worker.updated is not None


async def test_register_worker_upsert_keeps_id_and_renews_timestamps(
    service: WorkerService,
) -> None:
    """Re-registering under the same name keeps id and created, renews updated."""
    first = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(kinds=["agent"]),
        runtime=WorkerRuntime(platform="bare"),
        metadata={"region": "eu"},
        actor=ACTOR,
    )
    second = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(kinds=["importer"]),
        runtime=WorkerRuntime(platform="docker"),
        metadata={"region": "us"},
        actor=ACTOR,
    )
    assert second.id == first.id
    assert second.created == first.created
    assert second.scope == WorkerScope(kinds=["importer"])
    assert second.runtime.platform == "docker"
    assert second.metadata == {"region": "us"}
    assert second.last_seen_at >= first.last_seen_at
    assert second.updated is not None
    assert first.updated is not None
    assert second.updated > first.updated


async def test_get_worker(service: WorkerService) -> None:
    """Load a stored worker by id."""
    created = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(),
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    loaded = await service.get_worker(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await service.get_worker(missing_id, actor=ACTOR)


async def test_list_workers(service: WorkerService) -> None:
    """List workers newest-first with filters."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await service.register_worker(
            name=name,
            scope=WorkerScope(),
            runtime=WorkerRuntime(platform="bare"),
            metadata={},
            actor=ACTOR,
        )

    workers, next_cursor = await service.list_workers(WorkerFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [worker.name for worker in workers] == ["worker-3", "worker-2", "worker-1"]

    workers, next_cursor = await service.list_workers(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="worker-2")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert workers[0].name == "worker-2"


async def test_list_workers_seen_after(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """Filter workers by last_seen_at, an internal filter field."""
    now = datetime.now(UTC)
    stale = await create_worker(
        repository,
        ACTOR.account.id,
        name="stale",
        last_seen_at=now - timedelta(hours=1),
    )
    fresh = await create_worker(
        repository, ACTOR.account.id, name="fresh", last_seen_at=now
    )

    workers, next_cursor = await service.list_workers(
        WorkerFilter(seen_after=now - timedelta(minutes=1)), actor=ACTOR
    )
    assert next_cursor is None
    assert [worker.id for worker in workers] == [fresh.id]
    assert stale.id not in [worker.id for worker in workers]


async def test_delete_worker(service: WorkerService) -> None:
    """Delete a stored worker."""
    created = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(),
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    await service.delete_worker(created.id, actor=ACTOR)
    with pytest.raises(WorkerNotFound):
        await service.get_worker(created.id, actor=ACTOR)


async def test_delete_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    with pytest.raises(WorkerNotFound):
        await service.delete_worker(uuid.uuid4(), actor=ACTOR)


async def test_worker_scope_round_trip(service: WorkerService) -> None:
    """Round-trip a scope carrying selectors and a job pin."""
    scope = WorkerScope(
        kinds=["agent", "evaluator"],
        selectors=[LabelSelector(key="agent_version", values=["v1", "v2"])],
        job_id=uuid.uuid4(),
    )
    created = await service.register_worker(
        name="worker-1",
        scope=scope,
        runtime=WorkerRuntime(platform="kubernetes", namespace="default"),
        metadata={},
        actor=ACTOR,
    )
    loaded = await service.get_worker(created.id, actor=ACTOR)
    assert loaded.scope == scope


async def test_is_live_true(service: WorkerService) -> None:
    """Report a recently seen worker as live."""
    created = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(),
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    assert created.is_live(datetime.now(UTC), timeout_seconds=60) is True


async def test_is_live_false(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """Report a worker outside the liveness window as not live."""
    stale = await create_worker(
        repository,
        ACTOR.account.id,
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    assert stale.is_live(datetime.now(UTC), timeout_seconds=60) is False
