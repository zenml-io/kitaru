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
"""Round-trip tests for the worker pools SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeTaskRepository,
    FakeWorkerPoolRepository,
    FakeWorkerRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.worker import WorkerScope
from kitaru.api_models.v1.worker_pool import (
    WorkerPoolCreateRequest,
    WorkerPoolListParams,
    WorkerPoolResponse,
    WorkerPoolUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_pool_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_pool_service import WorkerPoolService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    worker_pool_repository = FakeWorkerPoolRepository()
    app.dependency_overrides[get_worker_pool_service] = lambda: WorkerPoolService(
        repository=worker_pool_repository,
        task_repository=FakeTaskRepository(),
        worker_repository=FakeWorkerRepository(),
        liveness_timeout_seconds=60,
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a worker pool through the SDK."""
    worker_pool = await api_client.worker_pools.create(
        WorkerPoolCreateRequest(name="pool-1", scope=WorkerScope(kinds=["agent"]))
    )
    assert isinstance(worker_pool, WorkerPoolResponse)
    assert worker_pool.name == "pool-1"
    assert worker_pool.owner_id == ACCOUNT.id
    assert worker_pool.scope == WorkerScope(kinds=["agent"])


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.worker_pools.create(WorkerPoolCreateRequest(name="pool-1"))
    with pytest.raises(APIError) as exc_info:
        await api_client.worker_pools.create(WorkerPoolCreateRequest(name="pool-1"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Worker pool name 'pool-1' is already registered"


async def test_create_scope_pins_job(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 as a typed error when the scope names a job."""
    with pytest.raises(APIError) as exc_info:
        await api_client.worker_pools.create(
            WorkerPoolCreateRequest(
                name="pool-1", scope=WorkerScope(job_id=uuid.uuid4())
            )
        )
    assert exc_info.value.status_code == 422


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a worker pool by id through the SDK."""
    created = await api_client.worker_pools.create(
        WorkerPoolCreateRequest(name="pool-1")
    )
    loaded = await api_client.worker_pools.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.worker_pools.get(uuid.uuid4())


async def test_stats(api_client: KitaruAPIClient) -> None:
    """Get a worker pool's stats by id and by name through the SDK."""
    created = await api_client.worker_pools.create(
        WorkerPoolCreateRequest(name="pool-1")
    )

    stats = await api_client.worker_pools.stats(created.id)
    assert stats.pending_tasks == 0
    assert stats.in_flight_tasks == 0
    assert stats.oldest_pending_seconds is None
    assert stats.live_workers == 0
    assert stats.capacity == 0

    stats = await api_client.worker_pools.stats("pool-1")
    assert stats.live_workers == 0
    assert stats.capacity == 0


async def test_stats_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.worker_pools.stats(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List worker pools newest-first with filters through the SDK."""
    for name in ["alpha", "beta"]:
        await api_client.worker_pools.create(WorkerPoolCreateRequest(name=name))

    page = await api_client.worker_pools.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["beta", "alpha"]

    page = await api_client.worker_pools.list(
        WorkerPoolListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="alpha")
        )
    )
    assert page.items[0].name == "alpha"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every worker pool across pages through the SDK."""
    for name in ["alpha", "beta", "gamma"]:
        await api_client.worker_pools.create(WorkerPoolCreateRequest(name=name))

    collected = [
        item.name
        async for item in api_client.worker_pools.iter(WorkerPoolListParams(size=1))
    ]
    assert collected == ["gamma", "beta", "alpha"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a worker pool through the SDK."""
    created = await api_client.worker_pools.create(
        WorkerPoolCreateRequest(name="pool-1", scope=WorkerScope(kinds=["agent"]))
    )
    updated = await api_client.worker_pools.update(
        created.id,
        WorkerPoolUpdateRequest(name="renamed", scope=WorkerScope(kinds=["importer"])),
    )
    assert updated.name == "renamed"
    assert updated.scope == WorkerScope(kinds=["importer"])


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a worker pool through the SDK."""
    created = await api_client.worker_pools.create(
        WorkerPoolCreateRequest(name="pool-1")
    )
    await api_client.worker_pools.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.worker_pools.get(created.id)
