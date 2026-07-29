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
"""Round-trip tests for the workers SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeWorkerRepository, asgi_api_client
from kitaru.api_models.v1.task import WorkerScope
from kitaru.api_models.v1.worker import (
    WorkerCreateRequest,
    WorkerListParams,
    WorkerResponse,
    WorkerRuntime,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

RUNTIME = WorkerRuntime(platform="bare")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = WorkerService(repository=FakeWorkerRepository())
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Register a worker through the SDK."""
    worker = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=WorkerScope(), runtime=RUNTIME, metadata={}
        )
    )
    assert isinstance(worker, WorkerResponse)
    assert worker.name == "worker-1"
    assert worker.owner_id == ACCOUNT.id


async def test_create_upsert(api_client: KitaruAPIClient) -> None:
    """Re-registering under the same name keeps the id through the SDK."""
    first = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1",
            scope=WorkerScope(kinds=["agent"]),
            runtime=RUNTIME,
            metadata={},
        )
    )
    second = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1",
            scope=WorkerScope(kinds=["importer"]),
            runtime=RUNTIME,
            metadata={},
        )
    )
    assert second.id == first.id
    assert second.scope == WorkerScope(kinds=["importer"])


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a worker by id through the SDK."""
    created = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=WorkerScope(), runtime=RUNTIME, metadata={}
        )
    )
    loaded = await api_client.workers.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.workers.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List workers newest-first with filters through the SDK."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await api_client.workers.create(
            WorkerCreateRequest(
                name=name, scope=WorkerScope(), runtime=RUNTIME, metadata={}
            )
        )

    page = await api_client.workers.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["worker-3", "worker-2", "worker-1"]

    page = await api_client.workers.list(WorkerListParams(name="worker-2"))
    assert page.items[0].name == "worker-2"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every worker across pages through the SDK."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await api_client.workers.create(
            WorkerCreateRequest(
                name=name, scope=WorkerScope(), runtime=RUNTIME, metadata={}
            )
        )

    collected = [
        item.name async for item in api_client.workers.iter(WorkerListParams(size=2))
    ]

    assert collected == ["worker-3", "worker-2", "worker-1"]


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a worker through the SDK."""
    created = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=WorkerScope(), runtime=RUNTIME, metadata={}
        )
    )
    await api_client.workers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.workers.get(created.id)
