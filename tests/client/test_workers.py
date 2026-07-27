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
from kitaru.api_models.v1.jobs import WorkerScope
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = WorkerService(
        repository=FakeWorkerRepository(), liveness_timeout_seconds=60
    )
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Register a worker through the SDK."""
    version_id = uuid.uuid4()
    worker = await api_client.workers.create(
        WorkerCreateRequest(
            name="runner",
            scope=WorkerScope(agent_version_ids=[version_id]),
            metadata={"hostname": "pool-1"},
        )
    )
    assert worker.name == "runner"
    assert worker.owner_id == ACCOUNT.id
    assert worker.scope.agent_version_ids == [version_id]
    assert worker.metadata == {"hostname": "pool-1"}
    assert worker.live is True


async def test_create_upserts_by_name(api_client: KitaruAPIClient) -> None:
    """Re-register a worker under the same name through the SDK."""
    created = await api_client.workers.create(WorkerCreateRequest(name="runner"))
    version_id = uuid.uuid4()
    registered = await api_client.workers.create(
        WorkerCreateRequest(
            name="runner", scope=WorkerScope(agent_version_ids=[version_id])
        )
    )
    assert registered.id == created.id
    assert registered.scope.agent_version_ids == [version_id]
    assert registered.last_seen_at > created.last_seen_at


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a worker by id through the SDK."""
    created = await api_client.workers.create(WorkerCreateRequest(name="runner"))
    loaded = await api_client.workers.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.workers.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List workers with filters and pagination through the SDK."""
    for name in ["a", "b", "c"]:
        await api_client.workers.create(WorkerCreateRequest(name=name))

    page = await api_client.workers.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["a", "b", "c"]

    page = await api_client.workers.list(name="b")
    assert page.total == 1
    assert page.items[0].name == "b"

    page = await api_client.workers.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["c"]


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a worker through the SDK."""
    created = await api_client.workers.create(WorkerCreateRequest(name="runner"))
    await api_client.workers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.workers.get(created.id)
