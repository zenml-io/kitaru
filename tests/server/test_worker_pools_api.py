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
"""Tests for the worker pool routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeTaskRepository, FakeWorkerPoolRepository, FakeWorkerRepository
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_pool_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_pool_service import WorkerPoolService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
LIVENESS_TIMEOUT_SECONDS = 60


@pytest.fixture
def worker_pool_repository() -> FakeWorkerPoolRepository:
    """Provide the fake worker pool repository backing the app."""
    return FakeWorkerPoolRepository()


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide the fake task repository backing the app."""
    return FakeTaskRepository()


@pytest.fixture
def worker_repository() -> FakeWorkerRepository:
    """Provide the fake worker repository backing the app."""
    return FakeWorkerRepository()


@pytest.fixture
async def client(
    worker_pool_repository: FakeWorkerPoolRepository,
    task_repository: FakeTaskRepository,
    worker_repository: FakeWorkerRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed worker pool service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    worker_pool_service = WorkerPoolService(
        repository=worker_pool_repository,
        task_repository=task_repository,
        worker_repository=worker_repository,
        liveness_timeout_seconds=LIVENESS_TIMEOUT_SECONDS,
    )
    app.dependency_overrides[get_worker_pool_service] = lambda: worker_pool_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_worker_pool(client: httpx.AsyncClient) -> None:
    """Create a worker pool and observe HTTP 201."""
    response = await client.post(
        "/v1/worker-pools",
        json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "pool-1"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["scope"]["kinds"] == ["agent"]
    assert body["created"] is not None
    assert uuid.UUID(body["id"])


async def test_create_worker_pool_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate worker pool name."""
    body = {"name": "pool-1", "scope": {}}
    response = await client.post("/v1/worker-pools", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/worker-pools", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Worker pool name 'pool-1' is already registered"
    }


async def test_create_worker_pool_scope_pins_job(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when the scope names a job."""
    response = await client.post(
        "/v1/worker-pools",
        json={"name": "pool-1", "scope": {"job_id": str(uuid.uuid4())}},
    )
    assert response.status_code == 422


async def test_get_worker_pool(client: httpx.AsyncClient) -> None:
    """Get a worker pool by id."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.get(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_worker_pool_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing worker pool."""
    response = await client.get(f"/v1/worker-pools/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_get_worker_pool_stats_by_id(client: httpx.AsyncClient) -> None:
    """Get a worker pool's stats by id with the full response shape."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.get(f"/v1/worker-pools/{created['id']}/stats")
    assert response.status_code == 200
    assert response.json() == {
        "pending_tasks": 0,
        "in_flight_tasks": 0,
        "oldest_pending_seconds": None,
        "live_workers": 0,
        "capacity": 0,
    }


async def test_get_worker_pool_stats_by_name(client: httpx.AsyncClient) -> None:
    """Get a worker pool's stats by name."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.get(f"/v1/worker-pools/{created['name']}/stats")
    assert response.status_code == 200
    assert response.json()["live_workers"] == 0


async def test_get_worker_pool_stats_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing worker pool, by id and by name."""
    response = await client.get(f"/v1/worker-pools/{uuid.uuid4()}/stats")
    assert response.status_code == 404
    response = await client.get("/v1/worker-pools/missing/stats")
    assert response.status_code == 404


async def test_list_worker_pools(client: httpx.AsyncClient) -> None:
    """List worker pools newest-first with a name filter."""
    for name in ["alpha", "beta"]:
        await client.post("/v1/worker-pools", json={"name": name, "scope": {}})

    response = await client.get("/v1/worker-pools")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["beta", "alpha"]

    filter_expression = {"field": "name", "op": "eq", "value": "alpha"}
    response = await client.get(
        "/v1/worker-pools", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "alpha"


async def test_update_worker_pool(client: httpx.AsyncClient) -> None:
    """Update a worker pool's name and scope."""
    created = (
        await client.post(
            "/v1/worker-pools",
            json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
        )
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}",
        json={"name": "renamed", "scope": {"kinds": ["importer"]}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["scope"]["kinds"] == ["importer"]
    assert body["updated"] > created["updated"]


async def test_update_worker_pool_partial(client: httpx.AsyncClient) -> None:
    """Leave omitted fields unchanged on a partial update."""
    created = (
        await client.post(
            "/v1/worker-pools",
            json={"name": "pool-1", "scope": {"kinds": ["agent"]}},
        )
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "renamed"
    assert body["scope"] == created["scope"]


async def test_update_worker_pool_cannot_clear_name(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when clearing the worker pool name."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}", json={"name": None}
    )
    assert response.status_code == 422


async def test_update_worker_pool_cannot_clear_scope(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 when clearing the worker pool scope."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}", json={"scope": None}
    )
    assert response.status_code == 422


async def test_update_worker_pool_scope_pins_job(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when the new scope names a job."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.patch(
        f"/v1/worker-pools/{created['id']}",
        json={"scope": {"job_id": str(uuid.uuid4())}},
    )
    assert response.status_code == 422


async def test_update_worker_pool_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing worker pool."""
    response = await client.patch(
        f"/v1/worker-pools/{uuid.uuid4()}", json={"name": "renamed"}
    )
    assert response.status_code == 404


async def test_delete_worker_pool(client: httpx.AsyncClient) -> None:
    """Delete a worker pool."""
    created = (
        await client.post("/v1/worker-pools", json={"name": "pool-1", "scope": {}})
    ).json()
    response = await client.delete(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/worker-pools/{created['id']}")
    assert response.status_code == 404


async def test_delete_worker_pool_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing worker pool."""
    response = await client.delete(f"/v1/worker-pools/{uuid.uuid4()}")
    assert response.status_code == 404
