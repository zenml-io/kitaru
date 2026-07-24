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
"""Tests for the worker routes."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from conftest import FakeWorkerRepository, create_worker
from kitaru.server.adapters.rest.dependencies import authorize, get_worker_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def repository() -> FakeWorkerRepository:
    """Provide the fake worker repository backing the app."""
    return FakeWorkerRepository()


@pytest.fixture
async def client(
    repository: FakeWorkerRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed worker service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = WorkerService(repository=repository, liveness_timeout_seconds=60)
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_register_worker(client: httpx.AsyncClient) -> None:
    """Register a worker and observe HTTP 200."""
    agent_id = uuid.uuid4()
    response = await client.post(
        "/v1/workers",
        json={
            "name": "runner",
            "agent_ids": [str(agent_id)],
            "metadata": {"hostname": "pool-1"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "runner"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["agent_ids"] == [str(agent_id)]
    assert body["metadata"] == {"hostname": "pool-1"}
    assert body["live"] is True
    assert body["last_seen_at"] is not None
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_register_worker_upserts_by_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 200 and an update on a repeated registration."""
    response = await client.post("/v1/workers", json={"name": "runner"})
    assert response.status_code == 200
    created = response.json()

    agent_id = uuid.uuid4()
    response = await client.post(
        "/v1/workers",
        json={
            "name": "runner",
            "agent_ids": [str(agent_id)],
            "metadata": {"hostname": "pool-2"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == created["id"]
    assert body["agent_ids"] == [str(agent_id)]
    assert body["metadata"] == {"hostname": "pool-2"}
    assert body["last_seen_at"] > created["last_seen_at"]


async def test_register_worker_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid worker name."""
    response = await client.post("/v1/workers", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_workers(client: httpx.AsyncClient) -> None:
    """List workers with filters and pagination."""
    for name in ["a", "b", "c"]:
        response = await client.post("/v1/workers", json={"name": name})
        assert response.status_code == 200

    response = await client.get("/v1/workers")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["name"] for item in body["items"]] == ["a", "b", "c"]

    response = await client.get("/v1/workers", params={"name": "b"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["name"] == "b"

    response = await client.get("/v1/workers", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["c"]


async def test_list_workers_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/v1/workers", params={"page": 0})
    assert response.status_code == 422
    response = await client.get("/v1/workers", params={"page_size": 1001})
    assert response.status_code == 422


async def test_get_worker(client: httpx.AsyncClient) -> None:
    """Get a worker by id."""
    created = (await client.post("/v1/workers", json={"name": "runner"})).json()
    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/workers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Worker {missing_id} was not found"}


async def test_get_worker_not_live(
    client: httpx.AsyncClient, repository: FakeWorkerRepository
) -> None:
    """Report a worker outside the liveness timeout as not live."""
    created = await create_worker(
        repository,
        ACCOUNT.id,
        last_seen_at=datetime.now(UTC) - timedelta(seconds=61),
    )
    response = await client.get(f"/v1/workers/{created.id}")
    assert response.status_code == 200
    assert response.json()["live"] is False


async def test_delete_worker(client: httpx.AsyncClient) -> None:
    """Delete a worker and observe HTTP 204."""
    created = (await client.post("/v1/workers", json={"name": "runner"})).json()
    response = await client.delete(f"/v1/workers/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 404


async def test_delete_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    response = await client.delete(f"/v1/workers/{uuid.uuid4()}")
    assert response.status_code == 404
