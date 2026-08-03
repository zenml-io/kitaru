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

import json
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeSecretRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    FakeWorkerRepository,
    create_agent_task,
    create_job,
    create_worker,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_task_service,
    get_worker_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

RUNTIME = {"platform": "bare"}


@pytest.fixture
def repository() -> FakeWorkerRepository:
    """Provide the fake worker repository backing the app."""
    return FakeWorkerRepository()


@pytest.fixture
def task_repository() -> FakeTaskRepository:
    """Provide the fake task repository backing the app."""
    return FakeTaskRepository(sessions=FakeSessionRepository())


@pytest.fixture
def job_repository(task_repository: FakeTaskRepository) -> FakeJobRepository:
    """Provide the fake job repository backing the app."""
    return FakeJobRepository(tasks=task_repository)


@pytest.fixture
async def client(
    repository: FakeWorkerRepository,
    task_repository: FakeTaskRepository,
    job_repository: FakeJobRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed worker and task services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = WorkerService(repository=repository)
    transitions = TaskTransitions(
        task_repository=task_repository,
        job_repository=job_repository,
        dispatcher=EventDispatcher(),
    )
    agents = FakeAgentRepository()
    task_service = TaskService(
        repository=task_repository,
        worker_repository=repository,
        session_repository=FakeSessionRepository(),
        agent_version_repository=FakeAgentVersionRepository(agents),
        plugin_repository=FakePluginRepository(),
        blob_repository=FakeBlobRepository(),
        secret_repository=FakeSecretRepository(),
        transitions=transitions,
        policy=TaskPolicy(),
    )
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[get_task_service] = lambda: task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_register_worker(client: httpx.AsyncClient) -> None:
    """Register a worker and observe HTTP 200."""
    response = await client.post(
        "/v1/workers",
        json={
            "name": "worker-1",
            "scope": {},
            "runtime": RUNTIME,
            "metadata": {"region": "eu"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "worker-1"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["metadata"] == {"region": "eu"}
    assert body["live"] is True
    assert uuid.UUID(body["id"])


async def test_register_worker_upsert(client: httpx.AsyncClient) -> None:
    """Re-registering under the same name keeps the id and renews the state."""
    first = (
        await client.post(
            "/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"kinds": ["agent"]},
                "runtime": RUNTIME,
                "metadata": {"region": "eu"},
            },
        )
    ).json()
    second = (
        await client.post(
            "/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"kinds": ["importer"]},
                "runtime": {"platform": "docker"},
                "metadata": {"region": "us"},
            },
        )
    ).json()
    assert second["id"] == first["id"]
    assert second["created"] == first["created"]
    assert second["scope"]["kinds"] == ["importer"]
    assert second["runtime"]["platform"] == "docker"
    assert second["metadata"] == {"region": "us"}
    assert second["updated"] > first["updated"]


async def test_register_worker_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid worker name."""
    response = await client.post(
        "/v1/workers",
        json={"name": "in valid", "scope": {}, "runtime": RUNTIME, "metadata": {}},
    )
    assert response.status_code == 422


async def test_get_worker(client: httpx.AsyncClient) -> None:
    """Get a worker by id."""
    created = (
        await client.post(
            "/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()
    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/workers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Worker {missing_id} was not found"}


async def test_list_workers(client: httpx.AsyncClient) -> None:
    """List workers newest-first with filters."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        response = await client.post(
            "/v1/workers",
            json={"name": name, "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
        assert response.status_code == 200

    response = await client.get("/v1/workers")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == [
        "worker-3",
        "worker-2",
        "worker-1",
    ]

    filter_expression = {"field": "name", "op": "eq", "value": "worker-2"}
    response = await client.get(
        "/v1/workers", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "worker-2"


async def test_delete_worker(client: httpx.AsyncClient) -> None:
    """Delete a worker and observe HTTP 204."""
    created = (
        await client.post(
            "/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()
    response = await client.delete(f"/v1/workers/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/workers/{created['id']}")
    assert response.status_code == 404


async def test_delete_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    response = await client.delete(f"/v1/workers/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_worker_live_derivation(
    client: httpx.AsyncClient, repository: FakeWorkerRepository
) -> None:
    """Derive live from last_seen_at against the liveness setting."""
    live = await create_worker(repository, ACCOUNT.id, name="live")
    stale = await create_worker(
        repository,
        ACCOUNT.id,
        name="stale",
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )

    response = await client.get(f"/v1/workers/{live.id}")
    assert response.json()["live"] is True

    response = await client.get(f"/v1/workers/{stale.id}")
    assert response.json()["live"] is False


async def test_heartbeat_worker(
    client: httpx.AsyncClient,
    repository: FakeWorkerRepository,
    job_repository: FakeJobRepository,
    task_repository: FakeTaskRepository,
) -> None:
    """Report held tasks and observe the ones to stop in cancel_task_ids."""
    worker = await create_worker(repository, ACCOUNT.id)
    job = await create_job(job_repository, ACCOUNT.id)
    task = await create_agent_task(task_repository, job.id)
    task.claim(worker.id, datetime.now(UTC))
    await task_repository.update(task)

    response = await client.post(
        f"/v1/workers/{worker.id}/heartbeat", json={"task_ids": [str(task.id)]}
    )
    assert response.status_code == 200
    assert response.json()["cancel_task_ids"] == []


async def test_heartbeat_worker_returns_reported_ids_the_worker_no_longer_owns(
    client: httpx.AsyncClient,
    repository: FakeWorkerRepository,
) -> None:
    """A task the caller does not own comes back in cancel_task_ids."""
    worker = await create_worker(repository, ACCOUNT.id)
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/workers/{worker.id}/heartbeat", json={"task_ids": [str(missing_id)]}
    )
    assert response.status_code == 200
    assert response.json()["cancel_task_ids"] == [str(missing_id)]


async def test_heartbeat_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    response = await client.post(
        f"/v1/workers/{uuid.uuid4()}/heartbeat", json={"task_ids": []}
    )
    assert response.status_code == 404
