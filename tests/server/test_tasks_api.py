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
"""Tests for the task routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_job,
    create_worker,
)
from kitaru.server.adapters.rest.dependencies import authorize, get_task_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def client(
    services: JobAndTaskServices,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed task service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _claimable_agent_task(services: JobAndTaskServices, job_id: uuid.UUID):
    """Store an agent task backed by a real agent version, so its spec builds."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return await create_agent_task(services.tasks, job_id, agent_version_id=version.id)


async def test_get_task(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Get a task by id."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)
    response = await client.get(f"/v1/tasks/{task.id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(task.id)


async def test_get_task_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown task id."""
    response = await client.get(f"/v1/tasks/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_tasks_filters(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """List tasks filtered by job_id."""
    job = await create_job(services.jobs, ACCOUNT.id)
    other_job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)
    await create_agent_task(services.tasks, other_job.id)

    filter_expression = {"field": "job_id", "op": "eq", "value": str(job.id)}
    response = await client.get(
        "/v1/tasks", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == str(task.id)


async def test_claim_tasks(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Claim tasks and observe the spec shipped alongside each task."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)

    response = await client.post(
        "/v1/tasks/claim", json={"worker_id": str(worker.id), "max_tasks": 10}
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["tasks"]) == 1
    entry = body["tasks"][0]
    assert entry["task"]["id"] == str(task.id)
    assert entry["task"]["status"] == "claimed"
    assert entry["spec"]["kind"] == "agent"
    assert entry["spec"]["run"]["command"] == "run.sh"


async def test_claim_tasks_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    response = await client.post(
        "/v1/tasks/claim", json={"worker_id": str(uuid.uuid4()), "max_tasks": 10}
    )
    assert response.status_code == 404


async def test_get_task_spec(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Get a task's execution spec."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    response = await client.get(f"/v1/tasks/{task.id}/spec")
    assert response.status_code == 200
    assert response.json()["kind"] == "agent"


async def test_update_task_transitions(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """PATCH transitions a claimed task to running."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)
    await services.task_service.claim_tasks(
        worker.id, 10, actor=AuthContext(account=ACCOUNT)
    )

    response = await client.patch(
        f"/v1/tasks/{task.id}", json={"status": "running", "attempt": 1}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"


async def test_update_task_attempt_fencing_conflicts(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 409 when the attempt does not match."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)
    await services.task_service.claim_tasks(
        worker.id, 10, actor=AuthContext(account=ACCOUNT)
    )

    response = await client.patch(
        f"/v1/tasks/{task.id}", json={"status": "running", "attempt": 0}
    )
    assert response.status_code == 409


async def test_update_task_requires_a_status(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 when the body carries no status."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)
    response = await client.patch(f"/v1/tasks/{task.id}", json={})
    assert response.status_code == 422
