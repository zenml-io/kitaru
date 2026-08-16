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
"""Tests for the job routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent_task,
    create_job,
)
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_worker,
    get_job_service,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def client(
    services: JobAndTaskServices,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed job and task services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_job_service] = lambda: services.job_service
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_worker] = lambda: AuthContext(
        account=ACCOUNT
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_get_job(client: httpx.AsyncClient, services: JobAndTaskServices) -> None:
    """Get a job by id."""
    job = await create_job(services.jobs, ACCOUNT.id)
    response = await client.get(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(job.id)
    assert body["kind"] == "session_run"
    assert body["status"] == "pending"


async def test_get_job_reports_the_provisional_flag(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """A job response carries the provisional flag."""
    provisional_job = await create_job(services.jobs, ACCOUNT.id, provisional=True)
    response = await client.get(f"/api/v1/jobs/{provisional_job.id}")
    assert response.status_code == 200
    assert response.json()["provisional"] is True

    plain_job = await create_job(services.jobs, ACCOUNT.id)
    response = await client.get(f"/api/v1/jobs/{plain_job.id}")
    assert response.json()["provisional"] is False


async def test_get_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown job id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/jobs/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Job {missing_id} was not found"}


async def test_list_jobs_filters_by_status(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """List jobs filters by status."""
    await create_job(services.jobs, ACCOUNT.id, status=JobStatus.PENDING)
    await create_job(services.jobs, ACCOUNT.id, status=JobStatus.COMPLETED)

    response = await client.get("/api/v1/jobs")
    assert response.status_code == 200
    assert len(response.json()["items"]) == 2

    filter_expression = {"field": "status", "op": "eq", "value": "completed"}
    response = await client.get(
        "/api/v1/jobs", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "completed"


async def test_list_jobs_filters_by_kind(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """List jobs filters by kind."""
    await create_job(services.jobs, ACCOUNT.id, kind=JobKind.SESSION_RUN)
    await create_job(services.jobs, ACCOUNT.id, kind=JobKind.REPLAY)

    filter_expression = {"field": "kind", "op": "eq", "value": "replay"}
    response = await client.get(
        "/api/v1/jobs", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["kind"] == "replay"


async def test_list_job_tasks(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """List a job's tasks."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)

    response = await client.get(f"/api/v1/jobs/{job.id}/tasks")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == str(task.id)
    assert items[0]["kind"] == "agent"


async def test_list_job_tasks_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when listing the tasks of an unknown job."""
    response = await client.get(f"/api/v1/jobs/{uuid.uuid4()}/tasks")
    assert response.status_code == 404


async def test_cancel_job(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Cancel a job and observe its pending tasks canceled."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)

    response = await client.post(f"/api/v1/jobs/{job.id}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["cancel_requested_at"] is not None

    stored_task = await services.tasks.get(task.id)
    assert stored_task.status.value == "canceled"


async def test_cancel_job_conflicts_when_already_settled(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 409 when canceling a settled job."""
    job = await create_job(services.jobs, ACCOUNT.id, status=JobStatus.COMPLETED)
    response = await client.post(f"/api/v1/jobs/{job.id}/cancel")
    assert response.status_code == 409


async def test_delete_job(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Delete a job and observe HTTP 204."""
    job = await create_job(services.jobs, ACCOUNT.id)
    response = await client.delete(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/jobs/{job.id}")
    assert response.status_code == 404


async def test_delete_job_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when deleting an unknown job."""
    response = await client.delete(f"/api/v1/jobs/{uuid.uuid4()}")
    assert response.status_code == 404
