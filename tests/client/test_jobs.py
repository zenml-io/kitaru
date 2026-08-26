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
"""Round-trip tests for the jobs SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    JobAndTaskServices,
    asgi_api_client,
    build_job_and_task_services,
    create_agent_task,
    create_job,
)
from kitaru.api_models.v1.job import JobListParams, JobResponse, JobTasksListParams
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
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
from kitaru.server.domain.job import JobStatus

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def api_client(
    services: JobAndTaskServices,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
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
    async with asgi_api_client(app) as client:
        yield client


async def test_get(api_client: KitaruAPIClient, services: JobAndTaskServices) -> None:
    """Get a job by id through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    loaded = await api_client.jobs.get(job.id)
    assert isinstance(loaded, JobResponse)
    assert loaded.id == job.id


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.jobs.get(uuid.uuid4())


async def test_list_and_iter(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """List and iterate jobs newest-first through the SDK."""
    for _ in range(3):
        await create_job(services.jobs, ACCOUNT.id)

    page = await api_client.jobs.list()
    assert page.next_cursor is None
    assert len(page.items) == 3

    collected = [item async for item in api_client.jobs.iter(JobListParams(size=1))]
    assert len(collected) == 3


async def test_list_tasks(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """List and iterate a job's tasks through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)

    page = await api_client.jobs.list_tasks(job.id)
    assert page.next_cursor is None
    assert [item.id for item in page.items] == [task.id]

    collected = [
        item.id
        async for item in api_client.jobs.iter_tasks(job.id, JobTasksListParams(size=1))
    ]
    assert collected == [task.id]


async def test_cancel(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Cancel a job through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    canceled = await api_client.jobs.cancel(job.id)
    assert canceled.cancel_requested_at is not None


async def test_cancel_conflicts_when_settled(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Surface HTTP 409 as a typed error for a settled job."""
    job = await create_job(services.jobs, ACCOUNT.id, status=JobStatus.COMPLETED)
    with pytest.raises(APIError) as excinfo:
        await api_client.jobs.cancel(job.id)
    assert excinfo.value.status_code == 409


async def test_delete(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Delete a settled job through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id, status=JobStatus.COMPLETED)
    await api_client.jobs.delete(job.id)
    with pytest.raises(NotFoundError):
        await api_client.jobs.get(job.id)
