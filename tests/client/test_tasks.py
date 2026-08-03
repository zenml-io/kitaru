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
"""Round-trip tests for the tasks SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    JobAndTaskServices,
    asgi_api_client,
    build_job_and_task_services,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_job,
    create_worker,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.task import (
    TaskClaimRequest,
    TaskListParams,
    TaskResponse,
    TaskSpecResponse,
    TaskUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError, ValidationError
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
async def api_client(
    services: JobAndTaskServices,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed task service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _claimable_agent_task(services: JobAndTaskServices, job_id: uuid.UUID):
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return await create_agent_task(services.tasks, job_id, agent_version_id=version.id)


async def test_get(api_client: KitaruAPIClient, services: JobAndTaskServices) -> None:
    """Get a task by id through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)
    loaded = await api_client.tasks.get(task.id)
    assert isinstance(loaded, TaskResponse)
    assert loaded.id == task.id


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.tasks.get(uuid.uuid4())


async def test_list_and_iter(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """List and iterate tasks through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    for _ in range(3):
        await create_agent_task(services.tasks, job.id)

    job_filter = FilterCondition(field="job_id", op=FilterOp.EQ, value=job.id)
    page = await api_client.tasks.list(TaskListParams(filter=job_filter))
    assert page.next_cursor is None
    assert len(page.items) == 3

    collected = [
        item.id
        async for item in api_client.tasks.iter(
            TaskListParams(filter=job_filter, size=1)
        )
    ]
    assert len(collected) == 3


async def test_claim(api_client: KitaruAPIClient, services: JobAndTaskServices) -> None:
    """Claim tasks through the SDK, receiving the spec alongside each task."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)

    response = await api_client.tasks.claim(
        TaskClaimRequest(worker_id=worker.id, max_tasks=10)
    )
    assert len(response.tasks) == 1
    assert response.tasks[0].task.id == task.id
    assert isinstance(response.tasks[0].spec, TaskSpecResponse)


async def test_claim_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error for an unknown worker."""
    with pytest.raises(NotFoundError):
        await api_client.tasks.claim(
            TaskClaimRequest(worker_id=uuid.uuid4(), max_tasks=10)
        )


async def test_get_spec(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Get a task's execution spec through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    spec = await api_client.tasks.get_spec(task.id)
    assert spec.kind.value == "agent"


async def test_update(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Apply a status transition through the SDK."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)
    await services.task_service.claim_tasks(
        worker.id, 10, actor=AuthContext(account=ACCOUNT)
    )

    updated = await api_client.tasks.update(
        task.id, TaskUpdateRequest(status="running", attempt=1)
    )
    assert updated.status.value == "running"


async def test_update_attempt_fencing_conflicts(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Surface HTTP 409 as a typed error when the attempt does not match."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await _claimable_agent_task(services, job.id)
    worker = await create_worker(services.workers, ACCOUNT.id)
    await services.task_service.claim_tasks(
        worker.id, 10, actor=AuthContext(account=ACCOUNT)
    )

    with pytest.raises(APIError) as excinfo:
        await api_client.tasks.update(
            task.id, TaskUpdateRequest(status="running", attempt=0)
        )
    assert excinfo.value.status_code == 409


async def test_update_requires_a_status(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Surface HTTP 422 as a typed error when the body carries no status."""
    job = await create_job(services.jobs, ACCOUNT.id)
    task = await create_agent_task(services.tasks, job.id)
    with pytest.raises(ValidationError):
        await api_client.tasks.update(task.id, TaskUpdateRequest())
