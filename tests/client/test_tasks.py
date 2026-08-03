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
from datetime import UTC, datetime

import pytest

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    JobAndTaskServices,
    asgi_api_client,
    build_job_and_task_services,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_job,
    create_worker,
    local_settings,
    mint_worker_token,
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
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import get_auth_service, get_task_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
async def account(account_repository: FakeAccountRepository) -> Account:
    """Provide a stored account jobs and workers are owned by."""
    return await account_repository.create(Account(name="ann"))


@pytest.fixture
def auth_service(account_repository: FakeAccountRepository) -> AuthService:
    """Provide an authentication service backed by the fake account repository."""
    return AuthService(
        settings=local_settings(),
        account_repository=account_repository,
        api_key_repository=FakeApiKeyRepository(),
        password_hasher=FakePasswordHasher(),
    )


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def account_token(auth_service: AuthService, account: Account) -> str:
    """Provide a bearer token authenticating as the fixture account."""
    return auth_service.issue_token(AuthContext(account=account)).token


@pytest.fixture
async def api_client(
    services: JobAndTaskServices,
    auth_service: AuthService,
    account_token: str,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client authenticated as the fixture account by default."""
    app = create_app(local_settings())
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    client = asgi_api_client(app, api_key=account_token)
    async with client:
        yield client


async def _claimable_agent_task(
    services: JobAndTaskServices, job_id: uuid.UUID, account: Account
):
    agent = await create_agent(services.agents, account.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=account.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return await create_agent_task(services.tasks, job_id, agent_version_id=version.id)


async def test_get(
    api_client: KitaruAPIClient, services: JobAndTaskServices, account: Account
) -> None:
    """Get a task by id through the SDK."""
    job = await create_job(services.jobs, account.id)
    task = await create_agent_task(services.tasks, job.id)
    loaded = await api_client.tasks.get(task.id)
    assert isinstance(loaded, TaskResponse)
    assert loaded.id == task.id


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.tasks.get(uuid.uuid4())


async def test_list_and_iter(
    api_client: KitaruAPIClient, services: JobAndTaskServices, account: Account
) -> None:
    """List and iterate tasks through the SDK."""
    job = await create_job(services.jobs, account.id)
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


async def test_claim(
    api_client: KitaruAPIClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Claim tasks through the SDK, receiving the spec and token alongside each task."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, worker.id, account)
    )

    response = await worker_client.tasks.claim(TaskClaimRequest(max_tasks=10))
    assert len(response.tasks) == 1
    assert response.tasks[0].task.id == task.id
    assert isinstance(response.tasks[0].spec, TaskSpecResponse)
    assert response.tasks[0].token


async def test_claim_not_found(
    api_client: KitaruAPIClient, auth_service: AuthService, account: Account
) -> None:
    """Surface HTTP 404 as a typed error for an unknown worker."""
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, uuid.uuid4(), account)
    )
    with pytest.raises(NotFoundError):
        await worker_client.tasks.claim(TaskClaimRequest(max_tasks=10))


async def test_get_spec(
    api_client: KitaruAPIClient, services: JobAndTaskServices, account: Account
) -> None:
    """Get a task's execution spec through the SDK."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    spec = await api_client.tasks.get_spec(task.id)
    assert spec.kind.value == "agent"


async def test_update(
    api_client: KitaruAPIClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Apply a status transition through the SDK using the claimed task's token."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, worker.id, account)
    )
    claimed = await worker_client.tasks.claim(TaskClaimRequest(max_tasks=10))
    task_client = api_client.with_token(claimed.tasks[0].token.get_secret_value())

    updated = await task_client.tasks.update(
        task.id, TaskUpdateRequest(status="running")
    )
    assert updated.status.value == "running"


async def test_update_attempt_fencing_conflicts(
    api_client: KitaruAPIClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Surface HTTP 409 as a typed error when the held token's attempt is stale."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, worker.id, account)
    )
    claimed = await worker_client.tasks.claim(TaskClaimRequest(max_tasks=10))
    stale_token = claimed.tasks[0].token.get_secret_value()

    stored = await services.tasks.get(task.id)
    stored.requeue()
    stored.claim(worker.id, datetime.now(UTC))
    await services.tasks.update(stored)

    task_client = api_client.with_token(stale_token)
    with pytest.raises(APIError) as excinfo:
        await task_client.tasks.update(task.id, TaskUpdateRequest(status="running"))
    assert excinfo.value.status_code == 409


async def test_update_requires_a_status(
    api_client: KitaruAPIClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Surface HTTP 422 as a typed error when the body carries no status."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, worker.id, account)
    )
    claimed = await worker_client.tasks.claim(TaskClaimRequest(max_tasks=10))
    task_client = api_client.with_token(claimed.tasks[0].token.get_secret_value())

    with pytest.raises(ValidationError):
        await task_client.tasks.update(task.id, TaskUpdateRequest())
