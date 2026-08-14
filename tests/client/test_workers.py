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
from datetime import UTC, datetime

import pytest

from conftest import (
    UNSCOPED_WORKER_SCOPE,
    FakeAccountRepository,
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeApiKeyRepository,
    FakeBlobRepository,
    FakeJobRepository,
    FakePasswordHasher,
    FakePluginRepository,
    FakeReplayRepository,
    FakeSecretRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    FakeWorkerRepository,
    asgi_api_client,
    create_agent_task,
    create_job,
    local_settings,
    mint_worker_token,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    WorkerClaim,
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
    WorkerListParams,
    WorkerRegistrationResponse,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_auth_service,
    get_auth_session,
    get_task_service,
    get_worker_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account

RUNTIME = WorkerRuntime(platform="bare")


@pytest.fixture
def account_repository() -> FakeAccountRepository:
    """Provide a fake account repository."""
    return FakeAccountRepository()


@pytest.fixture
async def account(account_repository: FakeAccountRepository) -> Account:
    """Provide a stored account workers and jobs are owned by."""
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
def worker_repository() -> FakeWorkerRepository:
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
async def account_token(auth_service: AuthService, account: Account) -> str:
    """Provide a bearer token authenticating as the fixture account."""
    return auth_service.issue_token(AuthContext(account=account)).token


@pytest.fixture
async def api_client(
    worker_repository: FakeWorkerRepository,
    task_repository: FakeTaskRepository,
    job_repository: FakeJobRepository,
    auth_service: AuthService,
    account_token: str,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client authenticated as the fixture account by default."""
    app = create_app(local_settings())
    service = WorkerService(repository=worker_repository)
    agents = FakeAgentRepository()
    transitions = TaskTransitions(
        task_repository=task_repository,
        job_repository=job_repository,
        dispatcher=EventDispatcher(),
    )
    task_policy = TaskPolicy()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=FakeAgentVersionRepository(agents),
        plugin_repository=FakePluginRepository(),
        blob_repository=FakeBlobRepository(),
        secret_repository=FakeSecretRepository(),
        replay_repository=FakeReplayRepository(),
        policy=task_policy,
    )
    task_service = TaskService(
        repository=task_repository,
        worker_repository=worker_repository,
        session_repository=FakeSessionRepository(),
        job_repository=job_repository,
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[get_task_service] = lambda: task_service
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_auth_session] = lambda: None
    client = asgi_api_client(app, api_key=account_token)
    async with client:
        yield client


async def test_create(api_client: KitaruAPIClient, account: Account) -> None:
    """Register a worker through the SDK."""
    registered = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
        )
    )
    assert isinstance(registered, WorkerRegistrationResponse)
    assert registered.worker.name == "worker-1"
    assert registered.worker.owner_id == account.id
    assert registered.token.get_secret_value()


async def test_create_upsert(api_client: KitaruAPIClient) -> None:
    """Re-registering under the same name keeps the id through the SDK."""
    first = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1",
            scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
            runtime=RUNTIME,
            metadata={},
        )
    )
    second = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1",
            scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
            runtime=RUNTIME,
            metadata={},
        )
    )
    assert second.worker.id == first.worker.id
    assert second.worker.scope == WorkerScope(
        claims=[WorkerClaim(kind=TaskKind.IMPORTER)]
    )


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a worker by id through the SDK."""
    created = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
        )
    )
    loaded = await api_client.workers.get(created.worker.id)
    assert loaded == created.worker


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.workers.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List workers newest-first with filters through the SDK."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await api_client.workers.create(
            WorkerCreateRequest(
                name=name, scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
            )
        )

    page = await api_client.workers.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["worker-3", "worker-2", "worker-1"]

    page = await api_client.workers.list(
        WorkerListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="worker-2")
        )
    )
    assert page.items[0].name == "worker-2"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every worker across pages through the SDK."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await api_client.workers.create(
            WorkerCreateRequest(
                name=name, scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
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
            name="worker-1", scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
        )
    )
    await api_client.workers.delete(created.worker.id)
    with pytest.raises(NotFoundError):
        await api_client.workers.get(created.worker.id)


async def test_heartbeat(
    api_client: KitaruAPIClient,
    job_repository: FakeJobRepository,
    task_repository: FakeTaskRepository,
    auth_service: AuthService,
    account: Account,
) -> None:
    """Report held tasks through the SDK, receiving cancel_task_ids."""
    created = await api_client.workers.create(
        WorkerCreateRequest(
            name="worker-1", scope=UNSCOPED_WORKER_SCOPE, runtime=RUNTIME, metadata={}
        )
    )
    worker = created.worker
    job = await create_job(job_repository, account.id)
    task = await create_agent_task(task_repository, job.id)
    task.claim(worker.id, datetime.now(UTC))
    await task_repository.update(task)

    worker_client = api_client.with_token(
        mint_worker_token(auth_service, worker.id, account)
    )
    response = await worker_client.workers.heartbeat(
        worker.id, WorkerHeartbeatRequest(task_ids=[task.id])
    )
    assert response.cancel_task_ids == []


async def test_heartbeat_not_found(
    api_client: KitaruAPIClient, auth_service: AuthService, account: Account
) -> None:
    """Surface HTTP 404 as a typed error for an unknown worker id."""
    missing_id = uuid.uuid4()
    worker_client = api_client.with_token(
        mint_worker_token(auth_service, missing_id, account)
    )
    with pytest.raises(NotFoundError):
        await worker_client.workers.heartbeat(
            missing_id, WorkerHeartbeatRequest(task_ids=[])
        )
