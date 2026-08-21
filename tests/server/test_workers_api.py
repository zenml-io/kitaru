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
    FakeAccountRepository,
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeReplayRepository,
    FakeSecretRepository,
    FakeSessionRepository,
    FakeTaskRepository,
    FakeWorkerRepository,
    create_agent_task,
    create_job,
    create_worker,
    local_settings,
    mint_worker_token,
)
from kitaru.headers import CLIENT_HEADER
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_auth_service,
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

RUNTIME = {"platform": "bare"}
SCOPE = {"claims": [{"kind": "agent"}]}


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
async def account_token(auth_service: AuthService, account: Account) -> str:
    """Provide a bearer token authenticating as the fixture account."""
    return auth_service.issue_token(AuthContext(account=account)).token


@pytest.fixture
async def client(
    repository: FakeWorkerRepository,
    task_repository: FakeTaskRepository,
    job_repository: FakeJobRepository,
    auth_service: AuthService,
    account_token: str,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client authenticated as the fixture account by default."""
    app = create_app(local_settings())
    service = WorkerService(
        repository=repository,
        liveness_timeout_seconds=local_settings().WORKER_LIVENESS_TIMEOUT_SECONDS,
    )
    transitions = TaskTransitions(
        task_repository=task_repository,
        job_repository=job_repository,
        dispatcher=EventDispatcher(),
    )
    agents = FakeAgentRepository()
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
        worker_repository=repository,
        session_repository=FakeSessionRepository(),
        job_repository=job_repository,
        replay_repository=FakeReplayRepository(),
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    app.dependency_overrides[get_worker_service] = lambda: service
    app.dependency_overrides[get_task_service] = lambda: task_service
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {account_token}"},
    ) as client:
        yield client


async def test_register_worker(client: httpx.AsyncClient, account: Account) -> None:
    """Register a worker and observe a worker record plus a bearer token."""
    response = await client.post(
        "/api/v1/workers",
        json={
            "name": "worker-1",
            "scope": SCOPE,
            "runtime": RUNTIME,
            "metadata": {"region": "eu"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["worker"]["name"] == "worker-1"
    assert body["worker"]["owner_id"] == str(account.id)
    assert body["worker"]["metadata"] == {"region": "eu"}
    assert body["worker"]["live"] is True
    assert uuid.UUID(body["worker"]["id"])
    assert isinstance(body["token"], str) and body["token"]
    assert body["token_expires_at"]


async def test_register_worker_same_name(client: httpx.AsyncClient) -> None:
    """Registering under an existing name creates a separate worker."""
    first = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"claims": [{"kind": "agent"}]},
                "runtime": RUNTIME,
                "metadata": {"region": "eu"},
            },
        )
    ).json()
    second = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": {"claims": [{"kind": "importer"}]},
                "runtime": {"platform": "docker"},
                "metadata": {"region": "us"},
            },
        )
    ).json()
    assert second["worker"]["id"] != first["worker"]["id"]
    assert isinstance(second["token"], str) and second["token"]
    response = await client.get(f"/api/v1/workers/{first['worker']['id']}")
    assert response.json()["scope"]["claims"] == [
        {"kind": "agent", "agent_version_id": None}
    ]


async def test_renew_worker_token(client: httpx.AsyncClient) -> None:
    """Renew a worker token and observe a fresh token plus a seen stamp."""
    created = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    response = await client.post(f"/api/v1/workers/{created['worker']['id']}/token")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["token"], str) and body["token"]
    assert body["token_expires_at"]
    response = await client.get(f"/api/v1/workers/{created['worker']['id']}")
    assert response.json()["last_seen_at"] >= created["worker"]["last_seen_at"]


async def test_renew_worker_token_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when renewing an unknown worker."""
    response = await client.post(f"/api/v1/workers/{uuid.uuid4()}/token")
    assert response.status_code == 404


async def test_renew_worker_token_of_another_account(
    client: httpx.AsyncClient,
    auth_service: AuthService,
    account_repository: FakeAccountRepository,
) -> None:
    """Observe HTTP 403 when renewing a worker owned by another account."""
    created = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    other = await account_repository.create(Account(name="bob"))
    token = auth_service.issue_token(AuthContext(account=other)).token
    response = await client.post(
        f"/api/v1/workers/{created['worker']['id']}/token",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "client_header",
    ["kitaru-python/0.22.2", "kitaru-python/0.22.0rc10", "kitaru-python/0.9"],
)
async def test_register_worker_rejects_reregistering_client(
    client: httpx.AsyncClient, client_header: str
) -> None:
    """Observe HTTP 426 for an SDK version that renews by re-registering."""
    response = await client.post(
        "/api/v1/workers",
        json={"name": "worker-1", "scope": SCOPE, "runtime": RUNTIME, "metadata": {}},
        headers={CLIENT_HEADER: client_header},
    )
    assert response.status_code == 426
    assert "newer than 0.22.2" in response.json()["detail"]


@pytest.mark.parametrize(
    "client_header",
    [
        "kitaru-python/0.22.3.dev0",
        "kitaru-python/0.23.0.dev0",
        "kitaru-python/1.0.0",
        "kitaru-cli/0.22.2",
        "kitaru-mcp/0.9",
        "kitaru-ui/0.9",
        "kitaru-typescript/0.1.1",
        "curl/8.4.0",
        "kitaru-python",
        "kitaru-python/not-a-version",
        "",
    ],
)
async def test_register_worker_admits_supported_client(
    client: httpx.AsyncClient, client_header: str
) -> None:
    """Admit any non-SDK client and every SDK at or above the minimum."""
    response = await client.post(
        "/api/v1/workers",
        json={"name": "worker-1", "scope": SCOPE, "runtime": RUNTIME, "metadata": {}},
        headers={CLIENT_HEADER: client_header},
    )
    assert response.status_code == 200


async def test_register_worker_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid worker name."""
    response = await client.post(
        "/api/v1/workers",
        json={"name": "in valid", "scope": SCOPE, "runtime": RUNTIME, "metadata": {}},
    )
    assert response.status_code == 422


async def test_get_worker(client: httpx.AsyncClient) -> None:
    """Get a worker by id using the registering account's credential."""
    created = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    response = await client.get(f"/api/v1/workers/{created['worker']['id']}")
    assert response.status_code == 200
    assert response.json() == created["worker"]


async def test_get_worker_with_its_own_token(
    client: httpx.AsyncClient, auth_service: AuthService, account: Account
) -> None:
    """Get a worker using its own worker token."""
    created = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    worker_id = uuid.UUID(created["worker"]["id"])
    token = mint_worker_token(auth_service, worker_id, account)
    response = await client.get(
        f"/api/v1/workers/{worker_id}", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200
    assert response.json() == created["worker"]


async def test_get_worker_with_a_different_workers_token_is_forbidden(
    client: httpx.AsyncClient, auth_service: AuthService, account: Account
) -> None:
    """Observe HTTP 403 when a worker token names a different worker."""
    first = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    second = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-2",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    token = mint_worker_token(auth_service, uuid.UUID(first["worker"]["id"]), account)
    response = await client.get(
        f"/api/v1/workers/{second['worker']['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


async def test_get_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/workers/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Worker {missing_id} was not found"}


async def test_list_workers(client: httpx.AsyncClient) -> None:
    """List workers newest-first with filters."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        response = await client.post(
            "/api/v1/workers",
            json={"name": name, "scope": SCOPE, "runtime": RUNTIME, "metadata": {}},
        )
        assert response.status_code == 200

    response = await client.get("/api/v1/workers")
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
        "/api/v1/workers", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    assert response.json()["items"][0]["name"] == "worker-2"


async def test_list_workers_hides_stale_unless_asked(
    client: httpx.AsyncClient, repository: FakeWorkerRepository, account: Account
) -> None:
    """Leave stale workers out of the list unless include_stale is set."""
    await create_worker(repository, account.id, name="live")
    await create_worker(
        repository,
        account.id,
        name="stale",
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )

    response = await client.get("/api/v1/workers")
    assert [item["name"] for item in response.json()["items"]] == ["live"]

    response = await client.get("/api/v1/workers", params={"include_stale": "true"})
    assert [item["name"] for item in response.json()["items"]] == ["stale", "live"]


async def test_delete_worker(client: httpx.AsyncClient) -> None:
    """Delete a worker and observe HTTP 204."""
    created = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "worker-1",
                "scope": SCOPE,
                "runtime": RUNTIME,
                "metadata": {},
            },
        )
    ).json()
    response = await client.delete(f"/api/v1/workers/{created['worker']['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/workers/{created['worker']['id']}")
    assert response.status_code == 404


async def test_delete_worker_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    response = await client.delete(f"/api/v1/workers/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_worker_live_derivation(
    client: httpx.AsyncClient, repository: FakeWorkerRepository, account: Account
) -> None:
    """Derive live from last_seen_at against the liveness setting."""
    live = await create_worker(repository, account.id, name="live")
    stale = await create_worker(
        repository,
        account.id,
        name="stale",
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )

    response = await client.get(f"/api/v1/workers/{live.id}")
    assert response.json()["live"] is True

    response = await client.get(f"/api/v1/workers/{stale.id}")
    assert response.json()["live"] is False


async def test_heartbeat_worker(
    client: httpx.AsyncClient,
    repository: FakeWorkerRepository,
    job_repository: FakeJobRepository,
    task_repository: FakeTaskRepository,
    auth_service: AuthService,
    account: Account,
) -> None:
    """Report held tasks and observe the ones to stop in cancel_task_ids."""
    worker = await create_worker(repository, account.id)
    job = await create_job(job_repository, account.id)
    task = await create_agent_task(task_repository, job.id)
    task.claim(worker.id, datetime.now(UTC))
    await task_repository.update(task)
    token = mint_worker_token(auth_service, worker.id, account)

    response = await client.post(
        f"/api/v1/workers/{worker.id}/heartbeat",
        json={"task_ids": [str(task.id)]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["cancel_task_ids"] == []


async def test_heartbeat_worker_returns_reported_ids_the_worker_no_longer_owns(
    client: httpx.AsyncClient,
    repository: FakeWorkerRepository,
    auth_service: AuthService,
    account: Account,
) -> None:
    """A task the caller does not own comes back in cancel_task_ids."""
    worker = await create_worker(repository, account.id)
    missing_id = uuid.uuid4()
    token = mint_worker_token(auth_service, worker.id, account)
    response = await client.post(
        f"/api/v1/workers/{worker.id}/heartbeat",
        json={"task_ids": [str(missing_id)]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["cancel_task_ids"] == [str(missing_id)]


async def test_heartbeat_worker_not_found(
    client: httpx.AsyncClient, auth_service: AuthService, account: Account
) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    missing_id = uuid.uuid4()
    token = mint_worker_token(auth_service, missing_id, account)
    response = await client.post(
        f"/api/v1/workers/{missing_id}/heartbeat",
        json={"task_ids": []},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 404


async def test_heartbeat_worker_with_a_different_workers_token_is_forbidden(
    client: httpx.AsyncClient,
    repository: FakeWorkerRepository,
    auth_service: AuthService,
    account: Account,
) -> None:
    """Observe HTTP 403 when a worker token names a different worker."""
    worker = await create_worker(repository, account.id)
    other = await create_worker(repository, account.id, name="other")
    token = mint_worker_token(auth_service, other.id, account)
    response = await client.post(
        f"/api/v1/workers/{worker.id}/heartbeat",
        json={"task_ids": []},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


async def test_heartbeat_worker_rejects_an_account_credential(
    client: httpx.AsyncClient, repository: FakeWorkerRepository, account: Account
) -> None:
    """Observe HTTP 403 when the caller holds only an account credential."""
    worker = await create_worker(repository, account.id)
    response = await client.post(
        f"/api/v1/workers/{worker.id}/heartbeat", json={"task_ids": []}
    )
    assert response.status_code == 403
