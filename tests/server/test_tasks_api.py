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
from datetime import UTC, datetime

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
    local_settings,
    mint_worker_token,
)
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    get_auth_service,
    get_auth_session,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def account_token(auth_service: AuthService, account: Account) -> str:
    """Provide a bearer token authenticating as the fixture account."""
    return auth_service.issue_token(AuthContext(account=account)).token


@pytest.fixture
async def client(
    services: JobAndTaskServices,
    auth_service: AuthService,
    account_token: str,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client authenticated as the fixture account by default."""
    app = create_app(local_settings())
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_auth_session] = lambda: None
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {account_token}"},
    ) as client:
        yield client


async def _claimable_agent_task(
    services: JobAndTaskServices,
    job_id: uuid.UUID,
    account: Account,
    agent_name: str = "assistant",
):
    """Store an agent task backed by a real agent version, so its spec builds."""
    agent = await create_agent(services.agents, account.id, name=agent_name)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=account.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return await create_agent_task(services.tasks, job_id, agent_version_id=version.id)


async def test_get_task(
    client: httpx.AsyncClient, services: JobAndTaskServices, account: Account
) -> None:
    """Get a task by id."""
    job = await create_job(services.jobs, account.id)
    task = await create_agent_task(services.tasks, job.id)
    response = await client.get(f"/api/v1/tasks/{task.id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(task.id)


async def test_get_task_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown task id."""
    response = await client.get(f"/api/v1/tasks/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_tasks_filters(
    client: httpx.AsyncClient, services: JobAndTaskServices, account: Account
) -> None:
    """List tasks filtered by job_id."""
    job = await create_job(services.jobs, account.id)
    other_job = await create_job(services.jobs, account.id)
    task = await create_agent_task(services.tasks, job.id)
    await create_agent_task(services.tasks, other_job.id)

    filter_expression = {"field": "job_id", "op": "eq", "value": str(job.id)}
    response = await client.get(
        "/api/v1/tasks", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == str(task.id)


async def test_claim_tasks(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Claim tasks and observe the spec and per-task token shipped alongside each."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    token = mint_worker_token(auth_service, worker.id, account)

    response = await client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": 10},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["tasks"]) == 1
    entry = body["tasks"][0]
    assert entry["task"]["id"] == str(task.id)
    assert entry["task"]["status"] == "claimed"
    assert entry["spec"]["kind"] == "agent"
    assert entry["spec"]["run"]["command"] == "run.sh"
    assert isinstance(entry["token"], str) and entry["token"]


async def test_claim_tasks_not_found(
    client: httpx.AsyncClient, auth_service: AuthService, account: Account
) -> None:
    """Observe HTTP 404 for an unknown worker id."""
    token = mint_worker_token(auth_service, uuid.uuid4(), account)
    response = await client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": 10},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 404


async def test_claim_tasks_rejects_an_account_credential(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 403 when the caller holds only an account credential."""
    response = await client.post("/api/v1/tasks/claim", json={"max_tasks": 10})
    assert response.status_code == 403


async def test_get_task_spec(
    client: httpx.AsyncClient, services: JobAndTaskServices, account: Account
) -> None:
    """Get a task's execution spec."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    response = await client.get(f"/api/v1/tasks/{task.id}/spec")
    assert response.status_code == 200
    assert response.json()["kind"] == "agent"


async def test_get_task_spec_with_a_task_token(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """A task token reads its own spec and observes HTTP 403 on another task's."""
    job = await create_job(services.jobs, account.id)
    target = await _claimable_agent_task(services, job.id, account)
    await _claimable_agent_task(services, job.id, account, agent_name="other-assistant")
    worker = await create_worker(services.workers, account.id)
    worker_token = mint_worker_token(auth_service, worker.id, account)
    claimed = (
        await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 10},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
    ).json()
    tokens = {item["task"]["id"]: item["token"] for item in claimed["tasks"]}
    own_token = tokens[str(target.id)]
    other_token = next(
        token for task_id, token in tokens.items() if task_id != str(target.id)
    )

    response = await client.get(
        f"/api/v1/tasks/{target.id}/spec",
        headers={"Authorization": f"Bearer {own_token}"},
    )
    assert response.status_code == 200
    assert response.json()["kind"] == "agent"

    response = await client.get(
        f"/api/v1/tasks/{target.id}/spec",
        headers={"Authorization": f"Bearer {other_token}"},
    )
    assert response.status_code == 403


async def test_update_task_transitions(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """PATCH transitions a claimed task to running using its claimed token."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_token = mint_worker_token(auth_service, worker.id, account)
    claimed = (
        await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 10},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
    ).json()
    task_token = claimed["tasks"][0]["token"]

    response = await client.patch(
        f"/api/v1/tasks/{task.id}",
        json={"status": "running"},
        headers={"Authorization": f"Bearer {task_token}"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "running"


async def test_update_task_rejects_an_account_credential(
    client: httpx.AsyncClient, services: JobAndTaskServices, account: Account
) -> None:
    """Observe HTTP 403 when the caller holds only an account credential."""
    job = await create_job(services.jobs, account.id)
    task = await create_agent_task(services.tasks, job.id)
    response = await client.patch(
        f"/api/v1/tasks/{task.id}", json={"status": "running"}
    )
    assert response.status_code == 403


async def test_update_task_with_a_different_tasks_token_is_forbidden(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 403 when a task token names a different task."""
    job = await create_job(services.jobs, account.id)
    target = await _claimable_agent_task(services, job.id, account)
    await _claimable_agent_task(services, job.id, account, agent_name="other-assistant")
    worker = await create_worker(services.workers, account.id)
    worker_token = mint_worker_token(auth_service, worker.id, account)
    claimed = (
        await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 10},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
    ).json()
    tokens = {item["task"]["id"]: item["token"] for item in claimed["tasks"]}
    other_token = next(
        token for task_id, token in tokens.items() if task_id != str(target.id)
    )

    response = await client.patch(
        f"/api/v1/tasks/{target.id}",
        json={"status": "running"},
        headers={"Authorization": f"Bearer {other_token}"},
    )
    assert response.status_code == 403


async def test_update_task_attempt_fencing_conflicts(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 409 when the held token's attempt was superseded by a re-claim."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_token = mint_worker_token(auth_service, worker.id, account)
    claimed = (
        await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 10},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
    ).json()
    stale_token = claimed["tasks"][0]["token"]

    # Simulate the sweep requeuing the stale attempt and a fresh claim picking
    # it back up, which moves the task's attempt past the held token's.
    stored = await services.tasks.get(task.id)
    stored.requeue()
    stored.claim(worker.id, datetime.now(UTC))
    await services.tasks.update(stored)

    response = await client.patch(
        f"/api/v1/tasks/{task.id}",
        json={"status": "running"},
        headers={"Authorization": f"Bearer {stale_token}"},
    )
    assert response.status_code == 409


async def test_update_task_requires_a_status(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    account: Account,
    auth_service: AuthService,
) -> None:
    """Observe HTTP 422 when the body carries no status."""
    job = await create_job(services.jobs, account.id)
    task = await _claimable_agent_task(services, job.id, account)
    worker = await create_worker(services.workers, account.id)
    worker_token = mint_worker_token(auth_service, worker.id, account)
    claimed = (
        await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 10},
            headers={"Authorization": f"Bearer {worker_token}"},
        )
    ).json()
    task_token = claimed["tasks"][0]["token"]

    response = await client.patch(
        f"/api/v1/tasks/{task.id}",
        json={},
        headers={"Authorization": f"Bearer {task_token}"},
    )
    assert response.status_code == 422
