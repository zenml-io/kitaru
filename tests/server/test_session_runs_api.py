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
"""Tests for the session run routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_agent_version,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter
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
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_session_run(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Create a session run job holding one labeled agent task."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )

    response = await client.post(
        "/api/v1/session-runs",
        json={
            "agent_version_id": str(version.id),
            "inputs": {"q": "hi"},
            "name": "run-1",
        },
    )
    assert response.status_code == 201
    job = response.json()
    assert job["status"] == "pending"

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=uuid.UUID(job["id"])), actor=AuthContext(account=ACCOUNT)
    )
    assert len(tasks) == 1
    assert tasks[0].labels == {"agent_version": str(version.id)}
    assert tasks[0].env == {"KITARU_SESSION_NAME": "run-1"}


async def test_create_session_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown agent version id."""
    response = await client.post(
        "/api/v1/session-runs",
        json={"agent_version_id": str(uuid.uuid4()), "inputs": None},
    )
    assert response.status_code == 404


async def test_create_session_run_rejects_a_version_without_a_run_spec(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 when the agent version carries no run spec."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions, agent_id=agent.id, owner_id=ACCOUNT.id
    )
    response = await client.post(
        "/api/v1/session-runs",
        json={"agent_version_id": str(version.id), "inputs": None},
    )
    assert response.status_code == 422
