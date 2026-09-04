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
"""Tests for the import routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_blob,
    create_plugin,
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
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import ImportTask

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


async def test_create_import(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Create an import job holding one importer task."""
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, PluginKind.IMPORTER, name="csv"
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)

    response = await client.post(
        "/api/v1/imports",
        json={
            "importer": "csv",
            "agent_id": str(agent.id),
            "payload_blob_id": str(payload.id),
            "params": {
                "delimiter": ",",
                "join_on": "/metadata/customer~1case_id",
            },
        },
    )
    assert response.status_code == 201
    job = response.json()
    assert job["status"] == "pending"

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=uuid.UUID(job["id"])), actor=AuthContext(account=ACCOUNT)
    )
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.kind.value == "importer"
    assert task.plugin_version_id == version.id
    assert task.labels == {}
    assert task.params == {
        "delimiter": ",",
        "join_on": "/metadata/customer~1case_id",
    }


async def test_create_import_rejects_nul_byte_in_importer(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 for a NUL byte in the importer name."""
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)
    response = await client.post(
        "/api/v1/imports",
        json={
            "importer": "csv\x00",
            "agent_id": str(agent.id),
            "payload_blob_id": str(payload.id),
        },
    )
    assert response.status_code == 422


async def test_create_import_not_found_for_unknown_importer(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 404 for an unknown importer name."""
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)
    response = await client.post(
        "/api/v1/imports",
        json={
            "importer": "does-not-exist",
            "agent_id": str(agent.id),
            "payload_blob_id": str(payload.id),
        },
    )
    assert response.status_code == 404


async def test_create_import_not_found_for_unknown_payload(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 404 for an unknown payload blob id."""
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    agent = await create_agent(services.agents, ACCOUNT.id)
    response = await client.post(
        "/api/v1/imports",
        json={
            "importer": "csv",
            "agent_id": str(agent.id),
            "payload_blob_id": str(uuid.uuid4()),
        },
    )
    assert response.status_code == 404
