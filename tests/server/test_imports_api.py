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

import json
import uuid
from collections.abc import AsyncGenerator
from typing import Any

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
    get_import_service,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.plugin import PluginKind, PluginVersion, ScriptPluginSource
from kitaru.server.domain.task import ImportTask

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job, task, and import services."""
    return build_job_and_task_services()


@pytest.fixture
async def client(
    services: JobAndTaskServices,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed import services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_import_service] = lambda: services.import_service
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _importer_version(services: JobAndTaskServices) -> PluginVersion:
    """Register the csv importer with one version."""
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, PluginKind.IMPORTER, name="csv"
    )
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )


async def _evaluator_version(
    services: JobAndTaskServices, name: str, agent_id: uuid.UUID | None = None
) -> PluginVersion:
    """Register an evaluator with one version, scoped to an agent when given."""
    plugin = await create_plugin(
        services.plugins,
        ACCOUNT.id,
        PluginKind.EVALUATOR,
        name=name,
        agent_id=agent_id,
    )
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="score"),
        display_version=None,
    )


async def _import_request(
    services: JobAndTaskServices, agent: Agent | None = None, **overrides: Any
) -> dict[str, Any]:
    """Build a create request body naming a stored payload and agent."""
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    if agent is None:
        agent = await create_agent(services.agents, ACCOUNT.id)
    body: dict[str, Any] = {
        "importer": "csv",
        "agent_id": str(agent.id),
        "payload_blob_id": str(payload.id),
    }
    body.update(overrides)
    return body


async def test_create_import(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Create an import whose job holds one importer task linking the import."""
    version = await _importer_version(services)
    body = await _import_request(
        services,
        params={"delimiter": ",", "join_on": "/metadata/customer~1case_id"},
    )

    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 201
    created = response.json()
    assert created["owner_id"] == str(ACCOUNT.id)
    assert created["job_id"] is not None
    assert created["agent_id"] == body["agent_id"]
    assert created["agent_version_id"] is None
    assert created["importer_version_id"] == str(version.id)
    assert created["payload_blob_id"] == body["payload_blob_id"]
    assert created["params"] == body["params"]
    assert created["evaluators"] == []
    assert created["stats"] is None
    assert created["error"] is None

    job = await services.jobs.get(uuid.UUID(created["job_id"]))
    assert job.status.value == "pending"
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=AuthContext(account=ACCOUNT)
    )
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.kind.value == "importer"
    assert task.import_id == uuid.UUID(created["id"])
    assert task.labels == {}


async def test_create_import_with_evaluators(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Create an import carrying resolved evaluators and no outcome yet."""
    await _importer_version(services)
    await _evaluator_version(services, "accuracy")
    body = await _import_request(
        services, evaluators=[{"evaluator": "accuracy", "params": {"k": 1}}]
    )

    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 201
    created = response.json()
    assert created["evaluators"] == [
        {"evaluator": "accuracy", "version": 1, "params": {"k": 1}}
    ]
    assert created["stats"] is None
    assert created["error"] is None


async def test_create_import_not_found_for_unknown_evaluator(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 404 for an evaluator that does not exist."""
    await _importer_version(services)
    body = await _import_request(services, evaluators=[{"evaluator": "does-not-exist"}])
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 404


async def test_create_import_rejects_an_evaluator_scoped_to_another_agent(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 for an evaluator scoped to a different agent."""
    await _importer_version(services)
    other = await create_agent(services.agents, ACCOUNT.id, name="other")
    await _evaluator_version(services, "accuracy", agent_id=other.id)
    body = await _import_request(services, evaluators=[{"evaluator": "accuracy"}])
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 422


async def test_create_import_rejects_duplicate_evaluator_versions(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 when two evaluator configs resolve to one version."""
    await _importer_version(services)
    await _evaluator_version(services, "accuracy")
    body = await _import_request(
        services,
        evaluators=[
            {"evaluator": "accuracy"},
            {"evaluator": "accuracy", "version": 1},
        ],
    )
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 422


async def test_create_import_rejects_nul_byte_in_importer(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 422 for a NUL byte in the importer name."""
    body = await _import_request(services, importer="csv\x00")
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 422


async def test_create_import_not_found_for_unknown_importer(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 404 for an unknown importer name."""
    body = await _import_request(services, importer="does-not-exist")
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 404


async def test_create_import_not_found_for_unknown_payload(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Observe HTTP 404 for an unknown payload blob id."""
    await _importer_version(services)
    body = await _import_request(services, payload_blob_id=str(uuid.uuid4()))
    response = await client.post("/api/v1/imports", json=body)
    assert response.status_code == 404


async def test_get_import(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Read a created import back by id."""
    await _importer_version(services)
    body = await _import_request(services)
    created = (await client.post("/api/v1/imports", json=body)).json()

    response = await client.get(f"/api/v1/imports/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_import_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown import id."""
    response = await client.get(f"/api/v1/imports/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_imports_filters_by_agent_id(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """List every import, then only the ones of one agent."""
    await _importer_version(services)
    agent = await create_agent(services.agents, ACCOUNT.id)
    other = await create_agent(services.agents, ACCOUNT.id, name="other")
    first = (
        await client.post(
            "/api/v1/imports", json=await _import_request(services, agent=agent)
        )
    ).json()
    await client.post(
        "/api/v1/imports", json=await _import_request(services, agent=other)
    )

    response = await client.get("/api/v1/imports")
    assert response.status_code == 200
    assert len(response.json()["items"]) == 2

    agent_filter = {"field": "agent_id", "op": "eq", "value": str(agent.id)}
    response = await client.get(
        "/api/v1/imports", params={"filter": json.dumps(agent_filter)}
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [first["id"]]
