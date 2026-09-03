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

import logging
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakeEphemeralWorkers,
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_blob,
    create_plugin,
    create_worker,
    local_settings,
    override_idempotency,
    stub_auth_session,
)
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.jwt import JWTToken
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_auth_service,
    get_auth_session,
    get_job_service,
    get_task_service,
    get_worker_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import ImportTask
from kitaru.server.ephemeral_worker_settings import (
    EphemeralWorkerBackend,
    EphemeralWorkerSettings,
    ModalEphemeralWorkerSettings,
)

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
def ephemeral_workers() -> FakeEphemeralWorkers:
    """Provide a fake ephemeral worker backend recording starts."""
    return FakeEphemeralWorkers()


@pytest.fixture
async def app(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    auth_service: AuthService,
    account_repository: FakeAccountRepository,
) -> FastAPI:
    """Provide the app wired to fake services and a fake ephemeral worker backend."""
    # auth_service resolves tokens against this same repository, so the actor
    # a worker token is minted for must be loadable from it.
    await account_repository.create(ACCOUNT)
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
            SERVER_URL="https://kitaru.example.com",
            EPHEMERAL_WORKER=EphemeralWorkerSettings(
                backend=EphemeralWorkerBackend.MODAL,
                image="zenmldocker/kitaru-worker:1.0.0",
                timeout_seconds=120,
                modal=ModalEphemeralWorkerSettings(
                    token_id="ak-test", token_secret="as-test"
                ),
            ),
        )
    )
    app.dependency_overrides[get_job_service] = lambda: services.job_service
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    app.dependency_overrides[get_worker_service] = lambda: WorkerService(
        repository=services.workers, liveness_timeout_seconds=60
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_auth_session] = stub_auth_session
    app.state.ephemeral_workers = ephemeral_workers
    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed job and task services."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _create_import_inputs(
    services: JobAndTaskServices, builtin: bool = False
) -> dict[str, Any]:
    """Build the request body for a create-import request.

    Args:
        services: Fake-backed job and task services.
        builtin: Whether the importer lives in the reserved namespace.

    Returns:
        JSON body for a create-import request.
    """
    name = "kitaru/csv" if builtin else "csv"
    plugin = await create_plugin(
        services.plugins,
        None if builtin else ACCOUNT.id,
        PluginKind.IMPORTER,
        name=name,
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)
    return {
        "importer": name,
        "agent_id": str(agent.id),
        "payload_blob_id": str(payload.id),
        "params": {
            "delimiter": ",",
            "join_on": "/metadata/customer~1case_id",
        },
    }


async def test_create_import(
    client: httpx.AsyncClient, services: JobAndTaskServices
) -> None:
    """Create an import job holding one importer task."""
    response = await client.post(
        "/api/v1/imports", json=await _create_import_inputs(services)
    )
    assert response.status_code == 201
    job = response.json()
    assert job["status"] == "pending"

    plugin = await services.plugins.get_by_name(PluginKind.IMPORTER, "csv")
    version = await services.plugins.get_version(plugin.id, plugin.latest_version)
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=uuid.UUID(job["id"])), actor=AuthContext(account=ACCOUNT)
    )
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.kind.value == "importer"
    assert task.plugin_version_id == version.id
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


async def test_create_import_starts_an_ephemeral_worker(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    auth_service: AuthService,
) -> None:
    """Register and start an ephemeral worker pinned to the job."""
    response = await client.post(
        "/api/v1/imports", json=await _create_import_inputs(services, builtin=True)
    )
    assert response.status_code == 201
    job_id = uuid.UUID(response.json()["id"])

    assert len(ephemeral_workers.starts) == 1
    spec = ephemeral_workers.starts[0]
    assert spec.job_id == job_id
    assert spec.server_url == "https://kitaru.example.com"

    worker = await services.workers.get(spec.worker_id)
    assert worker.name == f"job-{job_id}"
    assert worker.scope == WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.IMPORTER),
            WorkerClaim(kind=TaskKind.EVALUATOR),
        ],
        job_id=job_id,
    )
    assert worker.runtime.platform == "modal"
    assert worker.metadata == {"ephemeral": "true"}

    context = await auth_service.resolve(spec.worker_token.get_secret_value())
    assert isinstance(context.principal, WorkerPrincipal)
    assert context.principal.worker_id == spec.worker_id

    decoded = JWTToken.decode(spec.worker_token.get_secret_value(), local_settings())
    expected_expiry = datetime.now(UTC) + timedelta(
        seconds=120 + local_settings().TASK_TOKEN_EXPIRY_LEEWAY_SECONDS
    )
    assert abs((decoded.expires_at - expected_expiry).total_seconds()) < 5


async def test_create_import_skips_the_start_when_a_live_worker_covers_the_task(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
) -> None:
    """Skip registering and starting a worker when a live worker covers the task."""
    live_worker = await create_worker(
        services.workers,
        ACCOUNT.id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
    )

    response = await client.post(
        "/api/v1/imports", json=await _create_import_inputs(services, builtin=True)
    )
    assert response.status_code == 201

    assert ephemeral_workers.starts == []
    workers, _ = await services.workers.query(WorkerFilter(), None)
    assert [worker.id for worker in workers] == [live_worker.id]


async def test_create_import_skips_the_start_for_a_user_plugin(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
) -> None:
    """Skip registering and starting a worker for a user importer."""
    response = await client.post(
        "/api/v1/imports", json=await _create_import_inputs(services)
    )
    assert response.status_code == 201
    workers, _ = await services.workers.query(WorkerFilter(include_stale=True), None)
    assert workers == []
    assert ephemeral_workers.starts == []


async def test_create_import_without_an_ephemeral_worker_backend_registers_nothing(
    app: FastAPI,
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
) -> None:
    """Skip registering and starting a worker when no backend is configured."""
    app.state.ephemeral_workers = None

    response = await client.post(
        "/api/v1/imports", json=await _create_import_inputs(services)
    )
    assert response.status_code == 201

    assert ephemeral_workers.starts == []
    workers, _ = await services.workers.query(WorkerFilter(), None)
    assert workers == []


async def test_create_import_returns_201_when_the_start_fails(
    client: httpx.AsyncClient,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Observe HTTP 201 when the background start fails, with the failure logged."""
    ephemeral_workers.error = RuntimeError("modal is down")

    with caplog.at_level(logging.ERROR):
        response = await client.post(
            "/api/v1/imports", json=await _create_import_inputs(services, builtin=True)
        )
    assert response.status_code == 201
    job_id = response.json()["id"]

    workers, _ = await services.workers.query(WorkerFilter(), None)
    assert len(workers) == 1
    worker = workers[0]

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Failed to start worker" in message
        and str(worker.id) in message
        and job_id in message
        for message in messages
    )
