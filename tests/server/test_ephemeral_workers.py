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
"""Tests for ephemeral worker start scheduling."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from importlib.metadata import version

import pytest
from fastapi import BackgroundTasks

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
)
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerClaim, WorkerScope
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.jwt import JWTToken
from kitaru.server.adapters.rest.ephemeral_workers import EphemeralWorkerStarter
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.imports import ImportCreate
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.ephemeral_worker_settings import (
    EphemeralWorkerBackend,
    EphemeralWorkerSettings,
    ModalEphemeralWorkerSettings,
)

ACCOUNT = Account(id=uuid.uuid4(), name="ann")
ACTOR = AuthContext(account=ACCOUNT)
SERVER_ID = uuid.uuid4()
SETTINGS = local_settings(
    SERVER_URL="https://kitaru.example.com",
    EPHEMERAL_WORKER=EphemeralWorkerSettings(
        backend=EphemeralWorkerBackend.MODAL,
        image="zenmldocker/kitaru-worker:1.0.0",
        timeout_seconds=120,
        modal=ModalEphemeralWorkerSettings(token_id="ak-test", token_secret="as-test"),
    ),
)


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
def ephemeral_workers() -> FakeEphemeralWorkers:
    """Provide a fake ephemeral worker backend recording starts."""
    return FakeEphemeralWorkers()


@pytest.fixture
async def stored_auth_service(
    auth_service: AuthService, account_repository: FakeAccountRepository
) -> AuthService:
    """Provide an authentication service that can resolve the actor's account."""
    await account_repository.create(ACCOUNT)
    return auth_service


async def _create_import(
    services: JobAndTaskServices, builtin: bool = False, fetch: bool = False
) -> Job:
    """Create an import job for a user or a reserved namespace importer."""
    name = "kitaru/csv" if builtin else "csv"
    plugin = await create_plugin(
        services.plugins,
        None if builtin else ACCOUNT.id,
        PluginKind.IMPORTER,
        name=name,
        provider="langfuse",
        connection_schema={"type": "object"},
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    agent = await create_agent(services.agents, ACCOUNT.id)
    if fetch:
        command = ImportCreate(
            importer=name, agent_id=agent.id, fetch_query={"since": "2026-08-01"}
        )
    else:
        payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
        command = ImportCreate(
            importer=name, agent_id=agent.id, payload_blob_id=payload.id
        )
    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert import_.job_id is not None
    return await services.jobs.get(import_.job_id)


async def _start(
    job: Job,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    auth_service: AuthService,
    settings: APISettings = SETTINGS,
) -> None:
    """Schedule the start for a job and run the background tasks."""
    background_tasks = BackgroundTasks()
    starter = EphemeralWorkerStarter(
        job_service=services.job_service,
        worker_service=WorkerService(
            repository=services.workers, liveness_timeout_seconds=60
        ),
        auth_service=auth_service,
        ephemeral_workers=ephemeral_workers,
        settings=settings,
        server_id=SERVER_ID,
        background_tasks=background_tasks,
    )
    await starter.start(job.id, actor=ACTOR)
    await background_tasks()


async def test_start_registers_and_starts_a_worker_pinned_to_the_job(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """Register a worker with the ephemeral scope and start it with its token."""
    job = await _create_import(services, builtin=True)

    await _start(job, services, ephemeral_workers, stored_auth_service)

    assert len(ephemeral_workers.starts) == 1
    spec = ephemeral_workers.starts[0]
    assert spec.job_id == job.id
    assert spec.server_url == "https://kitaru.example.com"

    worker = await services.workers.get(spec.worker_id)
    assert worker.name == f"job-{job.id}"
    assert spec.name == worker.name
    assert spec.tags == {
        "kitaru/worker_id": str(worker.id),
        "kitaru/job_id": str(job.id),
        "kitaru/account_id": str(ACCOUNT.id),
        "kitaru/server_version": version("kitaru"),
        "kitaru/server_id": str(SERVER_ID),
    }
    assert worker.scope == WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.IMPORTER),
            WorkerClaim(kind=TaskKind.EVALUATOR),
            WorkerClaim(kind=TaskKind.ANALYZER),
        ],
        selectors=[
            LabelSelector(
                key="kitaru/plugin_namespace", values=["kitaru"], required=True
            ),
        ],
        job_id=job.id,
    )
    assert worker.runtime.platform == "modal"
    assert worker.metadata == {"ephemeral": "true"}

    context = await stored_auth_service.resolve(spec.worker_token.get_secret_value())
    assert isinstance(context.principal, WorkerPrincipal)
    assert context.principal.worker_id == spec.worker_id

    decoded = JWTToken.decode(spec.worker_token.get_secret_value(), local_settings())
    expected_expiry = datetime.now(UTC) + timedelta(
        seconds=120 + local_settings().TASK_TOKEN_EXPIRY_LEEWAY_SECONDS
    )
    assert abs((decoded.expires_at - expected_expiry).total_seconds()) < 5


async def test_start_skips_when_a_live_worker_covers_the_task(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """Skip registering and starting a worker when a live worker covers the task."""
    live_worker = await create_worker(
        services.workers,
        ACCOUNT.id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
    )
    job = await _create_import(services, builtin=True)

    await _start(job, services, ephemeral_workers, stored_auth_service)

    assert ephemeral_workers.starts == []
    workers, _ = await services.workers.query(WorkerFilter(), None)
    assert [worker.id for worker in workers] == [live_worker.id]


async def test_start_skips_a_task_needing_credentials_the_worker_lacks(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """Skip a task needing a provider's credentials the settings do not name."""
    job = await _create_import(services, builtin=True, fetch=True)

    await _start(job, services, ephemeral_workers, stored_auth_service)

    assert ephemeral_workers.starts == []
    workers, _ = await services.workers.query(WorkerFilter(include_stale=True), None)
    assert workers == []


async def test_start_covers_a_task_needing_a_configured_provider(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """Register a worker with the selectors the settings carry."""
    settings = local_settings(
        SERVER_URL="https://kitaru.example.com",
        EPHEMERAL_WORKER=EphemeralWorkerSettings(
            backend=EphemeralWorkerBackend.MODAL,
            image="zenmldocker/kitaru-worker:1.0.0",
            selectors=[
                LabelSelector(key="kitaru/requires-credentials", values=["langfuse"])
            ],
            modal=ModalEphemeralWorkerSettings(
                token_id="ak-test", token_secret="as-test"
            ),
        ),
    )
    job = await _create_import(services, builtin=True, fetch=True)

    await _start(job, services, ephemeral_workers, stored_auth_service, settings)

    assert len(ephemeral_workers.starts) == 1
    worker = await services.workers.get(ephemeral_workers.starts[0].worker_id)
    assert worker.scope.selectors is not None
    assert worker.scope.selectors[-1] == LabelSelector(
        key="kitaru/requires-credentials", values=["langfuse"]
    )


async def test_start_skips_a_user_plugin(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """Skip registering and starting a worker for a user importer."""
    job = await _create_import(services)

    await _start(job, services, ephemeral_workers, stored_auth_service)

    assert ephemeral_workers.starts == []
    workers, _ = await services.workers.query(WorkerFilter(include_stale=True), None)
    assert workers == []


async def test_start_logs_a_failed_start(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log a failed background start with the worker and job ids."""
    ephemeral_workers.error = RuntimeError("modal is down")
    job = await _create_import(services, builtin=True)

    with caplog.at_level(logging.ERROR):
        await _start(job, services, ephemeral_workers, stored_auth_service)

    workers, _ = await services.workers.query(WorkerFilter(), None)
    assert len(workers) == 1
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Failed to start worker" in message
        and str(workers[0].id) in message
        and str(job.id) in message
        for message in messages
    )
