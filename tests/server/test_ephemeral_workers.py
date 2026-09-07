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
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_blob,
    create_plugin,
    create_worker,
    local_settings,
)
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import TaskKind, TaskStatus
from kitaru.api_models.v1.worker import LabelSelector, WorkerClaim, WorkerScope
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.auth.jwt import JWTToken
from kitaru.server.adapters.rest.ephemeral_workers import start_ephemeral_worker
from kitaru.server.api.bootstrap import register_default_plugins
from kitaru.server.application.models.auth import AuthContext, WorkerPrincipal
from kitaru.server.application.models.imports import ImportCreate
from kitaru.server.application.models.task import TaskUpdate
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import AnalysisTask
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


async def _create_import(services: JobAndTaskServices, builtin: bool = False) -> Job:
    """Create an import job for a user or a reserved namespace importer."""
    await register_default_plugins(services.plugins)
    name = "kitaru/test-csv" if builtin else "csv"
    plugin = await create_plugin(
        services.plugins,
        None if builtin else ACCOUNT.id,
        PluginKind.IMPORTER,
        name=name,
    )
    code = await create_blob(services.blobs, ACCOUNT.id, content=b"def run(): pass")
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code.id, entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)
    import_ = await services.import_service.create_import(
        ImportCreate(importer=name, agent_id=agent.id, payload_blob_id=payload.id),
        actor=ACTOR,
    )
    assert import_.job_id is not None
    return await services.jobs.get(import_.job_id)


async def _start(
    job: Job,
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    auth_service: AuthService,
) -> None:
    """Schedule the start for a job and run the background tasks."""
    background_tasks = BackgroundTasks()
    await start_ephemeral_worker(
        job,
        services.job_service,
        WorkerService(repository=services.workers, liveness_timeout_seconds=60),
        auth_service,
        ephemeral_workers,
        SETTINGS,
        SERVER_ID,
        background_tasks,
        ACTOR,
    )
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
            )
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


async def test_worker_drains_analysis_added_after_import_claim(
    services: JobAndTaskServices,
    ephemeral_workers: FakeEphemeralWorkers,
    stored_auth_service: AuthService,
) -> None:
    """The same worker claims later analysis tasks and settles its import job."""
    job = await _create_import(services, builtin=True)
    await _start(job, services, ephemeral_workers, stored_auth_service)
    worker_id = ephemeral_workers.starts[0].worker_id
    actor = build_worker_actor(ACCOUNT, worker_id)
    [claimed] = await services.task_service.claim_tasks(10, actor=actor)
    importer = claimed.task
    await services.task_service.update_task(
        importer.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACCOUNT, importer.id, importer.attempt, worker_id),
    )

    plugin = await services.plugins.get_by_name(
        PluginKind.ANALYZER, "kitaru/post-import-insights"
    )
    assert plugin.latest_version is not None
    plugin_version = await services.plugins.get_version(
        plugin.id, plugin.latest_version
    )
    analysis = await services.tasks.create(
        AnalysisTask(
            job_id=job.id,
            plugin_version_id=plugin_version.id,
            agent_id=uuid.uuid4(),
            import_id=uuid.uuid4(),
            labels={"kitaru/plugin_namespace": "kitaru"},
        )
    )
    await services.task_service.update_task(
        importer.id,
        TaskUpdate(status=TaskStatus.COMPLETED, result={}),
        actor=build_task_actor(ACCOUNT, importer.id, importer.attempt, worker_id),
    )
    assert (await services.jobs.get(job.id)).status is JobStatus.RUNNING
    [claimed_analysis] = await services.task_service.claim_tasks(10, actor=actor)
    assert claimed_analysis.task.id == analysis.id
    await services.task_service.update_task(
        analysis.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(
            ACCOUNT, analysis.id, claimed_analysis.task.attempt, worker_id
        ),
    )
    await services.task_service.update_task(
        analysis.id,
        TaskUpdate(status=TaskStatus.COMPLETED, result=[]),
        actor=build_task_actor(
            ACCOUNT, analysis.id, claimed_analysis.task.attempt, worker_id
        ),
    )
    assert (await services.jobs.get(job.id)).status is JobStatus.COMPLETED


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
