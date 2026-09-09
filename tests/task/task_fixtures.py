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
"""Shared task-app setup and helpers for the task package tests.

Imported directly into test modules rather than placed in a conftest.py,
since pytest resolves every conftest.py under a bare "conftest" module name
and a second file with that name here would shadow the top-level
tests/conftest.py that this module itself needs to import from. Each test
module wraps build_task_app() in its own @pytest.fixture named task_app,
so pytest fixture lookup by parameter name keeps working without a second
conftest.py.
"""

import uuid
from collections.abc import AsyncGenerator
from typing import NamedTuple

from conftest import (
    FakeReplayRepository,
    FakeSessionNodeRepository,
    JobAndTaskServices,
    asgi_api_client,
    build_job_and_task_services,
    build_payload_store,
    create_agent,
    create_blob,
    create_plugin,
    create_worker,
    override_idempotency,
)
from kitaru.api_models.v1.task import TaskStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_session_node_service,
    get_session_service,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.task import TaskUpdate
from kitaru.server.application.services.session_node_service import (
    SessionNodeService,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.plugin import PluginKind, PluginVersion, ScriptPluginSource

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


class TaskAppFixture(NamedTuple):
    """API client routed to the real app plus the fake services behind it."""

    client: KitaruAPIClient
    services: JobAndTaskServices
    agent: Agent


async def build_task_app() -> AsyncGenerator[TaskAppFixture, None]:
    """Build an API client routed to the app with fake-backed services."""
    services = build_job_and_task_services()
    payload_store = build_payload_store().store
    node_service = SessionNodeService(
        repository=FakeSessionNodeRepository(),
        session_repository=services.sessions,
        task_repository=services.tasks,
        payload_store=payload_store,
    )
    session_service = SessionService(
        repository=services.sessions,
        task_repository=services.tasks,
        agent_version_repository=services.agent_versions,
        replay_repository=FakeReplayRepository(),
        import_repository=services.imports,
        payload_store=payload_store,
    )
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    app.dependency_overrides[get_task_service] = lambda: services.task_service
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_session_node_service] = lambda: node_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    agent = await create_agent(services.agents, ACCOUNT.id)
    async with asgi_api_client(app) as client:
        yield TaskAppFixture(client=client, services=services, agent=agent)


async def start_task(fixture: TaskAppFixture, task_id: uuid.UUID) -> None:
    """Claim a task with a fresh worker and transition it to running.

    Mirrors what the worker does before spawning the task process, so the
    session create calls the flow makes see a running task. Goes through the
    task service directly, since the running transition requires a task
    principal that the fixture's static auth override cannot vary per task.

    Args:
        fixture: Task app fixture the task was created against.
        task_id: Id of the task to start.
    """
    worker = await create_worker(fixture.services.workers, ACCOUNT.id)
    worker_actor = WorkerAuthContext(
        account=ACCOUNT, principal=WorkerPrincipal(worker_id=worker.id)
    )
    await fixture.services.task_service.claim_tasks(10, actor=worker_actor)
    task = await fixture.services.tasks.get(task_id)
    task_actor = TaskAuthContext(
        account=ACCOUNT,
        principal=TaskPrincipal(
            task_id=task_id, attempt=1, worker_id=worker.id, job_id=task.job_id
        ),
    )
    await fixture.services.task_service.update_task(
        task_id, TaskUpdate(status=TaskStatus.RUNNING), actor=task_actor
    )


async def create_script_plugin_version(
    fixture: TaskAppFixture,
    kind: PluginKind,
    entrypoint: str,
    name: str = "plugin",
    provider: str | None = None,
) -> PluginVersion:
    """Register a script plugin version backed by a dummy blob.

    The task flows never read the blob's content, they load the entrypoint
    from the file at KITARU_TASK_PLUGIN_PATH, so the blob only stands in for
    the registry record.

    Args:
        fixture: Task app fixture to register the plugin against.
        kind: Plugin kind.
        entrypoint: Attribute name the entrypoint resolves to.
        name: Plugin name.
        provider: Source system, evaluators must leave this unset.

    Returns:
        Stored plugin version.
    """
    blob = await create_blob(fixture.services.blobs, ACCOUNT.id)
    plugin = await create_plugin(
        fixture.services.plugins, ACCOUNT.id, kind, name=name, provider=provider
    )
    return await fixture.services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint=entrypoint),
        display_version=None,
    )
