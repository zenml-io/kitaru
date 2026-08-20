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
"""Round-trip tests for the imports SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    JobAndTaskServices,
    asgi_api_client,
    build_job_and_task_services,
    create_agent,
    create_blob,
    create_plugin,
    override_idempotency,
)
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.job import JobResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_job_service,
    get_task_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


@pytest.fixture
async def api_client(
    services: JobAndTaskServices,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
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
    async with asgi_api_client(app) as client:
        yield client


async def test_create(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Create an import job through the SDK."""
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)

    job = await api_client.imports.create(
        ImportCreateRequest(
            importer="csv", agent_id=agent.id, payload_blob_id=payload.id
        )
    )
    assert isinstance(job, JobResponse)
    assert job.status.value == "pending"


async def test_create_not_found_for_unknown_importer(
    api_client: KitaruAPIClient, services: JobAndTaskServices
) -> None:
    """Surface HTTP 404 as a typed error for an unknown importer."""
    payload = await create_blob(services.blobs, ACCOUNT.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACCOUNT.id)
    with pytest.raises(NotFoundError):
        await api_client.imports.create(
            ImportCreateRequest(
                importer="does-not-exist",
                agent_id=agent.id,
                payload_blob_id=payload.id,
            )
        )
