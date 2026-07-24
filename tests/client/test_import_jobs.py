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
"""Round-trip tests for the import jobs SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportJobRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.import_jobs import ImportJobStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.server.adapters.importers.registry import ImporterRegistry
from kitaru.server.adapters.rest.dependencies import authorize, get_import_job_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.import_job_service import ImportJobService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client_and_version() -> AsyncGenerator[
    tuple[KitaruAPIClient, AgentVersion], None
]:
    """Provide an SDK client and target agent version."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    version_repository = FakeAgentVersionRepository(agent_repository)
    agent = await agent_repository.create(
        Agent(owner_id=ACCOUNT.id, name="support-bot")
    )
    version = await version_repository.create(
        AgentVersion(
            owner_id=ACCOUNT.id,
            agent_id=agent.id,
            version="v1",
        )
    )
    service = ImportJobService(
        repository=FakeImportJobRepository(),
        agent_version_repository=version_repository,
        registry=ImporterRegistry(),
    )
    app.dependency_overrides[get_import_job_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client, version


async def test_list_create_and_get(
    api_client_and_version: tuple[KitaruAPIClient, AgentVersion],
) -> None:
    """Round-trip importer discovery, upload, and job status."""
    client, version = api_client_and_version

    importers = await client.import_jobs.list_importers()
    created = await client.import_jobs.create(
        content=b'{"id":"root","traceId":"trace-1"}\n',
        filename="traces.jsonl",
        importer_id="langfuse",
        agent_version_id=version.id,
        source_instance="project-1",
    )
    loaded = await client.import_jobs.get(created.id)

    assert [importer.id for importer in importers] == ["langfuse"]
    assert created.status is ImportJobStatus.PENDING
    assert created.agent_version_id == version.id
    assert created.source_instance == "project-1"
    assert loaded == created
