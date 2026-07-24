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
"""Trace importer and import job route tests."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportJobRepository,
)
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
async def client_and_version() -> AsyncGenerator[
    tuple[httpx.AsyncClient, AgentVersion], None
]:
    """Provide an HTTP client and target version backed by fakes."""
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
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, version


async def test_list_importers(
    client_and_version: tuple[httpx.AsyncClient, AgentVersion],
) -> None:
    """List importer capabilities available in this deployment."""
    client, _ = client_and_version

    response = await client.get("/v1/importers")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "langfuse",
            "display_name": "Langfuse JSONL",
            "version": "1",
            "file_extensions": [".jsonl"],
            "max_upload_bytes": 50 * 1024 * 1024,
        }
    ]


async def test_create_and_get_import_job(
    client_and_version: tuple[httpx.AsyncClient, AgentVersion],
) -> None:
    """Upload JSONL as a pending background job and retrieve its status."""
    client, version = client_and_version

    response = await client.post(
        "/v1/import-jobs",
        data={
            "importer_id": "langfuse",
            "agent_version_id": str(version.id),
            "source_instance": "project-1",
        },
        files={
            "file": (
                "traces.jsonl",
                b'{"id":"root","traceId":"trace-1"}\n',
                "application/x-ndjson",
            )
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending"
    assert body["agent_version_id"] == str(version.id)
    assert body["source_instance"] == "project-1"
    assert body["filename"] == "traces.jsonl"
    assert body["source_session_count"] == 0
    assert "content" not in body

    loaded = await client.get(f"/v1/import-jobs/{body['id']}")
    assert loaded.status_code == 200
    assert loaded.json() == body


async def test_create_job_rejects_unknown_importer(
    client_and_version: tuple[httpx.AsyncClient, AgentVersion],
) -> None:
    """Reject an importer type absent from this deployment."""
    client, version = client_and_version

    response = await client.post(
        "/v1/import-jobs",
        data={
            "importer_id": "missing",
            "agent_version_id": str(version.id),
        },
        files={"file": ("traces.jsonl", b"{}\n", "application/x-ndjson")},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Importer 'missing' was not found"}


async def test_create_job_rejects_empty_file(
    client_and_version: tuple[httpx.AsyncClient, AgentVersion],
) -> None:
    """Reject an empty upload before queuing a job."""
    client, version = client_and_version

    response = await client.post(
        "/v1/import-jobs",
        data={
            "importer_id": "langfuse",
            "agent_version_id": str(version.id),
        },
        files={"file": ("traces.jsonl", b"", "application/x-ndjson")},
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "Import file is empty"}
