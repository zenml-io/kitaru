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
from pathlib import Path

import pytest

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.jobs import JobKind, JobStatus
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError

CODE = b"def parse(payload):\n    return []\n"
PAYLOAD = b'{"external_id": "abc"}\n'


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def register_importer(
    api_client: KitaruAPIClient, tmp_path: Path, name: str = "langfuse"
) -> None:
    """Register an importer from a source file.

    Args:
        api_client: API client routed to the app.
        tmp_path: Temporary directory for the source file.
        name: Importer name.
    """
    source = tmp_path / "importer.py"
    source.write_bytes(CODE)
    await api_client.importers.register(
        name, source, entrypoint="parse", provider="langfuse"
    )


async def create_agent(api_client: KitaruAPIClient) -> uuid.UUID:
    """Create the agent imported sessions bind to.

    Args:
        api_client: API client routed to the app.

    Returns:
        Id of the created agent.
    """
    agent = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    return agent.id


async def test_create_import(api_client: KitaruAPIClient, tmp_path: Path) -> None:
    """Round-trip an import create request."""
    await register_importer(api_client, tmp_path)
    agent_id = await create_agent(api_client)
    payload = await api_client.blobs.upload(PAYLOAD, "application/jsonl")
    created = await api_client.imports.create(
        ImportCreateRequest(
            importer="langfuse",
            agent_id=agent_id,
            payload_blob_id=payload.id,
            params={"project": "demo"},
        )
    )
    assert created.kind is JobKind.IMPORT
    assert created.status is JobStatus.PENDING
    assert created.agent_id == agent_id
    assert created.payload_blob_id == payload.id
    assert created.inputs == {"project": "demo"}
    assert created.stats is None

    fetched = await api_client.jobs.get(created.id)
    assert fetched == created


async def test_create_import_unknown_importer(
    api_client: KitaruAPIClient,
) -> None:
    """Surface HTTP 404 for an unregistered importer name."""
    agent_id = await create_agent(api_client)
    payload = await api_client.blobs.upload(PAYLOAD, "application/jsonl")
    with pytest.raises(NotFoundError) as exc_info:
        await api_client.imports.create(
            ImportCreateRequest(
                importer="langfuse", agent_id=agent_id, payload_blob_id=payload.id
            )
        )
    assert exc_info.value.status_code == 404


async def test_create_import_unknown_payload(
    api_client: KitaruAPIClient, tmp_path: Path
) -> None:
    """Surface HTTP 404 for a payload blob that does not exist."""
    await register_importer(api_client, tmp_path)
    agent_id = await create_agent(api_client)
    with pytest.raises(NotFoundError) as exc_info:
        await api_client.imports.create(
            ImportCreateRequest(
                importer="langfuse", agent_id=agent_id, payload_blob_id=uuid.uuid4()
            )
        )
    assert exc_info.value.status_code == 404
