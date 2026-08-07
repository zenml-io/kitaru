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
"""Tests for the agent version routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeAgentRepository, FakeAgentVersionRepository, FakeTagRepository
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
    get_tag_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def tag_repository() -> FakeTagRepository:
    """Provide the fake tag repository backing the app."""
    return FakeTagRepository()


@pytest.fixture
def agent_version_repository(
    agent_repository: FakeAgentRepository,
    tag_repository: FakeTagRepository,
) -> FakeAgentVersionRepository:
    """Provide the fake agent version repository backing the app, tag-aware."""
    return FakeAgentVersionRepository(agent_repository, tags=tag_repository)


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    tag_repository: FakeTagRepository,
    agent_version_repository: FakeAgentVersionRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed agent services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(repository=agent_version_repository)
    tag_service = TagService(repository=tag_repository)
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[get_tag_service] = lambda: tag_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent to version."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def test_list_agent_versions_filters_by_tag(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Filter agent versions linked to a tag through tag_link."""
    tagged = (await client.post(f"/api/v1/agents/{agent_id}/versions", json={})).json()
    await client.post(f"/api/v1/agents/{agent_id}/versions", json={})

    tag = (await client.post("/api/v1/tags", json={"name": "smoke-test"})).json()
    await client.post(
        f"/api/v1/tags/{tag['id']}/links",
        json={"resource_type": "agent_version", "resource_id": tagged["id"]},
    )

    filter_expression = {"field": "tag", "op": "eq", "value": "smoke-test"}
    response = await client.get(
        f"/api/v1/agents/{agent_id}/versions",
        params={"filter": json.dumps(filter_expression)},
    )
    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["id"] for item in items] == [tagged["id"]]


async def test_get_agent_version(client: httpx.AsyncClient, agent_id: str) -> None:
    """Get an agent version by id."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_agent_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent version."""
    response = await client.get(f"/api/v1/agent-versions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_update_agent_version(client: httpx.AsyncClient, agent_id: str) -> None:
    """Update the display version and description of an agent version."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={"display_version": "v1", "description": "First cut"},
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}",
        json={"display_version": "v1.1", "description": "Second cut"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["display_version"] == "v1.1"
    assert body["description"] == "Second cut"


async def test_update_agent_version_clears_display_version(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Clear the display version with an explicit null."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions", json={"display_version": "v1"}
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}", json={"display_version": None}
    )
    assert response.status_code == 200
    assert response.json()["display_version"] is None


async def test_update_agent_version_replaces_run_spec(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Replace the run spec of an agent version."""
    old_secret_id = str(uuid.uuid4())
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={
                "run_spec": {
                    "type": "command",
                    "command": "old.sh",
                    "secret_ids": [old_secret_id],
                }
            },
        )
    ).json()
    new_secret_id = str(uuid.uuid4())
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}",
        json={
            "run_spec": {
                "type": "command",
                "command": "new.sh",
                "secret_ids": [new_secret_id],
            }
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["run_spec"]["command"] == "new.sh"
    assert body["run_spec"]["secret_ids"] == [new_secret_id]


async def test_update_agent_version_clears_run_spec(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Clear the run spec with an explicit null."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={"run_spec": {"type": "command", "command": "run.sh"}},
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}", json={"run_spec": None}
    )
    assert response.status_code == 200
    assert response.json()["run_spec"] is None


async def test_update_agent_version_capabilities_null_clears_to_empty(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Clear capabilities to an empty value with an explicit null."""
    created = (
        await client.post(
            f"/api/v1/agents/{agent_id}/versions",
            json={"capabilities": {"tools": ["search"]}},
        )
    ).json()
    response = await client.patch(
        f"/api/v1/agent-versions/{created['id']}", json={"capabilities": None}
    )
    assert response.status_code == 200
    assert response.json()["capabilities"] == {
        "tools": [],
        "mcp_servers": [],
        "skills": [],
    }


async def test_update_agent_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent version."""
    response = await client.patch(
        f"/api/v1/agent-versions/{uuid.uuid4()}", json={"description": "x"}
    )
    assert response.status_code == 404


async def test_delete_agent_version(client: httpx.AsyncClient, agent_id: str) -> None:
    """Delete an agent version."""
    created = (await client.post(f"/api/v1/agents/{agent_id}/versions", json={})).json()
    response = await client.delete(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/agent-versions/{created['id']}")
    assert response.status_code == 404


async def test_delete_agent_version_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing agent version."""
    response = await client.delete(f"/api/v1/agent-versions/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_agent_version_allows_agent_delete(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Allow deleting the agent again once its only version is gone."""
    created = (await client.post(f"/api/v1/agents/{agent_id}/versions", json={})).json()
    await client.delete(f"/api/v1/agent-versions/{created['id']}")
    response = await client.delete(f"/api/v1/agents/{agent_id}")
    assert response.status_code == 204
