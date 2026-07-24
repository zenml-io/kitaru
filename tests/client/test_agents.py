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
"""Round-trip tests for the agents SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeAgentRepository, asgi_api_client
from kitaru.api_models.v1.agents import (
    AgentCreateRequest,
    AgentUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_agent_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = AgentService(repository=FakeAgentRepository())
    app.dependency_overrides[get_agent_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an agent through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name="support-bot", description="Answers tickets")
    )
    assert agent.name == "support-bot"
    assert agent.owner_id == ACCOUNT.id
    assert agent.description == "Answers tickets"


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    with pytest.raises(APIError) as exc_info:
        await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Agent name 'support-bot' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an agent by id through the SDK."""
    created = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    loaded = await api_client.agents.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.agents.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List agents with filters and pagination through the SDK."""
    for name in ["support-bot", "triage-bot", "coder"]:
        await api_client.agents.create(AgentCreateRequest(name=name))

    page = await api_client.agents.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["support-bot", "triage-bot", "coder"]

    page = await api_client.agents.list(name="triage-bot")
    assert page.total == 1
    assert page.items[0].name == "triage-bot"

    page = await api_client.agents.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["coder"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an agent through the SDK."""
    created = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    updated = await api_client.agents.update(
        created.id, AgentUpdateRequest(description="Answers tickets")
    )
    assert updated.name == "support-bot"
    assert updated.description == "Answers tickets"
    updated = await api_client.agents.update(
        created.id, AgentUpdateRequest(name="triage-bot")
    )
    assert updated.name == "triage-bot"
    assert updated.description == "Answers tickets"


async def test_update_null_clears_description(api_client: KitaruAPIClient) -> None:
    """Clear the description through an explicit None on the request."""
    created = await api_client.agents.create(
        AgentCreateRequest(name="support-bot", description="Answers tickets")
    )
    updated = await api_client.agents.update(
        created.id, AgentUpdateRequest(description=None)
    )
    assert updated.name == "support-bot"
    assert updated.description is None
    loaded = await api_client.agents.get(created.id)
    assert loaded.description is None


async def test_update_unset_fields_unchanged(api_client: KitaruAPIClient) -> None:
    """Keep every field on an update request without set fields."""
    created = await api_client.agents.create(
        AgentCreateRequest(name="support-bot", description="Answers tickets")
    )
    updated = await api_client.agents.update(created.id, AgentUpdateRequest())
    assert updated.name == "support-bot"
    assert updated.description == "Answers tickets"


async def test_update_null_name_rejected(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 for an explicit None name."""
    created = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    with pytest.raises(APIError) as exc_info:
        await api_client.agents.update(created.id, AgentUpdateRequest(name=None))
    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "Agent name cannot be null"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an agent through the SDK."""
    created = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    await api_client.agents.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.agents.get(created.id)
