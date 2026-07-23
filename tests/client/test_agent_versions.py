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
"""Round-trip tests for the agent versions SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeSecretRepository,
    asgi_api_client,
    create_secret,
)
from kitaru.api_models.v1.agent_versions import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest, AgentResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide the fake secret repository backing the app."""
    return FakeSecretRepository()


@pytest.fixture
async def api_client(
    secret_repository: FakeSecretRepository,
) -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    agent_repository = FakeAgentRepository()
    version_repository = FakeAgentVersionRepository(agent_repository, secret_repository)
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(
        repository=version_repository,
        agent_repository=agent_repository,
        secret_repository=secret_repository,
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def create_agent(api_client: KitaruAPIClient) -> AgentResponse:
    """Store an agent through the SDK.

    Args:
        api_client: API client routed to the app.

    Returns:
        Created agent.
    """
    return await api_client.agents.create(AgentCreateRequest(name="support-bot"))


async def test_create(
    api_client: KitaruAPIClient, secret_repository: FakeSecretRepository
) -> None:
    """Create an agent version through the SDK."""
    agent = await create_agent(api_client)
    secret = await create_secret(secret_repository, ACCOUNT.id)
    run_spec = RunSpec(
        command="python agent.py",
        working_dir="/app",
        env={"MODE": "replay"},
        secret_ids=[secret.id],
        timeout_seconds=600,
    )
    version = await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=run_spec,
            capabilities=AgentCapabilities(tools=["search"]),
        ),
    )
    assert version.agent_id == agent.id
    assert version.owner_id == ACCOUNT.id
    assert version.version == "v1"
    assert version.run_spec == run_spec
    assert version.capabilities == AgentCapabilities(tools=["search"])


async def test_create_duplicate_version(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    agent = await create_agent(api_client)
    await api_client.agent_versions.create(
        agent.id, AgentVersionCreateRequest(version="v1")
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.agent_versions.create(
            agent.id, AgentVersionCreateRequest(version="v1")
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Agent version 'v1' is already registered"


async def test_create_unknown_agent(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.agent_versions.create(
            uuid.uuid4(), AgentVersionCreateRequest(version="v1")
        )


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an agent version by id through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.agent_versions.create(
        agent.id, AgentVersionCreateRequest(version="v1")
    )
    loaded = await api_client.agent_versions.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.agent_versions.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List agent versions with pagination through the SDK."""
    agent = await create_agent(api_client)
    for label in ["v1", "v2", "v3"]:
        await api_client.agent_versions.create(
            agent.id, AgentVersionCreateRequest(version=label)
        )

    page = await api_client.agent_versions.list(agent.id)
    assert page.total == 3
    assert [item.version for item in page.items] == ["v1", "v2", "v3"]

    page = await api_client.agent_versions.list(agent.id, page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.version for item in page.items] == ["v3"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an agent version through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.agent_versions.create(
        agent.id, AgentVersionCreateRequest(version="v1")
    )
    updated = await api_client.agent_versions.update(
        created.id, AgentVersionUpdateRequest(description="Tuned prompt")
    )
    assert updated.description == "Tuned prompt"
    run_spec = RunSpec(command="python agent.py", timeout_seconds=600)
    updated = await api_client.agent_versions.update(
        created.id, AgentVersionUpdateRequest(run_spec=run_spec)
    )
    assert updated.description == "Tuned prompt"
    assert updated.run_spec == run_spec


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an agent version through the SDK."""
    agent = await create_agent(api_client)
    created = await api_client.agent_versions.create(
        agent.id, AgentVersionCreateRequest(version="v1")
    )
    await api_client.agent_versions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.agent_versions.get(created.id)
