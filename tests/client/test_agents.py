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
"""Round-trip tests for the agents and agent versions SDK resources."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeTagRepository,
    asgi_api_client,
)
from kitaru.api_models.v1.agent import (
    AgentCreateRequest,
    AgentListParams,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionListParams,
    AgentVersionResponse,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagResourceType,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
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
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_repository = FakeAgentRepository()
    tag_repository = FakeTagRepository()
    agent_service = AgentService(repository=agent_repository)
    version_service = AgentVersionService(
        repository=FakeAgentVersionRepository(agent_repository, tags=tag_repository)
    )
    app.dependency_overrides[get_agent_service] = lambda: agent_service
    app.dependency_overrides[get_agent_version_service] = lambda: version_service
    app.dependency_overrides[get_tag_service] = lambda: TagService(
        repository=tag_repository
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an agent through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name="assistant", description="Helps")
    )
    assert isinstance(agent, AgentResponse)
    assert agent.name == "assistant"
    assert agent.owner_id == ACCOUNT.id
    assert agent.description == "Helps"
    assert agent.latest_version == 0


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.agents.create(AgentCreateRequest(name="assistant"))
    with pytest.raises(APIError) as exc_info:
        await api_client.agents.create(AgentCreateRequest(name="assistant"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Agent name 'assistant' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an agent by id through the SDK."""
    created = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    loaded = await api_client.agents.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.agents.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List agents newest-first with filters through the SDK."""
    for name in ["assistant", "reviewer", "triager"]:
        await api_client.agents.create(AgentCreateRequest(name=name))

    page = await api_client.agents.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["triager", "reviewer", "assistant"]

    page = await api_client.agents.list(
        AgentListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="reviewer")
        )
    )
    assert page.next_cursor is None
    assert page.items[0].name == "reviewer"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every agent across pages through the SDK."""
    for name in ["assistant", "reviewer", "triager"]:
        await api_client.agents.create(AgentCreateRequest(name=name))

    collected = [
        item.name async for item in api_client.agents.iter(AgentListParams(size=2))
    ]

    assert collected == ["triager", "reviewer", "assistant"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an agent through the SDK."""
    created = await api_client.agents.create(
        AgentCreateRequest(name="assistant", description="Helps")
    )
    updated = await api_client.agents.update(
        created.id, AgentUpdateRequest(description="Reviews")
    )
    assert updated.description == "Reviews"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an agent through the SDK."""
    created = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    await api_client.agents.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.agents.get(created.id)


async def test_delete_in_use(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error when the agent has versions."""
    created = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    await api_client.agents.create_version(created.id, AgentVersionCreateRequest())
    with pytest.raises(APIError) as exc_info:
        await api_client.agents.delete(created.id)
    assert exc_info.value.status_code == 409


async def test_create_version(api_client: KitaruAPIClient) -> None:
    """Create a version of an agent through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    secret_id = uuid.uuid4()
    version = await api_client.agents.create_version(
        agent.id,
        AgentVersionCreateRequest(
            display_version="v1",
            description="First cut",
            run_spec=RunSpec(command="run.sh", secret_ids=[secret_id]),
            capabilities=AgentCapabilities(tools=["search"]),
        ),
    )
    assert isinstance(version, AgentVersionResponse)
    assert version.agent_id == agent.id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.run_spec is not None
    assert version.run_spec.secret_ids == [secret_id]
    assert version.capabilities.tools == ["search"]


async def test_list_versions(api_client: KitaruAPIClient) -> None:
    """List the versions of an agent through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    other_agent = await api_client.agents.create(AgentCreateRequest(name="reviewer"))
    v1 = await api_client.agents.create_version(agent.id, AgentVersionCreateRequest())
    v2 = await api_client.agents.create_version(agent.id, AgentVersionCreateRequest())
    await api_client.agents.create_version(other_agent.id, AgentVersionCreateRequest())

    page = await api_client.agents.list_versions(agent.id)
    assert page.next_cursor is None
    assert [item.id for item in page.items] == [v2.id, v1.id]


async def test_list_versions_filters_by_tag(api_client: KitaruAPIClient) -> None:
    """List versions filtered by tag through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    tagged = await api_client.agents.create_version(
        agent.id, AgentVersionCreateRequest()
    )
    await api_client.agents.create_version(agent.id, AgentVersionCreateRequest())
    tag = await api_client.tags.create(TagCreateRequest(name="smoke-test"))
    await api_client.tags.create_link(
        tag.id,
        TagLinkCreateRequest(
            resource_type=TagResourceType.AGENT_VERSION, resource_id=tagged.id
        ),
    )

    page = await api_client.agents.list_versions(
        agent.id,
        AgentVersionListParams(
            filter=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke-test")
        ),
    )
    assert [item.id for item in page.items] == [tagged.id]


async def test_iter_versions(api_client: KitaruAPIClient) -> None:
    """Iterate every version of an agent across pages through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    created = [
        await api_client.agents.create_version(agent.id, AgentVersionCreateRequest())
        for _ in range(3)
    ]

    collected = [
        item.id
        async for item in api_client.agents.iter_versions(
            agent.id, AgentVersionListParams(size=1)
        )
    ]

    assert collected == list(reversed([version.id for version in created]))


async def test_get_version(api_client: KitaruAPIClient) -> None:
    """Get an agent version by id through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    created = await api_client.agents.create_version(
        agent.id, AgentVersionCreateRequest(display_version="v1")
    )
    loaded = await api_client.agent_versions.get(created.id)
    assert loaded == created


async def test_get_version_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.agent_versions.get(uuid.uuid4())


async def test_update_version(api_client: KitaruAPIClient) -> None:
    """Update an agent version through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    created = await api_client.agents.create_version(
        agent.id, AgentVersionCreateRequest(display_version="v1")
    )
    updated = await api_client.agent_versions.update(
        created.id, AgentVersionUpdateRequest(display_version="v1.1")
    )
    assert updated.display_version == "v1.1"


async def test_update_version_clears_display_version(
    api_client: KitaruAPIClient,
) -> None:
    """Clear the display version with an explicit null through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    created = await api_client.agents.create_version(
        agent.id, AgentVersionCreateRequest(display_version="v1")
    )
    updated = await api_client.agent_versions.update(
        created.id, AgentVersionUpdateRequest(display_version=None)
    )
    assert updated.display_version is None


async def test_delete_version(api_client: KitaruAPIClient) -> None:
    """Delete an agent version through the SDK."""
    agent = await api_client.agents.create(AgentCreateRequest(name="assistant"))
    created = await api_client.agents.create_version(
        agent.id, AgentVersionCreateRequest()
    )
    await api_client.agent_versions.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.agent_versions.get(created.id)
