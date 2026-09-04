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
"""Round-trip tests for the insights SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeInsightRepository,
    asgi_api_client,
    override_idempotency,
)
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightListParams,
    InsightResponse,
    InsightUpdateRequest,
    TextInsightData,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_insight_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed insight service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    agent_repository = FakeAgentRepository()
    insight_repository = FakeInsightRepository()
    app.dependency_overrides[get_agent_service] = lambda: AgentService(
        repository=agent_repository
    )
    app.dependency_overrides[get_insight_service] = lambda: InsightService(
        repository=insight_repository, agent_repository=agent_repository
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def _make_agent(api_client: KitaruAPIClient) -> uuid.UUID:
    """Create an agent through the SDK."""
    agent = await api_client.agents.create(
        AgentCreateRequest(name=f"assistant-{uuid.uuid4().hex[:8]}")
    )
    return agent.id


def _insight_input(
    name: str = "insight", title: str = "insight", description: str | None = None
) -> InsightInput:
    """Build a minimal text insight input.

    Args:
        name: Insight name.
        title: Insight title.
        description: Insight description.

    Returns:
        Insight input ready to pass to insights.create.
    """
    return InsightInput(
        name=name,
        title=title,
        description=description,
        data=TextInsightData(content="root cause"),
    )


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a batch of insights through the SDK."""
    agent_id = await _make_agent(api_client)
    insights = await api_client.insights.create(
        agent_id,
        [
            _insight_input(title="first", description="rationale"),
            _insight_input(title="second"),
        ],
    )
    assert [insight.title for insight in insights] == ["first", "second"]
    assert isinstance(insights[0], InsightResponse)
    assert insights[0].owner_id == ACCOUNT.id
    assert insights[0].agent_id == agent_id
    assert insights[0].description == "rationale"
    assert insights[1].description is None


async def test_create_missing_agent(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.insights.create(uuid.uuid4(), [_insight_input()])


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an insight by id through the SDK."""
    agent_id = await _make_agent(api_client)
    created = (await api_client.insights.create(agent_id, [_insight_input()]))[0]
    loaded = await api_client.insights.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.insights.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate insights through the SDK."""
    agent_id = await _make_agent(api_client)
    await api_client.insights.create(
        agent_id,
        [_insight_input(title=title) for title in ["alpha", "beta", "gamma"]],
    )

    page = await api_client.insights.list(InsightListParams(size=2))
    assert len(page.items) == 2

    collected = [item.title async for item in api_client.insights.iter()]
    assert collected == ["gamma", "beta", "alpha"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an insight's title and description through the SDK."""
    agent_id = await _make_agent(api_client)
    created = (
        await api_client.insights.create(agent_id, [_insight_input(description="old")])
    )[0]
    updated = await api_client.insights.update(
        created.id, InsightUpdateRequest(description="new")
    )
    assert updated.title == created.title
    assert updated.description == "new"

    updated = await api_client.insights.update(
        created.id, InsightUpdateRequest(title="renamed")
    )
    assert updated.title == "renamed"
    assert updated.description == "new"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an insight through the SDK."""
    agent_id = await _make_agent(api_client)
    created = (await api_client.insights.create(agent_id, [_insight_input()]))[0]
    await api_client.insights.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.insights.get(created.id)
