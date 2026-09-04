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
"""Tests for the insight routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeAgentRepository,
    FakeInsightRepository,
    create_agent,
    override_idempotency,
)
from kitaru.api_models.v1.insight import MAX_INSIGHT_BATCH_SIZE
from kitaru.server.adapters.rest.dependencies import authorize, get_insight_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.insight_service import InsightService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


def _text_insight(title: str = "insight", name: str = "insight") -> dict[str, object]:
    """Build a text insight input payload."""
    return {
        "name": name,
        "title": title,
        "data": {"type": "text", "content": "Latency regressed."},
    }


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide the fake agent repository backing the app."""
    return FakeAgentRepository()


@pytest.fixture
def insight_repository() -> FakeInsightRepository:
    """Provide the fake insight repository backing the app."""
    return FakeInsightRepository()


@pytest.fixture
async def client(
    agent_repository: FakeAgentRepository,
    insight_repository: FakeInsightRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed insight services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    insight_service = InsightService(
        repository=insight_repository,
        agent_repository=agent_repository,
    )
    app.dependency_overrides[get_insight_service] = lambda: insight_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> str:
    """Provide the id of an agent to own insights."""
    agent = await create_agent(agent_repository, ACCOUNT.id)
    return str(agent.id)


async def test_create_insights(client: httpx.AsyncClient, agent_id: str) -> None:
    """Create a batch of insights and observe HTTP 201 with items in input order."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [_text_insight("first"), _text_insight("second")],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert [item["title"] for item in body] == ["first", "second"]
    for item in body:
        assert uuid.UUID(item["id"])
        assert item["owner_id"] == str(ACCOUNT.id)
        assert item["agent_id"] == agent_id
        assert item["created"] is not None
        assert item["updated"] is not None


async def test_create_insights_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/api/v1/insights",
        json={"agent_id": str(uuid.uuid4()), "insights": [_text_insight()]},
    )
    assert response.status_code == 404


async def test_create_insights_empty_batch(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 for an empty insight batch."""
    response = await client.post(
        "/api/v1/insights", json={"agent_id": agent_id, "insights": []}
    )
    assert response.status_code == 422


async def test_create_insights_empty_title(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 for an insight with an empty title."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {"name": "n", "title": "", "data": {"type": "text", "content": "a"}}
            ],
        },
    )
    assert response.status_code == 422


async def test_create_insights_empty_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 for an insight with an empty name."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {"name": "", "title": "t", "data": {"type": "text", "content": "a"}}
            ],
        },
    )
    assert response.status_code == 422


async def test_create_insights_over_batch_limit(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when the batch exceeds the maximum size."""
    insights = [
        _text_insight(f"insight-{i}") for i in range(MAX_INSIGHT_BATCH_SIZE + 1)
    ]
    response = await client.post(
        "/api/v1/insights", json={"agent_id": agent_id, "insights": insights}
    )
    assert response.status_code == 422


async def test_create_insights_duplicate_category_labels(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when a categorical insight repeats a category label."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {
                    "name": "categories",
                    "title": "categories",
                    "data": {
                        "type": "categorical",
                        "values": [
                            {"label": "a", "value": 1.0},
                            {"label": "a", "value": 2.0},
                        ],
                    },
                }
            ],
        },
    )
    assert response.status_code == 422


async def test_create_insights_non_contiguous_bins(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when bins are not contiguous."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {
                    "name": "latency",
                    "title": "latency",
                    "data": {
                        "type": "binned",
                        "bins": [
                            {"lower_bound": None, "upper_bound": 1.0, "count": 3},
                            {"lower_bound": 2.0, "upper_bound": None, "count": 1},
                        ],
                    },
                }
            ],
        },
    )
    assert response.status_code == 422


async def test_get_insight(client: httpx.AsyncClient, agent_id: str) -> None:
    """Get an insight by id."""
    created = (
        await client.post(
            "/api/v1/insights",
            json={"agent_id": agent_id, "insights": [_text_insight()]},
        )
    ).json()[0]
    response = await client.get(f"/api/v1/insights/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_insight_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing insight."""
    response = await client.get(f"/api/v1/insights/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_insights_filters_by_agent_id(
    client: httpx.AsyncClient, agent_repository: FakeAgentRepository, agent_id: str
) -> None:
    """Filter insights scoped to one agent."""
    await client.post(
        "/api/v1/insights",
        json={"agent_id": agent_id, "insights": [_text_insight()]},
    )
    other_agent = await create_agent(agent_repository, ACCOUNT.id, name="other")
    await client.post(
        "/api/v1/insights",
        json={"agent_id": str(other_agent.id), "insights": [_text_insight()]},
    )

    filter_expression = {"field": "agent_id", "op": "eq", "value": agent_id}
    response = await client.get(
        "/api/v1/insights", params={"filter": json.dumps(filter_expression)}
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["agent_id"] == agent_id


async def test_list_insights_filters_by_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Filter insights by exact name."""
    await client.post(
        "/api/v1/insights",
        json={"agent_id": agent_id, "insights": [_text_insight(name="first")]},
    )
    await client.post(
        "/api/v1/insights",
        json={"agent_id": agent_id, "insights": [_text_insight(name="second")]},
    )

    filter_expression = {"field": "name", "op": "eq", "value": "first"}
    response = await client.get(
        "/api/v1/insights", params={"filter": json.dumps(filter_expression)}
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["name"] == "first"


async def test_list_insights_filters_by_type(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Filter insights by their data type."""
    await client.post(
        "/api/v1/insights",
        json={"agent_id": agent_id, "insights": [_text_insight()]},
    )
    await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {
                    "name": "categories",
                    "title": "categories",
                    "data": {
                        "type": "categorical",
                        "values": [{"label": "a", "value": 1.0}],
                    },
                }
            ],
        },
    )

    filter_expression = {"field": "type", "op": "eq", "value": "categorical"}
    response = await client.get(
        "/api/v1/insights", params={"filter": json.dumps(filter_expression)}
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["data"]["type"] == "categorical"


async def test_list_insights_walks_pages(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Walk every page of insights via next_cursor."""
    await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [_text_insight("a"), _text_insight("b"), _text_insight("c")],
        },
    )

    collected: list[str] = []
    cursor = None
    while True:
        params = {"size": 1}
        if cursor is not None:
            params["cursor"] = cursor
        response = await client.get("/api/v1/insights", params=params)
        body = response.json()
        collected.extend(item["id"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        cursor = body["next_cursor"]

    assert len(collected) == 3
    assert len(set(collected)) == 3


async def test_update_insight_title_and_description(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Update an insight's title and description."""
    created = (
        await client.post(
            "/api/v1/insights",
            json={"agent_id": agent_id, "insights": [_text_insight()]},
        )
    ).json()[0]
    response = await client.patch(
        f"/api/v1/insights/{created['id']}",
        json={"title": "renamed", "description": "new description"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["title"] == "renamed"
    assert body["description"] == "new description"


async def test_update_insight_clears_description_with_explicit_null(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Clear an insight's description with an explicit null."""
    insight = _text_insight()
    insight["description"] = "old description"
    created = (
        await client.post(
            "/api/v1/insights", json={"agent_id": agent_id, "insights": [insight]}
        )
    ).json()[0]
    assert created["description"] == "old description"

    response = await client.patch(
        f"/api/v1/insights/{created['id']}", json={"description": None}
    )
    assert response.status_code == 200
    assert response.json()["description"] is None


async def test_update_insight_omitted_description_leaves_it_unchanged(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Leave an insight's description untouched when the field is omitted."""
    insight = _text_insight()
    insight["description"] = "old description"
    created = (
        await client.post(
            "/api/v1/insights", json={"agent_id": agent_id, "insights": [insight]}
        )
    ).json()[0]

    response = await client.patch(
        f"/api/v1/insights/{created['id']}", json={"title": "renamed"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["title"] == "renamed"
    assert body["description"] == "old description"


async def test_update_insight_rejects_name(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Observe HTTP 422 when the update body carries a name field."""
    created = (
        await client.post(
            "/api/v1/insights",
            json={"agent_id": agent_id, "insights": [_text_insight()]},
        )
    ).json()[0]
    response = await client.patch(
        f"/api/v1/insights/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 422


async def test_update_insight_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing insight."""
    response = await client.patch(
        f"/api/v1/insights/{uuid.uuid4()}", json={"title": "renamed"}
    )
    assert response.status_code == 404


async def test_delete_insight(client: httpx.AsyncClient, agent_id: str) -> None:
    """Delete an insight."""
    created = (
        await client.post(
            "/api/v1/insights",
            json={"agent_id": agent_id, "insights": [_text_insight()]},
        )
    ).json()[0]
    response = await client.delete(f"/api/v1/insights/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/insights/{created['id']}")
    assert response.status_code == 404


async def test_delete_insight_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing insight."""
    response = await client.delete(f"/api/v1/insights/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_insights_is_idempotent(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Replay a stored response for a repeated idempotency key and identical body."""
    headers = {"Idempotency-Key": "create-insights"}
    body = {"agent_id": agent_id, "insights": [_text_insight()]}

    first = await client.post("/api/v1/insights", json=body, headers=headers)
    assert first.status_code == 201
    assert "Idempotent-Replayed" not in first.headers

    second = await client.post("/api/v1/insights", json=body, headers=headers)
    assert second.status_code == 201
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()

    listing = await client.get("/api/v1/insights")
    assert len(listing.json()["items"]) == 1
