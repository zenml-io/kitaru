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
"""End-to-end insight tests against PostgreSQL."""

import json
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import db_settings, lifespan_client
from kitaru.api_models.v1.insight import TextInsightData
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.insight_repository import (
    SQLInsightRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.plugin import Plugin, PluginKind, ScriptPluginSource


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


@asynccontextmanager
async def _raw_session(
    settings: APISettings,
) -> AsyncGenerator[AsyncSession, None]:
    """Open a session bound to the same database a lifespan_client migrated.

    Args:
        settings: Settings naming the database, matching the ones passed to
            lifespan_client.

    Yields:
        Session on the shared database.
    """
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as session:
            yield session
    finally:
        await engine.dispose()


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent to own insights."""
    created = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def _create_insights(
    client: httpx.AsyncClient, agent_id: str
) -> list[dict[str, object]]:
    """Store two text insights on the given agent and return their responses."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": agent_id,
            "insights": [
                {
                    "name": "first",
                    "title": "first",
                    "data": {"type": "text", "content": "Latency regressed."},
                },
                {
                    "name": "second",
                    "title": "second",
                    "data": {"type": "text", "content": "Error rate dropped."},
                },
            ],
        },
    )
    assert response.status_code == 201
    return response.json()


async def test_batch_create_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    created = await _create_insights(client, agent_id)
    assert [item["title"] for item in created] == ["first", "second"]

    for item in created:
        response = await client.get(f"/api/v1/insights/{item['id']}")
        assert response.status_code == 200
        assert response.json() == item


async def test_list_by_agent_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """List insights scoped to their agent through a separate request."""
    created = await _create_insights(client, agent_id)

    filter_expression = {"field": "agent_id", "op": "eq", "value": agent_id}
    response = await client.get(
        "/api/v1/insights", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert {item["id"] for item in body["items"]} == {item["id"] for item in created}


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a title and description update across requests."""
    created = await _create_insights(client, agent_id)

    response = await client.patch(
        f"/api/v1/insights/{created[0]['id']}",
        json={"title": "renamed", "description": "updated rationale"},
    )
    assert response.status_code == 200

    response = await client.get(f"/api/v1/insights/{created[0]['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["title"] == "renamed"
    assert body["description"] == "updated rationale"
    assert body["updated"] > created[0]["updated"]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Delete an insight and observe its absence in a separate request."""
    created = await _create_insights(client, agent_id)

    response = await client.delete(f"/api/v1/insights/{created[0]['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/insights/{created[0]['id']}")
    assert response.status_code == 404


async def test_create_insights_missing_agent(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when the agent does not exist."""
    response = await client.post(
        "/api/v1/insights",
        json={
            "agent_id": "00000000-0000-0000-0000-000000000000",
            "insights": [
                {
                    "name": "insight",
                    "title": "insight",
                    "data": {"type": "text", "content": "x"},
                }
            ],
        },
    )
    assert response.status_code == 404


async def test_get_insight_carries_analyzer_info_for_a_task_born_insight() -> None:
    """Carry the analyzer name and version for an insight produced by a task."""
    settings = db_settings()
    async with lifespan_client(settings) as client:
        agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()

        async with _raw_session(settings) as session:
            owner_id = uuid.UUID(agent["owner_id"])
            code_blob, _ = await SQLBlobRepository(session).create(
                Blob(
                    owner_id=owner_id,
                    sha256="1" * 64,
                    size=4,
                    media_type="text/x-python",
                    stored_in=BlobStorageBackend.DATABASE,
                )
            )
            plugins = SQLPluginRepository(session)
            plugin = await plugins.create(
                Plugin(owner_id=owner_id, kind=PluginKind.ANALYZER, name="trends")
            )
            version = await plugins.create_version(
                plugin.id,
                ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
                display_version=None,
            )
            stored = await SQLInsightRepository(session).create_many(
                [
                    Insight(
                        owner_id=owner_id,
                        agent_id=uuid.UUID(agent["id"]),
                        name="insight",
                        title="insight",
                        data=TextInsightData(content="Latency regressed."),
                        analyzer_version_id=version.id,
                        analyzer_params={"window_days": 7},
                    )
                ]
            )
            await session.commit()

        response = await client.get(f"/api/v1/insights/{stored[0].id}")
        assert response.status_code == 200
        body = response.json()
        assert body["analyzer_version_id"] == str(version.id)
        assert body["analyzer_name"] == "trends"
        assert body["analyzer_version"] == 1
        assert body["analyzer_params"] == {"window_days": 7}
