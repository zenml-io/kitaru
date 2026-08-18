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
"""End-to-end tag tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def _create_session_id(client: httpx.AsyncClient) -> str:
    """Store an agent and a session on it, returning the session id."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    session = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent["id"],
                "origin": "recorded",
                "inputs": {"prompt": "hi"},
                "outputs": None,
                "metadata": {},
            },
        )
    ).json()
    return session["id"]


async def test_tags_persist_across_requests(client: httpx.AsyncClient) -> None:
    """Prove the per-request commit through separate requests."""
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    created = response.json()

    response = await client.get("/api/v1/tags")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(client: httpx.AsyncClient) -> None:
    """Translate the database constraint into HTTP 409."""
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Tag name 'prod' is already registered"}


async def test_update_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Persist a rename across requests."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    response = await client.patch(
        f"/api/v1/tags/{created['id']}", json={"name": "production"}
    )
    assert response.status_code == 200

    response = await client.get("/api/v1/tags")
    body = response.json()
    assert body["items"][0]["name"] == "production"
    assert body["items"][0]["updated"] > created["updated"]


async def test_link_and_delete_persist_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """Persist a tag link and its deletion across requests."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = await _create_session_id(client)
    response = await client.post(
        f"/api/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": resource_id},
    )
    assert response.status_code == 201
    link = response.json()

    response = await client.post(
        f"/api/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": resource_id},
    )
    assert response.status_code == 409

    response = await client.delete(
        f"/api/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 204
    assert link["tag_id"] == created["id"]


async def test_delete_cascades_links_across_requests(client: httpx.AsyncClient) -> None:
    """Cascade a tag's links when the tag is deleted."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = await _create_session_id(client)
    await client.post(
        f"/api/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": resource_id},
    )
    response = await client.delete(f"/api/v1/tags/{created['id']}")
    assert response.status_code == 204

    response = await client.delete(
        f"/api/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404
