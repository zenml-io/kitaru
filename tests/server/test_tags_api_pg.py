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

import json
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


async def test_session_tag_reads_and_alias_pagination(
    client: httpx.AsyncClient,
) -> None:
    """Load current tags across requests and share canonical alias cursors."""
    agent = (await client.post("/api/v1/agents", json={"name": "assistant"})).json()
    sessions = []
    for _ in range(3):
        response = await client.post(
            "/api/v1/sessions",
            json={
                "agent_id": agent["id"],
                "origin": "recorded",
                "inputs": {"private": "payload"},
                "outputs": None,
                "metadata": {},
            },
        )
        assert response.status_code == 201
        assert response.json()["tags"] == []
        sessions.append(response.json())
    tag = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    for session in sessions[:2]:
        response = await client.post(
            f"/api/v1/tags/{tag['id']}/links",
            json={"resource_type": "session", "resource_id": session["id"]},
        )
        assert response.status_code == 201
    first = await client.get(
        "/api/v1/sessions",
        params={
            "size": 1,
            "sort": "created:asc",
            "filter": json.dumps({"field": "tags", "op": "eq", "value": "prod"}),
        },
    )
    assert first.status_code == 200
    page = first.json()
    assert [s["id"] for s in page["items"]] == [sessions[0]["id"]]
    assert page["items"][0]["tags"] == [tag]
    assert "inputs" not in page["items"][0]
    second = await client.get(
        "/api/v1/sessions",
        params={
            "size": 1,
            "sort": "created:asc",
            "filter": json.dumps({"field": "tag", "op": "eq", "value": "prod"}),
            "cursor": page["next_cursor"],
        },
    )
    assert second.status_code == 200
    assert [s["id"] for s in second.json()["items"]] == [sessions[1]["id"]]
    assert second.json()["items"][0]["tags"] == [tag]
    assert second.json()["next_cursor"] is None
    renamed = (
        await client.patch(f"/api/v1/tags/{tag['id']}", json={"name": "production"})
    ).json()
    responses = [
        await client.get(f"/api/v1/sessions/{sessions[0]['id']}"),
        await client.patch(
            f"/api/v1/sessions/{sessions[0]['id']}", json={"name": "updated"}
        ),
    ]
    for response in responses:
        assert response.status_code == 200
        assert response.json()["tags"] == [renamed]
    response = await client.delete(
        f"/api/v1/tags/{tag['id']}/links/session/{sessions[0]['id']}"
    )
    assert response.status_code == 204
    assert (await client.get(f"/api/v1/sessions/{sessions[0]['id']}")).json()[
        "tags"
    ] == []
    assert (await client.get(f"/api/v1/sessions/{sessions[2]['id']}")).json()[
        "tags"
    ] == []
