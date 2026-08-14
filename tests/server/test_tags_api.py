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
"""Tests for the tag routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeTagRepository
from kitaru.server.adapters.rest.dependencies import authorize, get_tag_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def repository() -> FakeTagRepository:
    """Provide the fake tag repository backing the app."""
    return FakeTagRepository()


@pytest.fixture
async def client(
    repository: FakeTagRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed tag service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = TagService(repository=repository)
    app.dependency_overrides[get_tag_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_tag(client: httpx.AsyncClient) -> None:
    """Create a tag and observe HTTP 201."""
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "prod"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_tag_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate tag name."""
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    response = await client.post("/api/v1/tags", json={"name": "prod"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Tag name 'prod' is already registered"}


async def test_create_tag_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid tag name."""
    response = await client.post("/api/v1/tags", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_tags(client: httpx.AsyncClient) -> None:
    """List tags newest-first with filters."""
    for name in ["prod", "staging", "canary"]:
        response = await client.post("/api/v1/tags", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/api/v1/tags")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["canary", "staging", "prod"]

    filter_expression = {"field": "name", "op": "eq", "value": "staging"}
    response = await client.get(
        "/api/v1/tags", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["name"] == "staging"


async def test_list_tags_walks_pages_with_cursor(client: httpx.AsyncClient) -> None:
    """Walk every page of tags via next_cursor."""
    for name in ["prod", "staging", "canary"]:
        response = await client.post("/api/v1/tags", json={"name": name})
        assert response.status_code == 201

    collected: list[str] = []
    params: dict[str, str] = {"size": "2"}
    while True:
        response = await client.get("/api/v1/tags", params=params)
        assert response.status_code == 200
        body = response.json()
        collected.extend(item["name"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        params = {"size": "2", "cursor": body["next_cursor"]}

    assert collected == ["canary", "staging", "prod"]


async def test_update_tag(client: httpx.AsyncClient) -> None:
    """Rename a tag."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    response = await client.patch(
        f"/api/v1/tags/{created['id']}", json={"name": "production"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "production"


async def test_update_tag_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown tag id."""
    response = await client.patch(
        f"/api/v1/tags/{uuid.uuid4()}", json={"name": "production"}
    )
    assert response.status_code == 404


async def test_update_tag_conflict(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 renaming a tag to a registered name."""
    await client.post("/api/v1/tags", json={"name": "prod"})
    staging = (await client.post("/api/v1/tags", json={"name": "staging"})).json()
    response = await client.patch(
        f"/api/v1/tags/{staging['id']}", json={"name": "prod"}
    )
    assert response.status_code == 409


async def test_delete_tag(client: httpx.AsyncClient) -> None:
    """Delete a tag and observe HTTP 204."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    response = await client.delete(f"/api/v1/tags/{created['id']}")
    assert response.status_code == 204
    response = await client.patch(f"/api/v1/tags/{created['id']}", json={"name": "x"})
    assert response.status_code == 404


async def test_delete_tag_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown tag id."""
    response = await client.delete(f"/api/v1/tags/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_create_tag_link(client: httpx.AsyncClient) -> None:
    """Link a tag to a resource and observe HTTP 201."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = str(uuid.uuid4())
    response = await client.post(
        f"/api/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": resource_id},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["tag_id"] == created["id"]
    assert body["resource_type"] == "session"
    assert body["resource_id"] == resource_id
    assert uuid.UUID(body["id"])


async def test_create_tag_link_missing_tag(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 linking a resource to an unknown tag."""
    response = await client.post(
        f"/api/v1/tags/{uuid.uuid4()}/links",
        json={"resource_type": "session", "resource_id": str(uuid.uuid4())},
    )
    assert response.status_code == 404


async def test_create_tag_link_duplicate(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 linking the same tag and resource twice."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = str(uuid.uuid4())
    body = {"resource_type": "session", "resource_id": resource_id}
    response = await client.post(f"/api/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 201
    response = await client.post(f"/api/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 409


async def test_delete_tag_link(client: httpx.AsyncClient) -> None:
    """Delete a tag link by type and id, observing HTTP 204."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = str(uuid.uuid4())
    await client.post(
        f"/api/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": resource_id},
    )
    response = await client.delete(
        f"/api/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 204


async def test_delete_tag_link_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 deleting a link that does not exist."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    response = await client.delete(
        f"/api/v1/tags/{created['id']}/links/session/{uuid.uuid4()}"
    )
    assert response.status_code == 404


async def test_delete_tag_cascades_links(client: httpx.AsyncClient) -> None:
    """Deleting a tag also removes its links."""
    created = (await client.post("/api/v1/tags", json={"name": "prod"})).json()
    resource_id = str(uuid.uuid4())
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
