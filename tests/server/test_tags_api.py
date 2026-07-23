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
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = TagService(repository=repository)
    app.dependency_overrides[get_tag_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_tag(client: httpx.AsyncClient) -> None:
    """Create a tag and observe HTTP 201."""
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "prod"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_tag_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate tag name."""
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Tag name 'prod' is already registered"}


async def test_create_tag_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid tag name."""
    response = await client.post("/v1/tags", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_tags(client: httpx.AsyncClient) -> None:
    """List tags with filters and pagination."""
    for name in ["prod", "staging", "flaky"]:
        response = await client.post("/v1/tags", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/tags")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["name"] for item in body["items"]] == ["prod", "staging", "flaky"]

    response = await client.get("/v1/tags", params={"name": "staging"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["name"] == "staging"

    response = await client.get("/v1/tags", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["flaky"]


async def test_list_tags_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/v1/tags", params={"page": 0})
    assert response.status_code == 422
    response = await client.get("/v1/tags", params={"page_size": 1001})
    assert response.status_code == 422


async def test_delete_tag(client: httpx.AsyncClient) -> None:
    """Delete a tag and observe HTTP 204."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    response = await client.delete(f"/v1/tags/{created['id']}")
    assert response.status_code == 204
    response = await client.get("/v1/tags")
    assert response.json()["total"] == 0


async def test_delete_tag_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown tag id."""
    missing_id = uuid.uuid4()
    response = await client.delete(f"/v1/tags/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Tag {missing_id} was not found"}


async def test_create_tag_link(client: httpx.AsyncClient) -> None:
    """Attach a tag to a resource and observe HTTP 201."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    response = await client.post(
        f"/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": str(resource_id)},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["tag_id"] == created["id"]
    assert body["resource_type"] == "session"
    assert body["resource_id"] == str(resource_id)
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_tag_link_tag_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown tag id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        f"/v1/tags/{missing_id}/links",
        json={"resource_type": "session", "resource_id": str(uuid.uuid4())},
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Tag {missing_id} was not found"}


async def test_create_tag_link_duplicate(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate tag link."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    body = {"resource_type": "session", "resource_id": str(resource_id)}
    response = await client.post(f"/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 201
    response = await client.post(f"/v1/tags/{created['id']}/links", json=body)
    assert response.status_code == 409
    assert response.json() == {
        "detail": (f"Tag {created['id']} is already attached to session {resource_id}")
    }


async def test_create_tag_link_invalid_resource_type(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an unknown resource type."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    response = await client.post(
        f"/v1/tags/{created['id']}/links",
        json={"resource_type": "agent", "resource_id": str(uuid.uuid4())},
    )
    assert response.status_code == 422


async def test_delete_tag_link(client: httpx.AsyncClient) -> None:
    """Detach a tag from a resource and observe HTTP 204."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    response = await client.post(
        f"/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": str(resource_id)},
    )
    assert response.status_code == 201
    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 204
    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404


async def test_delete_tag_link_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown tag link."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": f"Tag {created['id']} is not attached to session {resource_id}"
    }


async def test_delete_tag_link_invalid_resource_type(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an unknown resource type in the path."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    response = await client.delete(
        f"/v1/tags/{created['id']}/links/agent/{uuid.uuid4()}"
    )
    assert response.status_code == 422


async def test_delete_tag_removes_links(client: httpx.AsyncClient) -> None:
    """Remove the links of a tag when deleting it."""
    created = (await client.post("/v1/tags", json={"name": "prod"})).json()
    resource_id = uuid.uuid4()
    response = await client.post(
        f"/v1/tags/{created['id']}/links",
        json={"resource_type": "session", "resource_id": str(resource_id)},
    )
    assert response.status_code == 201
    response = await client.delete(f"/v1/tags/{created['id']}")
    assert response.status_code == 204
    response = await client.delete(
        f"/v1/tags/{created['id']}/links/session/{resource_id}"
    )
    assert response.status_code == 404
