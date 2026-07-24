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
"""Tests for the API key routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeApiKeyRepository
from kitaru.server.adapters.rest.dependencies import authorize, get_api_key_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import API_KEY_PREFIX

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed API key service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = ApiKeyService(repository=FakeApiKeyRepository())
    app.dependency_overrides[get_api_key_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_api_key(client: httpx.AsyncClient) -> None:
    """Create an API key and observe HTTP 201 with the plaintext key."""
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "ci"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["active"] is True
    assert body["key"].startswith(API_KEY_PREFIX)
    assert body["last_used"] is None
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_api_key_response_has_no_hash(client: httpx.AsyncClient) -> None:
    """Never expose the key hash in the response."""
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 201
    assert set(response.json()) == {
        "id",
        "owner_id",
        "name",
        "active",
        "key",
        "last_used",
        "created",
        "updated",
    }


async def test_create_api_key_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate API key name."""
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 201
    response = await client.post("/v1/api-keys", json={"name": "ci"})
    assert response.status_code == 409
    assert response.json() == {"detail": "API key name 'ci' is already registered"}


async def test_create_api_key_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid API key name."""
    response = await client.post("/v1/api-keys", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_api_keys(client: httpx.AsyncClient) -> None:
    """List API keys with filters and pagination."""
    for name in ["ci", "deploy", "local"]:
        response = await client.post("/v1/api-keys", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/api-keys")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert [item["name"] for item in body["items"]] == ["ci", "deploy", "local"]
    assert all("key" not in item for item in body["items"])

    response = await client.get("/v1/api-keys", params={"name": "deploy"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["name"] == "deploy"

    response = await client.get("/v1/api-keys", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["local"]


async def test_list_api_keys_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/v1/api-keys", params={"page": 0})
    assert response.status_code == 422
    response = await client.get("/v1/api-keys", params={"page_size": 1001})
    assert response.status_code == 422


async def test_get_api_key(client: httpx.AsyncClient) -> None:
    """Get an API key by id without the plaintext key."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.get(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 200
    expected = {field: value for field, value in created.items() if field != "key"}
    assert response.json() == expected


async def test_get_api_key_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown API key id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/api-keys/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"API key {missing_id} was not found"}


async def test_update_api_key(client: httpx.AsyncClient) -> None:
    """Update an API key."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.patch(
        f"/v1/api-keys/{created['id']}", json={"active": False}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["active"] is False
    assert body["name"] == "ci"
    assert "key" not in body


async def test_update_api_key_null_active_rejected(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an explicit null active state."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.patch(
        f"/v1/api-keys/{created['id']}", json={"active": None}
    )
    assert response.status_code == 422


async def test_update_api_key_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown API key id."""
    response = await client.patch(
        f"/v1/api-keys/{uuid.uuid4()}", json={"active": False}
    )
    assert response.status_code == 404


async def test_delete_api_key(client: httpx.AsyncClient) -> None:
    """Delete an API key and observe HTTP 204."""
    created = (await client.post("/v1/api-keys", json={"name": "ci"})).json()
    response = await client.delete(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/api-keys/{created['id']}")
    assert response.status_code == 404


async def test_delete_api_key_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown API key id."""
    response = await client.delete(f"/v1/api-keys/{uuid.uuid4()}")
    assert response.status_code == 404
