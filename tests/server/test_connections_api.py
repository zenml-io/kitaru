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
"""Tests for the connection routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    FakeConnectionRepository,
    FakeSecretRepository,
    override_idempotency,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_connection_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.connection_service import ConnectionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.secret import SecretNotFound

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

ENV = {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}
SECRETS = {"LANGFUSE_PUBLIC_KEY": "pk", "LANGFUSE_SECRET_KEY": "sk"}


@pytest.fixture
def repository() -> FakeConnectionRepository:
    """Provide the fake connection repository backing the app."""
    return FakeConnectionRepository()


@pytest.fixture
def secret_repository(repository: FakeConnectionRepository) -> FakeSecretRepository:
    """Provide the fake secret repository backing the app."""
    return FakeSecretRepository(connections=repository)


@pytest.fixture
async def client(
    repository: FakeConnectionRepository,
    secret_repository: FakeSecretRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed connection service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = ConnectionService(
        repository=repository, secret_repository=secret_repository
    )
    app.dependency_overrides[get_connection_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _create(client: httpx.AsyncClient, **overrides: object) -> dict[str, object]:
    """Create a connection through the API and return the response body."""
    body: dict[str, object] = {
        "name": "langfuse-prod",
        "provider": "langfuse",
        "env": ENV,
        "secrets": SECRETS,
    }
    body.update(overrides)
    response = await client.post("/api/v1/connections", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_connection(client: httpx.AsyncClient) -> None:
    """Create a connection and observe HTTP 201 without the secret values."""
    body = await _create(client)
    assert body["name"] == "langfuse-prod"
    assert body["provider"] == "langfuse"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["env"] == ENV
    assert body["secret_keys"] == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert body["default"] is False
    assert uuid.UUID(str(body["id"]))
    assert uuid.UUID(str(body["secret_id"]))


async def test_create_connection_response_carries_no_secret_values(
    client: httpx.AsyncClient,
) -> None:
    """Never expose the secret values in the response."""
    body = await _create(client)
    assert set(body) == {
        "id",
        "owner_id",
        "name",
        "provider",
        "env",
        "secret_id",
        "secret_keys",
        "default",
        "created",
        "updated",
    }
    assert "sk" not in json.dumps(body)


async def test_create_connection_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate connection name."""
    await _create(client)
    response = await client.post(
        "/api/v1/connections", json={"name": "langfuse-prod", "provider": "langfuse"}
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Connection name 'langfuse-prod' is already registered"
    }


async def test_create_connection_reserved_env_key(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an env key using the reserved prefix."""
    response = await client.post(
        "/api/v1/connections",
        json={
            "name": "langfuse-prod",
            "provider": "langfuse",
            "env": {"KITARU_TASK_ID": "x"},
        },
    )
    assert response.status_code == 422


async def test_create_connection_reserved_secret_key(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for a secret key using the reserved prefix."""
    response = await client.post(
        "/api/v1/connections",
        json={
            "name": "langfuse-prod",
            "provider": "langfuse",
            "secrets": {"KITARU_TASK_ID": "x"},
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Key 'KITARU_TASK_ID' uses the reserved prefix 'KITARU_'"
    }


async def test_create_connection_overlapping_key(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a key set as both an env value and a secret."""
    response = await client.post(
        "/api/v1/connections",
        json={
            "name": "langfuse-prod",
            "provider": "langfuse",
            "env": {"LANGFUSE_SECRET_KEY": "plain"},
            "secrets": {"LANGFUSE_SECRET_KEY": "sk"},
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Key 'LANGFUSE_SECRET_KEY' is set as both an env value and a secret"
    }


async def test_create_connection_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid connection name."""
    response = await client.post(
        "/api/v1/connections", json={"name": "in valid", "provider": "langfuse"}
    )
    assert response.status_code == 422


async def test_create_connection_clears_the_previous_default(
    client: httpx.AsyncClient,
) -> None:
    """Clear the provider's previous default when a new default is created."""
    first = await _create(client, name="langfuse-a", default=True)
    await _create(client, name="langfuse-b", default=True)

    response = await client.get(f"/api/v1/connections/{first['id']}")
    assert response.status_code == 200
    assert response.json()["default"] is False


async def test_list_connections(client: httpx.AsyncClient) -> None:
    """List connections newest-first with filters."""
    for name in ["langfuse-a", "langfuse-b", "langfuse-c"]:
        await _create(client, name=name)

    response = await client.get("/api/v1/connections")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == [
        "langfuse-c",
        "langfuse-b",
        "langfuse-a",
    ]
    assert all("secrets" not in item for item in body["items"])

    filter_expression = {"field": "name", "op": "eq", "value": "langfuse-b"}
    response = await client.get(
        "/api/v1/connections", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["name"] == "langfuse-b"


async def test_list_connections_filter_by_provider(client: httpx.AsyncClient) -> None:
    """List connections filtered by provider."""
    await _create(client, name="langfuse-a", provider="langfuse")
    await _create(client, name="braintrust-a", provider="braintrust")

    filter_expression = {"field": "provider", "op": "eq", "value": "braintrust"}
    response = await client.get(
        "/api/v1/connections", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["name"] for item in body["items"]] == ["braintrust-a"]


async def test_list_connections_walks_pages_with_cursor(
    client: httpx.AsyncClient,
) -> None:
    """Walk every page of connections via next_cursor."""
    for name in ["langfuse-a", "langfuse-b", "langfuse-c"]:
        await _create(client, name=name)

    collected: list[str] = []
    params: dict[str, str] = {"size": "2"}
    while True:
        response = await client.get("/api/v1/connections", params=params)
        assert response.status_code == 200
        body = response.json()
        collected.extend(item["name"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        params = {"size": "2", "cursor": body["next_cursor"]}

    assert collected == ["langfuse-c", "langfuse-b", "langfuse-a"]


async def test_list_connections_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/api/v1/connections", params={"size": 0})
    assert response.status_code == 422
    response = await client.get("/api/v1/connections", params={"size": 1001})
    assert response.status_code == 422


async def test_get_connection(client: httpx.AsyncClient) -> None:
    """Get a connection by id."""
    created = await _create(client)
    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_connection_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown connection id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/connections/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Connection {missing_id} was not found"}


async def test_update_connection_merges_env_and_secrets(
    client: httpx.AsyncClient,
) -> None:
    """Merge env and secret entries by key rather than replacing them."""
    created = await _create(client)
    response = await client.patch(
        f"/api/v1/connections/{created['id']}",
        json={
            "env": {"LANGFUSE_PROJECT": "proj"},
            "secrets": {"LANGFUSE_SECRET_KEY": "rotated"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["env"] == {
        "LANGFUSE_BASE_URL": "https://cloud.langfuse.com",
        "LANGFUSE_PROJECT": "proj",
    }
    assert body["secret_keys"] == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert "rotated" not in json.dumps(body)


async def test_update_connection_leaves_unset_fields_unchanged(
    client: httpx.AsyncClient,
) -> None:
    """Leave env, secrets, and default unchanged when the body omits them."""
    created = await _create(client, default=True)
    response = await client.patch(f"/api/v1/connections/{created['id']}", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["env"] == ENV
    assert body["secret_keys"] == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert body["default"] is True


async def test_update_connection_sets_the_default(client: httpx.AsyncClient) -> None:
    """Clear the provider's previous default when another becomes default."""
    first = await _create(client, name="langfuse-a", default=True)
    second = await _create(client, name="langfuse-b")

    response = await client.patch(
        f"/api/v1/connections/{second['id']}", json={"default": True}
    )
    assert response.status_code == 200
    assert response.json()["default"] is True

    response = await client.get(f"/api/v1/connections/{first['id']}")
    assert response.status_code == 200
    assert response.json()["default"] is False


async def test_update_connection_reserved_env_key(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an env key using the reserved prefix on update."""
    created = await _create(client)
    response = await client.patch(
        f"/api/v1/connections/{created['id']}", json={"env": {"KITARU_TASK_ID": "x"}}
    )
    assert response.status_code == 422


async def test_update_connection_overlapping_key(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when an update makes a key both env and secret."""
    created = await _create(client)
    response = await client.patch(
        f"/api/v1/connections/{created['id']}",
        json={"env": {"LANGFUSE_SECRET_KEY": "plain"}},
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Key 'LANGFUSE_SECRET_KEY' is set as both an env value and a secret"
    }


async def test_update_connection_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown connection id."""
    response = await client.patch(
        f"/api/v1/connections/{uuid.uuid4()}", json={"default": True}
    )
    assert response.status_code == 404


async def test_delete_connection(
    client: httpx.AsyncClient, secret_repository: FakeSecretRepository
) -> None:
    """Delete a connection and its internal secret, observing HTTP 204."""
    created = await _create(client)
    response = await client.delete(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/api/v1/connections/{created['id']}")
    assert response.status_code == 404
    with pytest.raises(SecretNotFound):
        await secret_repository.get(uuid.UUID(str(created["secret_id"])))


async def test_delete_connection_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown connection id."""
    response = await client.delete(f"/api/v1/connections/{uuid.uuid4()}")
    assert response.status_code == 404
