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
"""Tests for the secret routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeSecretRepository, create_secret
from kitaru.server.adapters.rest.dependencies import authorize, get_secret_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

VALUES = {"username": "svc", "password": "hunter2"}


@pytest.fixture
def repository() -> FakeSecretRepository:
    """Provide the fake secret repository backing the app."""
    return FakeSecretRepository()


@pytest.fixture
async def client(
    repository: FakeSecretRepository,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed secret service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = SecretService(repository=repository)
    app.dependency_overrides[get_secret_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_secret(client: httpx.AsyncClient) -> None:
    """Create a secret and observe HTTP 201 without the values."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "type": "database", "values": VALUES}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "db"
    assert body["owner_id"] == str(ACCOUNT.id)
    assert body["type"] == "database"
    assert "values" not in body
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_secret_response_has_no_internal(
    client: httpx.AsyncClient,
) -> None:
    """Never expose the internal flag in the response."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 201
    assert set(response.json()) == {
        "id",
        "owner_id",
        "name",
        "type",
        "created",
        "updated",
    }


async def test_create_secret_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate secret name."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 201
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "Secret name 'db' is already registered"}


async def test_create_secret_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid secret name."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "in valid", "values": VALUES}
    )
    assert response.status_code == 422


async def test_create_secret_invalid_type(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an overlong secret type."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "type": "x" * 65, "values": VALUES}
    )
    assert response.status_code == 422


async def test_update_secret_invalid_type(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an overlong secret type on update."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 201
    secret_id = response.json()["id"]
    response = await client.patch(
        f"/api/v1/secrets/{secret_id}", json={"type": "x" * 65}
    )
    assert response.status_code == 422


async def test_create_secret_values_too_large(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for oversized secret values."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": {"key": "x" * (64 * 1024)}}
    )
    assert response.status_code == 422


async def test_update_secret_values_too_large(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for oversized secret values on update."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 201
    secret_id = response.json()["id"]
    response = await client.patch(
        f"/api/v1/secrets/{secret_id}", json={"values": {"key": "x" * (64 * 1024)}}
    )
    assert response.status_code == 422


async def test_list_secrets(client: httpx.AsyncClient) -> None:
    """List secrets newest-first with filters."""
    for name in ["db", "smtp", "s3"]:
        response = await client.post(
            "/api/v1/secrets", json={"name": name, "values": VALUES}
        )
        assert response.status_code == 201

    response = await client.get("/api/v1/secrets")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["s3", "smtp", "db"]
    assert all("values" not in item for item in body["items"])
    assert all("internal" not in item for item in body["items"])

    filter_expression = {"field": "name", "op": "eq", "value": "smtp"}
    response = await client.get(
        "/api/v1/secrets", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0]["name"] == "smtp"


async def test_list_secrets_walks_pages_with_cursor(
    client: httpx.AsyncClient,
) -> None:
    """Walk every page of secrets via next_cursor."""
    for name in ["db", "smtp", "s3"]:
        response = await client.post(
            "/api/v1/secrets", json={"name": name, "values": VALUES}
        )
        assert response.status_code == 201

    collected: list[str] = []
    params: dict[str, str] = {"size": "2"}
    while True:
        response = await client.get("/api/v1/secrets", params=params)
        assert response.status_code == 200
        body = response.json()
        collected.extend(item["name"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        params = {"size": "2", "cursor": body["next_cursor"]}

    assert collected == ["s3", "smtp", "db"]


async def test_list_secrets_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/api/v1/secrets", params={"size": 0})
    assert response.status_code == 422
    response = await client.get("/api/v1/secrets", params={"size": 1001})
    assert response.status_code == 422


async def test_list_secrets_invalid_cursor(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a cursor string that fails to decode."""
    response = await client.get("/api/v1/secrets", params={"cursor": "not-a-cursor"})
    assert response.status_code == 422


async def test_list_secrets_unknown_query_param(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown query parameter."""
    response = await client.get("/api/v1/secrets", params={"bogus": "x"})
    assert response.status_code == 422


async def test_get_secret(client: httpx.AsyncClient) -> None:
    """Get a secret by id without the values."""
    created = (
        await client.post("/api/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.get(f"/api/v1/secrets/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body == created
    assert "values" not in body


async def test_get_secret_with_values(client: httpx.AsyncClient) -> None:
    """Get a secret by id with the values when requested."""
    created = (
        await client.post("/api/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.get(
        f"/api/v1/secrets/{created['id']}", params={"include_values": "true"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["values"] == VALUES
    assert "internal" not in body


async def test_get_secret_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown secret id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/api/v1/secrets/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {missing_id} was not found"}


async def test_get_secret_internal(
    client: httpx.AsyncClient, repository: FakeSecretRepository
) -> None:
    """Observe HTTP 404 when reading an internal secret."""
    created = await create_secret(repository, ACCOUNT.id, internal=True)
    response = await client.get(
        f"/api/v1/secrets/{created.id}", params={"include_values": "true"}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {created.id} was not found"}
    response = await client.get(f"/api/v1/secrets/{created.id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {created.id} was not found"}


async def test_list_secrets_excludes_internal(
    client: httpx.AsyncClient, repository: FakeSecretRepository
) -> None:
    """Never list internal secrets."""
    response = await client.post(
        "/api/v1/secrets", json={"name": "db", "values": VALUES}
    )
    assert response.status_code == 201
    await create_secret(repository, ACCOUNT.id, name="hidden", internal=True)

    response = await client.get("/api/v1/secrets")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["db"]


async def test_update_secret(client: httpx.AsyncClient) -> None:
    """Update a secret."""
    created = (
        await client.post("/api/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.patch(
        f"/api/v1/secrets/{created['id']}",
        json={"type": "database", "values": {"password": "hunter3"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "database"
    assert body["name"] == "db"
    assert "values" not in body

    response = await client.get(
        f"/api/v1/secrets/{created['id']}", params={"include_values": "true"}
    )
    assert response.status_code == 200
    assert response.json()["values"] == {"password": "hunter3"}


async def test_update_secret_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown secret id."""
    response = await client.patch(
        f"/api/v1/secrets/{uuid.uuid4()}", json={"type": "database"}
    )
    assert response.status_code == 404


async def test_update_secret_internal(
    client: httpx.AsyncClient, repository: FakeSecretRepository
) -> None:
    """Observe HTTP 404 when updating an internal secret."""
    created = await create_secret(repository, ACCOUNT.id, internal=True)
    response = await client.patch(
        f"/api/v1/secrets/{created.id}", json={"type": "database"}
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {created.id} was not found"}


async def test_delete_secret(client: httpx.AsyncClient) -> None:
    """Delete a secret and observe HTTP 204."""
    created = (
        await client.post("/api/v1/secrets", json={"name": "db", "values": VALUES})
    ).json()
    response = await client.delete(f"/api/v1/secrets/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/api/v1/secrets/{created['id']}")
    assert response.status_code == 404


async def test_delete_secret_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown secret id."""
    response = await client.delete(f"/api/v1/secrets/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_delete_secret_internal(
    client: httpx.AsyncClient, repository: FakeSecretRepository
) -> None:
    """Observe HTTP 404 when deleting an internal secret."""
    created = await create_secret(repository, ACCOUNT.id, internal=True)
    response = await client.delete(f"/api/v1/secrets/{created.id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Secret {created.id} was not found"}
    response = await client.get(f"/api/v1/secrets/{created.id}")
    assert response.status_code == 404
