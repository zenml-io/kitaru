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
"""Tests for the account routes."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import FakeAccountRepository, FakePasswordHasher, local_settings
from kitaru.server.adapters.rest.dependencies import authorize, get_account_service
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.domain.account import Account

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="admin"))


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with a fake-backed account service."""
    app = create_app(local_settings())
    service = AccountService(
        repository=FakeAccountRepository(),
        password_hasher=FakePasswordHasher(),
    )
    app.dependency_overrides[get_account_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: ACTOR
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_create_account(client: httpx.AsyncClient) -> None:
    """Create an account and observe HTTP 201."""
    response = await client.post(
        "/v1/accounts",
        json={"name": "alice", "email": "alice@example.com", "password": "secret"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "alice"
    assert body["email"] == "alice@example.com"
    assert body["is_service_account"] is False
    assert body["active"] is True
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_account_response_has_no_password(
    client: httpx.AsyncClient,
) -> None:
    """Never expose password data in the response."""
    response = await client.post(
        "/v1/accounts", json={"name": "alice", "password": "secret"}
    )
    assert response.status_code == 201
    assert set(response.json()) == {
        "id",
        "name",
        "email",
        "is_service_account",
        "active",
        "created",
        "updated",
    }


async def test_create_account_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate account name."""
    response = await client.post("/v1/accounts", json={"name": "alice"})
    assert response.status_code == 201
    response = await client.post("/v1/accounts", json={"name": "alice"})
    assert response.status_code == 409
    assert response.json() == {"detail": "Account name 'alice' is already registered"}


async def test_create_account_invalid_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an invalid account name."""
    response = await client.post("/v1/accounts", json={"name": "in valid"})
    assert response.status_code == 422


async def test_list_accounts(client: httpx.AsyncClient) -> None:
    """List accounts newest-first with filters."""
    for name in ["alice", "bob", "carol"]:
        response = await client.post("/v1/accounts", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/accounts")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert [item["name"] for item in body["items"]] == ["carol", "bob", "alice"]

    filter_expression = {"field": "name", "op": "eq", "value": "bob"}
    response = await client.get(
        "/v1/accounts", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0]["name"] == "bob"


async def test_list_accounts_walks_pages_with_cursor(
    client: httpx.AsyncClient,
) -> None:
    """Walk every page of accounts via next_cursor."""
    for name in ["alice", "bob", "carol"]:
        response = await client.post("/v1/accounts", json={"name": name})
        assert response.status_code == 201

    collected: list[str] = []
    params: dict[str, str] = {"size": "2"}
    while True:
        response = await client.get("/v1/accounts", params=params)
        assert response.status_code == 200
        body = response.json()
        collected.extend(item["name"] for item in body["items"])
        if body["next_cursor"] is None:
            break
        params = {"size": "2", "cursor": body["next_cursor"]}

    assert collected == ["carol", "bob", "alice"]


async def test_list_accounts_sort_created_asc(client: httpx.AsyncClient) -> None:
    """Sort accounts oldest-first with sort=created:asc."""
    for name in ["alice", "bob", "carol"]:
        response = await client.post("/v1/accounts", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/accounts", params={"sort": "created:asc"})
    assert response.status_code == 200
    body = response.json()
    assert [item["name"] for item in body["items"]] == ["alice", "bob", "carol"]


async def test_list_accounts_invalid_pagination(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for out-of-bounds pagination parameters."""
    response = await client.get("/v1/accounts", params={"size": 0})
    assert response.status_code == 422
    response = await client.get("/v1/accounts", params={"size": 1001})
    assert response.status_code == 422


async def test_list_accounts_malformed_sort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a sort string that fails the wire pattern."""
    response = await client.get("/v1/accounts", params={"sort": "bogus"})
    assert response.status_code == 422


async def test_list_accounts_unknown_sort_field(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a sort field outside the allowlist."""
    response = await client.get("/v1/accounts", params={"sort": "name:asc"})
    assert response.status_code == 422


async def test_list_accounts_invalid_cursor(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a cursor string that fails to decode."""
    response = await client.get("/v1/accounts", params={"cursor": "not-a-cursor"})
    assert response.status_code == 422


async def test_list_accounts_cursor_sort_mismatch(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when a cursor is replayed with a different sort."""
    for name in ["alice", "bob"]:
        response = await client.post("/v1/accounts", json={"name": name})
        assert response.status_code == 201

    response = await client.get("/v1/accounts", params={"size": 1})
    next_cursor = response.json()["next_cursor"]
    assert next_cursor is not None

    response = await client.get(
        "/v1/accounts",
        params={"size": 1, "cursor": next_cursor, "sort": "created:asc"},
    )
    assert response.status_code == 422


async def test_list_accounts_cursor_filter_mismatch(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 when a cursor is replayed after the filter changes."""
    for name in ["alice", "bob", "carol"]:
        response = await client.post("/v1/accounts", json={"name": name})
        assert response.status_code == 201

    filter_expression = {"field": "active", "op": "eq", "value": True}
    response = await client.get(
        "/v1/accounts",
        params={"size": 1, "filter": json.dumps(filter_expression)},
    )
    next_cursor = response.json()["next_cursor"]
    assert next_cursor is not None

    response = await client.get(
        "/v1/accounts", params={"size": 1, "cursor": next_cursor}
    )
    assert response.status_code == 422


async def test_list_accounts_unknown_query_param(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an unknown query parameter."""
    response = await client.get("/v1/accounts", params={"bogus": "x"})
    assert response.status_code == 422


async def test_get_account(client: httpx.AsyncClient) -> None:
    """Get an account by id."""
    created = (await client.post("/v1/accounts", json={"name": "alice"})).json()
    response = await client.get(f"/v1/accounts/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_account_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown account id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/accounts/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Account {missing_id} was not found"}


async def test_update_account(client: httpx.AsyncClient) -> None:
    """Partially update an account."""
    created = (await client.post("/v1/accounts", json={"name": "alice"})).json()
    response = await client.patch(
        f"/v1/accounts/{created['id']}", json={"active": False}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["active"] is False
    assert body["name"] == "alice"

    response = await client.patch(
        f"/v1/accounts/{created['id']}", json={"password": "new"}
    )
    assert response.status_code == 200
    assert response.json()["active"] is False


async def test_update_account_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown account id."""
    response = await client.patch(
        f"/v1/accounts/{uuid.uuid4()}", json={"active": False}
    )
    assert response.status_code == 404


async def test_delete_account_not_allowed(client: httpx.AsyncClient) -> None:
    """Observe HTTP 405 for account deletion."""
    created = (await client.post("/v1/accounts", json={"name": "alice"})).json()
    response = await client.delete(f"/v1/accounts/{created['id']}")
    assert response.status_code == 405
