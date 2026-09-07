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
"""Round-trip tests for the connections SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from pydantic import SecretStr

from conftest import (
    FakeConnectionRepository,
    FakeSecretRepository,
    asgi_api_client,
    override_idempotency,
)
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_connection_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.connection_service import ConnectionService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

ENV = {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}
SECRETS = {
    "LANGFUSE_PUBLIC_KEY": SecretStr("pk"),
    "LANGFUSE_SECRET_KEY": SecretStr("sk"),
}


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    connections = FakeConnectionRepository()
    service = ConnectionService(
        repository=connections,
        secret_repository=FakeSecretRepository(connections=connections),
    )
    app.dependency_overrides[get_connection_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


def _create_request(name: str = "langfuse-prod") -> ConnectionCreateRequest:
    """Build a connection create request naming a langfuse account."""
    return ConnectionCreateRequest(
        name=name, provider="langfuse", env=ENV, secrets=SECRETS
    )


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a connection through the SDK."""
    connection = await api_client.connections.create(_create_request())
    assert isinstance(connection, ConnectionResponse)
    assert connection.name == "langfuse-prod"
    assert connection.provider == "langfuse"
    assert connection.owner_id == ACCOUNT.id
    assert connection.env == ENV
    assert connection.secret_keys == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert connection.default is False


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.connections.create(_create_request())
    with pytest.raises(APIError) as exc_info:
        await api_client.connections.create(_create_request())
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == (
        "Connection name 'langfuse-prod' is already registered"
    )


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a connection by id through the SDK."""
    created = await api_client.connections.create(_create_request())
    assert await api_client.connections.get(created.id) == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.connections.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List connections newest-first with filters through the SDK."""
    for name in ["langfuse-a", "langfuse-b", "langfuse-c"]:
        await api_client.connections.create(_create_request(name))

    page = await api_client.connections.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == [
        "langfuse-c",
        "langfuse-b",
        "langfuse-a",
    ]

    page = await api_client.connections.list(
        ConnectionListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="langfuse-b")
        )
    )
    assert page.next_cursor is None
    assert page.items[0].name == "langfuse-b"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every connection across pages through the SDK."""
    for name in ["langfuse-a", "langfuse-b", "langfuse-c"]:
        await api_client.connections.create(_create_request(name))

    collected = [
        item.name
        async for item in api_client.connections.iter(ConnectionListParams(size=2))
    ]

    assert collected == ["langfuse-c", "langfuse-b", "langfuse-a"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a connection, merging env and secrets, through the SDK."""
    created = await api_client.connections.create(_create_request())
    updated = await api_client.connections.update(
        created.id,
        ConnectionUpdateRequest(
            env={"LANGFUSE_PROJECT": "proj"},
            secrets={"LANGFUSE_SECRET_KEY": SecretStr("rotated")},
            default=True,
        ),
    )
    assert updated.env == {
        "LANGFUSE_BASE_URL": "https://cloud.langfuse.com",
        "LANGFUSE_PROJECT": "proj",
    }
    assert updated.secret_keys == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert updated.default is True


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a connection through the SDK."""
    created = await api_client.connections.create(_create_request())
    await api_client.connections.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.connections.get(created.id)
