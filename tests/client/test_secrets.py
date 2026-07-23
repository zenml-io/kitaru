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
"""Round-trip tests for the secrets SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from pydantic import SecretStr

from conftest import FakeSecretRepository, asgi_api_client
from kitaru.api_models.v1.secrets import (
    SecretCreateRequest,
    SecretResponse,
    SecretUpdateRequest,
    SecretWithValuesResponse,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_secret_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

VALUES = {"username": SecretStr("svc"), "password": SecretStr("hunter2")}


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = SecretService(repository=FakeSecretRepository())
    app.dependency_overrides[get_secret_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a secret through the SDK."""
    secret = await api_client.secrets.create(
        SecretCreateRequest(name="db", type="database", values=VALUES)
    )
    assert isinstance(secret, SecretResponse)
    assert secret.name == "db"
    assert secret.owner_id == ACCOUNT.id
    assert secret.type == "database"


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.secrets.create(SecretCreateRequest(name="db", values=VALUES))
    with pytest.raises(APIError) as exc_info:
        await api_client.secrets.create(SecretCreateRequest(name="db", values=VALUES))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Secret name 'db' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a secret by id through the SDK."""
    created = await api_client.secrets.create(
        SecretCreateRequest(name="db", values=VALUES)
    )
    loaded = await api_client.secrets.get(created.id)
    assert loaded == created


async def test_get_with_values(api_client: KitaruAPIClient) -> None:
    """Get a secret with its values through the SDK."""
    created = await api_client.secrets.create(
        SecretCreateRequest(name="db", values=VALUES)
    )
    loaded = await api_client.secrets.get(created.id, include_values=True)
    assert isinstance(loaded, SecretWithValuesResponse)
    assert loaded.values == VALUES


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.secrets.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List secrets with filters and pagination through the SDK."""
    for name in ["db", "smtp", "s3"]:
        await api_client.secrets.create(SecretCreateRequest(name=name, values=VALUES))

    page = await api_client.secrets.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["db", "smtp", "s3"]

    page = await api_client.secrets.list(name="smtp")
    assert page.total == 1
    assert page.items[0].name == "smtp"

    page = await api_client.secrets.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["s3"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update a secret through the SDK."""
    created = await api_client.secrets.create(
        SecretCreateRequest(name="db", values=VALUES)
    )
    updated = await api_client.secrets.update(
        created.id, SecretUpdateRequest(type="database")
    )
    assert updated.type == "database"
    await api_client.secrets.update(
        created.id, SecretUpdateRequest(values={"password": SecretStr("hunter3")})
    )
    loaded = await api_client.secrets.get(created.id, include_values=True)
    assert loaded.type == "database"
    assert loaded.values == {"password": SecretStr("hunter3")}


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a secret through the SDK."""
    created = await api_client.secrets.create(
        SecretCreateRequest(name="db", values=VALUES)
    )
    await api_client.secrets.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.secrets.get(created.id)
