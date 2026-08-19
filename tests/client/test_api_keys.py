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
"""Round-trip tests for the API keys SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeApiKeyRepository, asgi_api_client, override_idempotency
from kitaru.api_models.v1.api_key import (
    ApiKeyCreateRequest,
    ApiKeyIssuedResponse,
    ApiKeyListParams,
    ApiKeyResponse,
    ApiKeyRotateRequest,
    ApiKeyUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_api_key_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.api_key_service import ApiKeyService
from kitaru.server.domain.account import Account
from kitaru.server.domain.api_key import API_KEY_PREFIX

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


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
    service = ApiKeyService(repository=FakeApiKeyRepository())
    app.dependency_overrides[get_api_key_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an API key through the SDK."""
    api_key = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    assert isinstance(api_key, ApiKeyIssuedResponse)
    assert api_key.name == "ci"
    assert api_key.owner_id == ACCOUNT.id
    assert api_key.active is True
    assert api_key.key.startswith(API_KEY_PREFIX)


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    with pytest.raises(APIError) as exc_info:
        await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "API key name 'ci' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an API key by id through the SDK."""
    created = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    loaded = await api_client.api_keys.get(created.id)
    assert loaded == ApiKeyResponse(**created.model_dump(exclude={"key"}))


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.api_keys.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List API keys newest-first with filters through the SDK."""
    for name in ["ci", "deploy", "local"]:
        await api_client.api_keys.create(ApiKeyCreateRequest(name=name))

    page = await api_client.api_keys.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["local", "deploy", "ci"]

    page = await api_client.api_keys.list(
        ApiKeyListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value="deploy")
        )
    )
    assert page.next_cursor is None
    assert page.items[0].name == "deploy"


async def test_list_walks_pages_with_cursor(api_client: KitaruAPIClient) -> None:
    """Walk every page of API keys via next_cursor through the SDK."""
    for name in ["ci", "deploy", "local"]:
        await api_client.api_keys.create(ApiKeyCreateRequest(name=name))

    collected: list[str] = []
    params = ApiKeyListParams(size=2)
    while True:
        page = await api_client.api_keys.list(params)
        collected.extend(item.name for item in page.items)
        if page.next_cursor is None:
            break
        params = ApiKeyListParams(cursor=page.next_cursor, size=2)

    assert collected == ["local", "deploy", "ci"]


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every API key across pages through the SDK."""
    for name in ["ci", "deploy", "local"]:
        await api_client.api_keys.create(ApiKeyCreateRequest(name=name))

    collected = [
        item.name async for item in api_client.api_keys.iter(ApiKeyListParams(size=2))
    ]

    assert collected == ["local", "deploy", "ci"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an API key through the SDK."""
    created = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    updated = await api_client.api_keys.update(
        created.id, ApiKeyUpdateRequest(active=False)
    )
    assert updated.active is False
    updated = await api_client.api_keys.update(
        created.id, ApiKeyUpdateRequest(active=True)
    )
    assert updated.active is True


async def test_rotate(api_client: KitaruAPIClient) -> None:
    """Rotate an API key through the SDK."""
    created = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    rotated = await api_client.api_keys.rotate(
        created.id, ApiKeyRotateRequest(retain_period_minutes=5)
    )
    assert isinstance(rotated, ApiKeyIssuedResponse)
    assert rotated.id == created.id
    assert rotated.key.startswith(API_KEY_PREFIX)
    assert rotated.key != created.key
    assert rotated.last_rotated is not None


async def test_rotate_without_request(api_client: KitaruAPIClient) -> None:
    """Rotate an API key without a request body."""
    created = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    rotated = await api_client.api_keys.rotate(created.id)
    assert rotated.key != created.key


async def test_rotate_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.api_keys.rotate(uuid.uuid4())


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an API key through the SDK."""
    created = await api_client.api_keys.create(ApiKeyCreateRequest(name="ci"))
    await api_client.api_keys.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.api_keys.get(created.id)
