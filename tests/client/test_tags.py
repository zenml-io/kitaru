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
"""Round-trip tests for the tags SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeTagRepository, asgi_api_client
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagLinkResponse,
    TagListParams,
    TagResourceType,
    TagResponse,
    TagUpdateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_tag_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = TagService(repository=FakeTagRepository())
    app.dependency_overrides[get_tag_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a tag through the SDK."""
    tag = await api_client.tags.create(TagCreateRequest(name="prod"))
    assert isinstance(tag, TagResponse)
    assert tag.name == "prod"
    assert tag.owner_id == ACCOUNT.id


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.tags.create(TagCreateRequest(name="prod"))
    with pytest.raises(APIError) as exc_info:
        await api_client.tags.create(TagCreateRequest(name="prod"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Tag name 'prod' is already registered"


async def test_list(api_client: KitaruAPIClient) -> None:
    """List tags newest-first with filters through the SDK."""
    for name in ["prod", "staging", "canary"]:
        await api_client.tags.create(TagCreateRequest(name=name))

    page = await api_client.tags.list()
    assert page.next_cursor is None
    assert [item.name for item in page.items] == ["canary", "staging", "prod"]

    page = await api_client.tags.list(TagListParams(name="staging"))
    assert page.items[0].name == "staging"


async def test_iter(api_client: KitaruAPIClient) -> None:
    """Iterate every tag across pages through the SDK."""
    for name in ["prod", "staging", "canary"]:
        await api_client.tags.create(TagCreateRequest(name=name))

    collected = [
        item.name async for item in api_client.tags.iter(TagListParams(size=2))
    ]

    assert collected == ["canary", "staging", "prod"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Rename a tag through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    updated = await api_client.tags.update(
        created.id, TagUpdateRequest(name="production")
    )
    assert updated.name == "production"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a tag through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    await api_client.tags.delete(created.id)
    with pytest.raises(APIError):
        await api_client.tags.update(created.id, TagUpdateRequest(name="x"))


async def test_create_and_delete_link(api_client: KitaruAPIClient) -> None:
    """Link and unlink a resource through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    resource_id = uuid.uuid4()
    link = await api_client.tags.create_link(
        created.id,
        TagLinkCreateRequest(
            resource_type=TagResourceType.SESSION, resource_id=resource_id
        ),
    )
    assert isinstance(link, TagLinkResponse)
    assert link.tag_id == created.id
    assert link.resource_id == resource_id

    await api_client.tags.delete_link(created.id, TagResourceType.SESSION, resource_id)
    with pytest.raises(NotFoundError):
        await api_client.tags.delete_link(
            created.id, TagResourceType.SESSION, resource_id
        )
