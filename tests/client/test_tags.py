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
from kitaru.api_models.v1.tags import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagResourceType,
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
    """List tags with filters and pagination through the SDK."""
    for name in ["prod", "staging", "flaky"]:
        await api_client.tags.create(TagCreateRequest(name=name))

    page = await api_client.tags.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["prod", "staging", "flaky"]

    page = await api_client.tags.list(name="staging")
    assert page.total == 1
    assert page.items[0].name == "staging"

    page = await api_client.tags.list(page=2, page_size=2)
    assert page.total == 3
    assert page.page == 2
    assert page.page_size == 2
    assert [item.name for item in page.items] == ["flaky"]


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a tag through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    await api_client.tags.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.tags.delete(created.id)


async def test_create_link(api_client: KitaruAPIClient) -> None:
    """Attach a tag to a resource through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    resource_id = uuid.uuid4()
    link = await api_client.tags.create_link(
        created.id,
        TagLinkCreateRequest(
            resource_type=TagResourceType.SESSION, resource_id=resource_id
        ),
    )
    assert link.tag_id == created.id
    assert link.resource_type is TagResourceType.SESSION
    assert link.resource_id == resource_id


async def test_create_link_duplicate(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    request = TagLinkCreateRequest(
        resource_type=TagResourceType.SESSION, resource_id=uuid.uuid4()
    )
    await api_client.tags.create_link(created.id, request)
    with pytest.raises(APIError) as exc_info:
        await api_client.tags.create_link(created.id, request)
    assert exc_info.value.status_code == 409


async def test_create_link_tag_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.tags.create_link(
            uuid.uuid4(),
            TagLinkCreateRequest(
                resource_type=TagResourceType.SESSION, resource_id=uuid.uuid4()
            ),
        )


async def test_delete_link(api_client: KitaruAPIClient) -> None:
    """Detach a tag from a resource through the SDK."""
    created = await api_client.tags.create(TagCreateRequest(name="prod"))
    resource_id = uuid.uuid4()
    await api_client.tags.create_link(
        created.id,
        TagLinkCreateRequest(
            resource_type=TagResourceType.SESSION, resource_id=resource_id
        ),
    )
    await api_client.tags.delete_link(created.id, TagResourceType.SESSION, resource_id)
    with pytest.raises(NotFoundError):
        await api_client.tags.delete_link(
            created.id, TagResourceType.SESSION, resource_id
        )
