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
"""Round-trip tests for the importers SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobRepository, FakePluginRepository, asgi_api_client
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterUpdateRequest,
    ImporterVersionCreateRequest,
    ImporterVersionUpdateRequest,
)
from kitaru.api_models.v1.plugin import PackagePluginSource
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_importer_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = PluginService(
        kind=PluginKind.IMPORTER,
        repository=FakePluginRepository(),
        blob_repository=FakeBlobRepository(),
    )
    app.dependency_overrides[get_importer_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an importer through the SDK."""
    importer = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import", provider="langfuse")
    )
    assert importer.name == "langfuse-import"
    assert importer.provider == "langfuse"


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.importers.create(ImporterCreateRequest(name="langfuse-import"))
    with pytest.raises(APIError) as exc_info:
        await api_client.importers.create(ImporterCreateRequest(name="langfuse-import"))
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an importer by id through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    loaded = await api_client.importers.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.importers.get(uuid.uuid4())


async def test_list_filter_by_provider(api_client: KitaruAPIClient) -> None:
    """List importers filtered by provider through the SDK."""
    await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import", provider="langfuse")
    )
    await api_client.importers.create(
        ImporterCreateRequest(name="braintrust-import", provider="braintrust")
    )

    page = await api_client.importers.list(ImporterListParams(provider="langfuse"))
    assert [item.name for item in page.items] == ["langfuse-import"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an importer through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    updated = await api_client.importers.update(
        created.id, ImporterUpdateRequest(description="Imports from Langfuse")
    )
    assert updated.description == "Imports from Langfuse"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an importer through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    await api_client.importers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.importers.get(created.id)


async def test_create_and_get_version(api_client: KitaruAPIClient) -> None:
    """Create and get an importer version through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    version = await api_client.importers.create_version(
        created.id,
        ImporterVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-importer==1.0.0", entrypoint="pkg:run"
            ),
            display_version="v1",
        ),
    )
    assert version.version == 1
    assert version.importer_id == created.id

    loaded = await api_client.importers.get_version(created.id, version.version)
    assert loaded == version


async def test_list_and_iter_versions(api_client: KitaruAPIClient) -> None:
    """List and iterate an importer's versions through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    request = ImporterVersionCreateRequest(
        source=PackagePluginSource(
            requirement="kitaru-importer==1.0.0", entrypoint="pkg:run"
        )
    )
    await api_client.importers.create_version(created.id, request)
    await api_client.importers.create_version(created.id, request)

    page = await api_client.importers.list_versions(created.id)
    assert sorted(item.version for item in page.items) == [1, 2]

    collected = [
        item.version async for item in api_client.importers.iter_versions(created.id)
    ]
    assert sorted(collected) == [1, 2]


async def test_update_version(api_client: KitaruAPIClient) -> None:
    """Update an importer version's display version through the SDK."""
    created = await api_client.importers.create(
        ImporterCreateRequest(name="langfuse-import")
    )
    version = await api_client.importers.create_version(
        created.id,
        ImporterVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-importer==1.0.0", entrypoint="pkg:run"
            ),
            display_version="v1",
        ),
    )
    updated = await api_client.importers.update_version(
        created.id,
        version.version,
        ImporterVersionUpdateRequest(display_version="v1.0.1"),
    )
    assert updated.display_version == "v1.0.1"
