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
"""Round-trip tests for the analyzers SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeBlobRepository,
    FakePluginRepository,
    asgi_api_client,
    override_idempotency,
)
from kitaru.api_models.v1.analyzer import (
    AnalyzerCreateRequest,
    AnalyzerUpdateRequest,
    AnalyzerVersionCreateRequest,
    AnalyzerVersionUpdateRequest,
)
from kitaru.api_models.v1.plugin import PackagePluginSource
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import authorize, get_analyzer_service
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
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    service = PluginService(
        kind=PluginKind.ANALYZER,
        repository=FakePluginRepository(),
        blob_repository=FakeBlobRepository(),
    )
    app.dependency_overrides[get_analyzer_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an analyzer through the SDK."""
    analyzer = await api_client.analyzers.create(
        AnalyzerCreateRequest(name="drift", metadata={"a": 1})
    )
    assert analyzer.name == "drift"
    assert analyzer.metadata == {"a": 1}


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    with pytest.raises(APIError) as exc_info:
        await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an analyzer by id through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    loaded = await api_client.analyzers.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.analyzers.get(uuid.uuid4())


async def test_list_and_iter(api_client: KitaruAPIClient) -> None:
    """List and iterate analyzers through the SDK."""
    for name in ["drift", "usage"]:
        await api_client.analyzers.create(AnalyzerCreateRequest(name=name))

    page = await api_client.analyzers.list()
    assert [item.name for item in page.items] == ["usage", "drift"]

    collected = [item.name async for item in api_client.analyzers.iter()]
    assert collected == ["usage", "drift"]


async def test_update(api_client: KitaruAPIClient) -> None:
    """Update an analyzer through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    updated = await api_client.analyzers.update(
        created.id, AnalyzerUpdateRequest(description="Flags topic drift")
    )
    assert updated.description == "Flags topic drift"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an analyzer through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    await api_client.analyzers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.analyzers.get(created.id)


async def test_create_and_get_version(api_client: KitaruAPIClient) -> None:
    """Create and get an analyzer version through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    version = await api_client.analyzers.create_version(
        created.id,
        AnalyzerVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-analyzer==1.0.0", entrypoint="pkg:analyze"
            ),
            display_version="v1",
        ),
    )
    assert version.version == 1
    assert version.analyzer_id == created.id

    loaded = await api_client.analyzers.get_version(created.id, version.version)
    assert loaded == version


async def test_list_and_iter_versions(api_client: KitaruAPIClient) -> None:
    """List and iterate an analyzer's versions through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    request = AnalyzerVersionCreateRequest(
        source=PackagePluginSource(
            requirement="kitaru-analyzer==1.0.0", entrypoint="pkg:analyze"
        )
    )
    await api_client.analyzers.create_version(created.id, request)
    await api_client.analyzers.create_version(created.id, request)

    page = await api_client.analyzers.list_versions(created.id)
    assert sorted(item.version for item in page.items) == [1, 2]

    collected = [
        item.version async for item in api_client.analyzers.iter_versions(created.id)
    ]
    assert sorted(collected) == [1, 2]


async def test_update_version(api_client: KitaruAPIClient) -> None:
    """Update an analyzer version's display version through the SDK."""
    created = await api_client.analyzers.create(AnalyzerCreateRequest(name="drift"))
    version = await api_client.analyzers.create_version(
        created.id,
        AnalyzerVersionCreateRequest(
            source=PackagePluginSource(
                requirement="kitaru-analyzer==1.0.0", entrypoint="pkg:analyze"
            ),
            display_version="v1",
        ),
    )
    updated = await api_client.analyzers.update_version(
        created.id,
        version.version,
        AnalyzerVersionUpdateRequest(display_version="v1.0.1"),
    )
    assert updated.display_version == "v1.0.1"
