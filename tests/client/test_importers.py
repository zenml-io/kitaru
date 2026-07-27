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
from pathlib import Path

import pytest

from conftest import FakeBlobRepository, FakePluginRepository, asgi_api_client
from kitaru.api_models.v1.importers import (
    ImporterCreateRequest,
    ImporterResponse,
    ImporterVersionCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_blob_service,
    get_importer_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

CONTENT = b"def parse(payload):\n    return []\n"


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    blob_repository = FakeBlobRepository()
    blob_service = BlobService(
        repository=blob_repository,
        storage=DatabaseBlobStorage(),
        max_size_bytes=1024,
    )
    plugin_service = PluginService(
        repository=FakePluginRepository(blob_repository),
        blob_repository=blob_repository,
        kind=PluginKind.IMPORTER,
    )
    app.dependency_overrides[get_blob_service] = lambda: blob_service
    app.dependency_overrides[get_importer_service] = lambda: plugin_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create an importer through the SDK."""
    importer = await api_client.importers.create(
        ImporterCreateRequest(
            name="langfuse", provider="langfuse", metadata={"region": "eu"}
        )
    )
    assert isinstance(importer, ImporterResponse)
    assert importer.name == "langfuse"
    assert importer.provider == "langfuse"
    assert importer.metadata == {"region": "eu"}


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.importers.create(ImporterCreateRequest(name="langfuse"))
    with pytest.raises(APIError) as exc_info:
        await api_client.importers.create(ImporterCreateRequest(name="langfuse"))
    assert exc_info.value.status_code == 409


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get an importer by id through the SDK."""
    created = await api_client.importers.create(ImporterCreateRequest(name="langfuse"))
    assert await api_client.importers.get(created.id) == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.importers.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List importers with filters through the SDK."""
    await api_client.importers.create(
        ImporterCreateRequest(name="one", provider="langfuse")
    )
    await api_client.importers.create(
        ImporterCreateRequest(name="two", provider="braintrust")
    )

    page = await api_client.importers.list()
    assert page.total == 2

    page = await api_client.importers.list(provider="braintrust")
    assert page.total == 1
    assert page.items[0].name == "two"

    page = await api_client.importers.list(name="one")
    assert page.total == 1


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete an importer through the SDK."""
    created = await api_client.importers.create(ImporterCreateRequest(name="langfuse"))
    await api_client.importers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.importers.get(created.id)


async def test_versions(api_client: KitaruAPIClient) -> None:
    """Create, get, and list importer versions through the SDK."""
    blob = await api_client.blobs.upload(CONTENT, "text/x-python")
    importer = await api_client.importers.create(ImporterCreateRequest(name="langfuse"))
    version = await api_client.importers.create_version(
        importer.id,
        ImporterVersionCreateRequest(blob_id=blob.id, entrypoint="parse"),
    )
    assert version.importer_id == importer.id
    assert version.version == 1

    assert await api_client.importers.get_version(importer.id, 1) == version
    page = await api_client.importers.list_versions(importer.id)
    assert page.total == 1


async def test_register(api_client: KitaruAPIClient, tmp_path: Path) -> None:
    """Register an importer from a source file through the SDK."""
    file = tmp_path / "importer.py"
    file.write_bytes(CONTENT)

    version = await api_client.importers.register(
        name="langfuse", file=file, entrypoint="parse", provider="langfuse"
    )
    assert version.version == 1
    assert await api_client.blobs.download(version.blob_id) == CONTENT

    importer = await api_client.importers.get(version.importer_id)
    assert importer.provider == "langfuse"


async def test_register_existing_importer(
    api_client: KitaruAPIClient, tmp_path: Path
) -> None:
    """Add a version to an existing importer on a second registration."""
    file = tmp_path / "importer.py"
    file.write_bytes(CONTENT)
    first = await api_client.importers.register(
        name="langfuse", file=file, entrypoint="parse", provider="langfuse"
    )

    file.write_bytes(b"def parse(payload):\n    return [1]\n")
    second = await api_client.importers.register(
        name="langfuse", file=file, entrypoint="parse", provider="langfuse"
    )
    assert second.importer_id == first.importer_id
    assert second.version == 2
