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
"""Round-trip tests for the scorers SDK resource."""

import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from conftest import FakeBlobRepository, FakePluginRepository, asgi_api_client
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.scorers import (
    ScorerCreateRequest,
    ScorerResponse,
    ScorerVersionCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_blob_service,
    get_scorer_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import PluginKind

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

CONTENT = b"def score(session):\n    return 1.0\n"


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
        kind=PluginKind.SCORER,
    )
    app.dependency_overrides[get_blob_service] = lambda: blob_service
    app.dependency_overrides[get_scorer_service] = lambda: plugin_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_create(api_client: KitaruAPIClient) -> None:
    """Create a scorer through the SDK."""
    scorer = await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    assert isinstance(scorer, ScorerResponse)
    assert scorer.name == "relevance"
    assert scorer.owner_id == ACCOUNT.id
    assert scorer.latest_version == 0


async def test_create_duplicate_name(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 as a typed error."""
    await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    with pytest.raises(APIError) as exc_info:
        await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Plugin name 'relevance' is already registered"


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a scorer by id through the SDK."""
    created = await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    assert await api_client.scorers.get(created.id) == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.scorers.get(uuid.uuid4())


async def test_list(api_client: KitaruAPIClient) -> None:
    """List scorers with filters and pagination through the SDK."""
    for name in ["alpha", "beta", "gamma"]:
        await api_client.scorers.create(ScorerCreateRequest(name=name))

    page = await api_client.scorers.list()
    assert page.total == 3
    assert [item.name for item in page.items] == ["alpha", "beta", "gamma"]

    page = await api_client.scorers.list(name="beta")
    assert page.total == 1

    page = await api_client.scorers.list(page=2, page_size=2)
    assert page.total == 3
    assert [item.name for item in page.items] == ["gamma"]


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a scorer through the SDK."""
    created = await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    await api_client.scorers.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.scorers.get(created.id)


async def test_versions(api_client: KitaruAPIClient) -> None:
    """Create, get, and list scorer versions through the SDK."""
    blob = await api_client.blobs.upload(CONTENT, "text/x-python")
    scorer = await api_client.scorers.create(ScorerCreateRequest(name="relevance"))
    version = await api_client.scorers.create_version(
        scorer.id,
        ScorerVersionCreateRequest(blob_id=blob.id, entrypoint="score"),
    )
    assert version.scorer_id == scorer.id
    assert version.version == 1
    assert version.format is PluginFormat.INLINE
    assert version.blob_id == blob.id

    assert await api_client.scorers.get_version(scorer.id, 1) == version
    page = await api_client.scorers.list_versions(scorer.id)
    assert page.total == 1
    assert page.items[0] == version


async def test_register(api_client: KitaruAPIClient, tmp_path: Path) -> None:
    """Register a scorer from a source file through the SDK."""
    file = tmp_path / "scorer.py"
    file.write_bytes(CONTENT)

    version = await api_client.scorers.register(
        name="relevance", file=file, entrypoint="score"
    )
    assert version.version == 1
    assert version.entrypoint == "score"
    assert await api_client.blobs.download(version.blob_id) == CONTENT

    page = await api_client.scorers.list(name="relevance")
    assert page.total == 1
    assert page.items[0].id == version.scorer_id


async def test_register_existing_scorer(
    api_client: KitaruAPIClient, tmp_path: Path
) -> None:
    """Add a version to an existing scorer on a second registration."""
    file = tmp_path / "scorer.py"
    file.write_bytes(CONTENT)
    first = await api_client.scorers.register(
        name="relevance", file=file, entrypoint="score"
    )

    file.write_bytes(b"def score(session):\n    return 0.5\n")
    second = await api_client.scorers.register(
        name="relevance", file=file, entrypoint="score"
    )
    assert second.scorer_id == first.scorer_id
    assert second.version == 2
    assert second.blob_id != first.blob_id

    page = await api_client.scorers.list(name="relevance")
    assert page.total == 1
    assert page.items[0].latest_version == 2
