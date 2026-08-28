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
"""Round-trip tests for the blobs SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeBlobDataStore,
    FakeBlobRepository,
    asgi_api_client,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    authorize_with_worker_or_task,
    get_blob_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobStorageBackend

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
    service = BlobService(
        repository=FakeBlobRepository(),
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: FakeBlobDataStore()},
            BlobStorageBackend.DATABASE,
        ),
        max_size_bytes=1024,
    )
    app.dependency_overrides[get_blob_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_worker_or_task] = lambda: AuthContext(
        account=ACCOUNT
    )
    async with asgi_api_client(app) as client:
        yield client


async def test_upload(api_client: KitaruAPIClient) -> None:
    """Upload a blob through the SDK."""
    blob = await api_client.blobs.upload(b"print(1)", media_type="text/x-python")
    assert blob.media_type == "text/x-python"
    assert blob.size == len(b"print(1)")


async def test_upload_dedup(api_client: KitaruAPIClient) -> None:
    """Return the same blob id on a dedup hit through the SDK."""
    first = await api_client.blobs.upload(b"same content")
    second = await api_client.blobs.upload(b"same content")
    assert second.id == first.id


async def test_upload_too_large(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 413 as a typed error."""
    with pytest.raises(APIError) as exc_info:
        await api_client.blobs.upload(b"x" * 2000)
    assert exc_info.value.status_code == 413


async def test_get(api_client: KitaruAPIClient) -> None:
    """Get a blob's metadata by id through the SDK."""
    created = await api_client.blobs.upload(b"hello")
    loaded = await api_client.blobs.get(created.id)
    assert loaded == created


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.blobs.get(uuid.uuid4())


async def test_download(api_client: KitaruAPIClient) -> None:
    """Download a blob's content through the SDK."""
    created = await api_client.blobs.upload(b"hello")
    content = await api_client.blobs.download(created.id)
    assert content == b"hello"


async def test_delete(api_client: KitaruAPIClient) -> None:
    """Delete a blob through the SDK."""
    created = await api_client.blobs.upload(b"hello")
    await api_client.blobs.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.blobs.get(created.id)
