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

import hashlib
import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobRepository, asgi_api_client
from kitaru.api_models.v1.blobs import BlobResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import NotFoundError, ValidationError
from kitaru.server.adapters.db.blob_storage import DatabaseBlobStorage
from kitaru.server.adapters.rest.dependencies import authorize, get_blob_service
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.account import Account

ACCOUNT = Account(id=uuid.uuid4(), name="ann")

CONTENT = b"def score(session):\n    return 1.0\n"


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with a fake-backed service."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    service = BlobService(
        repository=FakeBlobRepository(),
        storage=DatabaseBlobStorage(),
        max_size_bytes=1024,
    )
    app.dependency_overrides[get_blob_service] = lambda: service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    async with asgi_api_client(app) as client:
        yield client


async def test_upload(api_client: KitaruAPIClient) -> None:
    """Upload a blob through the SDK."""
    blob = await api_client.blobs.upload(CONTENT, "text/x-python")
    assert isinstance(blob, BlobResponse)
    assert blob.sha256 == hashlib.sha256(CONTENT).hexdigest()
    assert blob.size == len(CONTENT)
    assert blob.media_type == "text/x-python"


async def test_upload_deduplicates(api_client: KitaruAPIClient) -> None:
    """Return the stored blob for content already uploaded."""
    first = await api_client.blobs.upload(CONTENT, "text/x-python")
    second = await api_client.blobs.upload(CONTENT, "text/plain")
    assert second == first


async def test_upload_too_large(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 422 as a typed error."""
    with pytest.raises(ValidationError) as exc_info:
        await api_client.blobs.upload(b"x" * 1025, "text/plain")
    assert exc_info.value.detail == "Blob exceeds 1024 bytes"


async def test_download(api_client: KitaruAPIClient) -> None:
    """Download blob content through the SDK."""
    blob = await api_client.blobs.upload(CONTENT, "text/x-python")
    assert await api_client.blobs.download(blob.id) == CONTENT


async def test_download_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a typed error."""
    with pytest.raises(NotFoundError):
        await api_client.blobs.download(uuid.uuid4())
